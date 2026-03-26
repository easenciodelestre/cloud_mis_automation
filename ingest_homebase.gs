/*******************************************************
 * HOMEBASE — INGEST (.csv -> homebase_timesheets_raw)
 * -----------------------------------------------------
 * - OPTIMIZADO PARA BIGQUERY: Tabla completamente plana.
 * - ESCUDO ANTI-BASURA v1.1: Filtro positivo (Exige 'Clock in date').
 * - Elimina filas de totales para evitar doble conteo en SQL.
 *******************************************************/

function ingest_homebase_all() {
  console.log("--- INICIANDO INGESTA HOMEBASE (FLAT & CLEAN) ---");
  
  const key = 'timesheets'; 

  try {
    ingest_homebase_one(key);
  } catch (err) {
    console.log(`❌ ERROR en [${key}]: ${String(err.message || err)}`);
  }

  console.log('Ingesta Homebase completada ✅');
}

function ingest_homebase_one(key) {
  const masterId = ENV.HB.masters[key]; 
  const subfolderName = ENV.HB.rawSubfolders[key];
  
  if (!masterId) throw new Error(`ENV.HB.masters.${key} no definido`);

  const ssMaster = SpreadsheetApp.openById(masterId);
  console.log(`🔍 Buscando en subcarpeta: ${subfolderName}`);

  ensureMisLog_(ssMaster);
  const shAudit = ensureIngestAudit_(ssMaster);

  const rawRoot = DriveApp.getFolderById(ENV.RAW_PARENT_FOLDER_ID || ENV.RU.RAW_ROOT_ID); 
  const inFolder = getChildFolderByName_(rawRoot, subfolderName);
  const processed = getOrCreateChildFolder_(inFolder, ENV.PROCESSED_FOLDER_ID);

  const files = [];
  const it = inFolder.getFiles();
  while (it.hasNext()) {
    const f = it.next();
    const name = f.getName().toLowerCase();
    const mime = f.getMimeType();
    
    if (name.endsWith('.csv') || mime === MimeType.CSV || name.endsWith('.tsv')) {
      files.push(f);
    }
  }

  if (!files.length) {
    console.log(`⏭️ Saltando... No hay archivos CSV válidos.`);
    return;
  }
  
  files.sort((a, b) => a.getDateCreated().getTime() - b.getDateCreated().getTime());

  const rawSheetName = `homebase_${key}_raw`;
  const shRaw = ssMaster.getSheetByName(rawSheetName) || ssMaster.insertSheet(rawSheetName);

  files.forEach(file => {
    const ingestDate = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
    const sig = getFileSignatureStrong_(file);

    if (wasAlreadyIngestedStrong_(shAudit, key, sig.fileId, sig.content_md5)) {
      console.log(`  ⏩ Archivo duplicado saltado: ${sig.name}`);
      auditRow_(ssMaster, {
        ingest_date: ingestDate, report_key: key, source_file: sig.name, file_id: sig.fileId, content_md5: sig.content_md5,
        action: 'SKIP_DUPLICATE', status: 'OK'
      });
      try { file.moveTo(processed); } catch (_) {}
      return;
    }

    try {
      console.log(`  ⚙️ Procesando CSV: ${sig.name}`);
      const blob = file.getBlob();
      const rows2D = parseDelimitedTo2D_(blob);
      
      const extracted = extractHomebaseFlatData_(rows2D);

      if (!extracted || !extracted.dataRows || extracted.dataRows.length === 0) {
        console.log(`  ⚠️ No se encontró data útil en: ${sig.name}`);
        auditRow_(ssMaster, { ingest_date: ingestDate, report_key: key, source_file: sig.name, action: 'IMPORTED_EMPTY', status: 'OK' });
        file.moveTo(processed);
        return;
      }

      if (shRaw.getLastRow() === 0) {
        const fullHeader = ['ingest_date', 'payroll_start', 'payroll_end', 'source_file', 'report_key'].concat(extracted.headerRow);
        shRaw.appendRow(fullHeader);
      }

      const flatData = extracted.dataRows.map(row => [
        ingestDate, 
        extracted.period.startYmd, 
        extracted.period.endYmd, 
        sig.name, 
        key
      ].concat(row));
      
      const startRow = shRaw.getLastRow() + 1;
      shRaw.getRange(startRow, 1, flatData.length, flatData[0].length).setValues(flatData);

      auditRow_(ssMaster, {
        ingest_date: ingestDate, report_key: key, source_file: sig.name, file_id: sig.fileId, content_md5: sig.content_md5,
        action: 'IMPORTED', rows: flatData.length, cols: flatData[0].length, status: 'OK'
      });

      console.log(`  ✅ Ingesta plana exitosa: ${sig.name} (${flatData.length} turnos reales) | Periodo: ${extracted.period.startYmd} a ${extracted.period.endYmd}`);
      file.moveTo(processed);

    } catch (err) {
      console.log(`  ❌ ERROR procesando ${sig.name}: ${err.message}`);
      logEvent_(ssMaster, `ingest_homebase_one(${key})`, 'ERROR', `${sig.name} | ${err.message}`);
    }
  });
}

// ==========================================
// APLANADOR INTELIGENTE HOMEBASE v1.1
// ==========================================
function extractHomebaseFlatData_(rows2D) {
  let headerRow = [];
  let dataRows = [];
  
  const period = parseHomebasePayrollPeriod_(rows2D) || { startYmd: '', endYmd: '' };
  
  for (let i = 0; i < rows2D.length; i++) {
    const row = rows2D[i];
    const cell0 = String(row[0] || "").trim(); // Name o Basura
    const cell1 = String(row[1] || "").trim(); // Clock in date
    
    // Capturar Encabezados Reales
    if (cell0.toLowerCase() === 'name' && headerRow.length === 0) {
      headerRow = row.map(h => String(h).trim());
      continue;
    }
    
    // Filtro Anti-Basura Positivo: Solo aceptamos filas que tengan un Nombre Y una Fecha de Entrada
    if (headerRow.length > 0) {
      if (
        cell0 !== '' && 
        cell0.toLowerCase() !== 'name' && 
        !cell0.toLowerCase().includes('total') && 
        cell1 !== '' // ESTA ES LA CLAVE: Si no hay fecha de entrada, no es un turno real
      ) {
        const w = headerRow.length;
        const cleanRow = row.slice(0, w);
        while (cleanRow.length < w) cleanRow.push(''); 
        dataRows.push(cleanRow);
      }
    }
  }
  
  if (headerRow.length === 0) return null;
  return { headerRow, dataRows, period };
}

// ==========================================
// FUNCIONES DE SOPORTE (Idempotencia y Parseo)
// ==========================================

function parseDelimitedTo2D_(blob) {
  const txt = blob.getDataAsString('UTF-8').replace(/^\uFEFF/, ''); 
  const firstLine = (txt.split(/\r?\n/)[0] || '');
  const tabCount = (firstLine.match(/\t/g) || []).length;
  const commaCount = (firstLine.match(/,/g) || []).length;
  if (tabCount > commaCount) return Utilities.parseCsv(txt, '\t');
  return Utilities.parseCsv(txt); 
}

function parseHomebasePayrollPeriod_(rows2D) {
  const max = Math.min(20, rows2D.length);
  for (let r = 0; r < max; r++) {
    const line = (rows2D[r] || []).join(' ').replace(/\s+/g, ' ').trim();
    if (!line) continue;
    const m = line.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})\s*(?:to|To|\-)\s*(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (m) {
      const s = `${m[3]}-${pad2_(m[1])}-${pad2_(m[2])}`;
      const e = `${m[6]}-${pad2_(m[4])}-${pad2_(m[5])}`;
      return { startYmd: s, endYmd: e };
    }
  }
  return null;
}

function pad2_(n) { return ('0' + n).slice(-2); }

function getFileSignatureStrong_(file) {
  const bytes = file.getBlob().getBytes();
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, bytes);
  const content_md5 = digest.map(b => ('0' + (b & 0xff).toString(16)).slice(-2)).join('');
  return { fileId: file.getId(), name: file.getName(), content_md5 };
}

function wasAlreadyIngestedStrong_(shAudit, reportKey, fileId, contentMd5) {
  const lastRow = shAudit.getLastRow();
  if (lastRow < 2) return false;
  const data = shAudit.getRange(Math.max(2, lastRow - 5000), 1, Math.min(lastRow - 1, 5000), shAudit.getLastColumn()).getValues();
  for (const row of data) {
    if (row[1] === reportKey && row[14] === 'OK' && (row[5] === contentMd5 || row[4] === fileId)) return true;
  }
  return false;
}

function ensureMisLog_(ss) {
  let sh = ss.getSheetByName('MIS_log') || ss.insertSheet('MIS_log');
  if (sh.getLastRow() === 0) sh.appendRow(['ts', 'function', 'level', 'message', 'details']);
  return sh;
}

function ensureIngestAudit_(ss) {
  let sh = ss.getSheetByName('ingest_audit') || ss.insertSheet('ingest_audit');
  if (sh.getLastRow() === 0) {
    sh.appendRow(['ingest_date', 'report_key', 'raw_folder', 'source_file', 'file_id', 'content_md5', 'size', 'created', 'updated', 'action', 'rows', 'cols', 'start_row', 'ms', 'status', 'message']);
    sh.setFrozenRows(1);
  }
  return sh;
}

function logEvent_(ss, fnName, level, message) {
  const tz = Session.getScriptTimeZone();
  ensureMisLog_(ss).appendRow([Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss'), fnName, level, message, '']);
}

function auditRow_(ss, o) {
  ensureIngestAudit_(ss).appendRow([
    o.ingest_date || '', o.report_key || '', '', o.source_file || '', o.file_id || '', o.content_md5 || '',
    0, '', '', o.action || '', o.rows || 0, o.cols || 0, 0, 0, o.status || '', o.message || ''
  ]);
}

function getChildFolderByName_(parent, name) {
  const it = parent.getFoldersByName(name);
  if (!it.hasNext()) throw new Error(`Falta folder ${name}`);
  return it.next();
}

function getOrCreateChildFolder_(parent, name) {
  const it = parent.getFoldersByName(name);
  return it.hasNext() ? it.next() : parent.createFolder(name);
}
