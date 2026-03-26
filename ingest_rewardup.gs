/*******************************************************
 * REWARDUP — INGEST (.xlsx -> rewardup_<key>_raw)
 * -----------------------------------------------------
 * - ESCÁNER INTELIGENTE v2.4.
 * - FIX v2.4 (RATE LIMIT): Añadido Utilities.sleep() para evitar bloqueos de la API de Drive.
 * - FIX v2.4 (1899 GHOST): Usa getDisplayValues() para evitar que los números muten a fechas.
 * - Extracción dinámica de 'report_date' desde el encabezado.
 *******************************************************/

function ingest_rewardup_all() {
  console.log("--- INICIANDO INGESTA REWARDUP v2.4 ---");
  const keys = Object.keys(ENV.RU.rawSubfolders);
  
  if (keys.length === 0) {
    console.log("⚠️ ALERTA: No hay llaves configuradas en ENV.RU.rawSubfolders");
  }

  // Si vas a correr de a un reporte a la vez, comenta las otras llaves en env.gs 
  // o ejecuta ingest_rewardup_one('nombre_del_reporte') directamente.
  keys.forEach(key => {
    try {
      ingest_rewardup_one(key);
    } catch (err) {
      console.log(`❌ ERROR en [${key}]: ${String(err.message || err)}`);
      try {
        const ss = SpreadsheetApp.openById(ENV.RU.masters[key]);
        logEvent(ss, `ingest_rewardup_all(${key})`, 'ERROR', String(err.message || err));
        auditRow_(ss, { report_key: key, status: 'ERROR', action: 'ERROR', message: String(err.message || err) });
      } catch (_) {}
    }
  });

  console.log('Ingest RewardUp completado ✅');
}

function ingest_rewardup_one(key) {
  if (!ENV?.RU?.masters?.[key]) throw new Error(`ENV.RU.masters.${key} no definido`);
  const ssMaster = SpreadsheetApp.openById(ENV.RU.masters[key]);
  const subfolderName = ENV.RU.rawSubfolders[key];

  console.log(`🔍 [${key}] Buscando subcarpeta: ${subfolderName}`);

  ensureMisLog_(ssMaster);
  const shAudit = ensureIngestAudit_(ssMaster);

  const rawRoot = DriveApp.getFolderById(ENV.RAW_PARENT_FOLDER_ID || ENV.RU.RAW_ROOT_ID);
  const inFolder = getChildFolderByName_(rawRoot, subfolderName);
  const processed = getOrCreateChildFolder_(inFolder, ENV.PROCESSED_FOLDER_ID);

  const files = [];
  const it = inFolder.getFiles();
  while (it.hasNext()) {
    const f = it.next();
    const mime = f.getMimeType();
    const name = f.getName().toLowerCase();
    if (name.endsWith('.xlsx') || mime.includes('spreadsheetml') || mime === MimeType.MICROSOFT_EXCEL) {
      files.push(f);
    }
  }

  if (!files.length) {
    console.log(`⏭️ [${key}] Saltando... No hay archivos.`);
    return;
  }
  
  files.sort((a, b) => a.getDateCreated().getTime() - b.getDateCreated().getTime());

  const rawSheetName = `rewardup_${key}_raw`;
  const shRaw = ssMaster.getSheetByName(rawSheetName) || ssMaster.insertSheet(rawSheetName);

  files.forEach((file, index) => {
    const ingestDate = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
    const sig = getFileSignatureStrong_(file);

    if (wasAlreadyIngestedStrong_(shAudit, key, sig.fileId, sig.content_md5)) {
      console.log(`  ⏩ Archivo duplicado saltado: ${sig.name}`);
      auditRow_(ssMaster, {
        ingest_date: ingestDate, report_key: key, source_file: sig.name, file_id: sig.fileId, content_md5: sig.content_md5,
        action: 'SKIP_DUPLICATE', status: 'OK', message: 'Duplicado detectado por MD5.'
      });
      try { file.moveTo(processed); } catch (_) {}
      return;
    }

    let tempSheetId = '';
    try {
      // 🛡️ ESCUDO ANTI-RATE LIMIT: Pausa de 2.5 segundos para no enfurecer a la API de Drive
      if (index > 0) {
        Utilities.sleep(2500); 
      }

      console.log(`  ⚙️ Convirtiendo a Google Sheets: ${sig.name}`);
      tempSheetId = convertXlsxToGoogleSheet_(file); 
      
      const tempSS = SpreadsheetApp.openById(tempSheetId);
      
      // 🛡️ ESCUDO ANTI-1899: Leemos "Display Values" para extraer texto plano, no objetos Date
      const values = tempSS.getSheets()[0].getDataRange().getDisplayValues();
      
      console.log(`  🧠 Escaneando filas buscando encabezados y fecha...`);
      const extracted = extractTableData_(values);

      if (!extracted || !extracted.dataRows || extracted.dataRows.length === 0) {
        console.log(`  ⚠️ No se encontró la tabla en: ${sig.name}`);
        auditRow_(ssMaster, { ingest_date: ingestDate, report_key: key, source_file: sig.name, action: 'IMPORTED_EMPTY', status: 'OK' });
        safeTrash_(tempSheetId);
        file.moveTo(processed);
        return;
      }

      // NUEVO ESQUEMA: Se añade 'report_date' como segunda columna
      if (shRaw.getLastRow() === 0) {
        const fullHeader = ['ingest_date', 'report_date', 'source_file', 'report_key'].concat(extracted.headerRow);
        shRaw.appendRow(fullHeader);
      }

      // Inyectar metadatos en cada fila
      const flatData = extracted.dataRows.map(row => [ingestDate, extracted.reportDate, sig.name, key].concat(row));
      
      const startRow = shRaw.getLastRow() + 1;
      shRaw.getRange(startRow, 1, flatData.length, flatData[0].length).setValues(flatData);

      auditRow_(ssMaster, {
        ingest_date: ingestDate, report_key: key, source_file: sig.name, file_id: sig.fileId, content_md5: sig.content_md5,
        action: 'IMPORTED', rows: flatData.length, cols: flatData[0].length, status: 'OK'
      });

      console.log(`  ✅ Ingesta exitosa: ${sig.name} (${flatData.length} filas) | Fecha: ${extracted.reportDate}`);
      
      // Destruir archivo temporal inmediatamente para liberar cuota
      safeTrash_(tempSheetId);
      file.moveTo(processed);

    } catch (err) {
      safeTrash_(tempSheetId);
      console.log(`  ❌ ERROR procesando ${sig.name}: ${err.message}`);
      logEvent(ssMaster, `ingest_rewardup_one(${key})`, 'ERROR', `${sig.name} | ${err.message}`);
      // No lanzamos el error para que continúe con el siguiente archivo
    }
  });
}

// ==========================================
// EL ESCÁNER INTELIGENTE
// ==========================================
function extractTableData_(values) {
  let headerIdx = -1;
  let reportDate = "";
  
  for (let i = 0; i < Math.min(values.length, 15); i++) {
    const cell0 = String(values[i][0] || "").trim();
    const cell1 = String(values[i][1] || "").trim();
    
    const combinedText = (cell0 + " " + cell1).replace(/\s+/g, " ");
    const dateMatch = combinedText.match(/from date\s*:\s*(.+)/i);
    
    if (dateMatch && !reportDate) {
      let rawDateStr = dateMatch[1].trim();
      try {
        const d = new Date(rawDateStr);
        if (!isNaN(d.getTime())) {
          reportDate = Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
        }
      } catch (e) {} 
    }

    const firstCellLower = cell0.toLowerCase();
    if (firstCellLower === 'member name' || firstCellLower === 'store location') {
      headerIdx = i;
      break;
    }
  }
  
  if (headerIdx === -1) return null;

  const headerRow = values[headerIdx].map(h => String(h).trim());
  const w = headerRow.length;

  let dataRows = values.slice(headerIdx + 1);
  dataRows = dataRows.filter(row => row.join('').trim() !== '');

  dataRows = dataRows.map(r => {
    const row = r.slice(0, w);
    while (row.length < w) row.push('');
    return row;
  });

  return { headerRow, dataRows, reportDate };
}

// ==========================================
// FUNCIONES DE SOPORTE Y AUDITORÍA
// ==========================================

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

function convertXlsxToGoogleSheet_(file) {
  const resource = { 
    name: `TMP_${file.getName()}`,
    title: `TMP_${file.getName()}`,
    mimeType: MimeType.GOOGLE_SHEETS 
  };
  
  const copied = Drive.Files.copy(resource, file.getId(), { 
    supportsAllDrives: true,
    convert: true 
  });
  
  return copied.id;
}

function safeTrash_(fileId) {
  if (fileId) try { DriveApp.getFileById(fileId).setTrashed(true); } catch (_) {}
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

function logEvent(ss, fnName, level, message) {
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
