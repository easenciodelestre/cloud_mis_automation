/**
 * UTILS.GS (Versión de Infraestructura MIS v7.2 - TOTALMENTE COMPLETO)
 * -------------------------------------------------------------------
 * Contiene todas las herramientas de Drive, Sheets, Seguridad y Auditoría.
 * Incluye soporte para AUDIT_CONCILIATOR y Motores de Idempotencia.
 */

/* ---------- 1. SEGURIDAD Y CONTROL DE CONCURRENCIA ---------- */

/**
 * Evita que dos ejecuciones del mismo script choquen entre sí.
 */
function withScriptLock_(name, fn, timeoutMs) {
  const lock = LockService.getScriptLock();
  const ok = lock.tryLock(timeoutMs || 30000);
  if (!ok) throw new Error(`LOCK_TIMEOUT: No pude obtener acceso para ${name}. Inténtalo en un momento.`);
  try {
    return fn();
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

/* ---------- 2. GESTIÓN DE CARPETAS DRIVE ---------- */

function safeGetFolderById(id, label) {
  try { return DriveApp.getFolderById(id); } catch (e) {
    throw new Error(`No pude abrir la carpeta ${label || ""} (ID=${id}). Revisa permisos.`);
  }
}

function getOrCreateFolder(parentId, name) {
  const parent = safeGetFolderById(parentId, 'parent');
  const it = parent.getFoldersByName(name);
  return it.hasNext() ? it.next() : parent.createFolder(name);
}

function findSubfolder(parentId, name) {
  const parent = safeGetFolderById(parentId, 'parent');
  const it = parent.getFoldersByName(name);
  return it.hasNext() ? it.next() : null;
}

function getRawFolderByRawSub_(rawSub) {
  return findSubfolder(ENV.RAW_PARENT_FOLDER_ID, rawSub);
}

function getProcessedFolder_() {
  return getOrCreateFolder(ENV.RAW_PARENT_FOLDER_ID, ENV.PROCESSED_FOLDER_ID);
}

/* ---------- 3. GESTIÓN DE HOJAS DE CÁLCULO (SHEETS) ---------- */

function getOrCreateSpreadsheet(parentFolderId, name) {
  try {
    const folder = safeGetFolderById(parentFolderId, 'destino');
    const files = folder.getFilesByName(name);
    if (files.hasNext()) return SpreadsheetApp.openById(files.next().getId());
    const ss = SpreadsheetApp.create(name);
    DriveApp.getFileById(ss.getId()).moveTo(folder);
    return ss;
  } catch (e) {
    const ss = SpreadsheetApp.create(name);
    return ss;
  }
}

function getOrCreateSheet(ss, name) {
  if (!ss) return null;
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

function ensureRectangular(rows) {
  if (!rows || rows.length === 0) return [['']];
  const w = Math.max.apply(null, rows.map(r => r.length));
  return rows.map(r => {
    const row = (r || []).slice();
    while (row.length < w) row.push('');
    return row;
  });
}

/* ---------- 4. LOGGING Y ERRORES ---------- */

function logEvent(ss, where, level, msg) {
  try {
    if (!ss) ss = SpreadsheetApp.getActive();
    const sh = getOrCreateSheet(ss, 'MIS_log');
    sh.insertRows(1);
    sh.getRange(1, 1, 1, 4).setValues([[
      Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "M/d/yyyy HH:mm:ss"),
      String(where), String(level), String(msg)
    ]]);
  } catch (e) { console.warn("Fallo logEvent: " + e.message); }
}

function logError(ss, context, err) {
  const msg = (err && err.message) ? err.message : String(err);
  console.error(`[ERROR] ${context}: ${msg}`);
  try { logEvent(ss, context, 'ERROR', msg); } catch (e) {}
}

/* ---------- 5. AUDITORÍA DE INGESTA (INGEST_AUDIT) ---------- */

function getOrCreateIngestAudit_(ss) {
  const sh = getOrCreateSheet(ss, 'INGEST_AUDIT');
  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, 8).setValues([[
      'ts','rawSub','report_date','file_name','file_id','md5','rows','status'
    ]]);
    sh.setFrozenRows(1);
  }
  return sh;
}

function auditHasReport_(auditSheet, rawSub, reportDate) {
  if (!reportDate || !auditSheet) return false;
  const lastRow = auditSheet.getLastRow();
  if (lastRow < 2) return false;
  const data = auditSheet.getRange(2, 1, lastRow - 1, 8).getValues();
  return data.some(row => String(row[1]) === rawSub && String(row[2]) === reportDate && String(row[7]) === 'INGEST_OK');
}

/**
 * NUEVA: Verifica si un archivo ya fue procesado mediante su HASH MD5
 */
function auditHasMd5_(auditSheet, rawSub, md5) {
  if (!md5 || !auditSheet) return false;
  const lastRow = auditSheet.getLastRow();
  if (lastRow < 2) return false;
  const data = auditSheet.getRange(2, 1, lastRow - 1, 8).getValues();
  return data.some(row => String(row[1]) === rawSub && String(row[5]) === md5 && String(row[7]) === 'INGEST_OK');
}

function auditAppend_(auditSheet, rawSub, reportDate, file, md5, rowsCount, status) {
  if (!auditSheet) return;
  auditSheet.appendRow([
    Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "M/d/yyyy HH:mm:ss"),
    rawSub, reportDate || '', file.getName(), file.getId(), md5 || '', rowsCount || 0, status
  ]);
}

/* ---------- 6. AUDITORÍA DE NORMALIZACIÓN (NORMALIZE_AUDIT) ---------- */

function getOrCreateNormalizeAudit_(ss) {
  const sh = getOrCreateSheet(ss, 'NORMALIZE_AUDIT');
  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, 8).setValues([[
      'ts','pipeline','report_id','report_date','source_file','rows_written','status','message'
    ]]);
    sh.setFrozenRows(1);
  }
  return sh;
}

function normalizeAuditAppend_(auditSh, rawSub, reportDate, rowsCount, md5, status, note) {
  if (!auditSh) return;
  auditSh.appendRow([
    Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "M/d/yyyy HH:mm:ss"),
    rawSub, '', reportDate || '', '', rowsCount || 0, status || '', (note || '') + (md5 ? ' | md5:'+md5 : '')
  ]);
}

/* ---------- 7. PIPELINE STATUS ---------- */

function getOrCreatePipelineStatus_(ss) {
  const sh = getOrCreateSheet(ss, 'PIPELINE_STATUS');
  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, 6).setValues([[
      'rawSub','report_date','ingest_status','normalize_status','kpis_status','updated_at'
    ]]);
    sh.setFrozenRows(1);
  }
  return sh;
}

function upsertPipelineStatus_(statusSh, rawSub, reportDate, patchObj) {
  if (!rawSub || !reportDate || !statusSh) return;
  const last = statusSh.getLastRow();
  const now = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "M/d/yyyy HH:mm:ss");
  if (last >= 2) {
    const data = statusSh.getRange(2,1,last-1,6).getValues();
    for (let i=0; i<data.length; i++) {
      if (String(data[i][0]) === String(rawSub) && String(data[i][1]) === String(reportDate)) {
        const out = [data[i][0], data[i][1], patchObj.ingest_status || data[i][2], patchObj.normalize_status || data[i][3], patchObj.kpis_status || data[i][4], now];
        statusSh.getRange(i + 2,1,1,6).setValues([out]);
        return;
      }
    }
  }
  statusSh.appendRow([rawSub, reportDate, patchObj.ingest_status || '', patchObj.normalize_status || '', patchObj.kpis_status || '', now]);
}

/* ---------- 8. FUNCIONES DE AYUDA (FECHAS Y PARSEO) ---------- */

function getReportDateFromHeader2D(rows2D) {
  const max = Math.min(rows2D.length, 30);
  for (let r = 0; r < max; r++) {
    const line = (rows2D[r] || []).join(' ');
    const m = line.match(/([A-Za-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})/i);
    if (m) {
      const months = {jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12};
      const mm = months[m[1].toLowerCase().substring(0,3)];
      return `${m[3]}-${String(mm).padStart(2,'0')}-${String(m[2]).padStart(2,'0')}`;
    }
  }
  return null;
}

/**
 * REFORZADA: Soporta Date objects, ISO strings y Formato USA (MM/DD/YYYY)
 */
function forceYMD_(v) {
  if (!v) return null;
  if (v instanceof Date) return Utilities.formatDate(v, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const s = String(v).trim();
  const mISO = s.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (mISO) return mISO[0];
  const mUS = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mUS) return `${mUS[3]}-${mUS[1].padStart(2,'0')}-${mUS[2].padStart(2,'0')}`;
  return null;
}

function generateIdempotencyKey(rowArray, salt) {
  const signature = (salt || '') + rowArray.join('|');
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, signature);
  return digest.map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');
}

function sliceAllReportBlocks_(allRows2D, startPredicate) {
  if (!allRows2D || !allRows2D.length) return [];
  const starts = [];
  for (let i = 0; i < allRows2D.length; i++) { if (startPredicate(allRows2D[i] || [], i)) starts.push(i); }
  const blocks = [];
  for (let k = 0; k < starts.length; k++) {
    const start = starts[k];
    const end = (k + 1 < starts.length) ? (starts[k + 1] - 1) : (allRows2D.length - 1);
    blocks.push({ start, end, rows: allRows2D.slice(start, end + 1) });
  }
  return blocks;
}

function triggerDrive() { DriveApp.getRootFolder(); }
