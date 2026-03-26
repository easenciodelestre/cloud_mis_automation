/**
 * NORMALIZE_REWARDUP (Versión Arquitecto v2.0 - Súper Escoba de RAM & Modular)
 * -------------------------------------------------------------------------
 * - ESTANDARIZACIÓN BQ: Encabezados limpios.
 * - IDEMPOTENCIA POR SET: Escudo matemático MD5 para evitar duplicados.
 * - NUEVO: Panel de control modular (Runners individuales).
 * - NUEVO: Súper Escoba de RAM (Vacía la hoja RAW al terminar para proteger memoria).
 */

// =========================================================================
// 🚀 CONTROLADORES PRINCIPALES (RUNNERS MODULARES)
// =========================================================================

// Ejecuta TODOS los reportes (Uso normal diario)
function normalize_rewardup_all() {
  console.log("--- INICIANDO NORMALIZACIÓN REWARDUP GLOBAL v2.0 ---");
  const keys = Object.keys(ENV.RU.rawSubfolders);

  keys.forEach(key => {
    try {
      normalize_rewardup_one(key);
    } catch (err) {
      console.error(`❌ ERROR normalizando [${key}]: ${String(err.message || err)}`);
      try {
        const ss = SpreadsheetApp.openById(ENV.RU.masters[key]);
        logEvent_(ss, `normalize_rewardup_all(${key})`, 'ERROR', String(err.message || err));
        auditNormRow_(ss, { report_key: key, status: 'ERROR', action: 'ERROR', message: String(err.message || err) });
      } catch (_) {}
    }
  });

  console.log('Normalización RewardUp completada ✅');
  if (SpreadsheetApp.getActive()) SpreadsheetApp.getActive().toast('RewardUp Normalizado v2.0 ✅');
}

// Ejecuciones Modulares (Para recuperación de errores o cargas pesadas aisladas)
function normalize_ru_user_activity_only() { normalize_rewardup_one('user_activity'); }
function normalize_ru_signup_source_only() { normalize_rewardup_one('signup_source'); }
function normalize_ru_program_stats_only() { normalize_rewardup_one('program_stats'); }
function normalize_ru_order_only()         { normalize_rewardup_one('order'); }
function normalize_ru_redeemed_rewards_only() { normalize_rewardup_one('redeemed_rewards'); }
function normalize_ru_vip_only()           { normalize_rewardup_one('vip'); }
function normalize_ru_point_only()         { normalize_rewardup_one('point'); }

// =========================================================================
// ⚙️ MOTOR DE NORMALIZACIÓN (LÓGICA INTERNA)
// =========================================================================

function normalize_rewardup_one(key) {
  if (!ENV?.RU?.masters?.[key]) throw new Error(`ENV.RU.masters.${key} no definido`);
  const ssMaster = SpreadsheetApp.openById(ENV.RU.masters[key]);
  
  const rawName = `rewardup_${key}_raw`;
  const normName = `rewardup_${key}`;

  console.log(`🔍 [${key}] Buscando pestaña RAW: ${rawName}`);
  const shRaw = ssMaster.getSheetByName(rawName);
  
  if (!shRaw) {
    console.log(`⏭️ [${key}] Pestaña RAW no existe. Saltando...`);
    return;
  }

  const rawData = shRaw.getDataRange().getValues();
  if (rawData.length < 2) {
    console.log(`⏭️ [${key}] Pestaña RAW vacía (sin datos). Saltando...`);
    return;
  }

  const shNorm = ssMaster.getSheetByName(normName) || ssMaster.insertSheet(normName);
  const normData = shNorm.getDataRange().getValues();

  // 1. ESTANDARIZAR ENCABEZADOS
  const rawHeaders = rawData[0];
  const cleanHeaders = rawHeaders.map(h => {
    return String(h).toLowerCase()
      .normalize("NFD").replace(/[\u0300-\u036f]/g, "") 
      .replace(/[^a-z0-9]/g, '_') 
      .replace(/_+/g, '_') 
      .replace(/^_|_$/g, ''); 
  });

  if (!cleanHeaders.includes('idempotency_key')) {
    cleanHeaders.push('idempotency_key');
  }
  const idempIndex = cleanHeaders.indexOf('idempotency_key');

  // 2. DETECTOR DE CAMBIO DE ESQUEMA & ESCUDO ANTI-DUPLICADOS (SET)
  let rebuildTable = false;
  const existingKeys = new Set();
  
  if (normData.length > 0) {
    const normHead = normData[0].map(h => String(h).trim());
    if (normHead.join('|') !== cleanHeaders.join('|')) {
      rebuildTable = true;
      console.log(`  ⚠️ Cambio de esquema detectado en [${key}]. Reconstruyendo tabla...`);
    } else {
      const idxKey = normHead.indexOf('idempotency_key');
      for (let i = 1; i < normData.length; i++) {
        existingKeys.add(String(normData[i][idxKey]));
      }
    }
  } else {
    rebuildTable = true;
  }

  if (rebuildTable) {
    shNorm.clear();
    existingKeys.clear(); 
  }

  // 3. PROCESAR FILAS (Data Cleansing)
  const newRows = [];
  
  for (let i = 1; i < rawData.length; i++) {
    const row = rawData[i];

    if (row.join('').trim() === '') continue;

    const rowString = key + "|" + row.join("|");
    const idempotency_key = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, rowString)
                                     .map(b => ('0' + (b & 0xff).toString(16)).slice(-2)).join('');

    // Escudo: Si ya existe en la normalizada, lo ignoramos
    if (existingKeys.has(idempotency_key)) continue; 

    const cleanRow = row.map(val => {
      if (val instanceof Date) return Utilities.formatDate(val, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
      let str = String(val).trim();
      if (/^\$?\s?-?[\d,]+(\.\d+)?$/.test(str) && str !== '') {
         const numStr = str.replace(/[$,\s]/g, '');
         const num = Number(numStr);
         if (!isNaN(num)) return num;
      }
      return str;
    });

    while (cleanRow.length < idempIndex) cleanRow.push('');
    cleanRow[idempIndex] = idempotency_key;

    newRows.push(cleanRow);
    existingKeys.add(idempotency_key);
  }

  // 4. ESCRIBIR DATOS
  if (newRows.length > 0 || rebuildTable) {
    if (rebuildTable) {
      shNorm.getRange(1, 1, 1, cleanHeaders.length).setValues([cleanHeaders]).setFontWeight("bold").setBackground("#e0e0e0");
      shNorm.setFrozenRows(1);
    }
    
    if (newRows.length > 0) {
      const startRow = shNorm.getLastRow() + 1;
      shNorm.getRange(startRow, 1, newRows.length, cleanHeaders.length).setValues(newRows);
      console.log(`  ✅ [${key}] Escritas ${newRows.length} filas normalizadas.`);
      
      auditNormRow_(ssMaster, { report_key: key, source_file: 'RAW_SHEET', rows_written: newRows.length, status: 'NORMALIZE_OK', message: rebuildTable ? 'Rebuilt & Inserted' : 'Appended' });
      updatePipelineStatus_(ssMaster, key, 'NORMALIZE_OK');
    }
  } else {
    console.log(`  ⏩ [${key}] Sin registros nuevos para normalizar.`);
  }

  // 5. PASAR LA ESCOBA
  // Limpia registros viejos de la hoja Normalizada
  purgeOldRows_(shNorm, 60);
  
  // ¡LA SÚPER ESCOBA DE RAM! (Vacía la RAW por completo)
  shRaw.clear();
  shRaw.appendRow(["SOURCE_FILE=CLEARED_BY_RAM_BROOM", "REPORT_KEY=" + key]);
}

// ==========================================
// EL MOTOR DE LA ESCOBA (Purga Dinámica)
// ==========================================
function purgeOldRows_(sheet, daysToKeep) {
  if (!sheet || sheet.getLastRow() <= 1) return;
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).toLowerCase());
  
  // Buscar columna de fecha ("date", "created", "time")
  let dateIdx = headers.findIndex(h => h.includes('date') || h.includes('created') || h.includes('time'));
  if (dateIdx === -1) dateIdx = 0; // Fallback: asume que la primera columna es fecha
  
  const today = new Date();
  const limitDate = new Date(today.getTime());
  limitDate.setDate(limitDate.getDate() - daysToKeep);
  const limitDateStr = Utilities.formatDate(limitDate, Session.getScriptTimeZone(), "yyyy-MM-dd");

  let rowsDeleted = 0;
  for (let i = data.length - 1; i >= 1; i--) {
    let cellVal = data[i][dateIdx];
    if (!cellVal) continue;
    
    let dateStr = "";
    if (cellVal instanceof Date) {
      dateStr = Utilities.formatDate(cellVal, Session.getScriptTimeZone(), "yyyy-MM-dd");
    } else {
      dateStr = String(cellVal).substring(0, 10);
    }

    if (dateStr !== '' && dateStr < limitDateStr && dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
      sheet.deleteRow(i + 1);
      rowsDeleted++;
    }
  }
  if (rowsDeleted > 0) console.log(`🧹 Purga: ${rowsDeleted} filas borradas en ${sheet.getName()}`);
}

// ==========================================
// FUNCIONES DE SOPORTE Y AUDITORÍA
// ==========================================
function ensureNormAudit_(ss) {
  let sh = ss.getSheetByName('NORMALIZE_AUDIT') || ss.insertSheet('NORMALIZE_AUDIT');
  if (sh.getLastRow() === 0) {
    sh.appendRow(['ts', 'report_key', 'source_file', 'rows_written', 'status', 'message']);
    sh.setFrozenRows(1);
  }
  return sh;
}

function ensureMisLog_(ss) {
  let sh = ss.getSheetByName('MIS_log') || ss.insertSheet('MIS_log');
  if (sh.getLastRow() === 0) sh.appendRow(['ts', 'function', 'level', 'message', 'details']);
  return sh;
}

function auditNormRow_(ss, o) {
  const tz = Session.getScriptTimeZone();
  const ts = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
  ensureNormAudit_(ss).appendRow([
    ts, o.report_key || '', o.source_file || '', o.rows_written || 0, o.status || '', o.message || ''
  ]);
}

function logEvent_(ss, fnName, level, message) {
  const tz = Session.getScriptTimeZone();
  const ts = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
  ensureMisLog_(ss).appendRow([ts, fnName, level, message, '']);
}

function updatePipelineStatus_(ssMaster, key, status) {
  const shStatus = ssMaster.getSheetByName('PIPELINE_STATUS') || ssMaster.insertSheet('PIPELINE_STATUS');
  if (shStatus.getLastRow() === 0) {
    shStatus.appendRow(['report_key', 'normalize_status', 'updated_at']);
    shStatus.setFrozenRows(1);
  }
  const tz = Session.getScriptTimeZone();
  const now = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
  
  const data = shStatus.getDataRange().getValues();
  let found = false;
  for (let i = 1; i < data.length; i++) {
    if (data[i][0] === key) {
      shStatus.getRange(i + 1, 2, 1, 2).setValues([[status, now]]);
      found = true;
      break;
    }
  }
  if (!found) {
    shStatus.appendRow([key, status, now]);
  }
}
