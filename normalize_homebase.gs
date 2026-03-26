/*******************************************************
 * HOMEBASE — NORMALIZE (Versión 1.7 - ESCOBA DE RAM)
 * -----------------------------------------------------
 * - PURE DATA WAREHOUSE VERSION.
 * - FIX v1.6: Usa getDisplayValues() para evitar bugs de 1899.
 * - Matriz de Formato Previo (@) para forzar texto plano.
 * - NUEVO v1.7: Purga dinámica de 60 días.
 *******************************************************/

function normalize_homebase_all() {
  console.log("--- INICIANDO NORMALIZACIÓN HOMEBASE (PURE BQ) v1.7 ---");
  const key = 'timesheets';

  try {
    normalize_homebase_one(key);
  } catch (err) {
    console.log(`❌ ERROR normalizando [${key}]: ${String(err.message || err)}`);
  }

  console.log('Normalización Homebase completada ✅');
  if (SpreadsheetApp.getActive()) SpreadsheetApp.getActive().toast('Homebase Normalizado y Purgado ✅');
}

function normalize_homebase_one(key) {
  const masterId = ENV.HB.masters[key];
  if (!masterId) throw new Error(`ENV.HB.masters.${key} no definido`);
  
  const ssMaster = SpreadsheetApp.openById(masterId);
  const rawName = `homebase_${key}_raw`;
  const normName = `homebase_${key}`;
  const payrollName = `payroll_summary`; // La otra pestaña que también vamos a purgar

  console.log(`🔍 Buscando pestaña RAW: ${rawName}`);
  const shRaw = ssMaster.getSheetByName(rawName);
  
  if (!shRaw) return;

  const rawData = shRaw.getDataRange().getDisplayValues();
  if (rawData.length < 2) return;

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
  const nameIdx = cleanHeaders.indexOf('name');

  // 2. DETECTOR DE SCHEMA DRIFT & LLAVES EXISTENTES
  let rebuildTable = false;
  const existingKeys = new Set();
  
  if (normData.length > 0) {
    const normHead = normData[0].map(h => String(h).trim());
    if (normHead.join('|') !== cleanHeaders.join('|')) {
      rebuildTable = true;
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

  // 3. PROCESAR FILAS
  const newRows = [];
  
  for (let i = 1; i < rawData.length; i++) {
    const row = rawData[i];
    
    if (row.join('').trim() === '') continue;
    if (nameIdx > -1) {
      const nameVal = String(row[nameIdx]).trim();
      if (nameVal.startsWith('-') || nameVal === '') continue;
    }

    const rowString = key + "|" + row.join("|");
    const idempotency_key = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, rowString)
                                     .map(b => ('0' + (b & 0xff).toString(16)).slice(-2)).join('');

    if (existingKeys.has(idempotency_key)) continue; 

    const cleanRow = row.map((val, colIndex) => {
      let str = String(val).trim();
      if (str === '-') return ''; 
      
      const colName = cleanHeaders[colIndex];

      if (colName && colName.includes('date') && str !== '') return parseHomebaseDate_(str);
      if (colName && colName.includes('time') && str !== '') return parseHomebaseTime_(str);
      
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
      const targetRange = shNorm.getRange(startRow, 1, newRows.length, cleanHeaders.length);
      
      const formatMatrix = [];
      for (let r = 0; r < newRows.length; r++) {
        const rowFmt = [];
        for (let c = 0; c < cleanHeaders.length; c++) {
          const h = cleanHeaders[c];
          if (h.includes('time')) {
            rowFmt.push('@'); 
          } else if (h.includes('date') || h.includes('period')) {
            rowFmt.push('yyyy-MM-dd');
          } else if (h.includes('wage') || h.includes('tips')) {
            rowFmt.push('$0.00');
          } else if (h.includes('hours') || h.includes('actual') || h.includes('regular') || h.includes('ot')) {
            rowFmt.push('0.00');
          } else {
            rowFmt.push('General');
          }
        }
        formatMatrix.push(rowFmt);
      }
      
      targetRange.setNumberFormats(formatMatrix);
      targetRange.setValues(newRows);
      
      console.log(`  ✅ Escritas ${newRows.length} filas normalizadas puras.`);
    }
  } else {
    console.log(`  ⏩ Sin registros nuevos para normalizar.`);
  }

  // 5. LA ESCOBA DE RAM (Purga de 60 días)
  purgeOldRowsHB_(shNorm, 60, 'clock_in_date'); // Limpia timesheets
  purgeOldRowsHB_(shRaw, 60, 'Clock in date'); // Limpia el RAW de timesheets
  
  const shPayroll = ssMaster.getSheetByName(payrollName);
  if (shPayroll) purgeOldRowsHB_(shPayroll, 60, 'report_date'); // Limpia nómina
}

// ==========================================
// EL MOTOR DE LA ESCOBA (Purga Dinámica HB)
// ==========================================
function purgeOldRowsHB_(sheet, daysToKeep, dateColumnName) {
  if (!sheet || sheet.getLastRow() <= 1) return;
  
  const data = sheet.getDataRange().getValues();
  const headers = data[0].map(h => String(h).toLowerCase().trim());
  const dateIdx = headers.indexOf(dateColumnName.toLowerCase());
  
  if (dateIdx === -1) return; // Si no encuentra la columna de fecha, no borra nada por seguridad

  const today = new Date();
  const limitDate = new Date(today.getTime());
  limitDate.setDate(limitDate.getDate() - daysToKeep);
  const limitDateStr = Utilities.formatDate(limitDate, Session.getScriptTimeZone(), "yyyy-MM-dd");

  let rowsDeleted = 0;
  // Recorremos de abajo hacia arriba
  for (let i = data.length - 1; i >= 1; i--) {
    let cellVal = data[i][dateIdx];
    if (!cellVal) continue;
    
    let dateStr = "";
    if (cellVal instanceof Date) {
      dateStr = Utilities.formatDate(cellVal, Session.getScriptTimeZone(), "yyyy-MM-dd");
    } else {
      // Intenta parsear la fecha si viene como texto
      dateStr = parseHomebaseDate_(String(cellVal)); 
    }

    if (dateStr !== '' && dateStr < limitDateStr && dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
      sheet.deleteRow(i + 1);
      rowsDeleted++;
    }
  }
  
  if (rowsDeleted > 0) {
    console.log(`🧹 Purga Homebase: ${rowsDeleted} filas viejas borradas en ${sheet.getName()}`);
  }
}

// ==========================================
// PARSERS ESPECÍFICOS PARA HOMEBASE
// ==========================================
function parseHomebaseDate_(t) {
  if (!t || t === '-') return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;

  let m = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${('0'+m[1]).slice(-2)}-${('0'+m[2]).slice(-2)}`;

  m = t.match(/^([A-Za-z]+)\s+(\d{1,2})\s+(\d{4})$/);
  if (m) {
    const months = { january:1,february:2,march:3,april:4,may:5,june:6, july:7,august:8,september:9,october:10,november:11,december:12 };
    const mon = months[m[1].toLowerCase()];
    if (mon) return `${m[3]}-${('0'+mon).slice(-2)}-${('0'+m[2]).slice(-2)}`;
  }
  
  try {
    const d = new Date(t);
    if (!isNaN(d.getTime())) return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  } catch(e) {}

  return t; 
}

function parseHomebaseTime_(t) {
  let str = String(t).trim();
  if (!str || str === '-') return '';
  
  if (/^\d{2}:\d{2}(:\d{2})?$/.test(str)) return str;

  const m = str.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$/i);
  if (!m) return str; 

  let hh = Number(m[1]);
  const mm = m[2] || '00';
  const ap = m[3].toLowerCase();

  if (ap === 'pm' && hh < 12) hh += 12;
  if (ap === 'am' && hh === 12) hh = 0;

  return ('0'+hh).slice(-2) + ":" + mm + ":00";
}
