/**
 * INGEST_SHOPVENTORY (Versión Arquitecto v7.1 - Modular & Anti-Crash)
 * -------------------------------------------------------------------------
 * - FIX: "Ceguera voluntaria" para reportes sin cabecera (Dead/Low Inventory) para evitar fechas erróneas.
 * - NUEVO: Funciones individuales para correr cada reporte por separado (Modularidad).
 * - Try-Catch individual por archivo para máxima resiliencia.
 */

const SHOPV_SOURCES = {
  inventory: { rawSub: 'shopventory_inventory', masterId: ENV.SHOPV.masters.inventory, rawSheet: 'shopventory_inventory_raw' },
  product_sales: { rawSub: 'shopventory_product_sales', masterId: ENV.SHOPV.masters.product_sales, rawSheet: 'shopventory_product_sales_raw' },
  dead_inventory: { rawSub: 'shopventory_dead_inventory', masterId: ENV.SHOPV.masters.dead_inventory, rawSheet: 'shopventory_dead_inventory_raw' },
  inventory_change: { rawSub: 'shopventory_inventory_change', masterId: ENV.SHOPV.masters.inventory_change, rawSheet: 'inventory_change_raw' },
  low_inventory: { rawSub: 'shopventory_low_inventory', masterId: ENV.SHOPV.masters.low_inventory, rawSheet: 'low_inventory_raw' },
  vendor_sales: { rawSub: 'shopventory_vendors_sales', masterId: ENV.SHOPV.masters.vendor_sales, rawSheet: 'vendor_sales_raw' }
};

// =========================================================================
// 🚀 CONTROLADORES PRINCIPALES (RUNNERS)
// =========================================================================

// Ejecuta TODOS los reportes (Uso normal diario)
function ingest_shopventory_all() {
  console.log("--- INICIANDO INGESTA SHOPVENTORY GLOBAL v7.1 ---");
  Object.keys(SHOPV_SOURCES).forEach(key => {
    try { ingestShopvOne_(SHOPV_SOURCES[key]); } catch (err) { console.error(`Error en lote ${key}: ${err.message}`); }
  });
  try { ingestShopvPurchaseOrders_(); } catch (err) { console.error("Error en POs: " + err.message); }
  if (SpreadsheetApp.getActive()) SpreadsheetApp.getActive().toast('Ingesta Shopventory v7.1 completada ✅');
}

// Ejecuciones Modulares (Para recuperación de errores o cargas pesadas)
function ingest_shopv_inventory_only() { ingestShopvOne_(SHOPV_SOURCES.inventory); }
function ingest_shopv_product_sales_only() { ingestShopvOne_(SHOPV_SOURCES.product_sales); }
function ingest_shopv_dead_inventory_only() { ingestShopvOne_(SHOPV_SOURCES.dead_inventory); }
function ingest_shopv_inventory_change_only() { ingestShopvOne_(SHOPV_SOURCES.inventory_change); }
function ingest_shopv_low_inventory_only() { ingestShopvOne_(SHOPV_SOURCES.low_inventory); }
function ingest_shopv_vendor_sales_only() { ingestShopvOne_(SHOPV_SOURCES.vendor_sales); }
function ingest_shopv_purchase_orders_only() { ingestShopvPurchaseOrders_(); }

// =========================================================================
// ⚙️ MOTORES DE INGESTA (LÓGICA INTERNA)
// =========================================================================

function ingestShopvOne_(src) {
  return withScriptLock_(`ingest_${src.rawSub}`, () => {
    const ss = SpreadsheetApp.openById(src.masterId);
    const folder = getRawFolderByRawSub_(src.rawSub);
    if (!folder) return;

    const processedFolder = getProcessedFolder_();
    const shRaw = getOrCreateSheet(ss, src.rawSheet);
    const auditSh = getOrCreateIngestAudit_(ss);

    const files = folder.getFiles();
    let count = 0;
    
    while (files.hasNext()) {
      const f = files.next();
      try {
        const md5 = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, f.getBlob().getBytes())
                      .map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');

        if (auditHasMd5_(auditSh, src.rawSub, md5)) { f.moveTo(processedFolder); continue; }

        const rows = parseDelimitedFile_(f);
        if (!rows.length) { f.moveTo(processedFolder); continue; }

        const reportDate = getShopvReportDateFromFile_(src.rawSub, rows, f);
        
        if (reportDate && auditHasReport_(auditSh, src.rawSub, reportDate)) {
          f.moveTo(processedFolder);
          auditAppend_(auditSh, src.rawSub, reportDate, f, md5, rows.length, 'INGEST_SKIP_DATE');
          continue;
        }

        const metaRows = [[`SOURCE_FILE=${f.getName()}`], [`REPORT_DATE=${reportDate || ''}`]];
        const safeMeta = ensureRectangular(metaRows);
        const safeRows = ensureRectangular(rows);

        const start = shRaw.getLastRow() + 1;
        shRaw.getRange(start, 1, safeMeta.length, safeMeta[0].length).setValues(safeMeta);
        shRaw.getRange(start + safeMeta.length, 1, safeRows.length, safeRows[0].length).setValues(safeRows);

        auditAppend_(auditSh, src.rawSub, reportDate || '', f, md5, safeRows.length, 'INGEST_OK');
        f.moveTo(processedFolder);
        count++;
      } catch(fileErr) {
        console.error(`Error aislado en archivo ${f.getName()}: ${fileErr.message}`);
      }
    }
    if (count > 0) console.log(`✅ [${src.rawSub}] procesó ${count} archivos nuevos.`);
  }, 180000);
}

function ingestShopvPurchaseOrders_() {
  const rawSub = 'shopventory_purchase_orders';
  const masterId = ENV.SHOPV.masters.purchase_orders;
  if (!masterId) return;

  return withScriptLock_(`ingest_${rawSub}`, () => {
    const ss = SpreadsheetApp.openById(masterId);
    const folder = getRawFolderByRawSub_(rawSub);
    if (!folder) return;
    const processedFolder = getProcessedFolder_();
    const shRaw = getOrCreateSheet(ss, 'shopventory_purchase_orders_raw');
    const auditSh = getOrCreateIngestAudit_(ss);
    const files = folder.getFiles();
    let count = 0;

    while (files.hasNext()) {
      const f = files.next();
      try {
        const fileName = f.getName();
        const md5 = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, f.getBlob().getBytes()).map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');
        if (md5 && auditHasMd5_(auditSh, rawSub, md5)) { f.moveTo(processedFolder); continue; }

        const rows = parseDelimitedFile_(f);
        if (!rows.length) { f.moveTo(processedFolder); continue; }

        let poNumber = '', poDateRaw = '', vendor = '', itemStartIndex = -1;
        for (let i = 0; i < rows.length; i++) {
          let colA = rows[i][0] ? rows[i][0].toString().trim() : '';
          if (colA.includes('Cloud Smoke Shop')) poNumber = rows[i][1];
          if (colA === 'Date Created') poDateRaw = rows[i][1];
          if (colA === 'Vendor') vendor = rows[i][1];
          if (colA === 'Product' && rows[i][1] === 'SKU') { itemStartIndex = i + 1; break; }
        }

        if (itemStartIndex === -1) continue;

        let formattedPoDate = poDateRaw;
        try {
          let parsedDate = new Date(poDateRaw);
          if (!isNaN(parsedDate.getTime())) formattedPoDate = Utilities.formatDate(parsedDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
        } catch (e) {}

        let outData = [];
        const timestamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "MM/dd/yyyy HH:mm:ss");

        for (let i = itemStartIndex; i < rows.length; i++) {
          let colA = rows[i][0] ? rows[i][0].toString().trim() : '';
          if (colA === '' || colA === 'Total Quantity' || colA === ',') break;
          let flatRow = [timestamp, fileName, poNumber, formattedPoDate, vendor].concat(rows[i].slice(0, 14)); 
          outData.push(flatRow);
        }

        if (outData.length > 0) {
          const safeData = ensureRectangular(outData);
          shRaw.getRange(shRaw.getLastRow() + 1, 1, safeData.length, safeData[0].length).setValues(safeData);
        }
        auditAppend_(auditSh, rawSub, formattedPoDate, f, md5, outData.length, 'INGEST_OK');
        f.moveTo(processedFolder);
        count++;
      } catch(fileErr) {
        console.error(`Error en PO ${f.getName()}: ${fileErr.message}`);
      }
    }
    if (count > 0) console.log(`✅ [Purchase Orders] procesó ${count} archivos nuevos.`);
  }, 180000);
}

// =========================================================================
// 🧠 EXTRACTORES Y PARSERS BLINDADOS
// =========================================================================

function getShopvReportDateFromFile_(rawSub, rows2D, file) {
  const fileName = file.getName();
  const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12,jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12};
  
  // 1. Mes escrito (Feb 15, 2026)
  const mNameFull = fileName.match(/([A-Za-z]{3,9})[\s_-]+(\d{1,2})(?:st|nd|rd|th)?[\s_-]+(\d{4})/i);
  if (mNameFull) {
    const mm = months[mNameFull[1].toLowerCase()];
    if (mm) return `${mNameFull[3]}-${String(mm).padStart(2,'0')}-${String(mNameFull[2]).padStart(2,'0')}`;
  }
  
  // 2. ISO (2026-02-15 o 2026_02_15)
  const mISO = fileName.match(/(\d{4})[\s_-](\d{2})[\s_-](\d{2})/);
  if (mISO) return `${mISO[1]}-${mISO[2]}-${mISO[3]}`;
  
  // 3. USA (02-15-2026 o 02_15_2026)
  const mUS = fileName.match(/(\d{1,2})[\s_-](\d{1,2})[\s_-](\d{4})/);
  if (mUS) return `${mUS[3]}-${String(mUS[1]).padStart(2,'0')}-${String(mUS[2]).padStart(2,'0')}`;

  // 4. 🛑 EL ESCUDO DE CEGUERA: Si es Dead o Low Inventory, PROHIBIDO leer el contenido interno
  if (rawSub !== 'shopventory_dead_inventory' && rawSub !== 'shopventory_low_inventory') {
    // Si la función getReportDateFromHeader2D existe en utils.gs, la usamos
    if (typeof getReportDateFromHeader2D === "function") {
       const contentDate = getReportDateFromHeader2D(rows2D); 
       if (contentDate) return contentDate;
    }
  }
  
  // 5. Fallback absoluto: Si no hay fecha, el reporte asume que es de HOY
  return Utilities.formatDate(file.getLastUpdated(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

function auditHasMd5_(auditSheet, rawSub, md5) {
  if (!md5 || !auditSheet) return false;
  const lastRow = auditSheet.getLastRow();
  if (lastRow < 2) return false;
  return auditSheet.getRange(2, 1, lastRow - 1, 8).getValues().some(r => String(r[1]) === rawSub && String(r[5]) === md5 && String(r[7]) === 'INGEST_OK');
}

function parseDelimitedFile_(file) {
  let content = "";
  try { content = file.getBlob().getDataAsString(); } catch(e) { content = file.getBlob().getDataAsString('ISO-8859-1'); }
  if (!content) return [];
  const lines = content.split('\n');
  const tabs = (lines[0].match(/\t/g) || []).length;
  const commas = (lines[0].match(/,/g) || []).length;
  if (tabs > commas) return lines.map(l => l.split('\t'));
  return parseCSVRobust_(content);
}

function parseCSVRobust_(text) {
  const result = []; let row = [], inQuotes = false, currentVal = '';
  for (let i = 0; i < text.length; i++) {
    let char = text[i], nextChar = text[i + 1];
    if (char === '"') {
      if (inQuotes && nextChar === '"') { currentVal += '"'; i++; } else { inQuotes = !inQuotes; }
    } else if (char === ',' && !inQuotes) { row.push(currentVal); currentVal = '';
    } else if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && nextChar === '\n') i++; 
      row.push(currentVal);
      if (row.length > 1 || row[0] !== "") result.push(row);
      row = []; currentVal = '';
    } else { currentVal += char; }
  }
  if (currentVal || row.length > 0) { row.push(currentVal); if (row.length > 1 || row[0] !== "") result.push(row); }
  return result;
}
