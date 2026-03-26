/**
 * NORMALIZE_SHOPVENTORY (Versión Arquitecto v7.1 - Set Engine, RAM Broom & MODULAR)
 * -------------------------------------------------------------------------
 * - ADIÓS a upsertByReportDate (que causaba duplicados y columnas fantasmas).
 * - HOLA al motor de Set Idempotency (Garantía matemática anti-duplicados).
 * - NUEVO: Se vacía la hoja RAW automáticamente al terminar para liberar RAM.
 * - NUEVO (v7.1): Funciones individuales (Runners) para procesar cada reporte por separado.
 */

const SHOPV_CONFIG = {
  inventory: { masterId: ENV.SHOPV.masters.inventory, rawSheet: 'shopventory_inventory_raw', cleanSheet: 'shopventory_inventory' },
  product_sales: { masterId: ENV.SHOPV.masters.product_sales, rawSheet: 'shopventory_product_sales_raw', cleanSheet: 'shopventory_product_sales' },
  dead_inventory: { masterId: ENV.SHOPV.masters.dead_inventory, rawSheet: 'shopventory_dead_inventory_raw', cleanSheet: 'shopventory_dead_inventory' },
  inventory_change: { masterId: ENV.SHOPV.masters.inventory_change, rawSheet: 'inventory_change_raw', cleanSheet: 'inventory_change' },
  low_inventory: { masterId: ENV.SHOPV.masters.low_inventory, rawSheet: 'low_inventory_raw', cleanSheet: 'low_inventory' },
  vendor_sales: { masterId: ENV.SHOPV.masters.vendor_sales, rawSheet: 'vendor_sales_raw', cleanSheet: 'vendor_sales' },
  purchase_orders: { masterId: ENV.SHOPV.masters.purchase_orders, rawSheet: 'shopventory_purchase_orders_raw', cleanSheet: 'shopventory_purchase_orders' }
};

// =========================================================================
// 🚀 CONTROLADORES PRINCIPALES (RUNNERS MODULARES)
// =========================================================================

// Ejecuta TODOS los reportes (Uso normal diario)
function normalize_all_shopventory() {
  console.log("--- INICIANDO NORMALIZACIÓN SHOPVENTORY GLOBAL v7.1 ---");
  Object.keys(SHOPV_CONFIG).forEach(key => {
    try { 
      if (key === 'purchase_orders') normalizeShopvPurchaseOrders_(); 
      else normalizeShopvOne_(key); 
    } catch (e) { console.error(`❌ Error en ${key}: ${e.message}`); }
  });
  if (SpreadsheetApp.getActive()) SpreadsheetApp.getActive().toast('Shopventory Normalizado v7.1 ✅');
}

// Ejecuciones Modulares (Para cargas pesadas o recuperación de errores)
function normalize_shopv_inventory_only() { normalizeShopvOne_('inventory'); }
function normalize_shopv_product_sales_only() { normalizeShopvOne_('product_sales'); }
function normalize_shopv_dead_inventory_only() { normalizeShopvOne_('dead_inventory'); }
function normalize_shopv_inventory_change_only() { normalizeShopvOne_('inventory_change'); }
function normalize_shopv_low_inventory_only() { normalizeShopvOne_('low_inventory'); }
function normalize_shopv_vendor_sales_only() { normalizeShopvOne_('vendor_sales'); }
function normalize_shopv_purchase_orders_only() { normalizeShopvPurchaseOrders_(); }

// =========================================================================
// ⚙️ MOTORES DE NORMALIZACIÓN (LÓGICA INTERNA)
// =========================================================================

function normalizeShopvOne_(key) {
  const conf = SHOPV_CONFIG[key];
  if (!conf || !conf.masterId) return;
  const ss = SpreadsheetApp.openById(conf.masterId);
  const shRaw = ss.getSheetByName(conf.rawSheet);
  if (!shRaw) return;

  const rawData = shRaw.getDataRange().getValues();
  if (rawData.length < 2) return; // Nada que procesar

  const shNorm = ss.getSheetByName(conf.cleanSheet) || ss.insertSheet(conf.cleanSheet);
  const normData = shNorm.getDataRange().getValues();

  // 1. CARGAR HASHES EXISTENTES (El Escudo Anti-Duplicados)
  const existingKeys = new Set();
  let rebuildTable = false;

  if (normData.length > 0) {
    const idempIndex = normData[0].indexOf('idempotency_key');
    if (idempIndex === -1) {
      rebuildTable = true;
    } else {
      for (let i = 1; i < normData.length; i++) existingKeys.add(String(normData[i][idempIndex]));
    }
  } else {
    rebuildTable = true;
  }

  if (rebuildTable) { shNorm.clear(); existingKeys.clear(); }

  // 2. PROCESAR BLOQUES DE LA RAW
  let currentFile = '', reportDate = '';
  const newRows = [];
  let finalHeaders = [];

  for (let r = 0; r < rawData.length; r++) {
    const firstCell = String(rawData[r][0] || '').trim();
    
    // Captura de metadatos robusta
    if (firstCell.startsWith('SOURCE_FILE=')) {
      currentFile = firstCell.split('=')[1];
      const mISO = currentFile.match(/(\d{4})[_-](\d{2})[_-](\d{2})/);
      if (mISO) reportDate = `${mISO[1]}-${mISO[2]}-${mISO[3]}`;
    }
    if (firstCell.startsWith('REPORT_DATE=')) {
      let rd = firstCell.split('=')[1].trim();
      if (rd) reportDate = rd;
    }
    
    if (isShopvHeader_(rawData[r])) {
      const header = rawData[r].map(h => String(h).toLowerCase().trim());
      const blockRows = [];
      let i = r + 1;
      
      while (i < rawData.length && !String(rawData[i][0]).startsWith('SOURCE_FILE=')) {
        if (rawData[i][0] && !String(rawData[i][0]).toLowerCase().includes('total')) {
          blockRows.push(rawData[i]);
        }
        i++;
      }
      
      // Transformación y Filtrado por Set
      const transformed = transformShopvBlock_(key, header, blockRows, reportDate);
      if (transformed.length > 0) {
        if (finalHeaders.length === 0) finalHeaders = transformed[0]; // Capturar encabezados

        for (let j = 1; j < transformed.length; j++) {
          const tRow = transformed[j];
          const hash = tRow[tRow.length - 1]; // El key siempre está al final

          if (!existingKeys.has(hash)) {
            newRows.push(tRow);
            existingKeys.add(hash);
          }
        }
      }
      r = i - 1;
      reportDate = ''; 
    }
  }

  // 3. INYECCIÓN BATCH SÚPER RÁPIDA
  if (rebuildTable && finalHeaders.length > 0) {
    shNorm.getRange(1, 1, 1, finalHeaders.length).setValues([finalHeaders]).setFontWeight("bold").setBackground("#e0e0e0");
    shNorm.setFrozenRows(1);
  }

  if (newRows.length > 0) {
    shNorm.getRange(shNorm.getLastRow() + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
    console.log(`✅ [${key}] Insertadas ${newRows.length} filas nuevas.`);
  } else {
    console.log(`⏩ [${key}] Sin registros nuevos.`);
  }

  // 4. LA SÚPER ESCOBA DE RAM (Limpieza extrema)
  let daysToKeep = key === 'dead_inventory' ? 120 :100;
  purgeOldRowsShopv_(shNorm, daysToKeep);
  
  // ¡MAGIA! Vaciamos el RAW porque ya procesamos todo. Evita cuellos de botella de RAM.
  shRaw.clear();
  shRaw.appendRow(["SOURCE_FILE=CLEARED_BY_RAM_BROOM"]);
}

function normalizeShopvPurchaseOrders_() {
  const conf = SHOPV_CONFIG['purchase_orders'];
  if (!conf || !conf.masterId) return;
  const ss = SpreadsheetApp.openById(conf.masterId);
  const shRaw = ss.getSheetByName(conf.rawSheet);
  if (!shRaw) return;

  const rawData = shRaw.getDataRange().getValues();
  if (rawData.length < 2) return; 

  const shNorm = ss.getSheetByName(conf.cleanSheet) || ss.insertSheet(conf.cleanSheet);
  const normData = shNorm.getDataRange().getValues();

  const existingKeys = new Set();
  let rebuildTable = false;

  if (normData.length > 0) {
    const idempIndex = normData[0].indexOf('idempotency_key');
    if (idempIndex === -1) rebuildTable = true;
    else for (let i = 1; i < normData.length; i++) existingKeys.add(String(normData[i][idempIndex]));
  } else { rebuildTable = true; }

  if (rebuildTable) { shNorm.clear(); existingKeys.clear(); }

  const newRows = [];
  const finalHeaders = ['report_date', 'po_number', 'vendor', 'product', 'sku', 'barcode', 'quantity', 'cost_unit', 'total_cost', 'idempotency_key'];
  const num = v => { if (v === 'N/A' || v === '-' || v === '') return 0; const n = Number(String(v || 0).replace(/[^0-9.\-]/g, '')); return isFinite(n) ? n : 0; };

  for (let i = 1; i < rawData.length; i++) {
    const r = rawData[i];
    if (!r[2] || !r[5]) continue; 
    let poDate = r[3];
    try { let d = new Date(poDate); if (!isNaN(d.getTime())) poDate = Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd"); } catch(e) {}
    
    const idempKey = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, 'shopv_po|' + [String(r[2]).trim(), String(r[4]).trim(), String(r[5]).trim(), String(r[6]).trim()].join('|')).map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');
    
    if (!existingKeys.has(idempKey)) {
      newRows.push([poDate, String(r[2]).trim(), String(r[4]).trim(), String(r[5]).trim(), String(r[6]).trim(), String(r[7]).trim(), num(r[9]), num(r[11]), num(r[16]), idempKey]);
      existingKeys.add(idempKey);
    }
  }

  if (rebuildTable) {
    shNorm.getRange(1, 1, 1, finalHeaders.length).setValues([finalHeaders]).setFontWeight("bold").setBackground("#e0e0e0");
  }
  if (newRows.length > 0) {
    shNorm.getRange(shNorm.getLastRow() + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
    console.log(`✅ [Purchase Orders] Insertadas ${newRows.length} filas nuevas.`);
  } else {
    console.log(`⏩ [Purchase Orders] Sin registros nuevos.`);
  }

  purgeOldRowsShopv_(shNorm,100);
  shRaw.clear(); shRaw.appendRow(["SOURCE_FILE=CLEARED_BY_RAM_BROOM"]);
}

function transformShopvBlock_(type, h, rows, date) {
  const num = v => { if (v === 'N/A' || v === '-') return 0; const n = Number(String(v || 0).replace(/[^0-9.\-]/g, '')); return isFinite(n) ? n : 0; };
  const getHash = (arr, prefix) => Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, prefix + '|' + arr.join('|')).map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');
  let out = [];

  if (type === 'inventory') {
    out.push(['report_date','product','variant','categories','vendors','sku','barcode','in_stock','cost_unit','total_value','idempotency_key']);
    rows.forEach(r => { const rowData = [ date, r[h.indexOf('product')], r[h.indexOf('variant')], r[h.indexOf('categories')], r[h.indexOf('vendors')], r[h.indexOf('sku')], r[h.indexOf('barcode')], num(r[h.indexOf('in stock')]), num(r[h.indexOf('cost/unit')]), num(r[h.indexOf('total value')]) ]; rowData.push(getHash([date, rowData[1], rowData[2]], 'shopv_inv')); out.push(rowData); });
  } else if (type === 'dead_inventory') {
    out.push(['report_date','product','variant','days_since_last_sale','in_stock_qty','in_stock_value','idempotency_key']);
    rows.forEach(r => { const rowData = [ date, r[h.indexOf('product')], r[h.indexOf('variant')], num(r[h.indexOf('days since last sale')]), num(r[h.indexOf('qty. in stock')]), num(r[h.indexOf('tot. value')]) ]; rowData.push(getHash([date, rowData[1], rowData[2]], 'shopv_dead')); out.push(rowData); });
  } else if (type === 'product_sales') {
    out.push(['report_date','product','variant','sold','gross_sales','net_sales','net_profit','idempotency_key']);
    rows.forEach(r => { const rowData = [ date, r[h.indexOf('product')], r[h.indexOf('variant')], num(r[h.indexOf('sold')]), num(r[h.indexOf('gross sales')]), num(r[h.indexOf('net product sales')]), num(r[h.indexOf('net profit')]) ]; rowData.push(getHash([date, rowData[1], rowData[2]], 'shopv_sales')); out.push(rowData); });
  } else if (type === 'vendor_sales') {
    out.push(['report_date','vendor','product','variant','sold','total_revenue','total_profit','idempotency_key']);
    rows.forEach(r => { const rowData = [ date, r[h.indexOf('vendor')], r[h.indexOf('product')], r[h.indexOf('variant')], num(r[h.indexOf('sold') !== -1 ? h.indexOf('sold') : h.indexOf('quantity')]), num(r[h.indexOf('total revenue')]), num(r[h.indexOf('total profit')]) ]; rowData.push(getHash([date, rowData[1], rowData[2]], 'shopv_vendor')); out.push(rowData); });
  } else if (type === 'low_inventory') {
    out.push(['report_date','product','variant','in_stock','reorder_point','idempotency_key']);
    rows.forEach(r => { const rowData = [ date, r[h.indexOf('product')], r[h.indexOf('variant')], num(r[h.indexOf('in stock')]), num(r[h.indexOf('reorder point')]) ]; rowData.push(getHash([date, rowData[1], rowData[2]], 'shopv_low')); out.push(rowData); });
  } else if (type === 'inventory_change') {
    out.push(['report_date','product','variant','qty_change','value_change','idempotency_key']);
    rows.forEach(r => { const rowData = [ date, r[h.indexOf('product')], r[h.indexOf('variant')], num(r[h.indexOf('net qty change')]), num(r[h.indexOf('net value change')]) ]; rowData.push(getHash([date, rowData[1], rowData[2], r.join('')], 'shopv_chg')); out.push(rowData); });
  }
  return out;
}

function isShopvHeader_(row) {
  const low = row.join(',').toLowerCase();
  return (low.includes('product') && low.includes('sku')) || 
         (low.includes('product') && low.includes('variant')) || 
         (low.includes('vendor') && (low.includes('revenue') || low.includes('sold'))) || 
         (low.includes('last sale') && low.includes('days since'));
}

function purgeOldRowsShopv_(sheet, daysToKeep) {
  if (!sheet || sheet.getLastRow() <= 1) return;
  const data = sheet.getRange(1, 1, sheet.getLastRow(), 1).getValues();
  const limitDate = new Date(); limitDate.setDate(limitDate.getDate() - daysToKeep);
  const limitDateStr = Utilities.formatDate(limitDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
  let deleted = 0;
  for (let i = data.length - 1; i >= 1; i--) {
    let cellVal = data[i][0]; if (!cellVal) continue;
    let dateStr = (cellVal instanceof Date) ? Utilities.formatDate(cellVal, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(cellVal).substring(0, 10);
    if (dateStr !== '' && dateStr < limitDateStr && dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) { sheet.deleteRow(i + 1); deleted++; }
  }
  if (deleted > 0) console.log(`🧹 Purga completada: ${deleted} filas borradas.`);
}
