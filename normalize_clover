/**
 * NORMALIZE_CLOVER (Versión Arquitecto v55.2 - MARKET BASKET + MODULAR + AUTOCLEAN)
 * -------------------------------------------------------------------
 * - NUEVO: Integración transaccional de Line Items (Customer Profiling)
 * - NUEVO: Funciones individuales para normalizar por reporte.
 * - NUEVO: Vaciado automático de la pestaña RAW post-normalización.
 * - MANTIENE: Purga dinámica de 100 días en la hoja limpia.
 */

const CLOVER_CONFIG = {
  RETENTION_DAYS: 100,
  MASTERS: {
    clover_sales:      '132gRXwETwraKDAQS10fOKJ__PljEtDm400ss0Pwicdg',
    clover_discounts:  '1RvkOcV56WHC_ZQJ8YfYctXQUayhZDl9MFeoYyHrHCfw',
    clover_employees:  '1wK1_kHeeNj2I1pmuzgJkIW849WHhYxu3ZisEDaRMDRQ',
    clover_overview:   '1ApTGaTS9yKIaQXIAQ1zPsw07fGimrPFzX9SYDeY148c',
    clover_tenders:    '1ttkC_VnXte6NnuSeN01rdYNmAwa_tMCQcHr_ZHGTS9s',
    clover_items:      '1B_Xp4F3LQF5EEO3jHMCqaSZ8K2_hI0rA59_fkcxoVXg',
    clover_orders_line_items: '1SEUuLXjQ7pzOYFUm81bVh6z6OYHq2HNMNF-4GN_kvz8'
  }
};

// =========================================================
// ORQUESTADOR MAESTRO
// =========================================================

function normalize_all_clover() {
  console.log("--- INICIANDO NORMALIZE CLOVER (ALL) v55.2 ---");
  normalize_clover_items();
  normalize_clover_sales();
  normalize_clover_overview();
  normalize_clover_employees();
  normalize_clover_tenders();
  normalize_clover_discounts();
  normalize_clover_lineitems();
  console.log("--- NORMALIZE CLOVER COMPLETADO ---");
  if (SpreadsheetApp.getActive()) SpreadsheetApp.getActive().toast('Todo Clover Normalizado ✅');
}

// =========================================================
// FUNCIONES MODULARES INDIVIDUALES
// =========================================================

function normalize_clover_items() {
  const task = { name: 'Items', id: CLOVER_CONFIG.MASTERS.clover_items, raw: 'clover_items_raw', clean: 'clover_items', fn: process_items_v54_FIX };
  executeTask_(task);
}

function normalize_clover_sales() {
  const task = { name: 'Sales', id: CLOVER_CONFIG.MASTERS.clover_sales, raw: 'clover_sales_raw', clean: 'clover_sales', fn: process_sales_v37_STABLE };
  executeTask_(task);
}

function normalize_clover_overview() {
  const task = { name: 'Overview', id: CLOVER_CONFIG.MASTERS.clover_overview, raw: 'clover_overview_raw', clean: 'clover_overview', fn: process_overview_v53_FIX };
  executeTask_(task);
}

function normalize_clover_employees() {
  const task = { name: 'Employees', id: CLOVER_CONFIG.MASTERS.clover_employees, raw: 'clover_employees_raw', clean: 'clover_employees', fn: process_employees_v50_STABLE };
  executeTask_(task);
}

function normalize_clover_tenders() {
  const task = { name: 'Tenders', id: CLOVER_CONFIG.MASTERS.clover_tenders, raw: 'clover_tenders_raw', clean: 'clover_tenders', fn: process_tenders_v37_STABLE };
  executeTask_(task);
}

function normalize_clover_discounts() {
  const task = { name: 'Discounts', id: CLOVER_CONFIG.MASTERS.clover_discounts, raw: 'clover_discounts_raw', clean: 'clover_discounts', fn: process_discounts_v53_FIX };
  executeTask_(task);
}

function normalize_clover_lineitems() {
  const task = { name: 'LineItems', id: CLOVER_CONFIG.MASTERS.clover_orders_line_items, raw: 'clover_orders_line_items_raw', clean: 'clover_orders_line_items', fn: process_line_items_v55_NEW };
  executeTask_(task);
}


// =========================================================
// MOTOR DE EJECUCIÓN (RADAR + PURGA + LIMPIEZA RAW)
// =========================================================

function executeTask_(t) {
  try {
    console.log(`--- Ejecutando: ${t.name} ---`);
    const ss = SpreadsheetApp.openById(t.id);
    const shRaw = ss.getSheetByName(t.raw);
    
    // 1. Verificar si hay datos
    if (!shRaw || shRaw.getLastRow() <= 1) {
      console.log(`  ⏭️ Sin datos nuevos en ${t.raw}`);
      return;
    }

    // 2. Correr Radar y normalizar
    runRadarV52_(t, ss, shRaw); 
    
    // 3. Purgar datos viejos (>100 días) en la hoja limpia
    const shClean = ss.getSheetByName(t.clean);
    purgeOldRowsV55_(shClean, CLOVER_CONFIG.RETENTION_DAYS);
    
    // 4. NUEVO: Vaciar la hoja RAW para liberar memoria
    console.log(`  🧹 Limpiando ${t.raw}...`);
    const maxRows = shRaw.getMaxRows();
    if (maxRows > 2) {
      shRaw.deleteRows(2, maxRows - 1);
    } else {
      shRaw.clear();
    }
    
    console.log(`  ✅ ${t.name} completado.`);

  } catch (e) {
    console.error(`❌ Error en ${t.name}: ${e.message}`);
  }
}

function runRadarV52_(t, ss, raw) {
  const data = raw.getDataRange().getValues();
  
  let currentReportDate = '';
  let dataBuffer = [];
  let isCollecting = false;
  let currentHeaderRow = null;

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const cellA = String(row[0] || "");

    if (cellA.includes('REPORT_DATE=')) {
      currentReportDate = cellA.split('=')[1].replace(/[^0-9\-]/g, '').trim();
    }

    if (cellA.includes('SOURCE_FILE=')) {
      if (isCollecting && dataBuffer.length > 0 && currentReportDate) {
        const result = t.fn(currentHeaderRow, dataBuffer, currentReportDate);
        if (result && result.length > 1) {
          upsertByDateV52_(getOrCreateSheetV52_(ss, t.clean), result, currentReportDate);
        }
      }
      isCollecting = false;
      dataBuffer = [];
    }

    if (isHeaderV52_(row, t.name)) {
      isCollecting = true;
      currentHeaderRow = row;
      dataBuffer = [];
      continue;
    }

    if (isCollecting) dataBuffer.push(row);

    if (i === data.length - 1) {
      if (isCollecting && dataBuffer.length > 0 && currentReportDate) {
        const result = t.fn(currentHeaderRow, dataBuffer, currentReportDate);
        if (result && result.length > 1) {
          upsertByDateV52_(getOrCreateSheetV52_(ss, t.clean), result, currentReportDate);
        }
      }
    }
  }
}


// =========================================================
// PARSERS ESPECÍFICOS (DATA WRANGLERS)
// =========================================================

// 7. LINE ITEMS (MARKET BASKET)
function process_line_items_v55_NEW(hRow, rows, date) {
  const out = [['report_date','line_item_date','order_id','employee','item_name','sku','qty','item_revenue','total_discount','net_revenue','refunded','payment_state','idempotency_key']];

  let hClean = hRow.map(v => cleanV52_(v));
  const m = {
    ldate: hClean.indexOf('lineitemdate'),
    emp: hClean.indexOf('orderemployeename'),
    order: hClean.indexOf('orderid'),
    item: hClean.indexOf('itemname'),
    sku: hClean.indexOf('itemsku'),
    qty: hClean.indexOf('perunitquantity'),
    rev: hClean.indexOf('itemrevenue'),
    disc: hClean.indexOf('totaldiscount'),
    net: hClean.indexOf('totalrevenue'),
    ref: hClean.indexOf('refunded'),
    state: hClean.indexOf('orderpaymentstate')
  };

  if (m.order === -1) m.order = 7;
  if (m.item === -1) m.item = 8;
  if (m.rev === -1) m.rev = 12;

  rows.forEach(r => {
    if (!r || r.length < 10) return;
    const orderId = String(r[m.order] || '').trim();
    const itemName = String(r[m.item] || '').trim();
    if (!orderId || !itemName || cleanV52_(itemName).includes('itemname')) return;

    let qtyVal = String(r[m.qty] || '').trim();
    let qty = qtyVal === "" ? 1 : numV52_(qtyVal);

    const rowArr = [
      date,
      String(r[m.ldate] || '').trim(),
      orderId,
      String(r[m.emp] || '').trim(),
      itemName,
      String(r[m.sku] || '').trim(),
      qty,
      numV52_(r[m.rev]),
      numV52_(r[m.disc]),
      numV52_(r[m.net]),
      String(r[m.ref] || '').trim().toLowerCase() === 'true' ? 'TRUE' : 'FALSE',
      String(r[m.state] || '').trim()
    ];
    rowArr.push(generateIdemV52_(rowArr, 'lineitems'));
    out.push(rowArr);
  });
  return out;
}

// 1. ITEMS
function process_items_v54_FIX(hRow, rows, date) {
  const out = [['report_date','category_name','product_name','sku','barcode','gross_sales','net_sales','qty_sold','refunded_items','exchanged_items','net_sold','modifier_name','modifier_sold','modifier_amount','discounts','refunds_amt','exchanges_amt','cogs','gross_profit','idempotency_key']];
  let curCat = "Uncategorized"; let inDetail = false;
  if (hRow && hRow.length > 2 && (cleanV52_(hRow[1]).includes('name') || cleanV52_(hRow[0]).includes('category'))) { inDetail = true; }
  rows.forEach(r => {
    if (!r) return;
    if (!inDetail) { if (cleanV52_(r[1]).includes('name') && (cleanV52_(r[2]).includes('sku') || cleanV52_(r[3]).includes('barcode'))) { inDetail = true; } return; }
    if (r[0] !== "" && r[1] === "") { curCat = String(r[0]).split(/[,;]/)[0].trim(); return; }
    if (r[1] !== "" && !cleanV52_(r[1]).includes('total') && !cleanV52_(r[1]).includes('name')) {
      const rowArr = [date, curCat, r[1], r[2], r[3], numV52_(r[4]), numV52_(r[5]), numV52_(r[6]), numV52_(r[7]), numV52_(r[8]), numV52_(r[9]), r[11], numV52_(r[12]), numV52_(r[13]), numV52_(r[14]), numV52_(r[15]), numV52_(r[16]), numV52_(r[21]), numV52_(r[22])];
      rowArr.push(generateIdemV52_(rowArr, 'items')); out.push(rowArr);
    }
  }); return out;
}

// 2. DISCOUNTS
function process_discounts_v53_FIX(hRow, rows, date) {
  const out = [['report_date','discount_name','order_info','on_orders','on_items','total','idempotency_key']];
  let curDiscountName = ""; let foundAny = false; let headerReached = false;
  rows.forEach(r => { 
    if (!r) return; 
    const colA = String(r[0] || '').trim(); const colB = String(r[1] || '').trim(); const colA_low = colA.toLowerCase();
    if (!headerReached) { if (colA_low.includes('discount name')) { headerReached = true; } return; }
    if (colA_low.startsWith('order:')) { foundAny = true; const rowArr = [date, curDiscountName, colA, numV52_(r[1]), numV52_(r[3]), numV52_(r[5])]; rowArr.push(generateIdemV52_(rowArr, 'disc')); out.push(rowArr); }
    else if (colB === "" && colA !== "" && !colA_low.includes('total') && !colA_low.includes('discount name')) { curDiscountName = colA; } 
  });
  if (!foundAny) { const emptyRow = [date, "N/A", "No Discounts Recorded", 0, 0, 0]; emptyRow.push(generateIdemV52_(emptyRow, 'disc')); out.push(emptyRow); }
  return out;
}

// 3. OVERVIEW
function process_overview_v53_FIX(hRow, rows, date) {
  const find = (label) => { const r = rows.find(row => row && cleanV52_(row[0]).includes(cleanV52_(label))); return r ? numV52_(r[1]) : 0; };
  const row = [date, find('gross sales'), find('discounts'), find('refunds'), find('net sales'), find('taxes'), find('tips'), find('amount collected')];
  row.push(generateIdemV52_(row, 'over')); return [['report_date','gross','disc','ref','net','tax','tip','coll','idempotency_key'], row];
}

// 4. EMPLOYEES
function process_employees_v50_STABLE(hRow, rows, date) {
  const out = [['report_date', 'employee', 'gross_sales', 'discounts', 'num_discounts', 'refunds', 'net_sales', 'taxes_collected', 'tips', 'amount_collected', 'idempotency_key']];
  let startIdx = rows.findIndex(r => r && cleanV52_(r[0]) === 'employee');
  if (startIdx === -1) return null;
  for (let i = startIdx + 1; i < rows.length; i++) {
    const r = rows[i]; const nameRaw = String(r[0] || "").trim();
    if (!nameRaw) continue; if (cleanV52_(nameRaw).includes('total')) break;
    const rowArr = [date, nameRaw.split('(')[0].trim(), numV52_(r[1]), numV52_(r[2]), numV52_(r[3]), numV52_(r[4]), numV52_(r[6]), numV52_(r[10]), numV52_(r[11]), numV52_(r[14])];
    rowArr.push(generateIdemV52_(rowArr, 'emp')); out.push(rowArr);
  } return out;
}

// 5. SALES
function process_sales_v37_STABLE(hRow, rows, date) {
  const out = [['report_date','hour','gross_sales','refunds','net_sales','taxes','amount_collected','idempotency_key']];
  const isTime = c => (c instanceof Date && c.getFullYear() < 1910) || /\b(1[0-2]|0?[1-9])(:00)?\s*(AM|PM)\b/i.test(String(c));
  let hIdx = rows.findIndex(r => r && cleanV52_(r[1]) === 'total' && r.some(isTime));
  if (hIdx === -1) return null;
  let hR = rows[hIdx]; let sub = rows.slice(hIdx + 1);
  const getR = (l) => sub.find(r => r && cleanV52_(r[0]).includes(cleanV52_(l)));
  let rG = getR('Gross sales'), rR = getR('Refunds'), rN = getR('Net sales'), rT = getR('Taxes'), rC = getR('Amount collected');
  for (let col = 2; col < hR.length; col++) {
    if (isTime(hR[col])) {
      let cell = hR[col]; let hS = "";
      if (cell instanceof Date) { let h = cell.getHours(); let am = h >= 12 ? 'PM' : 'AM'; h = h % 12 || 12; hS = (h < 10 ? '0' + h : h) + ":00 " + am; } else { hS = String(cell).trim().toUpperCase(); }
      const row = [date, hS, numV52_(rG?.[col]), numV52_(rR?.[col]), numV52_(rN?.[col]), numV52_(rT?.[col]), numV52_(rC?.[col])];
      row.push(generateIdemV52_(row, 'sales')); out.push(row);
    }
  } return out;
}

// 6. TENDERS
function process_tenders_v37_STABLE(hRow, rows, date) {
  const out = [['report_date','tender_type','sales_total','order_refunds','manual_refunds','amount_collected','transaction_count','idempotency_key']];
  const whitelist = ['credit card', 'debit card', 'cash'];
  let tableHdrIdx = rows.findIndex(r => r && (cleanV52_(r[0]).includes('cardtype') || (cleanV52_(r[0]).includes('tender') && cleanV52_(r[0]).includes('sales'))));
  let header = (tableHdrIdx === -1) ? hRow : rows[tableHdrIdx]; let hClean = header.map(v => cleanV52_(v));
  const m = { tx: hClean.indexOf('transactions') !== -1 ? hClean.indexOf('transactions') : 1, sales: hClean.indexOf('sales') !== -1 ? hClean.indexOf('sales') : 3, order: 4, manual: 5, coll: 6 };
  rows.forEach(r => { if (!r) return; for (let col = 0; col <= 2; col++) { let cell = String(r[col] || '').trim().toLowerCase();
      if (whitelist.includes(cell)) { const rowArr = [date, cell.toUpperCase(), numV52_(r[m.sales]), numV52_(r[m.order]), numV52_(r[m.manual]), numV52_(r[m.coll]), numV52_(r[m.tx])];
        rowArr.push(generateIdemV52_(rowArr, 'tender')); out.push(rowArr); break; } } });
  return out;
}

// =========================================================
// UTILIDADES (HELPERS)
// =========================================================

function cleanV52_(s) { return String(s || '').replace(/[^\x20-\x7E]/g, '').toLowerCase().replace(/\s/g, ''); }
function numV52_(v) { return (v instanceof Date) ? 0 : Number(String(v || 0).replace(/[^0-9.\-]/g, '')) || 0; }
function getOrCreateSheetV52_(ss, name) { let sh = ss.getSheetByName(name); if (!sh) sh = ss.insertSheet(name); return sh; }
function generateIdemV52_(row, salt) { return Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, salt + row.join('|')).map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join(''); }

function isHeaderV52_(row, type) {
  const s = cleanV52_(row.join('|'));
  if (type === 'Items') return s.includes('itemsreport') || s.includes('revenueitemsales') || s.includes('categoryname');
  if (type === 'Sales') return s.includes('salesreport');
  if (type === 'Overview') return s.includes('overviewreport');
  if (type === 'Employees') return s.includes('employeesalesreport');
  if (type === 'Tenders') return s.includes('tender') || s.includes('cardtype');
  if (type === 'Discounts') return s.includes('discountsreport');
  if (type === 'LineItems') return s.includes('lineitemdate') && s.includes('orderid'); 
  return false;
}

function upsertByDateV52_(sh, rows2D, date) {
  if (!date || rows2D.length <= 1) return;
  const last = sh.getLastRow();
  if (last > 0) {
    const data = sh.getRange(1, 1, last, 1).getDisplayValues();
    for (let i = last - 1; i >= 0; i--) { if (data[i][0] === date) sh.deleteRow(i + 1); }
  }
  sh.getRange(sh.getLastRow() + 1, 1, rows2D.length - (sh.getLastRow() === 0 ? 0 : 1), rows2D[0].length)
    .setValues(sh.getLastRow() === 0 ? rows2D : rows2D.slice(1));
}

function purgeOldRowsV55_(sheet, daysToKeep) {
  if (!sheet || sheet.getLastRow() <= 1) return;
  const data = sheet.getRange(1, 1, sheet.getLastRow(), 1).getValues();
  const limitDate = new Date(); limitDate.setDate(limitDate.getDate() - daysToKeep);
  const limitDateStr = Utilities.formatDate(limitDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
  let rowsDeleted = 0;
  for (let i = data.length - 1; i >= 1; i--) {
    let cellVal = data[i][0]; if (!cellVal) continue;
    let dateStr = (cellVal instanceof Date) ? Utilities.formatDate(cellVal, Session.getScriptTimeZone(), "yyyy-MM-dd") : String(cellVal).substring(0, 10);
    if (dateStr !== '' && dateStr < limitDateStr && dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) { sheet.deleteRow(i + 1); rowsDeleted++; }
  }
  if (rowsDeleted > 0) console.log(`🧹 Purga Clover: ${rowsDeleted} filas borradas en ${sheet.getName()}`);
}
