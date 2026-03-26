/**
 * AUDIT_CONCILIATOR (Versión Certificada v24.1 - PRODUCCIÓN REAL)
 * -------------------------------------------------------------------
 * - CONFIG: Rango extendido a Febrero/Marzo 2026.
 * - FIX: Nombre de pestaña clover_items (normalizada).
 * - FIX: Protección contra variantes nulas en Shopventory.
 * - MOTOR: Lexical Matcher (Bolsa de palabras al 75%).
 */

const AUDIT_CONFIG = {
  MASTER_AUDIT_ID: "1YT4pZYqrIxsadO247X29g0rV_6Xh_wo4Ozg8TJfu_48",
  CLOVER_ITEMS_ID: "1B_Xp4F3LQF5EEO3jHMCqaSZ8K2_hI0rA59_fkcxoVXg",
  SHOPV_SALES_ID: "12AvbV6K7yAKi04WQESBnEd_sMWhlqnUVM5pnmOi2QLI", 
  START_DATE: "2026-02-01", // Ajustado para incluir Febrero
  END_DATE: "2026-03-31"    // Ajustado para incluir Marzo
};

function run_audit_validation() {
  console.log("--- INICIANDO CONCILIACIÓN v24.1 (PRODUCCIÓN REAL) ---");
  const ssAudit = SpreadsheetApp.openById(AUDIT_CONFIG.MASTER_AUDIT_ID);
  const shReport = ssAudit.getSheetByName("VALIDATION_REPORT") || ssAudit.insertSheet("VALIDATION_REPORT");
  const shDebug = ssAudit.getSheetByName("AUDIT_DEBUG") || ssAudit.insertSheet("AUDIT_DEBUG");
  
  // Acceso a Masters
  const cloverSS = SpreadsheetApp.openById(AUDIT_CONFIG.CLOVER_ITEMS_ID);
  const shopvSS = SpreadsheetApp.openById(AUDIT_CONFIG.SHOPV_SALES_ID);
  
  const cloverRaw = cloverSS.getSheetByName("clover_items").getDataRange().getValues();
  const shopvRaw = shopvSS.getSheetByName("shopventory_product_sales").getDataRange().getValues();

  // 1. AUTO-MAPEO DE COLUMNAS
  const mapHeaders = (row, keywords) => {
    const map = {};
    const headerStr = row.map(h => String(h).toLowerCase().trim());
    for (const [key, searchList] of Object.entries(keywords)) {
      let idx = -1;
      for (const searchTerm of searchList) {
        idx = headerStr.findIndex(h => h.includes(searchTerm));
        if (idx !== -1) break;
      }
      map[key] = idx;
    }
    return map;
  };

  const cMap = mapHeaders(cloverRaw[0], { date: ["report_date", "date"], name: ["product_name", "item", "name"], qty: ["qty_sold", "qty", "quantity"], net: ["net_sales", "net"] });
  const sMap = mapHeaders(shopvRaw[0], { date: ["report_date", "date"], name: ["product"], variant: ["variant"], qty: ["sold", "qty", "quantity"], net: ["net_sales", "net"] });

  // 2. AGREGACIÓN DE DATOS (Filtro por fecha aplicado aquí)
  const cloverSales = aggregateData_v24(cloverRaw, cMap, "CLOVER");
  const shopvSales = aggregateData_v24(shopvRaw, sMap, "SHOPV");

  // 3. FASE DE CONCILIACIÓN (Lexical Match)
  const finalReport = {}; 
  let mismatches = 0;
  let totalDiffNet = 0;

  const allDates = [...new Set([...Object.keys(cloverSales).map(k=>k.split("|")[0]), ...Object.keys(shopvSales).map(k=>k.split("|")[0])])];

  allDates.forEach(date => {
    let c_items = Object.keys(cloverSales).filter(k => k.startsWith(date+"|"));
    let s_items = Object.keys(shopvSales).filter(k => k.startsWith(date+"|"));

    let c_orphans = [];
    c_items.forEach(cKey => {
      if (shopvSales[cKey]) {
        finalReport[cKey] = mergeRecords(cloverSales[cKey], shopvSales[cKey]);
        s_items = s_items.filter(sKey => sKey !== cKey); 
      } else {
        c_orphans.push(cKey);
      }
    });

    let s_orphans = [...s_items];
    c_orphans.forEach(cKey => {
      let cData = cloverSales[cKey];
      let bestMatchKey = null;

      for (let sKey of s_orphans) {
        let sData = shopvSales[sKey];
        if (isSmartMatch_v24(cData.originalName, sData.originalName)) {
          bestMatchKey = sKey;
          break;
        }
      }

      if (bestMatchKey) {
        finalReport[`${date}|[SMART] ${cData.originalName}`] = mergeRecords(cData, shopvSales[bestMatchKey]);
        s_orphans = s_orphans.filter(k => k !== bestMatchKey); 
      } else {
        finalReport[cKey] = mergeRecords(cData, null);
      }
    });

    s_orphans.forEach(sKey => {
      finalReport[sKey] = mergeRecords(null, shopvSales[sKey]);
    });
  });

  // 4. CONSTRUCCIÓN DEL REPORTE FINAL
  const reportBody = [];
  const debugData = [["FECHA", "STATUS", "PRODUCTO", "QTY_CLO", "QTY_SHP", "DIFF_Q", "NET_CLO", "NET_SHP"]];

  Object.keys(finalReport).forEach(key => {
    const row = finalReport[key];
    const diffQty = row.c_qty - row.s_qty;
    const diffNet = row.c_net - row.s_net;
    const date = key.split("|")[0];
    const isCustom = row.name.toLowerCase().includes("custom item") || row.name.toLowerCase().includes("custom amount");
    
    let status = "MATCH ✅";
    if (Math.abs(diffQty) > 0.01) { status = "QTY_MISMATCH ⚠️"; mismatches++; }
    if (Math.abs(diffNet) > 0.10 && status === "MATCH ✅") { status = "NET_MISMATCH 💰"; mismatches++; }
    
    if (row.c_qty > 0 && row.s_qty === 0) { status = "MISSING_IN_SHOPV ❌"; mismatches++; }
    if (row.s_qty > 0 && row.c_qty === 0) { 
      status = isCustom ? "CUSTOM_ITEM (EXPECTED) 👻" : "MISSING_IN_CLOVER ❌"; 
      if (!isCustom) mismatches++; 
    }

    if (!isCustom) totalDiffNet += diffNet;
    
    reportBody.push([date, row.name, row.c_qty, row.s_qty, diffQty, row.c_net, row.s_net, diffNet, status]);
    debugData.push([date, status, row.name, row.c_qty, row.s_qty, diffQty, row.c_net, row.s_net]);
  });

  // 5. ESCRITURA EN HOJAS
  shDebug.clear().getRange(1, 1, debugData.length, 8).setValues(debugData);
  shReport.clear();
  
  const summary = [
    ["RESUMEN EJECUTIVO (Trimestre 2026)", "", "Dif. Neta Total:", totalDiffNet.toFixed(2), "Alertas Reales:", mismatches, "", "", ""],
    ["FECHA", "PRODUCTO", "QTY_CLO", "QTY_SHP", "DIFF_Q", "NET_CLO", "NET_SHP", "DIFF_N", "STATUS"]
  ];
  shReport.getRange(1, 1, 2, 9).setValues(summary);
  if (reportBody.length > 0) shReport.getRange(3, 1, reportBody.length, 9).setValues(reportBody);
  shReport.getRange(2, 1, 1, 9).setBackground("#444444").setFontColor("white").setFontWeight("bold");
  shReport.autoResizeColumns(1, 9);
  console.log("✅ Conciliación terminada.");
}

function aggregateData_v24(rows, map, label) {
  const result = {};
  const isClover = (label === "CLOVER");
  const seenRows = new Set(); 

  for (let i = 1; i < rows.length; i++) {
    const rowHash = rows[i].join("|||");
    if (seenRows.has(rowHash)) continue; 
    seenRows.add(rowHash);

    const date = forceYMD_v24(rows[i][map.date]);
    if (date && date >= AUDIT_CONFIG.START_DATE && date <= AUDIT_CONFIG.END_DATE) {
      
      let name = String(rows[i][map.name] || "");
      // FIX: Protección contra variante nula
      let variant = isClover ? "" : (rows[i][map.variant] ? String(rows[i][map.variant]) : "");
      if (variant.toLowerCase().includes("default title")) variant = "";

      const cleanString = (str) => str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, " ");
      const uniqueAdn = [...new Set(cleanString(name + " " + variant).split(/\s+/).filter(w => w.length > 0))].sort().join("");

      const key = `${date}|${uniqueAdn}`;
      if (!result[key]) {
        result[key] = {qty: 0, net: 0, originalName: (name + " " + variant).trim(), adn: uniqueAdn};
      }
      
      const cleanNum = (val) => Number(String(val || 0).replace(/[^0-9.\-]/g, '')) || 0;
      result[key].qty += cleanNum(rows[i][map.qty]);
      result[key].net += cleanNum(rows[i][map.net]);
    }
  }
  return result;
}

function isSmartMatch_v24(nameC, nameS) {
  const cleanStr = (str) => str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9 ]/g, " ");
  const arrC = cleanStr(nameC).split(/\s+/).filter(w => w.length > 1 && !(!isNaN(w) && w.length < 3));
  const arrS = cleanStr(nameS).split(/\s+/).filter(w => w.length > 1 && !(!isNaN(w) && w.length < 3));
  if (arrC.length === 0 || arrS.length === 0) return false;
  let shared = 0;
  arrC.forEach(w => { if (arrS.includes(w)) shared++; });
  const minLen = Math.min(arrC.length, arrS.length);
  if (minLen > 0 && (shared / minLen) >= 0.75) return true;
  return false;
}

function mergeRecords(cData, sData) {
  return {
    c_qty: cData ? cData.qty : 0,
    s_qty: sData ? sData.qty : 0,
    c_net: cData ? cData.net : 0,
    s_net: sData ? sData.net : 0,
    name: cData ? cData.originalName : (sData ? sData.originalName : "Unknown")
  };
}

function forceYMD_v24(v) {
  if (!v) return null;
  if (v instanceof Date) return Utilities.formatDate(v, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const s = String(v).trim();
  const mISO = s.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (mISO) return mISO[0];
  const mUS = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (mUS) return `${mUS[3]}-${mUS[1].padStart(2,'0')}-${mUS[2].padStart(2,'0')}`;
  return null;
}
