/**
 * INGEST_CLOVER (Versión Arquitecto v6.1 - Integración Line Items)
 * - Conectado a ENV y utils.gs para enrutamiento exacto.
 * - Regex mejorado para capturar fechas transaccionales (ej. 17-Mar-2026).
 */

function ingest_all_clover() {
  console.log("--- INICIANDO INGEST CLOVER (7 REPORTES) v6.1 ---");
  
  const SUBS = [
    { sub: 'clover_items', masterId: ENV.CLOVER_MASTER_IDS.clover_items },
    { sub: 'clover_sales', masterId: ENV.CLOVER_MASTER_IDS.clover_sales },
    { sub: 'clover_employees', masterId: ENV.CLOVER_MASTER_IDS.clover_employees },
    { sub: 'clover_overview', masterId: ENV.CLOVER_MASTER_IDS.clover_overview },
    { sub: 'clover_tenders', masterId: ENV.CLOVER_MASTER_IDS.clover_tenders },
    { sub: 'clover_discounts', masterId: ENV.CLOVER_MASTER_IDS.clover_discounts },
    { sub: 'clover_orders_line_items', masterId: ENV.CLOVER_MASTER_IDS.clover_orders_line_items } // NUEVO
  ];

  SUBS.forEach(src => {
    try {
      ingestCloverOne_(src);
    } catch (err) {
      console.error(`Error crítico en fuente ${src.sub}: ${err.message}`);
    }
  });

  if (SpreadsheetApp.getActive()) SpreadsheetApp.getActive().toast('Ingesta Clover completada ✅');
}

function ingestCloverOne_(src) {
  const rawFolder = getRawFolderByRawSub_(src.sub);
  const processedFolder = getProcessedFolder_();
  
  if (!rawFolder) {
    console.warn(`ADVERTENCIA: No se encontró la carpeta: ${src.sub}. Verifica que esté dentro de la carpeta RAW.`);
    return;
  }

  const ss = SpreadsheetApp.openById(src.masterId);
  const shRaw = getOrCreateSheet(ss, `${src.sub}_raw`); 
  const auditSh = getOrCreateIngestAudit_(ss);          

  const files = rawFolder.getFiles();
  let count = 0;

  while (files.hasNext()) {
    const f = files.next();
    const mime = (f.getMimeType() || '').toLowerCase();
    
    if (!mime.includes('text') && !mime.includes('csv') && !mime.includes('tsv')) continue;

    const data2D = parseCloverFileLocal_(f);
    if (!data2D.length) {
      f.moveTo(processedFolder);
      continue;
    }

    const md5 = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, f.getBlob().getBytes())
                .map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');

    if (auditHasMd5_(auditSh, src.sub, md5)) { 
      console.log(`⏩ Salto por duplicado (MD5): ${f.getName()}`);
      f.moveTo(processedFolder);
      continue;
    }

    const reportDate = extractDateFromCloverText_(data2D);

    const metaRows = [
      [`SOURCE_FILE=${f.getName()}`],
      [`REPORT_DATE=${reportDate || ""}`]
    ];

    const rectMeta = ensureRectangular(metaRows); 
    const rectData = ensureRectangular(data2D);   

    const startRow = shRaw.getLastRow() + 1;
    shRaw.getRange(startRow, 1, rectMeta.length, rectMeta[0].length).setValues(rectMeta);
    shRaw.getRange(startRow + rectMeta.length, 1, rectData.length, rectData[0].length).setValues(rectData);

    auditAppend_(auditSh, src.sub, reportDate, f, md5, data2D.length, 'INGEST_OK'); 
    f.moveTo(processedFolder);
    count++;
  }
  
  if (count > 0) {
    console.log(`✅ Total archivos procesados en ${src.sub}: ${count}`);
  } else {
    console.log(`⏭️ No hay archivos nuevos en ${src.sub}`);
  }
}

function parseCloverFileLocal_(file) {
  const content = file.getBlob().getDataAsString();
  try { return Utilities.parseCsv(content); } catch(e) {
    return content.split('\n').map(l => l.split(','));
  }
}

function extractDateFromCloverText_(data2D) {
  const max = Math.min(35, data2D.length);
  for (let r = 0; r < max; r++) {
    const line = (data2D[r] || []).join(' ');
    
    // Formato 1: Mar 17, 2026
    let m = line.match(/([A-Za-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?,\s*(\d{4})/i);
    if (m) {
      const months = {jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12};
      const mm = months[m[1].toLowerCase().substring(0,3)];
      return `${m[3]}-${String(mm).padStart(2,'0')}-${String(m[2]).padStart(2,'0')}`;
    }
    
    // Formato 2: 17-Mar-2026 (Nuevo soporte para Line Items)
    m = line.match(/(\d{1,2})-([A-Za-z]{3})-(\d{4})/i);
    if (m) {
      const months = {jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12};
      const mm = months[m[2].toLowerCase().substring(0,3)];
      return `${m[3]}-${String(mm).padStart(2,'0')}-${String(m[1]).padStart(2,'0')}`;
    }
  }
  return null;
}
