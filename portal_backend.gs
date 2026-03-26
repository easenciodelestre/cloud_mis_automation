/**
 * =========================================================================
 * 🏛️ CLOUD MIS: MIS OPERATOR PORTAL (BACKEND)
 * =========================================================================
 * Arquitectura: Google Apps Script Web App (doGet)
 * Patrón: Server-Side Rendering & Poka-yoke Validation
 */

// 1. DESPLIEGUE DE LA INTERFAZ Y API ROUTER
function doGet(e) {
  // 🚨 INTERCEPTOR AUTO-FIX: Si la URL trae "?action=autofix", ejecutamos el rescate
  if (e && e.parameter && e.parameter.action === 'autofix') {
    // Disparamos el motor de AutoFix
    executeAutoFixProtocol();
    
    // Devolvemos una mini-pantalla con instrucción manual (bypass de seguridad del navegador)
    return HtmlService.createHtmlOutput(`
      <div style="font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif; text-align: center; padding-top: 15vh; background: #111827; color: white; height: 100vh; margin: 0;">
        <div style="max-width: 500px; margin: 0 auto; background: #1f2937; padding: 40px; border-radius: 12px; border: 1px solid #374151; box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5);">
          <div style="font-size: 48px; margin-bottom: 20px;">🛠️</div>
          <h1 style="color: #3b82f6; font-size: 24px; margin-bottom: 10px;">Protocolo Auto-Fix Ejecutado</h1>
          <p style="color: #d1d5db; line-height: 1.5; margin-bottom: 30px;">El motor de rescate está analizando la Dropzone en segundo plano.</p>
          
          <div style="background: #064e3b; border: 1px solid #059669; padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <p style="color: #34d399; font-weight: bold; margin: 0;">✅ Ya puedes cerrar esta pestaña de forma segura.</p>
          </div>
          
          <p style="color: #9ca3af; font-size: 14px;">Revisa tu Google Chat para leer el reporte detallado.</p>
        </div>
      </div>
    `).setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
  }

  // Comportamiento Normal (Renderiza el portal del operador)
  let html = HtmlService.createTemplateFromFile('Index');
  html.userEmail = Session.getActiveUser().getEmail() || "Operador Desconocido";
  
  return html.evaluate()
      .setTitle('Cloud MIS - Operator Portal')
      .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
      .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

// 2. GENERADOR DE DEEP LINKS DINÁMICOS (ACTUALIZADO CON RUTAS EXACTAS)
function getDynamicLinks() {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);
  
  // A. Formato Clover (Epoch Time en Milisegundos)
  const startOfDay = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 0, 0, 0).getTime();
  const endOfDay = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999).getTime();
  
  // B. Formato Shopventory (YYYY-MM-DD)
  const yyyy = yesterday.getFullYear();
  const mm = String(yesterday.getMonth() + 1).padStart(2, '0');
  const dd = String(yesterday.getDate()).padStart(2, '0');
  const shopDate = `${yyyy}-${mm}-${dd}`;

  // C. Formato Homebase (MM/DD/YYYY codificado para URL)
  const hbMm = String(yesterday.getMonth() + 1).padStart(2, '0');
  const hbDd = String(yesterday.getDate()).padStart(2, '0');
  const hbYyyy = yesterday.getFullYear();
  const hbEndDate = `${hbMm}%2F${hbDd}%2F${hbYyyy}`;

  return {
    dateDisplay: `${dd}/${mm}/${yyyy}`,
    isFriday: today.getDay() === 5,
    
    // Diccionario de URLs exactas por Sistema
    links: {
      clover: [
        { name: 'Overview', url: `https://www.clover.com/reporting/sales-overview?comparison=SAME_PERIOD_LAST_WEEK&endTimestamp=${endOfDay}&startTimestamp=${startOfDay}` },
        { name: 'Sales', url: `https://www.clover.com/reporting/m/365A3C7X1AKT1/sales-report?startTimestamp=${startOfDay}&endTimestamp=${endOfDay}` },
        { name: 'Employees', url: `https://www.clover.com/reporting/employee-sales?startTimestamp=${startOfDay}&endTimestamp=${endOfDay}` },
        { name: 'Tenders', url: `https://www.clover.com/reporting/tender-types?startTimestamp=${startOfDay}&endTimestamp=${endOfDay}` },
        { name: 'Discounts', url: `https://www.clover.com/reporting/discounts?startTimestamp=${startOfDay}&endTimestamp=${endOfDay}` },
        { name: 'Items', url: `https://www.clover.com/reporting/items?startTimestamp=${startOfDay}&endTimestamp=${endOfDay}` },
        { name: 'Order Line Items', url: `https://www.clover.com/orders/m/365A3C7X1AKT1/line-items/?startTimestamp=${startOfDay}&endTimestamp=${endOfDay}` }
      ],
      shopventory: [
        { name: 'Product Sales', url: `https://cloud.thrivemetrics.com/reports/sales-products/${shopDate}/00:00/${shopDate}/23:59` },
        { name: 'Vendor Sales', url: `https://cloud.thrivemetrics.com/reports/vendor-sales/${shopDate}/${shopDate}/locations/all/integrations/all` },
        { name: 'Inventory', url: `https://cloud.thrivemetrics.com/reports/inventory/${shopDate}/23:59` },
        { name: 'Inventory Change', url: `https://cloud.thrivemetrics.com/reports/inventory-change/${shopDate}/00:00/${shopDate}/23:59` },
        { name: 'Purchase Orders', url: `https://cloud.thrivemetrics.com/po`, note: '(Solo si hay PO nuevos)' },
        { name: 'Dead Inventory', url: `https://cloud.thrivemetrics.com/reports/dead-inventory/60/days/location/` },
        { name: 'Low Inventory', url: `https://cloud.thrivemetrics.com/reports/low-inventory` }
      ],
      rewardup: [
        { name: 'Points', url: `https://store.rewardup.io/rewards-program/activity?tab=points` },
        { name: 'Orders', url: `https://store.rewardup.io/rewards-program/activity?tab=orders` },
        { name: 'Redeemed', url: `https://store.rewardup.io/rewards-program/activity?tab=redeemed_rewards` },
        { name: 'VIP', url: `https://store.rewardup.io/rewards-program/activity?tab=vip` },
        { name: 'Program Stats', url: `https://store.rewardup.io/rewards-program/reports?tab=program_stats` },
        { name: 'Sign Up Source', url: `https://store.rewardup.io/rewards-program/reports?tab=signup_source` },
        { name: 'User Activity', url: `https://store.rewardup.io/rewards-program/reports?tab=user_activity` }
      ],
      homebase: [
        { name: 'Timesheets (Pay Period)', url: `https://app.joinhomebase.com/timesheets/pay_period_review?endDate=${hbEndDate}&filter=all&groupBy=employee` }
      ]
    }
  };
}

// 3. EL GUARDIÁN: ESCÁNER POKA-YOKE DE LA DROPZONE
function scanDropzoneGuardian() {
  const expectedFiles = [
    'rewardup_order', 'rewardup_vip', 'rewardup_user_activity', 'rewardup_redeemed_rewards', 
    'rewardup_signup_source', 'rewardup_point', 'rewardup_program_stats',
    'shopventory_inventory_change', 'shopventory_products_sales', 'shopventory_low_inventory', 
    'shopventory_dead_inventory', 'shopventory_purchase_orders', 'shopventory_inventory', 'shopventory_vendor_sales',
    'clover_employees', 'clover_items', 'clover_sales', 'clover_overview', 'clover_tenders', 'clover_orders_line_items'
  ];

  if (new Date().getDay() === 5) {
    expectedFiles.push('homebase_timesheets');
  }

  const dropzoneId = (typeof ENV !== 'undefined') ? ENV.DROPZONE_FOLDER_ID : 'TU_ID_DE_DROPZONE_AQUI'; 
  const folder = DriveApp.getFolderById(dropzoneId);
  const files = folder.getFiles();
  
  let foundFilenames = [];
  while (files.hasNext()) {
    foundFilenames.push(files.next().getName().toLowerCase());
  }

  let scanResults = { totalExpected: expectedFiles.length, foundCount: 0, missingFiles: [], foundFiles: [], isReady: false };

  expectedFiles.forEach(expected => {
    if (foundFilenames.some(fname => fname.includes(expected))) {
      scanResults.foundCount++;
      scanResults.foundFiles.push(expected);
    } else {
      scanResults.missingFiles.push(expected);
    }
  });

  scanResults.isReady = (scanResults.foundCount === scanResults.totalExpected);
  return scanResults;
}

// 4. AUTO-DISPATCHER: ENRUTAMIENTO INTELIGENTE A /01_raw
function executeAutoDispatcher() {
  try {
    const dropzoneId = (typeof ENV !== 'undefined') ? ENV.DROPZONE_FOLDER_ID : 'TU_ID_DE_DROPZONE';
    const rawParentId = (typeof ENV !== 'undefined') ? ENV.RAW_PARENT_FOLDER_ID : 'TU_ID_DE_RAW';
    
    const dropzone = DriveApp.getFolderById(dropzoneId);
    const rawParentFolder = DriveApp.getFolderById(rawParentId);
    
    const routingMap = [
      'rewardup_order', 'rewardup_vip', 'rewardup_user_activity', 'rewardup_redeemed_rewards', 
      'rewardup_signup_source', 'rewardup_point', 'rewardup_program_stats',
      'shopventory_inventory_change', 'shopventory_products_sales', 'shopventory_low_inventory', 
      'shopventory_dead_inventory', 'shopventory_purchase_orders', 'shopventory_inventory', 'shopventory_vendor_sales',
      'clover_employees', 'clover_items', 'clover_sales', 'clover_overview', 'clover_tenders', 'clover_orders_line_items',
      'homebase_timesheets'
    ];

    const files = dropzone.getFiles();
    let results = { processed: 0, errors: [], success: false };

    while (files.hasNext()) {
      let file = files.next();
      let fileName = file.getName().toLowerCase();
      
      try {
        let targetCategory = routingMap.find(cat => fileName.includes(cat));
        if (targetCategory) {
          let subFolders = rawParentFolder.getFoldersByName(targetCategory);
          let targetFolder = subFolders.hasNext() ? subFolders.next() : rawParentFolder.createFolder(targetCategory);
          file.moveTo(targetFolder);
          file.setName(`${targetCategory}_raw.csv`);
          results.processed++;
        }
      } catch (fileError) {
        results.errors.push(`Fallo al mover ${fileName}: ${fileError.message}`);
      }
    }
    
    results.success = (results.errors.length === 0 && results.processed > 0);
    return results;
  } catch (globalError) {
    return { success: false, error: globalError.message };
  }
}

// 🛠️ HELPER SCRIPT: GENERADOR DE DUMMY FILES
function generateDummyTestFiles() {
  const dropzoneId = (typeof ENV !== 'undefined') ? ENV.DROPZONE_FOLDER_ID : 'TU_ID_DE_DROPZONE';
  const dropzone = DriveApp.getFolderById(dropzoneId);
  const filesToGenerate = [
    'rewardup_order', 'rewardup_vip', 'rewardup_user_activity', 'rewardup_redeemed_rewards', 
    'rewardup_signup_source', 'rewardup_point', 'rewardup_program_stats',
    'shopventory_inventory_change', 'shopventory_products_sales', 'shopventory_low_inventory', 
    'shopventory_dead_inventory', 'shopventory_purchase_orders', 'shopventory_inventory', 'shopventory_vendor_sales',
    'clover_employees', 'clover_items', 'clover_sales', 'clover_overview', 'clover_tenders', 'clover_orders_line_items'
  ];
  let count = 0;
  filesToGenerate.forEach(name => {
    dropzone.createFile(`master_${name}_test.csv`, "", MimeType.CSV);
    count++;
  });
  console.log(`✅ Prueba de estrés lista: Se generaron ${count} archivos vacíos.`);
}
