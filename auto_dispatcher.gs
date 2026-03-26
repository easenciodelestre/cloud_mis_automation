/*******************************************************
 * AUTO-DISPATCHER (El Buzón Inteligente)
 * -----------------------------------------------------
 * Lee todos los archivos de la Dropzone.
 * Evalúa su nombre contra reglas estrictas (Regex/Keywords).
 * 1. Crea una copia en la subcarpeta /01_raw correspondiente para Ingesta.
 * 2. Mueve el archivo original a la carpeta corporativa de la empresa.
 *******************************************************/

function run_auto_dispatcher() {
  console.log("--- INICIANDO EL BUZÓN INTELIGENTE (DISPATCHER) ---");

  if (!ENV || !ENV.DROPZONE_FOLDER_ID) {
    throw new Error("❌ Falla crítica: ENV.DROPZONE_FOLDER_ID no está definido en env.gs");
  }

  const dropzone = DriveApp.getFolderById(ENV.DROPZONE_FOLDER_ID);
  const rawParent = DriveApp.getFolderById(ENV.RAW_PARENT_FOLDER_ID);
  const files = dropzone.getFiles();
  let processedCount = 0;

  // REGLAS DE ENRUTAMIENTO (El orden importa para evitar colisiones)
  const ROUTING_RULES = [
    // ==========================================
    // CLOVER (Archive: 1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy)
    // ==========================================
    { match: /discounts/i,            rawSub: 'clover_discounts',         archiveId: '1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy' },
    { match: /employee sales/i,       rawSub: 'clover_employees',         archiveId: '1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy' },
    { match: /revenue item/i,         rawSub: 'clover_items',             archiveId: '1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy' },
    { match: /tender and card/i,      rawSub: 'clover_tenders',           archiveId: '1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy' },
    { match: /sales overview/i,       rawSub: 'clover_overview',          archiveId: '1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy' },
    { match: /LineItemsExport/i,      rawSub: 'clover_orders_line_items', archiveId: '1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy' }, // NUEVO: Line Items Transaccionales
    { match: /sales report/i,         rawSub: 'clover_sales',             archiveId: '1k14BLgOnNb_CKYYfN_gkaHehe-SkWugy' }, // Va de último

    // ==========================================
    // SHOPVENTORY (Archive: 1XUj8t312LnZvzokyJgZGmzcaZh0uQ7bL)
    // ==========================================
    { match: /dead_inventory/i,       rawSub: 'shopventory_dead_inventory',   archiveId: '1XUj8t312LnZvzokyJgZGmzcaZh0uQ7bL' },
    { match: /inventory_change/i,     rawSub: 'shopventory_inventory_change', archiveId: '1XUj8t312LnZvzokyJgZGmzcaZh0uQ7bL' },
    { match: /low_inventory/i,        rawSub: 'shopventory_low_inventory',    archiveId: '1XUj8t312LnZvzokyJgZGmzcaZh0uQ7bL' },
    { match: /product_sales/i,        rawSub: 'shopventory_product_sales',    archiveId: '1XUj8t312LnZvzokyJgZGmzcaZh0uQ7bL' },
    { match: /vendor_sales/i,         rawSub: 'shopventory_vendors_sales',    archiveId: '1XUj8t312LnZvzokyJgZGmzcaZh0uQ7bL' },
    { match: /inventory/i,            rawSub: 'shopventory_inventory',        archiveId: '1XUj8t312LnZvzokyJgZGmzcaZh0uQ7bL' },

    // ==========================================
    // REWARDUP (Archive: 1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD)
    // ==========================================
    { match: /order-activity/i,       rawSub: 'rewardup_order',            archiveId: '1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD' },
    { match: /point-activity/i,       rawSub: 'rewardup_point',            archiveId: '1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD' },
    { match: /program-stats/i,        rawSub: 'rewardup_program_stats',    archiveId: '1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD' },
    { match: /redeemed-rewards/i,     rawSub: 'rewardup_redeemed_rewards', archiveId: '1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD' },
    { match: /signup-source/i,        rawSub: 'rewardup_signup_source',    archiveId: '1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD' },
    { match: /user-activity/i,        rawSub: 'rewardup_user_activity',    archiveId: '1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD' },
    { match: /vip-activity/i,         rawSub: 'rewardup_vip',              archiveId: '1n9-_aGx8FitM7YO7cefnTQ5QBMeUc-VD' },

    // ==========================================
    // HOMEBASE (Archive: 1JE44bE1Wf8NS2tBMsUBq09haSp_G8Dli)
    // ==========================================
    { match: /timesheets/i,           rawSub: 'homebase_timesheets',       archiveId: '1JE44bE1Wf8NS2tBMsUBq09haSp_G8Dli' }
  ];

  if (!files.hasNext()) {
    console.log("📭 El buzón está vacío. Nada que procesar.");
    return;
  }

  while (files.hasNext()) {
    const file = files.next();
    const name = file.getName();
    let routed = false;

    // Buscar si el archivo coincide con alguna regla
    for (const rule of ROUTING_RULES) {
      if (rule.match.test(name)) {
        console.log(`  📦 Archivo detectado: ${name}`);
        console.log(`     ↳ Ruta destino: [${rule.rawSub}]`);

        try {
          const targetRawSub = getOrCreateChildFolder_(rawParent, rule.rawSub);
          file.makeCopy(name, targetRawSub);
          const archiveFolder = DriveApp.getFolderById(rule.archiveId);
          file.moveTo(archiveFolder);
          console.log(`     ✅ Enviado a Ingesta y Archivado con éxito.`);
          routed = true;
          processedCount++;
          break; 
        } catch (e) {
          console.log(`     ❌ ERROR al enrutar ${name}: ${e.message}`);
          routed = true; 
          break;
        }
      }
    }

    if (!routed) {
      console.log(`  ⚠️ ARCHIVO HUÉRFANO: ${name}`);
    }
  }
  console.log(`\n📬 Dispatcher finalizado. ${processedCount} archivos enrutados correctamente.`);
}

function getOrCreateChildFolder_(parentFolder, name) {
  const it = parentFolder.getFoldersByName(name);
  return it.hasNext() ? it.next() : parentFolder.createFolder(name);
}
