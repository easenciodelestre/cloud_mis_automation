/*** env.gs ***/
/* ==== CONFIGURACIÓN DEL PROYECTO ==== */
const ENV = {
  // === BIGQUERY (ORQUESTADOR FASE 2) ===
  BQ_PROJECT_ID: 'fiery-catwalk-486715-g3',
  BQ_DATASET_ID: 'mis_data',

  // === TORRE DE CONTROL (GOOGLE CHAT WEBHOOK) ===
  CHAT_WEBHOOK_URL: 'https://chat.googleapis.com/v1/spaces/AAQAlJXXk7w/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=KzmIIx9csihBgPhjOYPTlvd2o_cy7G1Bh2qbr9XB3FE',

  // === AUTO-FIX & QUARANTINE ===
  WEB_APP_URL: 'https://script.google.com/a/macros/cloudsmokespr.com/s/AKfycbxWInxSpc_0hcpHQvX6TBgPWMfEDgw6QrkSwIh-gqRRqZyg6ZhLZMbKSi030goKga3z/exec', 
  QUARANTINE_FOLDER_ID: '', // Déjalo vacío. El script creará la carpeta automáticamente si la necesita.

  // === CARPETAS RAÍZ MIS ===
  DROPZONE_FOLDER_ID: '19AG35ELKCMfFhp018FED21O5H0ft2nGX', // 📥 00_dispatcher (Buzón MIS)
  RAW_PARENT_FOLDER_ID: '1m3281tK99DqDNe9PXgGmFNgAwpQEpLVb',   // 01_raw
  MASTER_PARENT_FOLDER_ID: '18IBs0jr6KbibFGWHIgEAbQCG44LR4mdB', // 02_master_sheets
  PROCESSED_FOLDER_ID: '1o0RYjSWr6cWoLB8u3O9bX-Yetpm5L0r5', // 99_processed

  // === SUBCARPETAS VÁLIDAS DENTRO DE /01_raw/ ===
  RAW_SUBS: [
    // Clover
    'clover_items',
    'clover_sales',
    'clover_employees',
    'clover_overview',
    'clover_tenders',
    'clover_discounts',
    'clover_orders_line_items', // NUEVO: Line Items Transaccionales

    // Shopventory
    'shopventory_inventory',
    'shopventory_product_sales',
    'shopventory_dead_inventory',
    'shopventory_inventory_change',
    'shopventory_low_inventory',
    'shopventory_vendors_sales',

    // Homebase
    'homebase_timesheets'
  ],

  EMAIL_ON_ERROR: '',

  // === Clover master IDs (por rawSub) ===
  CLOVER_MASTER_IDS: {
    clover_sales:      '132gRXwETwraKDAQS10fOKJ__PljEtDm400ss0Pwicdg',
    clover_discounts:  '1RvkOcV56WHC_ZQJ8YfYctXQUayhZDl9MFeoYyHrHCfw',
    clover_employees:  '1wK1_kHeeNj2I1pmuzgJkIW849WHhYxu3ZisEDaRMDRQ',
    clover_overview:   '1ApTGaTS9yKIaQXIAQ1zPsw07fGimrPFzX9SYDeY148c',
    clover_tenders:    '1ttkC_VnXte6NnuSeN01rdYNmAwa_tMCQcHr_ZHGTS9s',
    clover_items:      '1B_Xp4F3LQF5EEO3jHMCqaSZ8K2_hI0rA59_fkcxoVXg',
    clover_orders_line_items: '1SEUuLXjQ7pzOYFUm81bVh6z6OYHq2HNMNF-4GN_kvz8' // NUEVO
  },

  // === Shopventory master IDs ===
  SHOPV: {
    masters: {
      inventory:        '1UX2-o9RpNjrU3kHSwiLjJx64-bbDborT5ARA77Igaek',
      dead_inventory:   '1LVKLCUuzMykOflp_15O1N_svw3_pwfB6nJq2WCXcjPM',
      inventory_change: '1XkeIzv6jaGrAR6ehrMW1Rq27KEyej0aoK8eBGzGOTLA',
      low_inventory:    '1iuRSjPhg8nTxnbBMNJveZ4_hh1i_y1nLEZvN1mMIOPc',
      product_sales:    '12AvbV6K7yAKi04WQESBnEd_sMWhlqnUVM5pnmOi2QLI',
      vendor_sales:     '1t5brSPDPkxt7cnaB_LLlf78JPS310vm82BLBcy2mQVk',
      purchase_orders:  '1TgbeW2dHne5kcvk1ZUVf0EHdNM-PO3_JIHBOYmkhgrQ'
    }
  }
};

// =========================
// RewardUp (Loyalty)
// =========================
ENV.RU = {
  RAW_ROOT_ID: ENV.RAW_PARENT_FOLDER_ID,

  masters: {
    user_activity:    '1bESasE1D5JskaFwIifoeY47SV0Fn5na_ZbaFN5j8DSQ',
    signup_source:    '1Y0EMiw4jgtcecqyu2CakgbDxLip6G8f6sYVisJlXAvA',
    program_stats:    '1phgX6lvs_00pjT2gG9VTIYzYH9ee20MZyaS9yFkoR0g',
    order:            '1fX0o9jg8pFr7M5ceZTNxYIFfn0nMgqdVwMDtz83zsJQ',
    redeemed_rewards: '14SSB0Is6TLeVS2NADcfVJqlCVNft2ixld03uywRZoGA',
    vip:              '1u4YwV0PSEAhCpk0vCKOKCHCPjDsN4baH65fk0lqMFPc',
    point:            '1U_RGAtBf5_FacK86mP0TzeCcYJbjGAsXEomn3Hjg-7Y'
  },

  rawSubfolders: {
    user_activity:    'rewardup_user_activity',
    signup_source:    'rewardup_signup_source',
    program_stats:    'rewardup_program_stats',
    order:            'rewardup_order',
    redeemed_rewards: 'rewardup_redeemed_rewards',
    vip:              'rewardup_vip',
    point:            'rewardup_point'
  }
};

// =========================
// Homebase (Timesheets)
// =========================
ENV.HB = {
  masters: {
    timesheets: '1Nvcp0hpbiEsVOMAv5n4QCSF0kX77snzJNubz0aUZMw4'
  },
  rawSubfolders: {
    timesheets: 'homebase_timesheets'
  }
};
