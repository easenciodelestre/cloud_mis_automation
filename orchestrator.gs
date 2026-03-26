/**
 * =========================================================================
 * ⚙️ FASE 2: THE PIPELINE ORCHESTRATOR
 * =========================================================================
 * Arquitectura: Separación de Responsabilidades. El SQL vive nativo en BQ.
 */

function executePipelineOrchestrator() {
  console.log("🚀 Iniciando Pipeline ELT Maestro...");

  // Envolvemos TODO el proceso en el try/catch para garantizar alertas globales
  try {

    // 1. AUTO-DISPATCHER (Primer Intento)
    console.log("Paso 1: Ejecutando Auto-Dispatcher...");
    let dispatchResults = executeAutoDispatcher();

    // 2. LÓGICA DE REINTENTO
    if (!dispatchResults.success) {
      console.warn("⚠️ Dispatcher reportó fallos. Esperando 5 segundos para auto-reintento...");
      Utilities.sleep(5000); 
      let retryResults = executeAutoDispatcher();
      
      if (!retryResults.success) {
        throw new Error("Falla Crítica: El Dispatcher no pudo limpiar la Dropzone tras 2 intentos.");
      }
    }
    console.log("✅ Dropzone limpia. Archivos en /01_raw.");

    // 3. INGESTA MASIVA
    console.log("Paso 2: Iniciando Ingesta a Sheets...");
    ingest_all_clover();
    ingest_shopventory_all();
    ingest_rewardup_all();
    
    if (new Date().getDay() === 5) {
      ingest_homebase_all();
    }
    console.log("✅ Ingesta Completada.");

    // 4. NORMALIZACIÓN MASIVA
    console.log("Paso 3: Iniciando Normalización...");
    normalize_all_clover();
    normalize_shopventory_all();
    normalize_rewardup_all();
    
    if (new Date().getDay() === 5) {
      normalize_homebase_all();
    }
    console.log("✅ Normalización Completada.");

    // 5. BIGQUERY PUSH API (Trigger Inmutable)
    console.log("Paso 4: Disparando Procedimientos Almacenados en BigQuery...");
    triggerBigQueryVaultUpdates();
    console.log("✅ Bóvedas actualizadas matemáticamente.");

    console.log("🎉 Pipeline 100% Completado con Éxito.");
    
    // 📢 FASE 3: Enviar Alerta de Éxito a la Torre de Control
    sendSuccessAlert();
    
    return { success: true, message: "Pipeline ejecutado correctamente." };

  } catch (error) {
    console.error("❌ Error durante el Pipeline: " + error.message);
    
    // 📢 FASE 3: Enviar Alerta Crítica a la Torre de Control
    sendErrorAlert(error.message);
    
    throw error; 
  }
}

/**
 * Llama a los Stored Procedures en BigQuery de forma síncrona.
 * Cero hardcoding de SQL. Máxima seguridad.
 */
function triggerBigQueryVaultUpdates() {
  // Asegurar lectura desde el entorno centralizado
  const projectId = (typeof ENV !== 'undefined' && ENV.BQ_PROJECT_ID) ? ENV.BQ_PROJECT_ID : 'TU_ID_PROYECTO_GCP';
  const datasetId = (typeof ENV !== 'undefined' && ENV.BQ_DATASET_ID) ? ENV.BQ_DATASET_ID : 'mis_data';
  
  // Comandos inmutables 'CALL'
  const procedures = [
    `CALL \`${projectId}.${datasetId}.sp_merge_clover\`();`,
    `CALL \`${projectId}.${datasetId}.sp_merge_shopventory\`();`,
    `CALL \`${projectId}.${datasetId}.sp_merge_rewardup\`();`
  ];

  if (new Date().getDay() === 5) {
    procedures.push(`CALL \`${projectId}.${datasetId}.sp_merge_homebase\`();`);
  }

  // Ejecución vía API Avanzada de BigQuery
  procedures.forEach(queryStr => {
    let request = {
      query: queryStr,
      useLegacySql: false
    };
    
    try {
      // El método query() espera a que el Job termine, garantizando la sincronicidad
      BigQuery.Jobs.query(request, projectId);
      console.log(`✅ Procedimiento BQ ejecutado: ${queryStr}`);
    } catch (e) {
      throw new Error(`Error en la base de datos al ejecutar ${queryStr}: ${e.message}`);
    }
  });
}
