autofix_engine.gs/**
 * =========================================================================
 * 🛠️ THE AUTO-FIX ENGINE (BULLETPROOF PROTOCOL)
 * =========================================================================
 * Límite estricto: 2 Intentos. Nunca elimina archivos, solo aísla (Cuarentena).
 */

function executeAutoFixProtocol() {
  console.log("Iniciando Protocolo Auto-Fix...");
  
  let summary = {
    problem: "Se detectó una falla en el Pipeline ELT. Diagnóstico inicial: Posible atasco en Dropzone o Timeout de Google Drive API.",
    resolved: false,
    solution: "",
    hypothesis: "",
    attempts: 0
  };

  try {
    // -------------------------------------------------------------
    // INTENTO 1: Re-Enrutamiento Forzado (Soft Reset)
    // -------------------------------------------------------------
    summary.attempts++;
    console.log(`Intento ${summary.attempts}: Ejecutando Dispatcher Forzado...`);
    
    Utilities.sleep(3000); // Pausa para refrescar cuotas de API
    let dispatch1 = executeAutoDispatcher();
    
    // Verificamos si la Dropzone quedó limpia
    let dropzone = DriveApp.getFolderById(ENV.DROPZONE_FOLDER_ID);
    if (!dropzone.getFiles().hasNext()) {
      summary.resolved = true;
      summary.solution = "Se aplicó un Soft-Reset a las llamadas de red y se forzó el Auto-Dispatcher. Los archivos desincronizados fueron enrutados a /01_raw. Pipeline desbloqueado.";
      sendAutoFixSummary(summary);
      return;
    }

    // -------------------------------------------------------------
    // INTENTO 2: Protocolo de Cuarentena (Hard Reset Seguro)
    // -------------------------------------------------------------
    summary.attempts++;
    console.log(`Intento ${summary.attempts}: Aplicando Protocolo de Cuarentena...`);
    
    summary.problem += " El re-enrutamiento falló. Hay archivos huérfanos o corruptos atascando la Dropzone.";
    
    // Crear o localizar carpeta de Cuarentena en la raíz del proyecto
    let parentRaw = DriveApp.getFolderById(ENV.RAW_PARENT_FOLDER_ID);
    let folders = parentRaw.getFoldersByName("999_QUARANTINE");
    let quarantineFolder = folders.hasNext() ? folders.next() : parentRaw.createFolder("999_QUARANTINE");
    
    // Aislar Archivos Problemáticos
    let files = dropzone.getFiles();
    let isolatedCount = 0;
    
    while(files.hasNext()){
      let badFile = files.next();
      badFile.moveTo(quarantineFolder);
      isolatedCount++;
    }

    // Verificación Final de Seguridad
    if (!dropzone.getFiles().hasNext()) {
      summary.resolved = true;
      summary.solution = `Se aplicó la Solución Permanente: ${isolatedCount} archivo(s) no reconocido(s) o bloqueado(s) fueron movidos a la carpeta segura [999_QUARANTINE] dentro de /01_raw/. La Dropzone está limpia y lista para operar mañana.`;
      sendAutoFixSummary(summary);
      return;
    }

    // -------------------------------------------------------------
    // FALLO TOTAL: ESCALAR AL ARQUITECTO
    // -------------------------------------------------------------
    throw new Error("Archivos inmunes a la manipulación en Drive.");

  } catch (error) {
    summary.resolved = false;
    summary.hypothesis = `1. Archivos bloqueados por permisos estrictos de propietario ajeno al dominio.<br>2. Google Drive API Rate Limit excedido permanentemente por hoy.<br>3. Error Crítico en Apps Script: ${error.message}`;
    sendAutoFixSummary(summary);
  }
}
