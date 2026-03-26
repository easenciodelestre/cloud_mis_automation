/**
 * =========================================================================
 * ⏱️ FASE 3: EL SUPERVISOR AUTOMÁTICO (PRE-10 AM REMINDER)
 * =========================================================================
 * Arquitectura: Time-Driven Trigger que vigila la inactividad del operador.
 */

// 1. EL MOTOR DE REVISIÓN DIARIA
function runDailySupervisor() {
  console.log("Iniciando revisión de rutina del Supervisor (9:30 AM)...");
  
  const dropzoneId = (typeof ENV !== 'undefined' && ENV.DROPZONE_FOLDER_ID) ? ENV.DROPZONE_FOLDER_ID : null;
  if (!dropzoneId) return console.error("Falta el ID de la Dropzone.");

  const folder = DriveApp.getFolderById(dropzoneId);
  const files = folder.getFiles();

  let fileCount = 0;
  while (files.hasNext()) {
    files.next();
    fileCount++;
  }

  // Lógica de Negocio: Si hay archivos, el operador no corrió el pipeline de ayer.
  if (fileCount > 0) {
    console.warn(`Alerta: Se detectaron ${fileCount} archivos sin procesar.`);
    sendSupervisorAlert(fileCount);
  } else {
    // Si está en 0, el operador hizo su trabajo. El bot no molesta.
    console.log("✅ Todo en orden. La Dropzone está limpia. Silenciando bot.");
  }
}

// 2. LA TARJETA DE ALERTA AMARILLA
function sendSupervisorAlert(fileCount) {
  const url = (typeof ENV !== 'undefined' && ENV.CHAT_WEBHOOK_URL) ? ENV.CHAT_WEBHOOK_URL : null;
  const webAppUrl = (typeof ENV !== 'undefined' && ENV.WEB_APP_URL) ? ENV.WEB_APP_URL : null;
  if (!url) return;

  const timestamp = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Puerto_Rico', hour: '2-digit', minute:'2-digit' });

  const payload = {
    "cardsV2": [{
      "cardId": "warning_supervisor",
      "card": {
        "header": {
          "title": "⚠️ ALERTA PREVENTIVA: Retraso Operativo",
          "subtitle": "MIS Control Tower | " + timestamp,
          "imageUrl": "https://cdn-icons-png.flaticon.com/512/845/845013.png", // Icono de Advertencia Amarillo
          "imageType": "CIRCLE"
        },
        "sections": [
          {
            "widgets": [
              {
                "textParagraph": {
                  "text": "<b>Estado:</b> <font color=\"#fbbf24\">Pendiente de Ejecución</font><br><br>Se han detectado <b>" + fileCount + " archivo(s)</b> esperando en la Dropzone. El operador de turno aún no ha iniciado el Pipeline ELT para la data de ayer.<br><br><i>📊 Los tableros de Looker Studio muestran información desactualizada.</i>"
                }
              }
            ]
          },
          {
            "widgets": [
              {
                "buttonList": {
                  "buttons": [
                    {
                      "text": "🌐 Abrir Portal del Operador",
                      "onClick": { "openLink": { "url": webAppUrl } } // Redirige al portal normal
                    }
                  ]
                }
              }
            ]
          }
        ]
      }
    }]
  };
  
  try {
    UrlFetchApp.fetch(url, { method: 'post', contentType: 'application/json', payload: JSON.stringify(payload) });
  } catch (e) {
    console.error("Error enviando alerta preventiva: " + e.message);
  }
}

// 3. EL INSTALADOR DEL RELOJ (Se ejecuta manualmente solo una vez)
function installSupervisorClock() {
  // 3.1 Limpiar relojes anteriores para no enviar alertas duplicadas
  const triggers = ScriptApp.getProjectTriggers();
  for (let i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'runDailySupervisor') {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  // 3.2 Crear el reloj preciso (Todos los días a las 9:30 AM hora local)
  ScriptApp.newTrigger('runDailySupervisor')
    .timeBased()
    .atHour(9)
    .nearMinute(30)
    .everyDays(1)
    .inTimezone('America/Puerto_Rico')
    .create();

  console.log("⏰ ¡Reloj del Supervisor instalado con éxito! Revisará la Dropzone todos los días a las 9:30 AM.");
}
