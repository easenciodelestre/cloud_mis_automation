/**
 * =========================================================================
 * 📡 FASE 3: MIS CONTROL TOWER (GOOGLE CHAT ALERTS + AUTO-FIX)
 * =========================================================================
 */

function sendSuccessAlert() {
  const url = (typeof ENV !== 'undefined' && ENV.CHAT_WEBHOOK_URL) ? ENV.CHAT_WEBHOOK_URL : null;
  if (!url) return;
  const timestamp = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Puerto_Rico', hour: '2-digit', minute:'2-digit' });

  const payload = {
    "cardsV2": [{
      "cardId": "success_pipeline",
      "card": {
        "header": { "title": "✅ Pipeline ELT Completado", "subtitle": "MIS Control Tower | " + timestamp, "imageUrl": "https://cdn-icons-png.flaticon.com/512/190/190411.png", "imageType": "CIRCLE" },
        "sections": [{ "widgets": [{ "textParagraph": { "text": "<b>Estado:</b> <font color=\"#22c55e\">100% Exitoso</font><br><b>Base de Datos:</b> Bóvedas de BigQuery actualizadas (MERGE).<br><br>📊 <i>Datos listos en Looker Studio.</i>" } }] }]
      }
    }]
  };
  UrlFetchApp.fetch(url, { method: 'post', contentType: 'application/json', payload: JSON.stringify(payload) });
}

function sendErrorAlert(errorMessage) {
  const url = (typeof ENV !== 'undefined' && ENV.CHAT_WEBHOOK_URL) ? ENV.CHAT_WEBHOOK_URL : null;
  const webAppUrl = (typeof ENV !== 'undefined' && ENV.WEB_APP_URL) ? ENV.WEB_APP_URL : null;
  if (!url) return;
  const timestamp = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Puerto_Rico', hour: '2-digit', minute:'2-digit' });

  const payload = {
    "cardsV2": [{
      "cardId": "error_pipeline",
      "card": {
        "header": { "title": "🚨 ALERTA CRÍTICA: Fallo en Pipeline", "subtitle": "MIS Control Tower | " + timestamp, "imageUrl": "https://cdn-icons-png.flaticon.com/512/564/564619.png", "imageType": "CIRCLE" },
        "sections": [
          { "widgets": [{ "textParagraph": { "text": "<b>Diagnóstico Técnico:</b><br><font color=\"#ef4444\">" + errorMessage + "</font><br><br>⚠️ <i>Pipeline abortado para proteger integridad de datos.</i>" } }] },
          { "widgets": [{ "buttonList": { "buttons": [{ "text": "🛠️ Forzar Auto-Fix (2 Intentos)", "onClick": { "openLink": { "url": webAppUrl + "?action=autofix" } } }] } }] }
        ]
      }
    }]
  };
  UrlFetchApp.fetch(url, { method: 'post', contentType: 'application/json', payload: JSON.stringify(payload) });
}

function sendAutoFixSummary(summary) {
  const url = (typeof ENV !== 'undefined' && ENV.CHAT_WEBHOOK_URL) ? ENV.CHAT_WEBHOOK_URL : null;
  if (!url) return;
  
  let headerTitle = summary.resolved ? "✅ Auto-Fix Exitoso" : "❌ Auto-Fix Fallido (Escalado)";
  let headerColor = summary.resolved ? "https://cdn-icons-png.flaticon.com/512/190/190411.png" : "https://cdn-icons-png.flaticon.com/512/564/564619.png";
  
  let bodyText = `<b>📝 Resumen del Problema:</b><br>${summary.problem}<br><br><b>🔄 Intentos Realizados:</b> ${summary.attempts} de 2<br><br>`;
  
  if (summary.resolved) {
    bodyText += `<b>✅ Solución Permanente Aplicada:</b><br><font color="#22c55e">${summary.solution}</font>`;
  } else {
    bodyText += `<b>❌ Resultado:</b> El sistema no pudo reparar el bloqueo de forma segura.<br><br><b>🧠 Hipótesis Técnica (Arquitecto requerido):</b><br><font color="#fbbf24">${summary.hypothesis}</font>`;
  }

  const payload = {
    "cardsV2": [{
      "cardId": "autofix_summary",
      "card": {
        "header": { "title": headerTitle, "subtitle": "Reporte de Rescate Automático", "imageUrl": headerColor, "imageType": "CIRCLE" },
        "sections": [{ "widgets": [{ "textParagraph": { "text": bodyText } }] }]
      }
    }]
  };
  UrlFetchApp.fetch(url, { method: 'post', contentType: 'application/json', payload: JSON.stringify(payload) });
}
