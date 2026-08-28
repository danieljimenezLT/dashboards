/**
 * SWEAT440 Brand Safety Audit — Status Log (Google Apps Script)
 *
 * SETUP (one time):
 * 1. Create a Google Sheet named "Brand Safety Audit Log"
 * 2. Rename the first tab to: statuses
 * 3. Add this header row in row 1:  flagId | status | cycle | updated
 * 4. Extensions → Apps Script → paste this entire file → Save
 * 5. Deploy → New deployment → type "Web app"
 *      - Execute as: Me
 *      - Who has access: Anyone with the link  (internal use — URL is unlisted)
 * 6. Copy the Web App URL and paste it into SHEET_ENDPOINT at the top of index.html
 *
 * BEHAVIOR:
 * - GET  ?action=get      → returns all statuses as JSON  { flagId: {status, updated}, ... }
 * - POST {action:"set", flagId, status, cycle} → appends/updates the row (cumulative log:
 *   every change is also appended to the "history" tab so nothing is ever lost)
 *
 * NOTE: this only tracks per-flag review status (Pending/Reported to Ops/Cleared).
 * Publishing a new audit cycle is a separate mechanism — see refresh-proxy/worker.js
 * (the "publish_brand_audit_cycle" action commits directly to data/latest.json in
 * the repo, so the dashboard's data source stays git, not this Sheet).
 */

const SHEET_NAME = "statuses";
const HISTORY_NAME = "history";

function doGet(e) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(SHEET_NAME);
  const rows = sh.getDataRange().getValues();
  const out = {};
  for (let i = 1; i < rows.length; i++) {
    const [flagId, status, cycle, updated] = rows[i];
    if (flagId) out[flagId] = { status: status, updated: String(updated).slice(0, 10) };
  }
  return ContentService.createTextOutput(JSON.stringify(out))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  const body = JSON.parse(e.postData.contents);
  if (body.action !== "set" || !body.flagId) {
    return ContentService.createTextOutput(JSON.stringify({ ok: false, error: "bad request" }))
      .setMimeType(ContentService.MimeType.JSON);
  }
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(SHEET_NAME);
  const now = new Date();

  // Update or insert in the statuses tab (current state)
  const rows = sh.getDataRange().getValues();
  let found = false;
  for (let i = 1; i < rows.length; i++) {
    if (rows[i][0] === body.flagId) {
      sh.getRange(i + 1, 2, 1, 3).setValues([[body.status, body.cycle || "", now]]);
      found = true;
      break;
    }
  }
  if (!found) sh.appendRow([body.flagId, body.status, body.cycle || "", now]);

  // Always append to history tab (cumulative — nothing is ever overwritten)
  let hist = ss.getSheetByName(HISTORY_NAME);
  if (!hist) {
    hist = ss.insertSheet(HISTORY_NAME);
    hist.appendRow(["flagId", "status", "cycle", "timestamp"]);
  }
  hist.appendRow([body.flagId, body.status, body.cycle || "", now]);

  return ContentService.createTextOutput(JSON.stringify({ ok: true }))
    .setMimeType(ContentService.MimeType.JSON);
}
