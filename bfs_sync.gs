/**
 * BFS Monthly Data Sync
 *
 * Fetches the Census Bureau BFS time series CSV, writes all rows to the
 * "BFS_Raw" sheet (creating it if needed), then imputes missing current-year
 * months with prior-year values in a separate "Sheet1" output sheet.
 *
 * SETUP
 * -----
 * 1. Open your Google Sheet → Extensions → Apps Script → paste this file.
 * 2. In the Apps Script editor go to Project Settings → Script Properties
 *    and add the following properties (see CREDENTIALS section below).
 * 3. Save, then run installTrigger() ONCE from the editor to register the
 *    daily check schedule.  You can also run syncBFS() manually any time.
 *
 * CREDENTIALS — configure these in Script Properties, NOT in this file
 * --------------------------------------------------------------------
 * NOTIFY_EMAIL       Personal email for error/summary alerts
 * TEAMS_EMAIL        Microsoft Teams channel email address
 * ADVERITY_KEY       Adverity API Bearer token (generate in Adverity UI → profile)
 *
 * HOW THE DAILY CHECK WORKS
 * -------------------------
 * checkAndSync() runs daily at 09:00.  It fetches only the Census press-release
 * page (~50 KB of HTML) and scrapes the "FOR IMMEDIATE RELEASE: …" date from
 * the top of the page.  That date is compared against the last-seen release
 * date stored in Script Properties.  If nothing has changed the function exits
 * in under a second — no CSV fetch, no sheet writes.  Only when a new release
 * date is detected does it call syncBFS() to do the full update.
 *
 * SHEET LAYOUT
 * ------------
 * Sheet1   – final imputed output; always positioned as the left-most tab so
 *             Adverity's fetch job targets it reliably.  Empty month cells for
 *             the current year are filled with the prior-year value for that
 *             row.  Imputed cells are highlighted yellow.  Fully replaced each
 *             run.
 * BFS_Raw  – verbatim copy of the Census CSV (header + all data rows).
 *             Fully replaced on every run.
 *
 * IMPUTATION LOGIC
 * ----------------
 * For each row whose `year` == current year:
 *   For each month column (jan … dec):
 *     If the cell is empty → look up the matching row for (year-1) using the
 *     composite key (sa + naics_sector + series + geo) and copy that month's
 *     value.  If no prior-year row exists the cell stays empty.
 *     "NA" values from Census are kept as-is and are NOT imputed over.
 */

// ── Config ────────────────────────────────────────────────────────────────────
var CSV_URL            = 'https://www.census.gov/econ/bfs/csv/bfs_monthly.csv';
var PAGE_URL           = 'https://www.census.gov/econ/bfs/current/index.html';
var RAW_SHEET          = 'BFS_Raw';
var IMP_SHEET          = 'Sheet1';    // Adverity fetch target — must stay left-most tab
var PROP_KEY           = 'BFS_LAST_RELEASE_DATE';  // Script Properties key
var ADVERITY_INSTANCE  = 'gainshare.us.adverity.com';
var ADVERITY_STREAM_ID = '367';

// Credentials and instance config — loaded from Script Properties at runtime (never hardcoded here)
var _props             = PropertiesService.getScriptProperties();
var NOTIFY_EMAIL       = _props.getProperty('NOTIFY_EMAIL')       || '';
var TEAMS_EMAIL        = _props.getProperty('TEAMS_EMAIL')        || '';
var ADVERITY_KEY       = _props.getProperty('ADVERITY_KEY')       || '';
var ADVERITY_INSTANCE  = _props.getProperty('ADVERITY_INSTANCE')  || '';
var ADVERITY_STREAM_ID = _props.getProperty('ADVERITY_STREAM_ID') || '';
// ─────────────────────────────────────────────────────────────────────────────


// ── Daily check entry point ───────────────────────────────────────────────────

/**
 * Called by the daily trigger.  Fetches the Census press-release page, extracts
 * the release date, and only proceeds to a full sync if it has changed since
 * the last run.
 */
function checkAndSync() {
  var releaseDate = getReleaseDate();
  if (!releaseDate) {
    Logger.log('Could not parse release date from Census page — skipping.');
    if (NOTIFY_EMAIL) {
      MailApp.sendEmail(NOTIFY_EMAIL,
        '[BFS Sync] WARNING – could not read release date',
        'checkAndSync() could not parse the release date from:\n' + PAGE_URL +
        '\n\nManual check recommended.');
    }
    return;
  }

  var props    = PropertiesService.getScriptProperties();
  var lastSeen = props.getProperty(PROP_KEY) || '';

  Logger.log('Census release date: ' + releaseDate + ' | Last seen: ' + (lastSeen || 'none'));

  if (releaseDate === lastSeen) {
    Logger.log('No new release — nothing to do.');
    return;
  }

  // New release detected — run full sync, then persist the new date
  Logger.log('New release detected — running syncBFS()…');
  var syncResult = syncBFS(releaseDate);
  props.setProperty(PROP_KEY, releaseDate);

  // Trigger Adverity datastream fetch
  var adverityJobId = null;
  if (ADVERITY_KEY) {
    adverityJobId = triggerAdverityFetch();
  }

  // Notify Teams channel on successful sync
  if (TEAMS_EMAIL) notifyTeams(releaseDate, syncResult, adverityJobId);
}

/**
 * Fetch the Census BFS press-release page and extract the "FOR IMMEDIATE
 * RELEASE" date string (e.g. "March 11, 2026").
 * Returns the date string, or null if it cannot be found.
 */
function getReleaseDate() {
  var response = UrlFetchApp.fetch(PAGE_URL, {muteHttpExceptions: true});
  if (response.getResponseCode() !== 200) {
    Logger.log('HTTP ' + response.getResponseCode() + ' fetching press-release page.');
    return null;
  }
  var html  = response.getContentText();
  // The page contains:  "FOR IMMEDIATE RELEASE: Wednesday, March 11, 2026"
  // Capture just the date portion after the weekday
  var match = html.match(/FOR IMMEDIATE RELEASE[^,]*,\s*([A-Za-z]+ \d{1,2},\s*\d{4})/i);
  return match ? match[1].trim() : null;
}


// ── Main sync ─────────────────────────────────────────────────────────────────

/**
 * Full sync: fetch CSV, write BFS_Raw, build imputed data, write Sheet1.
 * releaseDate is passed in from checkAndSync() for the audit stamp; when
 * called manually (no argument) it re-reads the page to get the current date.
 */
function syncBFS(releaseDate) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var startTime = new Date();
  var log = [];

  // If called manually without a releaseDate, fetch it now for the audit stamp
  if (!releaseDate) {
    releaseDate = getReleaseDate() || 'unknown';
  }

  try {
    // 1. Fetch & parse CSV
    log.push('Fetching CSV from Census…');
    var rows = fetchAndParseCSV(CSV_URL);
    log.push('Rows fetched (incl. header): ' + rows.length);

    if (rows.length < 2) throw new Error('CSV appears empty or failed to parse.');

    var headers   = rows[0];   // ['sa','naics_sector','series','geo','year','jan',…,'dec']
    var dataRows  = rows.slice(1);
    var monthCols = getMonthColIndices(headers);  // indices of jan…dec in header array
    var keyCols   = [0, 1, 2, 3];                 // sa, naics_sector, series, geo

    // 2. Write raw sheet
    log.push('Writing ' + RAW_SHEET + '…');
    writeSheet(ss, RAW_SHEET, rows, [], []);

    // 3. Build prior-year lookup  { compositeKey_prevYear : rowArray }
    var currentYear = new Date().getFullYear();
    var prevYear    = currentYear - 1;
    var yearIdx     = headers.indexOf('year');

    var priorYearLookup = buildLookup(dataRows, keyCols, yearIdx, prevYear);
    log.push('Prior-year rows indexed: ' + Object.keys(priorYearLookup).length);

    // 4. Build imputed rows + track which cells were imputed
    var imputedData      = [];
    var imputedCellFlags = [];    // parallel array: true = imputed, false = actual/NA

    for (var r = 0; r < dataRows.length; r++) {
      var row   = dataRows[r].slice();   // copy
      var flags = new Array(row.length).fill(false);

      var rowYear = parseInt(row[yearIdx], 10);
      if (rowYear === currentYear) {
        var key        = makeKey(row, keyCols);
        var priorRow   = priorYearLookup[key];

        for (var m = 0; m < monthCols.length; m++) {
          var ci  = monthCols[m];
          var val = row[ci];
          // Impute only if the cell is genuinely empty (not 'NA')
          if ((val === '' || val === null || val === undefined) && priorRow) {
            var priorVal = priorRow[ci];
            if (priorVal !== '' && priorVal !== null && priorVal !== undefined) {
              row[ci]   = priorVal;
              flags[ci] = true;
            }
          }
        }
      }

      imputedData.push(row);
      imputedCellFlags.push(flags);
    }

    // 5. Write imputed sheet (Sheet1) and ensure it is the left-most tab
    log.push('Writing ' + IMP_SHEET + ' (with highlights)…');
    var allImputedRows = [headers].concat(imputedData);
    var allFlags       = [new Array(headers.length).fill(false)].concat(imputedCellFlags);
    writeSheet(ss, IMP_SHEET, allImputedRows, allFlags, ['#FFF2CC']);

    // Pin Sheet1 to position 0 (left-most) so Adverity always finds it first
    ss.setActiveSheet(ss.getSheetByName(IMP_SHEET));
    ss.moveActiveSheet(1);

    // 6. Write two-line audit stamp in both sheets
    var pullTime = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm z');
    var stamp    = 'Census release date: ' + releaseDate + '  |  Data pulled: ' + pullTime;
    stampSheet(ss, IMP_SHEET, stamp);
    stampSheet(ss, RAW_SHEET, stamp);

    var elapsed = ((new Date() - startTime) / 1000).toFixed(1);
    var currentYearRowCount = imputedData.filter(function(r){ return parseInt(r[yearIdx],10)===currentYear; }).length;
    var imputedCellCount    = countImputedCells(imputedCellFlags);

    log.push('Done in ' + elapsed + 's.');
    log.push('Current-year rows: ' + currentYearRowCount);
    log.push('Imputed cells: '     + imputedCellCount);

    if (NOTIFY_EMAIL) sendSummary(NOTIFY_EMAIL, log.join('\n'), false);

    return {
      success:          true,
      elapsed:          elapsed,
      totalRows:        dataRows.length,
      currentYearRows:  currentYearRowCount,
      imputedCells:     imputedCellCount,
      pullTime:         pullTime,
      releaseDate:      releaseDate
    };

  } catch (e) {
    log.push('ERROR: ' + e.message);
    if (NOTIFY_EMAIL) sendSummary(NOTIFY_EMAIL, log.join('\n'), true);
    throw e;   // re-throw so Apps Script marks the run as failed
  }
}


// ── Helpers ───────────────────────────────────────────────────────────────────

/** Fetch CSV and split into 2-D array of strings. */
function fetchAndParseCSV(url) {
  var response = UrlFetchApp.fetch(url, {muteHttpExceptions: true});
  if (response.getResponseCode() !== 200) {
    throw new Error('HTTP ' + response.getResponseCode() + ' fetching CSV.');
  }
  var text = response.getContentText();
  var lines = text.split('\n');
  var result = [];
  for (var i = 0; i < lines.length; i++) {
    var line = lines[i].replace(/\r$/, '');
    if (line === '' && i === lines.length - 1) continue;  // skip trailing blank
    result.push(line.split(','));
  }
  return result;
}

/** Return array of column indices for jan…dec in the header row. */
function getMonthColIndices(headers) {
  var months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
  return months.map(function(m) { return headers.indexOf(m); })
               .filter(function(i) { return i !== -1; });
}

/** Build { compositeKey : rowArray } for rows matching targetYear. */
function buildLookup(dataRows, keyCols, yearIdx, targetYear) {
  var lookup = {};
  for (var r = 0; r < dataRows.length; r++) {
    var row = dataRows[r];
    if (parseInt(row[yearIdx], 10) === targetYear) {
      lookup[makeKey(row, keyCols)] = row;
    }
  }
  return lookup;
}

/** Composite key from specified column indices. */
function makeKey(row, keyCols) {
  return keyCols.map(function(i) { return row[i]; }).join('|');
}

/** Count cells flagged as imputed. */
function countImputedCells(flagMatrix) {
  var count = 0;
  for (var r = 0; r < flagMatrix.length; r++) {
    for (var c = 0; c < flagMatrix[r].length; c++) {
      if (flagMatrix[r][c]) count++;
    }
  }
  return count;
}

/**
 * Write data to a sheet, creating it if needed.
 * flagMatrix: parallel bool array — true = apply highlightColor to that cell.
 * highlightColors: array of hex strings (only [0] is used).
 */
function writeSheet(ss, sheetName, data, flagMatrix, highlightColors) {
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  } else {
    sheet.clearContents();
    sheet.clearFormats();
  }

  if (data.length === 0) return;

  var numRows = data.length;
  var numCols = data[0].length;

  // Write all data in one batch
  var range = sheet.getRange(1, 1, numRows, numCols);
  range.setValues(data);

  // Bold header row
  sheet.getRange(1, 1, 1, numCols).setFontWeight('bold');

  // Freeze header row
  sheet.setFrozenRows(1);

  // Apply highlights if provided
  if (flagMatrix.length > 0 && highlightColors.length > 0) {
    var highlightColor = highlightColors[0];
    // Build background color array in one pass to minimise API calls
    var bgColors = [];
    for (var r = 0; r < flagMatrix.length; r++) {
      var rowBg = [];
      for (var c = 0; c < (flagMatrix[r] || []).length; c++) {
        rowBg.push(flagMatrix[r][c] ? highlightColor : null);
      }
      bgColors.push(rowBg);
    }
    sheet.getRange(1, 1, bgColors.length, bgColors[0].length).setBackgrounds(bgColors);
  }

  // Auto-resize first 5 key columns for readability
  for (var col = 1; col <= Math.min(5, numCols); col++) {
    sheet.autoResizeColumn(col);
  }
}

/** Add a small last-updated note two rows below the data. */
function stampSheet(ss, sheetName, stamp) {
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) return;
  var lastRow = sheet.getLastRow();
  sheet.getRange(lastRow + 2, 1).setValue(stamp).setFontStyle('italic').setFontColor('#888888');
}

/** Send a run summary email. */
function sendSummary(email, body, isError) {
  var subject = isError
    ? '[BFS Sync] ERROR – manual check required'
    : '[BFS Sync] Success – ' + Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  MailApp.sendEmail(email, subject, body);
}


/** Send a plain-text update notification to a Teams channel via its email address. */
function notifyTeams(releaseDate, syncResult, adverityJobId) {
  var ss          = SpreadsheetApp.getActiveSpreadsheet();
  var sheetUrl    = ss.getUrl();
  var subject     = 'BFS Data Updated – Census release ' + releaseDate;
  var adverityLine = adverityJobId
    ? 'Adverity fetch triggered  : job ' + adverityJobId + ' queued on datastream ' + ADVERITY_STREAM_ID
    : 'Adverity fetch            : skipped (no API key configured)';

  var body = [
    'The Census Bureau BFS time series has been updated and your Google Sheet has been refreshed.',
    '',
    'Census release date : ' + releaseDate,
    'Data pulled at      : ' + syncResult.pullTime,
    'Total rows written  : ' + syncResult.totalRows,
    'Current-year rows   : ' + syncResult.currentYearRows,
    'Imputed cells       : ' + syncResult.imputedCells + ' (prior-year fill, highlighted yellow in Sheet1)',
    'Sync duration       : ' + syncResult.elapsed + 's',
    adverityLine,
    '',
    'Sheet: ' + sheetUrl,
    '',
    '— BFS Auto-Sync (Google Apps Script)'
  ].join('\n');

  MailApp.sendEmail(TEAMS_EMAIL, subject, body);
  Logger.log('Teams notification sent to ' + TEAMS_EMAIL);
}


/**
 * Trigger a fetch on the configured Adverity datastream via the Management API.
 * Uses fetch_fixed with today as both start and end — appropriate for a
 * "pull latest" operation on a Google Sheets datastream with no meaningful
 * date range parameter.
 * Returns the Adverity job ID string on success, or null on failure.
 */
function triggerAdverityFetch() {
  var today    = Utilities.formatDate(new Date(), 'UTC', "yyyy-MM-dd'T'HH:mm:ss'Z'");
  var endpoint = 'https://' + ADVERITY_INSTANCE + '/api/datastreams/' + ADVERITY_STREAM_ID + '/fetch_fixed/';

  var payload = JSON.stringify({ start: today, end: today, priority: 'high' });

  var options = {
    method:             'post',
    contentType:        'application/json',
    headers:            { 'Authorization': 'Bearer ' + ADVERITY_KEY },
    payload:            payload,
    muteHttpExceptions: true
  };

  var response = UrlFetchApp.fetch(endpoint, options);
  var code     = response.getResponseCode();
  var body     = response.getContentText();

  Logger.log('Adverity API response (' + code + '): ' + body);

  if (code === 200 || code === 201) {
    try {
      var parsed = JSON.parse(body);
      // Response contains a jobs array; grab the first job id
      var jobId = (parsed.jobs && parsed.jobs.length > 0) ? String(parsed.jobs[0].id) : 'unknown';
      Logger.log('Adverity fetch queued — job ID: ' + jobId);
      return jobId;
    } catch (e) {
      Logger.log('Adverity fetch succeeded but could not parse job ID: ' + e.message);
      return 'queued';
    }
  } else {
    var msg = 'Adverity fetch failed — HTTP ' + code + ': ' + body;
    Logger.log(msg);
    if (NOTIFY_EMAIL) {
      MailApp.sendEmail(NOTIFY_EMAIL, '[BFS Sync] Adverity fetch error', msg);
    }
    return null;
  }
}


// ── Trigger management ────────────────────────────────────────────────────────

/**
 * Run this function ONCE from the Apps Script editor to install a daily
 * check trigger.  checkAndSync() fires every day at 09:00 in the script
 * timezone; it only runs a full sync when Census publishes a new release.
 * Re-running won't duplicate the trigger (it deletes old ones first).
 */
function installTrigger() {
  // Remove any existing checkAndSync or syncBFS triggers
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'checkAndSync' ||
        t.getHandlerFunction() === 'syncBFS') {
      ScriptApp.deleteTrigger(t);
    }
  });

  // Daily trigger at 09:00 in the script timezone
  ScriptApp.newTrigger('checkAndSync')
    .timeBased()
    .everyDays(1)
    .atHour(9)
    .create();

  Logger.log('Daily trigger installed: checkAndSync() will run every day at 09:00.');
}

/** Convenience: remove all checkAndSync / syncBFS triggers. */
function removeTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'checkAndSync' ||
        t.getHandlerFunction() === 'syncBFS') {
      ScriptApp.deleteTrigger(t);
      Logger.log('Trigger removed: ' + t.getHandlerFunction());
    }
  });
}

/**
 * Utility: clear the stored release date so the next checkAndSync() run
 * treats the current release as "new" and forces a fresh sync.
 * Useful after manually editing the sheet or debugging.
 */
function resetLastSeenDate() {
  PropertiesService.getScriptProperties().deleteProperty(PROP_KEY);
  Logger.log('Last-seen release date cleared.');
}
