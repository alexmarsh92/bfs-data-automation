# BFS Data Automation

A Google Apps Script automation that monitors the U.S. Census Bureau for new Business Formation Statistics releases, syncs the data into Google Sheets with prior-year imputation, triggers a downstream Adverity data pipeline, and notifies a Microsoft Teams channel — all without any manual intervention.

Built to support a media mix modeling workflow where BFS data is used as a macroeconomic input. Previously a monthly manual task taking ~30 minutes. Now fully automated and self-maintaining.

**Stack:** Google Apps Script · Google Sheets · U.S. Census Bureau API · Adverity Management API · Microsoft Teams

---

## Background

Business Formation Statistics track new employer identification number (EIN) applications filed with the IRS each month — a leading indicator of business activity and entrepreneurial sentiment across the U.S. economy. In a media mix modeling context, this data serves as an external variable capturing macroeconomic conditions that influence market behavior.

The Census Bureau publishes this data monthly, but only through the most recently completed month. To provide the MMM with a full-year view for forecasting and scenario planning, unreleased months are imputed using the equivalent month from the prior year as a baseline assumption.

---

## What it does

1. **Checks daily** — fetches the Census BFS press-release page and reads the "FOR IMMEDIATE RELEASE" date
2. **Detects changes** — compares against the last-seen release date in Script Properties; exits in under a second if nothing is new
3. **Syncs data** — on a new release, fetches the full BFS time series CSV (~700 rows, all series and geographies) and writes it verbatim to `BFS_Raw`
4. **Imputes future months** — for the current year, fills empty month columns using the prior year's value for the same composite key (`sa` + `naics_sector` + `series` + `geo`); imputed cells highlighted yellow in `Sheet1`
5. **Triggers Adverity** — calls the Adverity Management API to kick off a downstream datastream fetch automatically
6. **Notifies via Teams** — posts a summary to a Microsoft Teams channel with release date, pull timestamp, row counts, and imputed cell count
7. **Stamps an audit trail** — writes Census release date and exact pull time below the data in both sheets

---

## Design decisions

**Why Apps Script?** Zero infrastructure overhead. The script lives inside the Google Sheet it writes to, requires no deployment, no servers, and no dependencies beyond built-in Google services. Total setup time is under 30 minutes.

**Why poll the press-release page instead of the CSV directly?** The release date is visible in the page HTML and parseable in a single lightweight fetch (~50KB). Pulling the full CSV (~300KB) daily when data only changes once a month would be wasteful. The page check acts as a cheap gate.

**Why prior-year imputation?** For MMM inputs, the goal is a reasonable baseline assumption for months not yet published — not a forecast. Prior-year same-month is the simplest defensible assumption that preserves seasonality without introducing model complexity.

**Why Script Properties for credentials?** Keeps secrets out of the codebase entirely. Anyone cloning this repo can configure their own credentials without touching the script logic.

---

## Sheet layout

| Sheet | Contents |
|---|---|
| `Sheet1` | Imputed output — pinned as the left-most tab for reliable downstream ingestion |
| `BFS_Raw` | Verbatim Census CSV, no modifications |

---

## Setup

### 1. Copy the script into Apps Script

- Open your Google Sheet → **Extensions → Apps Script**
- Delete any placeholder code, paste `bfs_sync.gs`, save

### 2. Configure Script Properties

**Project Settings → Script Properties** — add the following:

| Property | Description |
|---|---|
| `NOTIFY_EMAIL` | Email address for error and run-summary alerts |
| `TEAMS_EMAIL` | Microsoft Teams channel email address |
| `ADVERITY_KEY` | Adverity API Bearer token (Adverity UI → profile → API Token) |
| `ADVERITY_INSTANCE` | Adverity instance hostname (e.g. `your-org.us.adverity.com`) |
| `ADVERITY_STREAM_ID` | Numeric datastream ID to trigger (visible in the datastream URL) |

> Credentials are never stored in the script. Script Properties keep them out of version control entirely.

### 3. Run the initial sync

Select `syncBFS` in the function dropdown → **Run** → authorize permissions when prompted.

### 4. Install the daily trigger

Select `installTrigger` → **Run**. Registers a daily 09:00 trigger on `checkAndSync`. Confirm in the **Triggers** panel (clock icon, left sidebar).

---

## Functions reference

| Function | Description |
|---|---|
| `checkAndSync()` | Daily trigger target. Reads Census release date, syncs only if new |
| `syncBFS(releaseDate)` | Full sync: CSV fetch, sheet writes, imputation, Adverity trigger, audit stamp |
| `installTrigger()` | Registers daily trigger. Safe to re-run — removes duplicates first |
| `removeTrigger()` | Removes all registered triggers |
| `resetLastSeenDate()` | Clears stored release date to force a sync on next run. Useful for debugging |

---

## Data source

**U.S. Census Bureau — Business Formation Statistics**
- Press release: https://www.census.gov/econ/bfs/current/index.html
- Time series CSV: https://www.census.gov/econ/bfs/csv/bfs_monthly.csv
- Published monthly, ~11–12 days after end of reference month

### Key series

| Code | Description | Lag |
|---|---|---|
| `BA` | Business Applications (raw EIN filings) | ~2–4 weeks — finalized for previous month |
| `HBA` | High-Propensity Business Applications | Same as BA |
| `CBA` | Corporate Business Applications | Same as BA |
| `WBA` | Applications with Planned Wages | Same as BA |
| `SBF4Q` | Spliced Business Formations within 4 Quarters | Recent months are Census projections, not actuals |
| `SBF8Q` | Spliced Business Formations within 8 Quarters | Same — longer window |

> `BA` and its variants are the only series with a finalized value for the previous month. Formation series require payroll records to confirm — recent months are always Census-projected estimates spliced with confirmed historical actuals.

---

## Imputation logic

For rows where `year` == current year, empty month columns are filled from the matching prior-year row (composite key: `sa` + `naics_sector` + `series` + `geo`).

- `NA` values (Census designation for inapplicable series, e.g. Puerto Rico formation data) are preserved and never overwritten
- Imputed cells are highlighted yellow in `Sheet1` to distinguish them from Census-sourced actuals

---

## Adverity integration

Triggers a fetch on the configured datastream via `POST /api/datastreams/{ADVERITY_STREAM_ID}/fetch_fixed/`. Both `ADVERITY_INSTANCE` and `ADVERITY_STREAM_ID` are read from Script Properties. If `ADVERITY_KEY` is not set the step is silently skipped — the rest of the automation runs normally.

To adapt this for a different Adverity instance or datastream, update `ADVERITY_INSTANCE` and `ADVERITY_STREAM_ID` in the config block at the top of `bfs_sync.gs`.

---

## License

MIT
