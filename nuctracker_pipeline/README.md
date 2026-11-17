# HES Nuctracker Pipeline

**French nuclear availability monitoring, actual generation updates and HES production forecasting**  
**Documented version:** v4.2.1 (v4.1 interactive launcher + v4.2 annual report + v4.2.1 Datawrapper format and migration fix)  
**README updated:** 17 November 2026  
**Audience:** Haya Energy Solutions (HES) analysts, maintainers and approved administrators

> **Start here.** This is the operational guide for the assembled **v4.2.1 pipeline**. It replaces the older instructions that used `run_nuctracker.bat`, a manual `EVENT_DATE` refresh and only two Datawrapper charts. The original forecast methodology remains in place; the newer releases change data synchronization, execution and reporting.
>
> **One-time action before routine publication:** Existing installations whose annual Datawrapper table still has legacy columns and older calendar-year rows must run the **supervised annual-table migration** in [Section 4.3](#43-one-time-migration-of-the-existing-annual-datawrapper-table-v421). Merely copying the v4.2.1 files does not migrate a remote chart. Until the schema is migrated, Nuctracker intentionally refuses the three-chart publication.
>
> **Important:** Nuctracker can write to the shared MongoDB database and publish Datawrapper charts. Always check the destination chart IDs and keep recovery backups. A successful program run does not, by itself, establish that every model estimate is accurate.

## Contents

1. [What Nuctracker does](#1-what-nuctracker-does)
2. [Prerequisites](#2-prerequisites)
3. [First-time installation](#3-first-time-installation)
4. [Configuration and credentials](#4-configuration-and-credentials)
5. [Daily operation](#5-daily-operation)
6. [How date selection and synchronization work](#6-how-date-selection-and-synchronization-work)
7. [Outputs and Datawrapper publications](#7-outputs-and-datawrapper-publications)
8. [Quality checks and monitoring](#8-quality-checks-and-monitoring)
9. [Backups, recovery and rollback](#9-backups-recovery-and-rollback)
10. [Project files and maintenance](#10-project-files-and-maintenance)
11. [Troubleshooting](#11-troubleshooting)
12. [Model scope, limitations and version history](#12-model-scope-limitations-and-version-history)
13. [Ownership and references](#13-ownership-and-references)

---

## 1. What Nuctracker does

Nuctracker is a Windows-based HES pipeline for monitoring **French nuclear reactor availability**, updating **actual nuclear generation** and forecasting **nuclear electricity production**.

The current end-to-end workflow is:

```text
RTE generation-unavailability publications     RTE actual generation per unit
           |                                               |
   PUBLICATION_DATE synchronization              Complete French calendar days
   New nuclear event versions only               Recent-day correction overlap
           |                                               |
   MongoDB: data.unavs_update                  MongoDB: data.nuclear_production_auto_daily
           |                                               |
           +----------------------+------------------------+
                                  |
                      HES availability calculation
                      + historical production data
                                  |
                         HES SARIMAX forecast
                                  |
                          CSVs + automated QA
                                  |
                  Backup, upload, verify and publish
                                  |
                        THREE Datawrapper charts
                  (graph, monthly table, annual table)
```

The pipeline is designed to:

- Collect new and revised **nuclear** unavailability notices published by RTE.
- Preserve previously stored event versions rather than overwriting their history.
- Update actual generation for complete French calendar days, including possible late RTE corrections.
- Calculate reactor and fleet availability through the corrected Nuctracker computation.
- Produce HES nuclear production forecasts using the existing **SARIMAX** methodology.
- Generate local datasets and quality-control reports.
- Publish **three** approved Datawrapper datasets when publication is enabled.

**Terminology:** Actual generation, nuclear availability and forecast generation are different indicators. Nuctracker's daily production storage is not a real-time 5- or 15-minute telemetry feed.

## 2. Prerequisites

Before using the pipeline, confirm the following:

- **Windows 10 or 11**, with access to the approved HES code directory.
- **Python** and the project virtual environment (`.venv`). The original project documented Python 3.10+ and recommended Python 3.11; check package compatibility before changing the Python version.
- An approved **RTE API** account with access to the required generation and unavailability endpoints.
- **MongoDB Atlas** access with the permissions required by the approved workflow; the production update uses a dedicated collection.
- A **Datawrapper** API token and editing rights for the three existing HES charts. An annual table still using the legacy dataset must be migrated once before automated three-chart publishing.
- A working internet connection and network access to the RTE, MongoDB and Datawrapper services.
- An approved, initialized RTE publication synchronization checkpoint and suitable database backups.

**Recommended:** Visual Studio Code with Microsoft's Python extension; Git for Windows if working from the HES repository. Neither application needs to remain open for normal double-click execution.

Do not run the program directly from an unextracted ZIP. Avoid locations synchronized by OneDrive unless required and tested under company policy, as file locking can interfere with Python environments and generated CSVs.

## 3. First-time installation

The instructions below assume a **complete v4 installation with the v4.1, v4.2 and v4.2.1 updates applied**. These patches alone do not replace the complete base pipeline. The v4.2.1 code patch and the one-time remote annual-table migration are **two separate steps**.

### 3.1 Obtain and extract the project

1. Obtain the approved Nuctracker repository or distribution from HES.
2. Extract or clone it into a permanent, company-approved folder, for example:

   ```text
   C:\Users\<Windows-user>\Documents\Code\Nuctracker-main\Nuctracker-main\nuctracker_pipeline
   ```

3. Open the `nuctracker_pipeline` directory: it must contain `run_nuctracker_full.bat` and `nuctracker_run_full.py`.
4. If updating an existing installation, **close Nuctracker first**, back up the scripts being replaced and follow the approved patch procedure. Keep the existing `.env`, `.venv`, MongoDB/Datawrapper backup folders and historical data.

### 3.2 Prepare Python

For a first-time installation, use the provided setup program if it is present:

```cmd
setup_project.bat
```

The original setup script creates `.venv` and installs `requirements.txt`. If the distribution includes additional v4 requirements, install them in the **same** virtual environment, following the supplied add-on requirements file, for example:

```cmd
.venv\Scripts\python.exe -m pip install -r requirements_v4_addon.txt
```

Run the second command **only if that file is included**. Do not assume the original `setup_project.bat` automatically installs dependencies introduced by later patches.

To inspect the interpreter:

```cmd
.venv\Scripts\python.exe --version
```

In VS Code, select `.venv\Scripts\python.exe` as the project interpreter.

### 3.3 Confirm the assembled version

The normal v4.2.1 installation must include at least:

- `run_nuctracker_full.bat`, `nuctracker_manual.py`, `nuctracker_run_full.py`;
- `rte_autosync.py`, `rte_publication_sync.py`, `rte_production_sync.py`;
- `nuctracker_corrected.py`, `nucmonitor.py`, `nuctracker_final_pipeline.py`;
- `nuctracker_annual_report.py`, `restore_datawrapper_full.py`.

For v4.2.1, also keep `migrate_annual_datawrapper_once.py` and `restore_migration_annuelle.py` for the one-time migration and its rollback, plus the updated `restore_datawrapper_full.py`. If `nuctracker_annual_report.py` is missing, the **v4.2 annual-table extension is not fully installed**. If the manual interface or production-sync files are missing, check that the **v4.1 update** is installed.

### 3.4 Run local tests

From the `nuctracker_pipeline` folder:

```cmd
.venv\Scripts\python.exe -m unittest discover -s tests -p "test_ui_v41.py" -v
.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*v42.py" -v
.venv\Scripts\python.exe -m unittest discover -s tests -p "test_dw_annual_v421.py" -v
```

These tests are optional for daily use but strongly recommended after code changes. The patch documentation reports simulated test results; **running the tests locally does not validate the live RTE/MongoDB/Datawrapper services**.

## 4. Configuration and credentials

### 4.1 Create the local configuration

If the project provides `.env.example`, copy it to `.env` and fill in the approved HES values:

```cmd
copy .env.example .env
```

Illustrative format **only** (never paste actual secrets into a shared README):

```ini
MONGODB_USER=<approved-user>
MONGODB_PASSWORD=<secret>
MONGODB_HOST=<cluster-hostname>
RTE_CLIENT_CREDENTIALS=<client-id>:<client-secret>
DATAWRAPPER_API_TOKEN=<secret>
DATAWRAPPER_GRAPH_ID=<existing-graph-id>
DATAWRAPPER_TABLE_ID=<existing-monthly-table-id>
DATAWRAPPER_ANNUAL_TABLE_ID=<existing-annual-table-id>
```

The original pipeline documents `RTE_CLIENT_CREDENTIALS` in `client_id:client_secret` form. **Check the configuration expected by the RTE synchronization modules in your installed version** if it uses separate client-ID/secret variables. Do not duplicate or change established working credentials unnecessarily.

Keep only the host name in `MONGODB_HOST`, not a MongoDB connection URI, when using the documented host-based configuration. Environment variables may take precedence over `.env` settings.

### 4.2 Third Datawrapper chart: required in v4.2 and v4.2.1

`DATAWRAPPER_ANNUAL_TABLE_ID` is the ID of the **existing HES annual nuclear production table**, not the monthly table or graph. The three chart IDs must be different.

For example, the editor URL `https://app.datawrapper.de/chart/ABCDE/visualize` has the ID `ABCDE` (illustrative only).

The v4.2.1 publisher checks the expected annual HES **four-column, two-row** dataset before writing to Datawrapper. The third chart must be of type `tables`, with the intended chart title and schema. Do **not** bypass an ID or schema mismatch. The existing legacy annual chart must be migrated once, as described below. The new version can correctly read existing Datawrapper **TSV (tab-delimited) or CSV** responses.

### 4.3 One-time migration of the existing annual Datawrapper table (v4.2.1)

**When required:** The existing table titled **`Nuclear Production (Realized and Forecasted)`** may still contain four legacy columns (a blank first header, `Realized`, `EDF Forecast (As of ...)`, `HES Forecast`) and three historic rows such as `Cal 2023 [TW]`, `Cal 2024 [TW]`, `Cal 2025 [TW]`. These are **not** the current v4.2 annual-summary columns and rows. In the November 2026 diagnostic, Datawrapper returned a tab-delimited dataset; the old validator incorrectly interpreted it as one column. v4.2.1 fixes delimiter handling, but **does not silently overwrite the old annual table**.

**Install the approved v4.2.1 patch first:** close Nuctracker, back up the previous Python files **outside** the execution folder, and copy the following into `nuctracker_pipeline`:

| v4.2.1 patch file | Action |
| --- | --- |
| `nuctracker_run_full.py` | Replace (CSV/TSV reading, safer backup and publication checks) |
| `restore_datawrapper_full.py` | Replace (restores normal two- or three-chart backups) |
| `migrate_annual_datawrapper_once.py` | Add (preview and confirmed migration of **only** the annual chart) |
| `restore_migration_annuelle.py` | Add (dedicated annual-only migration rollback) |
| `run_migration_tableau_annuel.bat` | Optional: add (one-click **preview only**) |
| `tests/test_dw_annual_v421.py` | Optional: add under `tests/` |

Do **not** replace `run_nuctracker_full.bat`, `nuctracker_manual.py`, `nuctracker_annual_report.py`, `.env`, `rte_production_sync.py`, `nucmonitor.py` or `nuctracker_final_pipeline.py` as part of this targeted patch.

**A. Prepare and preview (READ ONLY).** Use the current, reviewed annual CSV from `outputs_full/tracker_annual_production_YYYY-MM-DD.csv` and verify the latest `full_pipeline_qa_*.json`. The script uses the newest matching CSV by default (you may specify `--annual-csv PATH` to select an approved file):

```cmd
.venv\Scripts\python.exe migrate_annual_datawrapper_once.py
```

You may also double-click `run_migration_tableau_annuel.bat` for a preview **without any Datawrapper write**. Check that the annual chart title/ID, the four **existing** headers, the historical years, and the **new** headers and years are all correct. The migration script validates the approved chart title and structure; do not alter these safety guards.

**B. Approve the one-time replacement.** The migration **replaces**, rather than appends to, the old annual dataset. If the old 2023–2025 table needs to remain accessible, first duplicate or archive it in Datawrapper. After reviewing the preview and obtaining approval:

```cmd
.venv\Scripts\python.exe migrate_annual_datawrapper_once.py --commit
```

Type exactly **`MIGRER TABLEAU ANNUEL`** at the confirmation prompt. This operation:

1. Saves the existing annual chart's raw data, normalized CSV, metadata snapshot and manifest under `backups_datawrapper/annual_migration_.../`.
2. Updates **only the annual chart**, reads the uploaded data back and republishes that chart.
3. Attempts an annual-only data restore if the migration fails. A failed restore requires manual intervention.

**C. Visual validation.** Open the third chart in Datawrapper and verify the 2026/2027 rows, four new columns, numeric display, headers and layout. The underlying dataset is replaced; **Datawrapper formatting settings may still need manual correction**. Verify before the next full publication.

**D. Return to routine updates.** Once the annual migration is successful and visually reviewed, launch `run_nuctracker_full.bat` as usual. It can then back up and publish all **three** charts. The migration is **one-time**; do not run `--commit` for everyday updates.

To roll back **only the annual migration**, use the separate restore procedure in [Section 9.3](#93-rollback-of-the-one-time-annual-table-migration).

### 4.4 Security and first-time database authorization

- Never commit or email `.env`, tokens, passwords or raw production backups.
- Keep `.env` outside shared ZIPs; use the HES-approved secret-management approach for unattended tasks.
- Do not run multiple Nuctracker instances simultaneously.
- Verify the synchronization state before the first full run:

  ```cmd
  .venv\Scripts\python.exe rte_autosync.py status
  ```

- If synchronization has **not** been initialized, request the approved initialization procedure from the MongoDB administrator. It requires a verified MongoDB backup and explicit authorization for future automated writes. **Do not reset an existing checkpoint merely to start a new run.**

## 5. Daily operation

### 5.1 Recommended: interactive full update

Double-click:

```text
run_nuctracker_full.bat
```

This is the **current routine launcher**. Do **not** use the legacy `run_nuctracker.bat`, which belongs to the pre-v4 workflow.

After any required **one-time annual-table migration** has been completed and verified, the terminal asks for an optional start date:

```text
HES NUCTRACKER - FULL UPDATE
Start date for the update (YYYY-MM-DD, Enter = automatic):
```

- Press **Enter** for the routine automatic update.
- Enter a date such as `2026-11-12` only when you want to extend the re-read period for **actual RTE generation**.
- The terminal shows the effective period, progress through the six steps and the annual production summary.
- The window remains open at the end so the result can be read.

The exact on-screen wording may be in French, depending on the installed launcher.

For a command-line full run **without the interactive date prompt**, use the orchestrator directly:

```cmd
.venv\Scripts\python.exe nuctracker_run_full.py --publish
```

This command **writes to MongoDB and publishes Datawrapper** when successful. Use the BAT for normal, guided operation.

### 5.2 The six execution stages

| Stage | Operation | Typical evidence |
| --- | --- | --- |
| 1/6 | Check configuration and RTE/MongoDB/Datawrapper access | Access checks / errors |
| 2/6 | Synchronize new nuclear-unavailability publications | RTE batches and audit CSVs |
| 3/6 | Refresh complete days of actual nuclear generation | Daily production report / MongoDB update |
| 4/6 | Calculate corrected availability and HES SARIMAX forecasts | Nuclear production summary in TWh |
| 5/6 | Generate and validate local CSVs | `[QA] passed` and JSON report |
| 6/6 | Verify the annual destination schema; back up, upload, verify and publish **three** Datawrapper datasets | `[DW] SUCCESS: graph, monthly table and annual production table published` |

**Publication is not considered complete** until the terminal shows success for the Datawrapper stage and the launcher exits with code `0`. In v4.2.1, an unmigrated legacy annual table deliberately **blocks** stage 6; do the controlled migration in Section 4.3 instead of rerunning the complete pipeline.

### 5.3 Controlled run without Datawrapper publication

Use the supplied launcher:

```text
run_nuctracker_full_sans_publication.bat
```

Or, from CMD in the project directory:

```cmd
.venv\Scripts\python.exe nuctracker_run_full.py
```

These modes **do not publish Datawrapper**, but they **can still write new RTE events and production days to MongoDB**. They are **not** fully read-only tests. Use a separate test environment if no database writes are permissible.

### 5.4 Scheduled execution

For Windows Task Scheduler, use:

```text
run_nuctracker_full_planifie.bat
```

The interactive BAT waits for a date and pauses at the end, so it is **not appropriate for unattended scheduling**. Check that the scheduled account has network access, a valid Python environment and secure non-interactive access to its credentials.

## 6. How date selection and synchronization work

**The start date entered in the terminal is not a universal RTE refresh date.** Two distinct mechanisms operate in parallel.

### 6.1 Nuclear unavailability: publication-date checkpoint

- The new mechanism follows RTE **`PUBLICATION_DATE`**, not just `EVENT_DATE`.
- It retrieves newly published versions of existing or new nuclear events, preserves previously stored versions and excludes other fuels.
- It maintains a checkpoint showing the last successfully processed publication period, with a safety overlap.
- A selected generation-refresh date **does not override or narrow this checkpoint**.
- If RTE has accumulated a large backlog, the pipeline may process several batches and **stop before forecasting/publication**. The message `Backlog only partly processed` means that the run must be repeated to resume from the checkpoint. Inspect progress if it repeatedly stops at exactly the same checkpoint.
- Do not recreate the legacy `EVENT_DATE` refresh behavior or assume that a recent event-date query guarantees complete version history.

### 6.2 Actual generation: complete French calendar days

- The interactive date requests a **re-read of actual generation** from RTE.
- The automatic end is the **previous complete French calendar day**; the current partial day is not included.
- A safety overlap of **at least 10 days** is retained to catch late RTE corrections, even if a more recent date is entered.
- The effective period, which may start earlier than the requested date, is printed in the terminal.
- The routine interactive window is limited to **35 days**; longer historical backfills require a separate controlled procedure.
- The updated production days are stored in `data.nuclear_production_auto_daily`. Historical observations in `data.nuclear_production` are retained.

French local-day boundaries matter, including daylight-saving transitions (23- and 25-hour days). For a comparison with RTE, align the indicator, timestamp and time zone.

### 6.3 Forecast dates

The start-date prompt **does not** select the SARIMAX forecast start or change the HES forecasting methodology. The forecast period follows the existing model logic and the execution date.

## 7. Outputs and Datawrapper publications

### 7.1 Local output directories

The current pipeline uses these directories:

```text
nuctracker_pipeline/
|-- outputs_full/
|   |-- nuctracker_dw_prod_vs_unavs.csv
|   |-- nuctracker_dw_prod_prediction.csv
|   |-- tracker_forecast_graph_YYYY-MM-DD_HH-MM.csv
|   |-- tracker_forecast_table_YYYY-MM-DD_HH-MM.csv
|   `-- tracker_annual_production_YYYY-MM-DD.csv
|-- outputs_diagnostic/
|   |-- full_pipeline_qa_*.json
|   |-- production_rte_*.csv
|   `-- auto_rte_pub_*.csv
|-- logs/
|   |-- full_pipeline_latest.log
|   `-- full_pipeline_YYYYMMDD_HHMMSS_*.log
|-- backups_datawrapper/
`-- backups_production_auto/
```

Filenames may contain timestamps or additional diagnostic suffixes. Retain the most recent files for audit purposes.

### 7.2 Datawrapper destinations

| Destination | Generated output | Publication status |
| --- | --- | --- |
| Availability / production chart | `tracker_forecast_graph_...csv` | Automatic with `--publish` |
| Monthly forecast table | `tracker_forecast_table_...csv` | Automatic with `--publish` |
| **Annual nuclear production table** | `tracker_annual_production_YYYY-MM-DD.csv` | **Automatic in v4.2.1 after one-time annual schema migration**, when applicable |

In v4.2.1, all three remote datasets are normalized from CSV/TSV when necessary and backed up before publication; new data are uploaded and verified before the charts are published. The third chart is checked against the **new annual schema** before the three-chart publication begins. **Legacy annual rows are not migrated during a normal full run.**

### 7.3 Annual production summary

The annual table uses the **same realized-production and SARIMAX forecast DataFrames** as the HES production summary. The model produces four values:

1. Realized nuclear production from the beginning of the current year to the last available complete day, in **TWh**.
2. HES forecast production for the remainder of the current year, in **TWh**.
3. Current-year realized-plus-forecast total, in **TWh**.
4. Forecast nuclear production for the following calendar year, in **TWh**.

The annual Datawrapper CSV has exactly these four columns, in this order:

```text
Year
Realized Production (Up to YYYY-MM-DD)
HES Production Forecast (As of YYYY-MM-DD)
Total
```

It has two rows: `Cal YYYY [TWh]` and `Cal YYYY+1 [TWh]`. The following year's realized value is zero until that year begins. CSV values are written to **two decimal places**; Datawrapper's display formatting may use fewer decimals.

**Do not confuse** the RTE production-realized cutoff with the forecast-as-of date. Their dates may differ because the last RTE complete day usually precedes the run date.

## 8. Quality checks and monitoring

### 8.1 After every run

Confirm all of the following:

1. The latest RTE unavailability checkpoint advanced to the expected publication window (or no new records were available).
2. The production report includes the latest **complete** RTE calendar day and no unexplained gaps.
3. The HES annual production summary has a consistent realized / forecast split.
4. The QA report contains `[QA] passed` and has no failed annual coverage checks.
5. All **three** Datawrapper charts were successfully published, and their data and titles are visually correct; in particular, confirm that the annual table has completed the one-time migration (if needed).

To open the latest interactive-run log from CMD:

```cmd
notepad logs\full_pipeline_latest.log
```

To inspect the checkpoint:

```cmd
.venv\Scripts\python.exe rte_autosync.py status
```

Check `outputs_diagnostic/full_pipeline_qa_*.json`, particularly the `annual_summary` and `annual_csv` fields. **`[QA] passed` checks the local outputs; it does not establish that an unmigrated Datawrapper table is compatible.**

### 8.2 Model and data-quality limits

- The v4.2.1 release **does not change SARIMAX**, the unavailability calculation, the RTE source data, the MongoDB collections or the interactive date logic. It changes Datawrapper I/O validation and the one-time annual dataset migration workflow.
- A `ConvergenceWarning` from SARIMAX indicates a model-fitting issue that should be reviewed; it is not proof that the published forecast is reliable.
- An automatic QA pass is a structural check; it is **not** an independent validation against official reactor-by-reactor availability.
- For reactor-level comparisons, match the same time, availability definition and data-publication cutoff. A declared outage's minimum available capacity is not necessarily the instantaneous operational availability.
- Do not fill missing production days with invented values to bypass the annual-report checks.

## 9. Backups, recovery and rollback

### 9.1 Automatic Datawrapper backup

Before a routine v4.2.1 three-chart publication, the program creates a directory such as:

```text
backups_datawrapper\YYYYMMDD_HHMMSSZ\
|-- MANIFEST.json
|-- <chart_id_1>.csv
|-- <chart_id_2>.csv
`-- <chart_id_3>.csv
```

Each backup contains the **existing remote datasets** before the upload, normalized as CSV even if Datawrapper supplied TSV. Routine v4.2.1 backups cover three charts; the restoration script also accepts older v4.1 backups containing two charts. **The one-time migration has a different, annual-only backup directory and restore script (Section 9.3).**

**Datawrapper publishing is not transactional.** If one upload or publication fails, v4.2.1 attempts a **best-effort restoration** of the affected datasets, but a partially updated chart may remain. Always inspect the chart editor and published output after an error.

### 9.2 Manual Datawrapper restore

From the project folder, with the **actual** existing backup directory:

```cmd
.venv\Scripts\python.exe restore_datawrapper_full.py --backup-dir "backups_datawrapper\YYYYMMDD_HHMMSSZ"
```

The script requests the exact confirmation:

```text
RESTAURER DATAWRAPPER
```

It re-uploads the backed-up data and republishes the corresponding charts. If columns were renamed, some Datawrapper table-formatting settings may need manual repair. **This procedure does not undo any MongoDB changes.**

### 9.3 Rollback of the one-time annual-table migration

The one-time annual migration produces a dedicated backup directory, for example:

```text
backups_datawrapper\annual_migration_YYYYMMDD_HHMMSS_XXXXXXZ\
|-- MANIFEST.json
|-- chart_metadata.json
|-- <annual_chart_id>.raw
`-- <annual_chart_id>.csv
```

To restore **only the previous annual dataset**, use the **actual** directory displayed during the migration:

```cmd
.venv\Scripts\python.exe restore_migration_annuelle.py --backup-dir "backups_datawrapper\annual_migration_YYYYMMDD_HHMMSS_XXXXXXZ"
```

Type exactly **`RESTAURER ANNUEL`** to confirm. This re-uploads and republishes the old annual CSV. It does **not** change the graph, the monthly table, MongoDB or RTE, and it does **not** undo manual Datawrapper styling/metadata changes. Review the restored chart visually. Use the regular `restore_datawrapper_full.py` only for standard two-/three-chart pipeline backups, **not** as a substitute for the migration-specific rollback.

### 9.4 MongoDB backups and recovery

- Keep verified MongoDB backups and their manifests in secure storage.
- Keep production-update backups in `backups_production_auto/` where applicable.
- Do not delete the working checkpoint or the historical event versions to address an ordinary synchronization failure.
- Coordinate any MongoDB rollback with the database owner; check the consistency of the RTE publication checkpoint and the affected collections before restoring.
- Treat any `.partial` backup as incomplete until independently verified; never substitute it for a validated recovery archive.

## 10. Project files and maintenance

### 10.1 Files required for normal operation

| File / directory | Purpose |
| --- | --- |
| `run_nuctracker_full.bat` | Interactive double-click entry point |
| `nuctracker_manual.py` | Date prompt, live terminal output and log handling |
| `nuctracker_run_full.py` | Main six-stage orchestrator |
| `rte_autosync.py` and `rte_publication_sync.py` | Nuclear publication-date synchronization |
| `rte_production_sync.py` | Actual generation refresh and safety overlap |
| `nuctracker_corrected.py` and `nucmonitor.py` | Reactor availability calculations |
| `nuctracker_final_pipeline.py` | Existing HES forecasting and legacy calculation functions reused by v4 |
| `nuctracker_annual_report.py` | v4.2 annual TWh summary and CSV generation (unchanged by v4.2.1) |
| `.env` | Local credentials and chart configuration: **do not share** |
| `.venv/` | Python environment used by the launcher |

### 10.2 Keep for maintenance and recovery

- `run_nuctracker_full_sans_publication.bat` and `run_nuctracker_full_planifie.bat` (as applicable).
- `restore_datawrapper_full.py` (v4.2.1), `migrate_annual_datawrapper_once.py`, `restore_migration_annuelle.py`, optional `run_migration_tableau_annuel.bat`, production restore scripts and MongoDB backup scripts.
- `requirements.txt`, optional v4 add-on requirements and `setup_project.bat`.
- `tests/`, the newest documentation and recent QA reports.
- `logs/`, `outputs_full/`, `outputs_diagnostic/` and verified backup folders.

### 10.3 Legacy components

The following belong to earlier workflows and are **not** the primary v4.2.1 entry point:

- `run_nuctracker.bat` and its old `--skip-upload` / `--refresh-start` / `--refresh-end` workflow.
- `nuctracker_run_auto.py` and old synchronization-only launchers.
- One-off shadow-comparison scripts and historical reference snapshots.
- Old `outputs/` contents and duplicated legacy scripts.

Archive these only after confirming that no external job, scheduled task or recovery procedure still references them. Keep the current backup and restore tools.

## 11. Troubleshooting

| Symptom / message | What to check |
| --- | --- |
| `NOT INITIALIZED` from `rte_autosync.py status` | The new RTE publication sync was not authorized/initialized. Use the administrator-approved verified-backup workflow. |
| `Backlog only partly processed` | The publication checkpoint is progressing through outstanding RTE batches. Re-run the normal update; publication is intentionally blocked until the backlog is complete. If the checkpoint does not advance, inspect the log. |
| Missing `DATAWRAPPER_ANNUAL_TABLE_ID` | Add the **existing annual table's** exact chart ID to `.env`. Do not use the monthly-table ID. |
| `Annual Datawrapper ID ... is not a table` | Confirm the third ID belongs to the approved **annual table** (not the graph or monthly table). Do not bypass validation. |
| `Third Datawrapper chart does not match the expected columns (different/legacy annual schema)` | The existing annual table still has the old format (or an incorrect ID). Run the v4.2.1 migration **preview**, check the exact remote title/headers/rows, then migrate with approval. Do not repeatedly rerun the whole pipeline. |
| Annual chart appears to contain **one column** with tab characters | Datawrapper may have returned TSV instead of CSV. Install the v4.2.1 delimiter-handling patch; do not edit or delete the chart to work around the old parser. |
| Annual-table migration refuses to run | Verify the expected chart title `Nuclear Production (Realized and Forecasted)`, the approved chart ID, local annual CSV and the legacy/current schema. The script deliberately blocks unknown chart formats. |
| `Annual realized production has ... missing calendar days` | Identify missing calendar days in actual production and check RTE/MongoDB completeness before publishing. |
| `Annual realized-production date does not match MongoDB latest production date` | Check the date loading and reconciliation between production data and the annual summary. |
| `Permission denied` while writing an output CSV | Close any CSV open in Excel or another application; check directory/file permissions. |
| Missing Python module (for example, `holidays`) | Install the project's dependencies into the interpreter actually used by the launcher. |
| MongoDB authentication/network error | Confirm authorized account, host name, network/IP access and collection permissions. |
| RTE 401/403 or empty API response | Check API credentials, subscriptions, requested period and relevant logs. |
| Datawrapper 401/403/404 | Check the token, chart IDs and workspace editing permissions. |
| `Datawrapper ... RESTORE FAILED` | Check whether one or more charts were partially modified; use the correct **routine or migration-specific** backup and involve the administrator. |
| SARIMAX `ConvergenceWarning` | Treat as a forecast-quality warning; investigate model fitting separately. |
| Python / NumPy `DeprecationWarning` | Usually non-blocking for the current run, but track for future dependency upgrades. |

When a run fails, first inspect `logs/full_pipeline_latest.log`. **MongoDB may have been updated before the error**, even if none of the Datawrapper charts was published. Do not infer a full rollback from an unsuccessful exit code.

## 12. Model scope, limitations and version history

### 12.1 What did not change

The newer synchronization and reporting releases **do not replace the HES SARIMAX forecast** with an RTE forecast. Actual generation is obtained from RTE, while future generation is estimated by the HES model. Known model-quality limitations, including residual reactor-availability differences, require separate investigation.

### 12.2 Release summary

| Version | Main development | Operational effect |
| --- | --- | --- |
| Original / pre-v4 | Event-date-oriented refresh, production forecast and two Datawrapper datasets | Manual refresh window and older launcher |
| v4 | Automatic nuclear publication-date synchronization, corrected availability calculation and actual-generation refresh | Incremental MongoDB updates and improved collection coverage |
| v4.1 | Interactive start date, live six-stage progress and logs | Routine double-click experience restored; date prompt affects actual-generation reread |
| v4.2 | Annual production report/CSV and third Datawrapper table | Annual TWh values shown in terminal; third chart requires compatible structure |
| **v4.2.1** | Robust CSV/TSV remote-data handling and supervised annual-table migration with dedicated backup/restore | **One-time approved migration** of the existing annual chart; thereafter guarded automatic three-chart publication |

The patch documents report locally simulated tests, not an external certification of RTE, MongoDB or Datawrapper correctness. Re-test the assembled installation after changes.

### 12.3 Superseded instructions

Do **not** follow the original README's blanket rule that the pipeline must be manually refreshed every 14 days using `run_nuctracker.bat --refresh-start ... --refresh-end ...`. That instruction applied to the older data-refresh approach. The v4.2.1 routine uses a publication-date checkpoint and can process outstanding batches across successive runs; the production reread uses its own complete-day window.

## 13. Ownership and references

Complete these fields with HES-approved information rather than inventing owners or access rights:

| Responsibility | Contact / location |
| --- | --- |
| Project owner | `[HES owner / team]` |
| Technical maintainer | `[Name or distribution list]` |
| Repository | `[Approved repository URL]` |
| MongoDB administrator | `[Owner / support contact]` |
| Datawrapper workspace | `[Workspace / responsible team]` |
| RTE API subscription owner | `[Owner / support contact]` |

The repository may still contain older documentation under `docs/` (installation, configuration, user guide, data management, function reference and maintenance). Consult it for underlying implementation details, but **prefer this v4.2.1 README for the current operational entry point, one-time migration and publication procedure**.

---

**Document maintenance:** Update this README whenever the launcher, MongoDB schemas, synchronization checkpoints, forecast logic or Datawrapper destinations change. Record whether the annual Datawrapper migration has actually been completed and visually verified; the presence of v4.2.1 files alone does not prove it. Keep credentials and customer-sensitive datasets out of documentation and source control.
