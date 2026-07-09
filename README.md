# DataSync Engine ETL Framework

A local, profile-driven ETL pipeline that synchronises Excel workbooks, CSV files, and Google Sheets into MongoDB. Designed to run entirely on-premise with no admin rights and no inbound network tunnels.

---

## Features

- **All-sheets extraction** — every worksheet is imported automatically; no sheet names are hardcoded
- **Incremental sync** — SHA-256 row fingerprinting skips unchanged records on every run
- **Idempotent writes** — `bulk_write` with `UpdateOne(upsert=True)`; safe to run repeatedly
- **Event-driven triggers** — watchdog file-watcher for Excel saves; MD5 hash poller for Google Sheets
- **Profile-driven config** — all settings live in a single JSON file; zero hardcoded business logic in Python
- **Parallel sheet processing** — `ThreadPoolExecutor` fans out independent sheets concurrently
- **Chunked / streaming reads** — openpyxl row-streaming (Excel) and pandas chunksize (CSV) for large files
- **Configurable connection pool** — MongoDB `MongoClient` created once per process, pool size controlled from the profile
- **Validation routing** — invalid rows go to a separate `validation_errors` collection, never silently dropped
- **Structured logging** — daily rotating log files with key=value audit events

---

## Requirements

| Dependency | Purpose |
|---|---|
| Python 3.10+ | f-strings, `match`, `dataclass` features used throughout |
| pandas + openpyxl | Excel and CSV reading |
| pymongo | MongoDB driver |
| watchdog | Excel file-save event detection |
| MongoDB 6+ | Destination database (zip install, no admin required) |

Install into your conda environment:

```powershell
pip install pandas openpyxl pymongo watchdog
```

---

## Project layout

```
mongodb-windows-x86_64-8.3.4/
│
├── start-mongodb.bat               # Start the local MongoDB server
│
├── profiles/
│   └── Sample_sheet.json          # Active profile — edit this to configure everything
│
├── etl_framework/
│   ├── settings.py                 # Resolves active profile path (ETL_PROFILE env var)
│   ├── profile_loader.py           # Loads + validates JSON → typed ProfileConfig
│   ├── pipeline.py                 # Entry point: bootstraps connectors and runs cycles
│   ├── models.py                   # Shared data contracts: RawSheet, Document, LoadResult
│   ├── logger.py                   # Structured logging + daily rotating files
│   │
│   ├── connectors/
│   │   ├── base.py                 # Abstract base classes for source/destination connectors
│   │   ├── sources/
│   │   │   ├── excel.py            # Pandas full-load or openpyxl streaming
│   │   │   ├── csv.py              # Pandas full-load or chunksize streaming
│   │   │   └── google_sheets.py    # Google Sheets API + MD5 change detection
│   │   └── destinations/
│   │       └── mongodb.py          # Bulk upsert, global client registry, batched writes
│   │
│   └── engine/
│       ├── core.py                 # ETLEngine — stateless, thread-safe sheet processor
│       ├── aliases.py              # Column alias normalisation
│       ├── transforms.py           # Type coercion (string/float/datetime/currency/…)
│       ├── validators.py           # Rule-based validation (required/numeric/range/…)
│       ├── key_detection.py        # Auto upsert-key detection from priority candidates
│       ├── fingerprint.py          # SHA-256 row hash for incremental sync
│       └── sanitizer.py            # NaN / None cleanup before transform
│
├── logs/                           # Created automatically on first run
└── FC Stats WAIS.xlsx              # Source workbook (place here before running)
```

---

## Quickstart

### 1. Start MongoDB

Double-click `start-mongodb.bat`, or from PowerShell:

```powershell
.\start-mongodb.bat
```

Leave that window open. MongoDB listens on `localhost:27017`.

### 2. Activate your environment

```powershell
conda activate db_sync_env
```

### 3. Install dependencies (first time only)

```powershell
pip install pandas openpyxl pymongo watchdog
```

### 4. Place your source file

Copy your Excel workbook into the project root so the path matches the profile:

```
mongodb-windows-x86_64-8.3.4\Sample_Sheet.xlsx
```

### 5. Run the pipeline

```powershell
cd C:\Users\tpe.ojt\Downloads\mongodb-windows-x86_64-8.3.4
python -m etl_framework.pipeline
```

On startup the pipeline:
1. Loads `profiles/fc_stats_wais.json`
2. Runs one full ETL cycle immediately
3. Watches the Excel file for saves and re-syncs automatically on every change

Stop with `Ctrl+C`.

---

## Profile reference

All configuration lives in `profiles/fc_stats_wais.json`. The sections are:

### `source`

```json
"source": {
  "type": "excel",          // "excel" | "csv" | "google_sheets"
  "excel": {
    "path": "FC Stats WAIS.xlsx",
    "chunk_size": 0         // 0 = full load; >0 = row-by-row streaming
  }
}
```

### `destination`

```json
"destination": {
  "type": "mongodb",
  "mongodb": {
    "uri": "mongodb://localhost:27017/",
    "database": "fc_stats_wais",
    "timeout_ms": 5000,
    "batch_size": 500       // rows per bulk_write call (legacy fallback)
  }
}
```

### `engine`

| Key | Purpose |
|---|---|
| `upsert_key_candidates` | Column names tried in order as the record's unique key |
| `column_aliases` | Map variant header names to one canonical name |
| `column_transforms` | Per-column type coercion (`string`, `float`, `datetime`, `percentage`, …) |
| `column_validations` | Per-column validation rules (`required`, `numeric`, `min`, `max`, `allowed_values`, …) |

### `orchestration`

```json
"orchestration": {
  "poll_interval_seconds": 60,    // Google Sheets check frequency
  "excel_debounce_seconds": 3     // wait after a file-save event before triggering
}
```

### `performance`

```json
"performance": {
  "parallel_processing": true,    // process sheets concurrently
  "worker_count": 4,              // thread pool size (0 = auto)
  "streaming": false,             // enable chunk-based reading
  "chunk_size": 0,                // rows per chunk (auto-defaults to 5000 when streaming=true)
  "batch_size": 500,              // rows per bulk_write call
  "connection_pooling": {
    "max_pool_size": 10,
    "min_pool_size": 1,
    "max_idle_time_ms": 30000
  }
}
```

### `logging`

```json
"logging": {
  "log_dir": "logs",
  "file_retention_days": 90
}
```

---

## Switching profiles

Point the pipeline at a different profile without touching code:

```powershell
# Environment variable
$env:ETL_PROFILE = "profiles/another_workbook.json"
python -m etl_framework.pipeline

# Or pass as a CLI argument
python -m etl_framework.pipeline profiles/another_workbook.json
```

---

## Common tuning scenarios

**Large file (100k+ rows) — reduce memory usage**

```json
"performance": {
  "streaming": true,
  "chunk_size": 10000,
  "parallel_processing": false
}
```

**Many small sheets — maximise speed**

```json
"performance": {
  "parallel_processing": true,
  "worker_count": 8,
  "streaming": false
}
```

**Conservative single-threaded run**

```json
"performance": {
  "parallel_processing": false,
  "worker_count": 1,
  "streaming": false
}
```

---

## MongoDB document structure

Every record is stored in the `records` collection with this shape:

```json
{
  "_meta": {
    "workbook":    "SAMPLE_SHEET.xlsx",
    "sheet":       "Sheet1",
    "row_number":  2,
    "imported_at": "2026-07-09T08:00:00+00:00",
    "record_key":  "Sheet1::EMP001",
    "row_hash":    "a3f2..."
  },
  "data": {
    "Employee ID":      "EMP001",
    "Agent Name":       "Juan dela Cruz",
    "Score":            95.5,
    "Transaction Date": "2026-07-01T00:00:00"
  }
}
```

Invalid rows are written to the `validation_errors` collection with an added `validation_errors` array.

---

## Logs

Daily log files are written to `logs/YYYY-MM-DD.log`. Each line is a structured audit event:

```
2026-07-09T08:00:01Z | INFO     | event=etl_start | workbook=FC Stats WAIS.xlsx | trigger=startup
2026-07-09T08:00:02Z | INFO     | event=transform_complete | sheet=Sheet1 | rows_total=500 | rows_valid=498 | rows_invalid=2 | elapsed_ms=120
2026-07-09T08:00:02Z | INFO     | event=load_complete | sheet=records | rows_total=498 | inserted=12 | updated=3 | skipped=483 | elapsed_ms=45
2026-07-09T08:00:02Z | INFO     | event=etl_complete | workbook=FC Stats WAIS.xlsx | inserted=12 | updated=3 | skipped=483 | elapsed_ms=310
```
