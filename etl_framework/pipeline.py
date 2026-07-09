"""
Pipeline entry point.

Loads the active profile from settings.ACTIVE_PROFILE, then:
  - Instantiates the source/destination connectors from the profile
  - Instantiates the ETL engine with profile-injected settings
  - Runs a single ETL cycle or starts the appropriate orchestrator
"""

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from pymongo import errors as mongo_errors

from etl_framework.connectors.base import BaseDestinationConnector, BaseSourceConnector
from etl_framework.engine.core import ETLEngine
from etl_framework.logger import audit, configure_logging, get_logger
from etl_framework.profile_loader import ProfileConfig, load_profile
from etl_framework.settings import ACTIVE_PROFILE


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def _bootstrap(profile_path: str) -> ProfileConfig:
    """Load profile first, then configure logging from it."""
    profile = load_profile(profile_path)
    configure_logging(
        log_dir=profile.logging.log_dir,
        retention_days=profile.logging.file_retention_days,
    )
    return profile


# ── Connector factories ───────────────────────────────────────────────────────

def build_source(profile: ProfileConfig) -> BaseSourceConnector:
    src = profile.source
    if src.type == "excel":
        from etl_framework.connectors.sources.excel import ExcelConnector
        return ExcelConnector(src.excel.path)
    if src.type == "google_sheets":
        from etl_framework.connectors.sources.google_sheets import GoogleSheetsConnector
        return GoogleSheetsConnector(
            src.google_sheets.spreadsheet_id,
            src.google_sheets.service_account_json,
        )
    if src.type == "csv":
        from etl_framework.connectors.sources.csv import CsvConnector
        return CsvConnector(
            path=src.csv.path,
            sheet_name=src.csv.sheet_name,
            encoding=src.csv.encoding,
        )
    raise ValueError(f"Unsupported source type: {src.type!r}")


def build_destination(profile: ProfileConfig) -> BaseDestinationConnector:
    dest = profile.destination
    perf = profile.performance
    if dest.type == "mongodb":
        from etl_framework.connectors.destinations.mongodb import MongoDBConnector
        pool = perf.connection_pooling
        return MongoDBConnector(
            uri=dest.mongodb.uri,
            database=dest.mongodb.database,
            timeout_ms=dest.mongodb.timeout_ms,
            batch_size=perf.batch_size,          # performance.batch_size overrides dest value
            max_pool_size=pool.max_pool_size,
            min_pool_size=pool.min_pool_size,
            max_idle_time_ms=pool.max_idle_time_ms,
        )
    raise ValueError(f"Unsupported destination type: {dest.type!r}")


def build_engine(profile: ProfileConfig) -> ETLEngine:
    eng = profile.engine
    return ETLEngine(
        alias_map=eng.column_aliases,
        upsert_candidates=eng.upsert_key_candidates,
        transforms=eng.column_transforms,
        validations=eng.column_validations,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _collection_name(source_name: str) -> str:
    """Derive the MongoDB collection name from the source file path.
    Strips directory and extension so 'C:/data/FC Stats WAIS.xlsx' → 'FC Stats WAIS'.
    Google Sheets sources use the spreadsheet ID as-is.
    """
    from pathlib import Path
    stem = Path(source_name).stem
    return stem if stem else source_name


def _source_chunk_size(profile: ProfileConfig) -> int:
    """
    Return the effective chunk_size for this cycle.
    performance.effective_chunk_size resolves streaming flag + chunk_size + fallbacks.
    Google Sheets has no file to stream so always returns 0.
    """
    if profile.source.type == "google_sheets":
        return 0
    return profile.performance.effective_chunk_size


def _accumulate(agg, result) -> None:
    """Add a single-sheet LoadResult into the cycle aggregate."""
    agg.inserted    += result.inserted
    agg.updated     += result.updated
    agg.skipped_hash += result.skipped_hash
    agg.skipped_key  += result.skipped_key
    agg.total        += result.total


# ── Parallel path (chunk_size == 0) ───────────────────────────────────────────

def _run_sheet(sheet, engine, destination, workbook, imported_at, collection) -> tuple:
    """
    Process and load one full sheet in a ThreadPoolExecutor worker.
    Engine reads only immutable config; MongoDBConnector uses a shared thread-safe
    client, so concurrent calls across sheets are safe and idempotent.
    Returns (LoadResult, n_invalid) for cycle-level aggregation.
    """
    valid_docs, invalid_docs = engine.process_sheet(sheet, workbook, imported_at)
    result = destination.load(valid_docs, collection=collection)
    if invalid_docs:
        destination.load(invalid_docs, collection="validation_errors")
    return result, len(invalid_docs)


def _run_parallel(sheets, engine, destination, workbook, imported_at,
                  max_workers, collection) -> tuple:
    """Fan out full sheets to a thread pool. Returns (aggregate LoadResult, total_invalid)."""
    from etl_framework.models import LoadResult as _LR
    agg           = _LR(total=0, collection=collection)
    total_invalid = 0
    # max_workers=0 → pass None, letting ThreadPoolExecutor auto-size.
    workers = max_workers or None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_run_sheet, sheet, engine, destination,
                        workbook, imported_at, collection): sheet.name
            for sheet in sheets
        }
        for future in as_completed(futures):
            sheet_result, n_invalid = future.result()   # re-raises on worker exception
            _accumulate(agg, sheet_result)
            total_invalid += n_invalid

    return agg, total_invalid


# ── Chunked path (chunk_size > 0) ─────────────────────────────────────────────

def _run_chunked(source, engine, destination, workbook, imported_at,
                 chunk_size, collection) -> tuple:
    """
    Stream all sheets in row-level chunks, processing and loading each chunk
    immediately to keep peak memory bounded at O(chunk_size) rows.

    Chunks from the same sheet are guaranteed to be yielded contiguously by
    the connector, so per-sheet row offsets can be tracked with a simple dict.
    row_number is computed as 2 + offset + position-in-chunk, which is identical
    to what the parallel path would produce if the sheet were processed whole —
    ensuring record_key stability when toggling between the two modes (provided
    the sheet has a natural upsert key column; fallback row-number keys will
    differ if the valid/invalid split changes between runs).

    This path is intentionally sequential: memory efficiency and parallelism are
    orthogonal optimisations. Use chunk_size=0 + max_workers>1 for speed; use
    chunk_size>0 + max_workers=1 for memory. Both can coexist in different profiles.
    """
    from etl_framework.models import LoadResult as _LR
    agg             = _LR(total=0, collection=collection)
    total_invalid   = 0
    valid_offsets:   dict[str, int] = {}
    invalid_offsets: dict[str, int] = {}

    for chunk in source.extract_chunks(chunk_size):
        sname = chunk.name
        v_off = valid_offsets.get(sname, 0)
        i_off = invalid_offsets.get(sname, 0)

        valid_docs, invalid_docs = engine.process_sheet(
            chunk, workbook, imported_at, v_off, i_off,
        )

        result = destination.load(valid_docs, collection=collection)
        if invalid_docs:
            destination.load(invalid_docs, collection="validation_errors")

        valid_offsets[sname]   = v_off + len(valid_docs)
        invalid_offsets[sname] = i_off + len(invalid_docs)

        _accumulate(agg, result)
        total_invalid += len(invalid_docs)

    return agg, total_invalid


# ── Single ETL cycle ──────────────────────────────────────────────────────────

def run_etl(profile: ProfileConfig, trigger: str = "scheduled") -> None:
    t_run       = time.monotonic()
    source      = build_source(profile)
    destination = build_destination(profile)
    engine      = build_engine(profile)
    chunk_size  = _source_chunk_size(profile)
    collection  = _collection_name(source.source_name)

    audit("etl_start", workbook=source.source_name, trigger=trigger)

    # Single imported_at stamp for the entire cycle — all documents produced in
    # this run (regardless of sheet or chunk) carry the same timestamp.
    imported_at = datetime.now(timezone.utc).isoformat()

    t0 = time.monotonic()

    perf = profile.performance

    if chunk_size > 0:
        # Memory-efficient path: stream one chunk at a time, load immediately.
        audit("etl_mode", workbook=source.source_name,
              trigger=f"chunked/chunk_size={chunk_size}")
        agg, total_invalid = _run_chunked(
            source, engine, destination,
            source.source_name, imported_at, chunk_size, collection,
        )
    else:
        # Speed-optimised path: load all sheets up front, fan out to thread pool.
        # parallel_processing=False forces worker_count=1 (sequential).
        sheets = source.extract()
        audit("extraction_complete", workbook=source.source_name,
              rows_total=len(sheets), elapsed_ms=int((time.monotonic() - t0) * 1000))
        workers = perf.effective_workers if perf.parallel_processing else 1
        audit("etl_mode", workbook=source.source_name,
              trigger=f"parallel/workers={workers or 'auto'}")
        agg, total_invalid = _run_parallel(
            sheets, engine, destination,
            source.source_name, imported_at,
            workers, collection,
        )

    audit(
        "etl_complete",
        workbook=source.source_name,
        trigger=trigger,
        rows_total=agg.total + total_invalid,
        rows_valid=agg.total,
        rows_invalid=total_invalid,
        inserted=agg.inserted,
        updated=agg.updated,
        skipped=agg.skipped_hash + agg.skipped_key,
        elapsed_ms=int((time.monotonic() - t_run) * 1000),
    )


# ── Safe wrapper ──────────────────────────────────────────────────────────────

def _safe_run_etl(profile: ProfileConfig, trigger: str = "unknown") -> None:
    try:
        run_etl(profile, trigger=trigger)
    except mongo_errors.ServerSelectionTimeoutError:
        audit("fatal_error", trigger=trigger,
              error="Cannot reach MongoDB — is mongod running?", level="error")
    except FileNotFoundError as exc:
        audit("fatal_error", trigger=trigger, error=f"File not found: {exc}", level="error")
    except Exception as exc:
        audit("fatal_error", trigger=trigger,
              error=f"{type(exc).__name__}: {exc}", level="error")


# ── Excel orchestrator (watchdog) ─────────────────────────────────────────────

def _start_excel_watcher(profile: ProfileConfig) -> None:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    watch_path     = Path(profile.source.excel.path).resolve()
    debounce_secs  = profile.orchestration.excel_debounce_seconds
    _timer: list[threading.Timer] = [None]

    class _Handler(FileSystemEventHandler):
        def on_modified(self, event):
            if Path(event.src_path).resolve() != watch_path:
                return
            if _timer[0] is not None:
                _timer[0].cancel()
            _timer[0] = threading.Timer(
                debounce_secs, _safe_run_etl,
                kwargs={"profile": profile, "trigger": "excel file-save event"},
            )
            _timer[0].start()

    observer = Observer()
    observer.schedule(_Handler(), path=str(watch_path.parent), recursive=False)
    observer.start()
    audit("watcher_start", workbook=str(watch_path), trigger="watchdog")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# ── Google Sheets orchestrator (hash poller) ──────────────────────────────────

def _start_sheets_poller(profile: ProfileConfig) -> None:
    from etl_framework.connectors.sources.google_sheets import GoogleSheetsConnector
    gs        = profile.source.google_sheets
    connector = GoogleSheetsConnector(gs.spreadsheet_id, gs.service_account_json)
    interval  = profile.orchestration.poll_interval_seconds
    last_hash = None
    logger    = get_logger()

    while True:
        cycle_start = time.time()
        try:
            current_hash = connector.fingerprint()
            if current_hash == last_hash:
                audit("poll_no_change", trigger="hash_match")
            else:
                audit("poll_change_detected", trigger="hash_mismatch")
                _safe_run_etl(profile, trigger="sheets content change")
                last_hash = current_hash
        except Exception as exc:
            audit("poll_error", error=f"{type(exc).__name__}: {exc}", level="error")

        sleep_for = max(0, interval - (time.time() - cycle_start))
        logger.debug("event=poll_sleep | seconds=%.1f", sleep_for)
        time.sleep(sleep_for)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Allow overriding profile path via CLI: python -m etl_framework.pipeline profiles/other.json
    profile_path = sys.argv[1] if len(sys.argv) > 1 else ACTIVE_PROFILE

    try:
        profile = _bootstrap(profile_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"[FATAL] {exc}")
        sys.exit(1)

    logger = get_logger()
    audit("startup", workbook=profile.source.excel.path
          if profile.source.type == "excel" else profile.source.google_sheets.spreadsheet_id
          if profile.source.type == "google_sheets" else profile.source.csv.path,
          trigger=profile.source.type)
    logger.info("Profile:     %s — %s", profile.profile_name, profile.description)
    logger.info("Destination: %s%s", profile.destination.mongodb.uri, profile.destination.mongodb.database)
    logger.info("Press Ctrl+C to stop.")

    if profile.source.type == "excel":
        logger.info("Mode: event-driven (watchdog) | debounce: %ss",
                    profile.orchestration.excel_debounce_seconds)
        _safe_run_etl(profile, trigger="startup")
        _start_excel_watcher(profile)

    elif profile.source.type == "google_sheets":
        logger.info("Mode: hash-based poller | interval: %ss",
                    profile.orchestration.poll_interval_seconds)
        _start_sheets_poller(profile)

    elif profile.source.type == "csv":
        logger.info("Mode: single run")
        _safe_run_etl(profile, trigger="startup")
