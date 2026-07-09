"""
Profile loader — reads, validates, and exposes a typed ProfileConfig.

Usage:
    from etl_framework.profile_loader import load_profile
    profile = load_profile("profiles/fc_stats_wais.json")
"""

import json
from dataclasses import dataclass, field
from pathlib import Path


# ── Typed sub-configs ─────────────────────────────────────────────────────────

@dataclass
class ExcelSourceConfig:
    path: str
    chunk_size: int = 0   # 0 = load full file at once (pandas); >0 = openpyxl row streaming

@dataclass
class GoogleSheetsSourceConfig:
    spreadsheet_id: str
    service_account_json: str

@dataclass
class CsvSourceConfig:
    path: str
    sheet_name: str = "csv"
    encoding: str   = "utf-8"
    chunk_size: int = 0   # 0 = load full file at once; >0 = pandas chunksize streaming

@dataclass
class SourceConfig:
    type: str                                              # "excel" | "google_sheets" | "csv"
    excel:         ExcelSourceConfig         | None = None
    google_sheets: GoogleSheetsSourceConfig  | None = None
    csv:           CsvSourceConfig           | None = None

@dataclass
class MongoDBDestinationConfig:
    uri: str
    database: str
    timeout_ms: int = 5000
    batch_size: int = 0   # 0 = single bulk_write (no batching)

@dataclass
class DestinationConfig:
    type: str                                              # "mongodb"
    mongodb: MongoDBDestinationConfig | None = None

@dataclass
class EngineConfig:
    upsert_key_candidates: list[str]
    column_aliases:        dict[str, str]
    column_transforms:     dict[str, str]
    column_validations:    dict[str, list[dict]]

@dataclass
class OrchestrationConfig:
    poll_interval_seconds:  int = 60
    excel_debounce_seconds: int = 3
    # Kept for backward compatibility. performance.worker_count takes precedence
    # when a performance section is present.
    max_workers:            int = 1

@dataclass
class ConnectionPoolingConfig:
    max_pool_size:    int = 10     # maximum open connections in the MongoClient pool
    min_pool_size:    int = 1      # connections kept alive between cycles
    max_idle_time_ms: int = 30000  # close idle connections after this many ms

@dataclass
class PerformanceConfig:
    """
    Single authoritative source for all performance knobs.

    When a performance section is present in the profile its values take
    precedence over the legacy per-section fields (orchestration.max_workers,
    source.*.chunk_size, destination.mongodb.batch_size).  Profiles that omit
    this section continue to work unchanged.

    Defaults reproduce the existing behaviour:
        parallel_processing = True   (parallel was the default path)
        worker_count        = 1      (single worker → sequential, matching old default)
        streaming           = False  (full-load path)
        chunk_size          = 0      (resolved to 5000 automatically when streaming=True)
        batch_size          = 0      (single bulk_write, no batching)
        connection_pooling  = ConnectionPoolingConfig()
    """
    parallel_processing: bool               = True
    worker_count:        int                = 1
    streaming:           bool               = False
    chunk_size:          int                = 0
    batch_size:          int                = 0
    connection_pooling:  ConnectionPoolingConfig = field(
        default_factory=ConnectionPoolingConfig
    )

    @property
    def effective_chunk_size(self) -> int:
        """
        Returns the chunk size the pipeline should actually use.
        - streaming=False and chunk_size=0  →  0  (full-load, no chunking)
        - streaming=True  and chunk_size=0  →  5000 (safe default)
        - streaming=True  and chunk_size>0  →  chunk_size (explicit)
        - streaming=False and chunk_size>0  →  chunk_size (explicit override)
        """
        if self.streaming and self.chunk_size == 0:
            return 5000
        return self.chunk_size

    @property
    def effective_workers(self) -> int:
        """Returns None when worker_count is 0, letting ThreadPoolExecutor auto-size."""
        return self.worker_count or None

@dataclass
class LoggingConfig:
    log_dir:             str = "logs"
    file_retention_days: int = 90

@dataclass
class ProfileConfig:
    profile_name: str
    description:  str
    source:       SourceConfig
    destination:  DestinationConfig
    engine:       EngineConfig
    orchestration: OrchestrationConfig
    performance:  PerformanceConfig
    logging:      LoggingConfig


# ── Validation ────────────────────────────────────────────────────────────────

_SUPPORTED_SOURCES      = {"excel", "google_sheets", "csv"}
_SUPPORTED_DESTINATIONS = {"mongodb"}

class ProfileValidationError(ValueError):
    """Raised when a profile file is missing required fields or has invalid values."""


def _require(obj: dict, key: str, context: str) -> object:
    if key not in obj:
        raise ProfileValidationError(f"Profile missing required field '{key}' in {context}")
    return obj[key]


def _validate(raw: dict) -> None:
    _require(raw, "profile_name", "root")
    _require(raw, "source", "root")
    _require(raw, "destination", "root")
    _require(raw, "engine", "root")

    source_type = _require(raw["source"], "type", "source")
    if source_type not in _SUPPORTED_SOURCES:
        raise ProfileValidationError(
            f"source.type {source_type!r} is not supported. "
            f"Supported: {sorted(_SUPPORTED_DESTINATIONS)}"
        )

    dest_type = _require(raw["destination"], "type", "destination")
    if dest_type not in _SUPPORTED_DESTINATIONS:
        raise ProfileValidationError(
            f"destination.type {dest_type!r} is not supported. "
            f"Supported: {sorted(_SUPPORTED_DESTINATIONS)}"
        )

    engine = raw["engine"]
    for key in ("upsert_key_candidates", "column_aliases", "column_transforms", "column_validations"):
        _require(engine, key, "engine")

    if source_type == "excel":
        excel = raw["source"].get("excel", {})
        if not excel.get("path"):
            raise ProfileValidationError("source.excel.path is required when source.type is 'excel'")

    if source_type == "google_sheets":
        gs = raw["source"].get("google_sheets", {})
        _require(gs, "spreadsheet_id", "source.google_sheets")
        _require(gs, "service_account_json", "source.google_sheets")

    if dest_type == "mongodb":
        mongo = raw["destination"].get("mongodb", {})
        _require(mongo, "uri", "destination.mongodb")
        _require(mongo, "database", "destination.mongodb")


# ── Builder ───────────────────────────────────────────────────────────────────

def _build(raw: dict) -> ProfileConfig:
    src_raw  = raw["source"]
    dest_raw = raw["destination"]
    eng_raw  = raw["engine"]
    orch_raw = raw.get("orchestration", {})
    perf_raw = raw.get("performance", {})
    log_raw  = raw.get("logging", {})

    excel_cfg = None
    gs_cfg    = None
    csv_cfg   = None

    if "excel" in src_raw:
        e = src_raw["excel"]
        excel_cfg = ExcelSourceConfig(
            path=e["path"],
            chunk_size=e.get("chunk_size", 0),
        )

    if "google_sheets" in src_raw:
        g = src_raw["google_sheets"]
        gs_cfg = GoogleSheetsSourceConfig(
            spreadsheet_id=g["spreadsheet_id"],
            service_account_json=g["service_account_json"],
        )

    if "csv" in src_raw:
        c = src_raw["csv"]
        csv_cfg = CsvSourceConfig(
            path=c.get("path", ""),
            sheet_name=c.get("sheet_name", "csv"),
            encoding=c.get("encoding", "utf-8"),
            chunk_size=c.get("chunk_size", 0),
        )

    mongo_cfg = None
    if "mongodb" in dest_raw:
        m = dest_raw["mongodb"]
        mongo_cfg = MongoDBDestinationConfig(
            uri=m["uri"],
            database=m["database"],
            timeout_ms=m.get("timeout_ms", 5000),
            batch_size=m.get("batch_size", 0),
        )

    # ── Performance resolution (performance.* > legacy locations > hardcoded default) ──
    #
    # worker_count  : performance.worker_count  > orchestration.max_workers  > 1
    # chunk_size    : performance.chunk_size     > source.<type>.chunk_size   > 0
    # batch_size    : performance.batch_size     > destination.mongodb.batch_size > 0
    #
    # The legacy fields continue to work unchanged in profiles that omit the
    # performance section entirely.

    _src_legacy_chunk = (
        src_raw.get("excel", {}).get("chunk_size", 0)
        or src_raw.get("csv", {}).get("chunk_size", 0)
    )
    _dest_legacy_batch = dest_raw.get("mongodb", {}).get("batch_size", 0)
    _orch_legacy_workers = orch_raw.get("max_workers", 1)

    pool_raw = perf_raw.get("connection_pooling", {})

    performance_cfg = PerformanceConfig(
        parallel_processing=perf_raw.get("parallel_processing", True),
        worker_count=perf_raw.get("worker_count", _orch_legacy_workers),
        streaming=perf_raw.get("streaming", False),
        chunk_size=perf_raw.get("chunk_size", _src_legacy_chunk),
        batch_size=perf_raw.get("batch_size", _dest_legacy_batch),
        connection_pooling=ConnectionPoolingConfig(
            max_pool_size=pool_raw.get("max_pool_size", 10),
            min_pool_size=pool_raw.get("min_pool_size", 1),
            max_idle_time_ms=pool_raw.get("max_idle_time_ms", 30000),
        ),
    )

    return ProfileConfig(
        profile_name=raw["profile_name"],
        description=raw.get("description", ""),
        source=SourceConfig(
            type=src_raw["type"],
            excel=excel_cfg,
            google_sheets=gs_cfg,
            csv=csv_cfg,
        ),
        destination=DestinationConfig(
            type=dest_raw["type"],
            mongodb=mongo_cfg,
        ),
        engine=EngineConfig(
            upsert_key_candidates=eng_raw["upsert_key_candidates"],
            column_aliases=eng_raw["column_aliases"],
            column_transforms=eng_raw["column_transforms"],
            column_validations=eng_raw["column_validations"],
        ),
        orchestration=OrchestrationConfig(
            poll_interval_seconds=orch_raw.get("poll_interval_seconds", 60),
            excel_debounce_seconds=orch_raw.get("excel_debounce_seconds", 3),
            max_workers=_orch_legacy_workers,
        ),
        performance=performance_cfg,
        logging=LoggingConfig(
            log_dir=log_raw.get("log_dir", "logs"),
            file_retention_days=log_raw.get("file_retention_days", 90),
        ),
    )


# ── Public API ────────────────────────────────────────────────────────────────

def load_profile(path: str) -> ProfileConfig:
    """
    Load and validate a profile JSON file.
    Raises ProfileValidationError on missing/invalid fields.
    Raises FileNotFoundError if the profile path does not exist.
    """
    profile_path = Path(path)
    if not profile_path.exists():
        raise FileNotFoundError(f"Profile not found: {profile_path.resolve()}")

    with profile_path.open(encoding="utf-8") as f:
        raw = json.load(f)

    _validate(raw)
    return _build(raw)
