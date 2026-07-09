"""
Logging setup — call get_logger() from any module.
audit() emits structured key=value log entries for lifecycle events.

configure_logging() must be called once at startup with values from the loaded profile.
"""

import logging
import logging.handlers
import time
from datetime import datetime, timezone
from pathlib import Path

_configured = False


def configure_logging(log_dir: str = "logs", retention_days: int = 90) -> None:
    """Call once at startup after the profile is loaded."""
    global _configured
    if _configured:
        return

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.log"

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    fmt.converter = time.gmtime

    logger = logging.getLogger("etl")
    logger.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        utc=True,
        backupCount=retention_days,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    file_handler.suffix = "%Y-%m-%d.log"

    logger.addHandler(console)
    logger.addHandler(file_handler)
    _configured = True


def get_logger(name: str = "etl") -> logging.Logger:
    if not _configured:
        configure_logging()   # fallback defaults if called before pipeline startup
    return logging.getLogger(name)


def audit(
    event:        str,
    *,
    workbook:     str = "",
    sheet:        str = "",
    trigger:      str = "",
    rows_total:   int = 0,
    rows_valid:   int = 0,
    rows_invalid: int = 0,
    inserted:     int = 0,
    updated:      int = 0,
    skipped:      int = 0,
    elapsed_ms:   int = 0,
    level:        str = "info",
    error:        str = "",
) -> None:
    parts = [f"event={event}"]
    if workbook:     parts.append(f"workbook={workbook}")
    if sheet:        parts.append(f"sheet={sheet}")
    if trigger:      parts.append(f"trigger={trigger}")
    if rows_total:   parts.append(f"rows_total={rows_total}")
    if rows_valid:   parts.append(f"rows_valid={rows_valid}")
    if rows_invalid: parts.append(f"rows_invalid={rows_invalid}")
    if inserted:     parts.append(f"inserted={inserted}")
    if updated:      parts.append(f"updated={updated}")
    if skipped:      parts.append(f"skipped={skipped}")
    if elapsed_ms:   parts.append(f"elapsed_ms={elapsed_ms}")
    if error:        parts.append(f"error={error!r}")

    method = getattr(get_logger(), level, get_logger().info)
    method(" | ".join(parts))
