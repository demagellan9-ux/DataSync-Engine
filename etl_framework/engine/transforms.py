from datetime import datetime
from typing import Any, Callable

from etl_framework.logger import get_logger

logger = get_logger()


# ── Coercion functions ────────────────────────────────────────────────────────

def _string(v) -> str | None:
    return str(v).strip() if v is not None else None

def _integer(v) -> int | None:
    try:    return int(float(str(v).replace(",", "").strip()))
    except: return None

def _float(v) -> float | None:
    try:    return float(str(v).replace(",", "").strip())
    except: return None

def _percentage(v) -> float | None:
    if v is None: return None
    s = str(v).strip().replace(",", "")
    try:
        if s.endswith("%"):
            return round(float(s[:-1]) / 100, 10)
        f = float(s)
        return f if f <= 1.0 else round(f / 100, 10)
    except: return None

def _datetime(v) -> str | None:
    if v is None: return None
    try:
        import pandas as pd
        return pd.to_datetime(str(v), dayfirst=False, errors="raise").isoformat()
    except: return None

def _duration_seconds(v) -> int | None:
    if v is None: return None
    s = str(v).strip()
    parts = s.split(":")
    try:
        if len(parts) == 3: return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))
        if len(parts) == 2: return int(parts[0]) * 60 + int(float(parts[1]))
        return int(float(s))
    except: return None

def _currency(v) -> float | None:
    if v is None: return None
    s = str(v).strip().replace(",", "").replace("₱", "").replace("$", "").replace("€", "")
    try:    return float(s)
    except: return None

def _trim(v)      -> str | None: return str(v).strip() if v is not None else None
def _uppercase(v) -> str | None: return str(v).strip().upper() if v is not None else None
def _lowercase(v) -> str | None: return str(v).strip().lower() if v is not None else None


TRANSFORM_FN: dict[str, Callable] = {
    "string":           _string,
    "integer":          _integer,
    "float":            _float,
    "percentage":       _percentage,
    "datetime":         _datetime,
    "duration_seconds": _duration_seconds,
    "currency":         _currency,
    "trim":             _trim,
    "uppercase":        _uppercase,
    "lowercase":        _lowercase,
}


# ── Public API ────────────────────────────────────────────────────────────────

def apply_transforms(
    rows:        list[dict[str, Any]],
    transforms:  dict[str, str],
    sheet_name:  str,
) -> list[dict[str, Any]]:
    """
    Apply per-column type coercions defined in `transforms`.
    Columns not present in `transforms` are left unchanged.
    """
    if not transforms:
        return rows

    lookup: dict[str, Callable] = {}
    for col, ttype in transforms.items():
        fn = TRANSFORM_FN.get(ttype)
        if fn is None:
            logger.warning(
                "event=unknown_transform | sheet=%s | column=%r | type=%r",
                sheet_name, col, ttype,
            )
        else:
            lookup[col.strip().lower()] = fn

    result = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            fn = lookup.get(k.strip().lower())
            new_row[k] = fn(v) if (fn and v is not None) else v
        result.append(new_row)
    return result
