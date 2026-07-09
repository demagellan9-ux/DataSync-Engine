from datetime import datetime
from typing import Any, Callable

from etl_framework.logger import audit, get_logger

logger = get_logger()


# ── Rule checkers ─────────────────────────────────────────────────────────────

def _required(v, _)  -> str | None:
    return None if (v is not None and str(v).strip() != "") else "Value is required"

def _numeric(v, _) -> str | None:
    if v is None: return None
    return None if isinstance(v, (int, float)) else f"Expected numeric, got {v!r}"

def _percentage(v, _) -> str | None:
    if v is None: return None
    return None if (isinstance(v, float) and 0.0 <= v <= 1.0) else f"Expected percentage 0.0–1.0, got {v!r}"

def _date_format(v, rule) -> str | None:
    if v is None: return None
    fmt = rule.get("format", "%Y-%m-%dT%H:%M:%S")
    try:
        datetime.strptime(str(v), fmt)
        return None
    except ValueError:
        return f"Expected date format {fmt!r}, got {v!r}"

def _allowed_values(v, rule) -> str | None:
    if v is None: return None
    allowed = rule.get("values", [])
    return None if v in allowed else f"{v!r} not in allowed values {allowed}"

def _min(v, rule) -> str | None:
    if v is None: return None
    try:    return None if float(v) >= rule["value"] else f"Value {v} is below minimum {rule['value']}"
    except: return f"Cannot compare {v!r} with min {rule['value']}"

def _max(v, rule) -> str | None:
    if v is None: return None
    try:    return None if float(v) <= rule["value"] else f"Value {v} exceeds maximum {rule['value']}"
    except: return f"Cannot compare {v!r} with max {rule['value']}"

def _custom(v, rule) -> str | None:
    fn = rule.get("fn")
    if fn is None or v is None: return None
    try:    return None if fn(v) else f"Custom validation failed for {v!r}"
    except Exception as exc: return f"Custom validator raised: {exc}"


VALIDATOR_FN: dict[str, Callable] = {
    "required":       _required,
    "numeric":        _numeric,
    "percentage":     _percentage,
    "date_format":    _date_format,
    "allowed_values": _allowed_values,
    "min":            _min,
    "max":            _max,
    "custom":         _custom,
}


# ── Public API ────────────────────────────────────────────────────────────────

def validate_records(
    rows:        list[dict[str, Any]],
    validations: dict[str, list[dict]],
    sheet_name:  str,
) -> tuple[list[dict], list[dict]]:
    """
    Split rows into (valid, invalid) based on COLUMN_VALIDATIONS rules.
    Invalid rows carry a '_validation_errors' key.
    """
    if not validations:
        return rows, []

    rules_lower = {k.strip().lower(): v for k, v in validations.items()}
    valid, invalid = [], []

    for row in rows:
        errors: list[str] = []
        for col, value in row.items():
            col_rules = rules_lower.get(col.strip().lower(), [])
            for rule in col_rules:
                rule_name = rule.get("rule")
                checker   = VALIDATOR_FN.get(rule_name)
                if checker is None:
                    logger.warning(
                        "event=unknown_validation_rule | sheet=%s | column=%r | rule=%r",
                        sheet_name, col, rule_name,
                    )
                    continue
                err = checker(value, rule)
                if err:
                    errors.append(f"{col}: {err}")

        if errors:
            row["_validation_errors"] = errors
            invalid.append(row)
            audit("validation_failure", sheet=sheet_name, rows_invalid=1,
                  error="; ".join(errors), level="warning")
        else:
            valid.append(row)

    return valid, invalid
