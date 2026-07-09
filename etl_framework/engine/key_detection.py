from etl_framework.logger import audit


def detect_upsert_key(
    column_names: list[str],
    candidates: list[str],
    sheet_name: str,
) -> str | None:
    """
    Return the first candidate found in column_names (case-insensitive).
    Called after alias mapping so canonical names are already in place.
    Returns None when no candidate matches — caller falls back to row_number.
    """
    columns_lower = {col.strip().lower(): col for col in column_names}
    for candidate in candidates:
        matched = columns_lower.get(candidate.strip().lower())
        if matched is not None:
            audit("upsert_key_detected", sheet=sheet_name, trigger=matched)
            return matched
    audit("upsert_key_fallback", sheet=sheet_name, trigger="row_number")
    return None
