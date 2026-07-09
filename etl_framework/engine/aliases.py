from etl_framework.logger import get_logger

logger = get_logger()


def apply_aliases(
    rows: list[dict],
    alias_map: dict[str, str],
    sheet_name: str,
) -> list[dict]:
    """
    Rename keys in every row dict according to alias_map.
    Matching is case-insensitive and whitespace-trimmed.
    Rows not matching any alias are returned unchanged.
    """
    if not alias_map:
        return rows

    lookup = {k.strip().lower(): v for k, v in alias_map.items()}
    # Build rename map from the first row's keys (all rows share the same headers)
    if not rows:
        return rows

    rename: dict[str, str] = {}
    for col in rows[0]:
        canonical = lookup.get(col.strip().lower())
        if canonical:
            rename[col] = canonical
            logger.debug(
                "event=alias_resolved | sheet=%s | raw=%r | canonical=%r",
                sheet_name, col, canonical,
            )

    if not rename:
        return rows

    return [
        {rename.get(k, k): v for k, v in row.items()}
        for row in rows
    ]
