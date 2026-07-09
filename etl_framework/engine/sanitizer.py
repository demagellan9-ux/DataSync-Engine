import math
from typing import Any

import pandas as pd


def sanitize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Replace NaN / pd.NaT / float nan with None in every row dict.
    Input rows come directly from RawSheet.rows — all values are strings
    at this point, but guard against any residual pandas sentinels.
    """
    cleaned = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if v is None:
                pass
            elif v is pd.NaT:
                v = None
            elif isinstance(v, float) and math.isnan(v):
                v = None
            clean[str(k).strip()] = v
        cleaned.append(clean)
    return cleaned
