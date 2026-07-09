"""
Selects the active profile.
Change ACTIVE_PROFILE to switch between different workbook configurations
without touching any pipeline or engine code.

The path is resolved relative to the working directory where the pipeline is launched.
"""

import os

# Override via environment variable:  ETL_PROFILE=profiles/other.json python -m etl_framework.pipeline
ACTIVE_PROFILE: str = os.environ.get("ETL_PROFILE", "profiles/fc_stats_wais.json")
