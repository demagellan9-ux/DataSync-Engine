"""
FastAPI wrapper around the existing ETL framework.

Treats etl_framework as a black box — only imports from it, never modifies it.
The ETL engine, connectors, profile loader, and pipeline logic are untouched.

Endpoints:
    GET  /health              — liveness check + MongoDB reachability
    GET  /status              — last ETL run result
    POST /etl/run             — trigger a cycle using the active profile
    POST /etl/run/{profile}   — trigger a cycle using profiles/{profile}.json
"""

import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse

# ── Import etl_framework as a black box ───────────────────────────────────────
from etl_framework.pipeline import _bootstrap, _safe_run_etl
from etl_framework.settings import ACTIVE_PROFILE

# ── In-memory run state ───────────────────────────────────────────────────────

_state_lock = threading.Lock()
_state: dict[str, Any] = {
    "status":     "idle",       # idle | running | completed | failed
    "profile":    None,
    "trigger":    None,
    "started_at": None,
    "ended_at":   None,
    "error":      None,
}


def _set_state(**kwargs) -> None:
    with _state_lock:
        _state.update(kwargs)


# ── ETL runner (called in background thread) ──────────────────────────────────

def _run_in_background(profile_path: str, trigger: str) -> None:
    _set_state(
        status="running",
        profile=profile_path,
        trigger=trigger,
        started_at=datetime.now(timezone.utc).isoformat(),
        ended_at=None,
        error=None,
    )
    try:
        profile = _bootstrap(profile_path)
        _safe_run_etl(profile, trigger=trigger)
        _set_state(
            status="completed",
            ended_at=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as exc:
        _set_state(
            status="failed",
            ended_at=datetime.now(timezone.utc).isoformat(),
            error=f"{type(exc).__name__}: {exc}",
        )


# ── App ───────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pre-load the default profile at startup to catch config errors early
    try:
        _bootstrap(ACTIVE_PROFILE)
    except Exception as exc:
        print(f"[WARN] Could not pre-load default profile: {exc}")
    yield


app = FastAPI(
    title="ETL Framework API",
    description="HTTP interface for the local profile-driven ETL pipeline.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", summary="Liveness + MongoDB reachability check")
def health():
    try:
        from etl_framework.connectors.destinations.mongodb import _registry
        from etl_framework.profile_loader import load_profile
        profile = load_profile(ACTIVE_PROFILE)
        m = profile.destination.mongodb
        p = profile.performance
        client = _registry.get(
            m.uri,
            m.timeout_ms,
            p.connection_pooling.max_pool_size,
            p.connection_pooling.min_pool_size,
            p.connection_pooling.max_idle_time_ms,
        )
        client.admin.command("ping")
        mongo_status = "ok"
    except Exception as exc:
        mongo_status = f"unreachable: {exc}"

    return {
        "api":            "ok",
        "mongodb":        mongo_status,
        "active_profile": ACTIVE_PROFILE,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


@app.get("/status", summary="Last ETL run result")
def status():
    with _state_lock:
        return dict(_state)


@app.post("/etl/run", summary="Trigger an ETL cycle using the active profile")
def run_etl(background_tasks: BackgroundTasks):
    with _state_lock:
        if _state["status"] == "running":
            raise HTTPException(
                status_code=409,
                detail="An ETL cycle is already running. Check /status.",
            )

    background_tasks.add_task(_run_in_background, ACTIVE_PROFILE, "api")
    return {
        "accepted":  True,
        "profile":   ACTIVE_PROFILE,
        "message":   "ETL cycle started. Poll /status for progress.",
    }


@app.post("/etl/run/{profile_name}", summary="Trigger an ETL cycle using a named profile")
def run_etl_profile(profile_name: str, background_tasks: BackgroundTasks):
    profile_path = f"profiles/{profile_name}.json"

    if not Path(profile_path).exists():
        raise HTTPException(
            status_code=404,
            detail=f"Profile not found: {profile_path}",
        )

    with _state_lock:
        if _state["status"] == "running":
            raise HTTPException(
                status_code=409,
                detail="An ETL cycle is already running. Check /status.",
            )

    background_tasks.add_task(_run_in_background, profile_path, f"api:{profile_name}")
    return {
        "accepted":  True,
        "profile":   profile_path,
        "message":   "ETL cycle started. Poll /status for progress.",
    }


# ── Local dev entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
