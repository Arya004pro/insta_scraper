from __future__ import annotations

from fastapi import FastAPI, HTTPException

from app.core.config import load_settings
from app.core.models import (
    ResumeRunRequest,
    RunArtifactsResponse,
    RunStatusResponse,
    StartRunRequest,
)
from app.core.url_validator import InvalidInstagramUrl
from app.runner.orchestrator import RunOrchestrator
from app.storage.sqlite_store import SQLiteStore


settings = load_settings()
store = SQLiteStore(settings.sqlite_path)
orchestrator = RunOrchestrator(settings=settings, store=store)

app = FastAPI(title="Instagram Public Scraper API", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/runs/start", response_model=RunStatusResponse)
def start_run(req: StartRunRequest) -> RunStatusResponse:
    try:
        run_id = orchestrator.submit_run(req)
    except InvalidInstagramUrl as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not start run: {exc}") from exc
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=500, detail="Run created but not retrievable")
    return RunStatusResponse(
        run_id=run.run_id,
        status=run.status,
        started_at_ist=run.started_at_ist,
        ended_at_ist=run.ended_at_ist,
        progress_message=run.progress_message,
        progress_pct=run.progress_pct,
        challenge_encountered=run.challenge_encountered,
        error_code=run.error_code,
        error_message=run.error_message,
    )


@app.get("/v1/runs/{run_id}", response_model=RunStatusResponse)
def get_run_status(run_id: str) -> RunStatusResponse:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return RunStatusResponse(
        run_id=run.run_id,
        status=run.status,
        started_at_ist=run.started_at_ist,
        ended_at_ist=run.ended_at_ist,
        progress_message=run.progress_message,
        progress_pct=run.progress_pct,
        challenge_encountered=run.challenge_encountered,
        error_code=run.error_code,
        error_message=run.error_message,
    )


@app.post("/v1/runs/{run_id}/resume", response_model=RunStatusResponse)
def resume_run(run_id: str, req: ResumeRunRequest) -> RunStatusResponse:
    _ = req  # reserved for operator notes in future
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status not in {"needs_human", "failed"}:
        raise HTTPException(status_code=400, detail=f"Run status {run.status} cannot be resumed")
    try:
        resumed = orchestrator.resume_run(run_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not resume run: {exc}") from exc
    return RunStatusResponse(
        run_id=resumed.run_id,
        status=resumed.status,
        started_at_ist=resumed.started_at_ist,
        ended_at_ist=resumed.ended_at_ist,
        progress_message=resumed.progress_message,
        progress_pct=resumed.progress_pct,
        challenge_encountered=resumed.challenge_encountered,
        error_code=resumed.error_code,
        error_message=resumed.error_message,
    )


@app.get("/v1/runs/{run_id}/artifacts", response_model=RunArtifactsResponse)
def get_artifacts(run_id: str) -> RunArtifactsResponse:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if not run.artifacts:
        raise HTTPException(status_code=404, detail="No artifacts available yet")
    return RunArtifactsResponse(run_id=run_id, status=run.status, artifacts=run.artifacts)


@app.get("/v1/runs/{run_id}/events")
def get_events(run_id: str) -> dict:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"run_id": run_id, "events": store.get_events(run_id)}

