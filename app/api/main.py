from __future__ import annotations

import csv
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

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

API_DIR = Path(__file__).resolve().parent
UI_INDEX_PATH = API_DIR / "static" / "index.html"

app.mount(
    "/output",
    StaticFiles(directory=str(settings.media_dir), check_dir=False),
    name="output",
)


def _read_csv_rows(path: Path, max_rows: int | None = None) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            rows.append({k: (v or "") for k, v in row.items()})
            if max_rows is not None and idx + 1 >= max_rows:
                break
    return rows


def _split_csv_values(value: str) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def _is_http_url(value: str | None) -> bool:
    if not value:
        return False
    v = value.strip().lower()
    return v.startswith("http://") or v.startswith("https://")


def _output_url_from_local_path(local_path: str) -> str | None:
    if not local_path:
        return None
    try:
        root = settings.media_dir.resolve()
        file_path = Path(local_path).resolve()
        if not file_path.exists() or not file_path.is_file():
            return None
        if not file_path.is_relative_to(root):
            return None
        rel = file_path.relative_to(root).as_posix()
        return f"/output/{quote(rel)}"
    except Exception:
        return None


def _pick_sample(
    posts_rows: list[dict[str, str]], bucket: str
) -> dict[str, str] | None:
    def _matches_bucket(row: dict[str, str]) -> bool:
        sample_bucket = (row.get("sample_bucket") or "").strip().lower()
        if sample_bucket == bucket:
            return True

        media_type = (row.get("media_type") or "").strip().lower()
        if bucket == "posts":
            return media_type in {"image_post", "video_post"}
        if bucket == "multi_image_posts":
            return media_type == "carousel_post"
        if bucket == "reels":
            return media_type == "reel"
        return False

    best_row: dict[str, str] | None = None
    best_score: tuple[int, int, int] = (-1, -1, -1)

    for row in posts_rows:
        post_url = row.get("post_url")
        if not _is_http_url(post_url) or not _matches_bucket(row):
            continue

        has_exact_bucket = int(
            (row.get("sample_bucket") or "").strip().lower() == bucket
        )
        has_any_media = int(
            bool(_split_csv_values(row.get("media_asset_local_paths_csv") or ""))
            or bool(_split_csv_values(row.get("media_asset_urls_csv") or ""))
        )
        has_full_parse = int(not (row.get("missing_reason_post") or "").strip())
        score = (has_exact_bucket, has_any_media, has_full_parse)
        if score > best_score:
            best_score = score
            best_row = row

    return best_row


def _serialize_sample_row(row: dict[str, str] | None) -> dict | None:
    if not row:
        return None

    return _serialize_output_row(row)


def _serialize_output_row(row: dict[str, str] | None) -> dict | None:
    if not row:
        return None

    local_paths = _split_csv_values(row.get("media_asset_local_paths_csv") or "")
    media_asset_urls = [
        u
        for u in _split_csv_values(row.get("media_asset_urls_csv") or "")
        if _is_http_url(u)
    ]
    post_url = row.get("post_url")
    if not _is_http_url(post_url):
        post_url = None

    return {
        "shortcode": row.get("shortcode"),
        "post_url": post_url,
        "posted_at_ist": row.get("posted_at_ist"),
        "media_type": row.get("media_type"),
        "sample_bucket": row.get("sample_bucket"),
        "likes_count": row.get("likes_count"),
        "comments_count": row.get("comments_count"),
        "views_count": row.get("views_count"),
        "is_remix_repost": row.get("is_remix_repost"),
        "is_tagged_post": row.get("is_tagged_post"),
        "tagged_users_count": row.get("tagged_users_count"),
        "hashtags_csv": row.get("hashtags_csv"),
        "keywords_csv": row.get("keywords_csv"),
        "mentions_csv": row.get("mentions_csv"),
        "caption_text": row.get("caption_text"),
        "location_name": row.get("location_name"),
        "missing_reason_post": row.get("missing_reason_post"),
        "media_asset_urls": media_asset_urls,
        "media_asset_local_paths": local_paths,
        "media_asset_local_urls": [
            u for u in (_output_url_from_local_path(x) for x in local_paths) if u
        ],
    }


def _profile_from_summary_row(summary_row: dict[str, str]) -> dict[str, str]:
    return {
        "username": summary_row.get("Username", ""),
        "full_name": summary_row.get("Full Name", ""),
        "followers_count": summary_row.get("Followers", ""),
        "following_count": summary_row.get("Following", ""),
        "total_posts_count": summary_row.get("Total Posts (Profile)", ""),
        "date_joined": summary_row.get("Date Joined", ""),
        "account_based_in": summary_row.get("Account Based In", ""),
        "time_verified": summary_row.get("Time Verified", ""),
    }


@app.get("/", response_class=HTMLResponse)
def ui_home() -> HTMLResponse:
    if not UI_INDEX_PATH.exists():
        return HTMLResponse(
            "<h3>UI file missing. Expected app/api/static/index.html</h3>",
            status_code=500,
        )
    return HTMLResponse(UI_INDEX_PATH.read_text(encoding="utf-8"))


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
        raise HTTPException(
            status_code=500, detail=f"Could not start run: {exc}"
        ) from exc
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
        raise HTTPException(
            status_code=400, detail=f"Run status {run.status} cannot be resumed"
        )
    try:
        resumed = orchestrator.resume_run(run_id)
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not resume run: {exc}"
        ) from exc
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
    return RunArtifactsResponse(
        run_id=run_id, status=run.status, artifacts=run.artifacts
    )


@app.get("/v1/runs/{run_id}/events")
def get_events(run_id: str) -> dict:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"run_id": run_id, "events": store.get_events(run_id)}


@app.get("/v1/runs/{run_id}/artifact/{artifact_key}")
def download_artifact(run_id: str, artifact_key: str) -> FileResponse:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")

    path_str = run.artifacts.get(artifact_key)
    if not path_str:
        raise HTTPException(
            status_code=404, detail=f"Artifact not found: {artifact_key}"
        )

    path = Path(path_str)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact file missing on disk")

    return FileResponse(path=str(path), filename=path.name)


@app.get("/v1/runs/{run_id}/report")
def get_run_report(run_id: str) -> dict:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")

    artifacts = run.artifacts or {}
    report_artifacts: dict[str, dict] = {}
    for key, value in artifacts.items():
        path = Path(value)
        report_artifacts[key] = {
            "path": value,
            "exists": path.exists(),
            "download_url": f"/v1/runs/{run_id}/artifact/{key}",
        }

    profile_row: dict[str, str] | None = None
    profile_csv_path = artifacts.get("profile_csv")
    if profile_csv_path:
        rows = _read_csv_rows(Path(profile_csv_path), max_rows=1)
        profile_row = rows[0] if rows else None
    if profile_row is None:
        summary_csv_path = artifacts.get("master_summary_csv")
        if summary_csv_path:
            rows = _read_csv_rows(Path(summary_csv_path), max_rows=1)
            if rows:
                profile_row = _profile_from_summary_row(rows[0])

    posts_rows: list[dict[str, str]] = []
    posts_csv_path = artifacts.get("posts_csv")
    if posts_csv_path:
        posts_rows = _read_csv_rows(Path(posts_csv_path))

    reels_rows: list[dict[str, str]] = []
    reels_csv_path = artifacts.get("reels_csv")
    if reels_csv_path:
        reels_rows = _read_csv_rows(Path(reels_csv_path))

    output_posts = [
        item
        for item in (_serialize_output_row(row) for row in posts_rows)
        if item is not None
    ]
    output_reels = [
        item
        for item in (_serialize_output_row(row) for row in reels_rows)
        if item is not None
    ]

    all_media_rows = posts_rows + reels_rows

    samples = {
        "single_image_post": _serialize_sample_row(
            _pick_sample(all_media_rows, "posts")
        ),
        "multi_image_post": _serialize_sample_row(
            _pick_sample(all_media_rows, "multi_image_posts")
        ),
        "reel": _serialize_sample_row(
            _pick_sample(reels_rows, "reels") or _pick_sample(all_media_rows, "reels")
        ),
    }

    return {
        "run_id": run_id,
        "status": run.status,
        "progress_pct": run.progress_pct,
        "progress_message": run.progress_message,
        "error_code": run.error_code,
        "error_message": run.error_message,
        "profile": profile_row,
        "samples": samples,
        "outputs": {
            "posts": output_posts,
            "reels": output_reels,
            "total_count": len(output_posts) + len(output_reels),
        },
        "artifacts": report_artifacts,
    }
