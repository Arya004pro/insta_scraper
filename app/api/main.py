from __future__ import annotations

import csv
import re
import uuid
from pathlib import Path
from urllib.parse import quote
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.core.config import load_settings
from app.core.models import (
    ResumeRunRequest,
    RunArtifactsResponse,
    RunStatusResponse,
    SyncReelsCountsRequest,
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


def _persist_uploaded_sync_csv(
    source_csv_text: str, source_csv_filename: str | None
) -> Path:
    uploads_dir = settings.media_dir / "_uploaded_sync_sources"
    uploads_dir.mkdir(parents=True, exist_ok=True)

    raw_name = (source_csv_filename or "uploaded_reels.csv").strip()
    base_name = Path(raw_name).name
    stem = Path(base_name).stem or "uploaded_reels"
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-")
    if not safe_stem:
        safe_stem = "uploaded_reels"

    target = uploads_dir / f"{safe_stem}_{uuid.uuid4().hex[:10]}.csv"
    target.write_text(source_csv_text, encoding="utf-8", newline="")
    return target


def _pick_sample(
    posts_rows: list[dict[str, str]], bucket: str
) -> dict[str, str] | None:
    def _matches_bucket(row: dict[str, str]) -> bool:
        sample_bucket = (row.get("sample_bucket") or "").strip().lower()
        if sample_bucket == bucket:
            return True

        media_type = (row.get("media_type") or "").strip().lower()
        content_type = (row.get("content_type") or "").strip().lower()
        if bucket == "posts":
            return (
                media_type in {"image_post", "video_post", "carousel_post"}
                or content_type == "post"
            )
        if bucket == "reels":
            return media_type == "reel" or content_type == "reel"
        return False

    best_row: dict[str, str] | None = None
    best_score: tuple[int, int, int, int] = (-1, -1, -1, -1)

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
        numeric_points = 0
        for key in ("likes_count", "comments_count", "views_count", "repost_count"):
            if (row.get(key) or "").strip():
                numeric_points += 1

        score = (has_any_media, has_full_parse, numeric_points, has_exact_bucket)
        if score > best_score:
            best_score = score
            best_row = row

    if best_row is None:
        return None

    # Do not present an "empty" sample card when row has almost no usable payload.
    has_media = best_score[0] > 0
    has_full_parse = best_score[1] > 0
    has_numeric = best_score[2] > 0
    if not (has_media or has_full_parse or has_numeric):
        return None

    return best_row


def _serialize_sample_row(row: dict[str, str] | None) -> dict | None:
    if not row:
        return None

    return _serialize_output_row(row)


def _serialize_output_row(row: dict[str, str] | None) -> dict | None:
    if not row:
        return None

    def _latest_count(field: str) -> str | None:
        updated = (row.get(f"updated_{field}") or "").strip()
        if updated:
            return updated
        base = row.get(field)
        return base

    local_paths = _split_csv_values(row.get("media_asset_local_paths_csv") or "")
    media_asset_urls = [
        u
        for u in _split_csv_values(row.get("media_asset_urls_csv") or "")
        if _is_http_url(u)
    ]
    post_url = row.get("post_url")
    if not _is_http_url(post_url):
        post_url = None

    media_type = (row.get("media_type") or "").strip().lower()
    content_type = (row.get("content_type") or "").strip().lower()
    normalized_type = content_type or ("reel" if media_type == "reel" else "post")
    views_count = _latest_count("views_count") if normalized_type == "reel" else None

    return {
        "shortcode": row.get("shortcode"),
        "post_url": post_url,
        "posted_at_ist": row.get("posted_at_ist"),
        "media_type": row.get("media_type"),
        "content_type": normalized_type,
        "sample_bucket": row.get("sample_bucket"),
        "likes_count": _latest_count("likes_count"),
        "comments_count": _latest_count("comments_count"),
        "views_count": views_count,
        "repost_count": _latest_count("repost_count"),
        "updated_likes_count": row.get("updated_likes_count"),
        "updated_comments_count": row.get("updated_comments_count"),
        "updated_views_count": row.get("updated_views_count"),
        "updated_repost_count": row.get("updated_repost_count"),
        "sync_checked_at_ist": row.get("sync_checked_at_ist"),
        "old_likes_count": row.get("likes_count"),
        "old_comments_count": row.get("comments_count"),
        "old_views_count": row.get("views_count"),
        "old_repost_count": row.get("repost_count"),
        "is_remix_repost": row.get("is_remix_repost"),
        "is_tagged_post": row.get("is_tagged_post"),
        "tagged_users_count": row.get("tagged_users_count"),
        "hashtags_csv": row.get("hashtags_csv"),
        "keywords_csv": row.get("keywords_csv"),
        "mentions_csv": row.get("mentions_csv"),
        "collaborators_csv": row.get("collaborators_csv"),
        "caption_text": row.get("caption_text"),
        "location_name": row.get("location_name"),
        "missing_reason_post": row.get("missing_reason_post"),
        "media_asset_urls": media_asset_urls,
        "media_asset_local_paths": local_paths,
        "media_asset_local_urls": [
            u for u in (_output_url_from_local_path(x) for x in local_paths) if u
        ],
    }


def _serialize_external_links(
    links_rows: list[dict[str, str]], profile_row: dict[str, str] | None
) -> list[dict[str, str]]:
    def _canonical_link(url: str) -> str:
        try:
            parsed = urlparse(url)
        except Exception:
            return url

        scheme = (parsed.scheme or "https").lower()
        host = (parsed.hostname or "").lower()
        if not host:
            return url

        path = parsed.path or ""
        if path == "/":
            path = ""

        tracking_keys = {
            "fbclid",
            "gclid",
            "mc_cid",
            "mc_eid",
            "igshid",
            "ig_rid",
            "igsh",
        }
        kept: list[tuple[str, str]] = []
        for key, values in parse_qs(parsed.query, keep_blank_values=False).items():
            lk = (key or "").lower()
            if lk.startswith("utm_") or lk in tracking_keys:
                continue
            for value in values:
                kept.append((key, value))

        query = urlencode(kept, doseq=True)
        return urlunparse((scheme, host, path, "", query, ""))

    seen: set[str] = set()
    output: list[dict[str, str]] = []

    for row in links_rows:
        raw_url = (row.get("raw_url") or "").strip()
        expanded_url = (row.get("expanded_url") or "").strip()
        final_url = (row.get("final_url") or "").strip()
        primary = final_url or expanded_url or raw_url
        if not _is_http_url(primary):
            continue
        canonical_primary = _canonical_link(primary)
        if canonical_primary in seen:
            continue
        seen.add(canonical_primary)

        output.append(
            {
                "url": primary,
                "raw_url": raw_url,
                "expanded_url": expanded_url,
                "final_url": final_url,
                "domain": (row.get("domain") or "").strip(),
                "http_status": (row.get("http_status") or "").strip(),
                "source_surface": (row.get("source_surface") or "").strip(),
            }
        )

    if not output:
        fallback_url = (profile_row or {}).get("external_url_primary", "").strip()
        if _is_http_url(fallback_url):
            output.append(
                {
                    "url": fallback_url,
                    "raw_url": fallback_url,
                    "expanded_url": "",
                    "final_url": "",
                    "domain": "",
                    "http_status": "",
                    "source_surface": "profile_header",
                }
            )

    return output


def _profile_from_summary_row(summary_row: dict[str, str]) -> dict[str, str]:
    return {
        "username": summary_row.get("Username", ""),
        "full_name": summary_row.get("Full Name", ""),
        "email_address": summary_row.get("Email Address", ""),
        "external_url_primary": summary_row.get("External URL (Primary)", ""),
        "followers_count": summary_row.get("Followers", ""),
        "following_count": summary_row.get("Following", ""),
        "total_posts_count": summary_row.get("Total Posts (Profile)", ""),
        "date_joined": summary_row.get("Date Joined", ""),
        "account_based_in": summary_row.get("Account Based In", ""),
        "active_ads_status": summary_row.get("Active Ads", ""),
        "active_ads_url": summary_row.get("Active Ads URL", ""),
        "time_verified": summary_row.get("Time Verified", ""),
    }


def _pick_summary_row_for_run(
    rows: list[dict[str, str]], run_id: str, normalized_profile_url: str | None
) -> dict[str, str] | None:
    if not rows:
        return None

    normalized = (normalized_profile_url or "").strip().lower().rstrip("/")
    for row in reversed(rows):
        if (row.get("Run ID") or "").strip() != run_id:
            continue
        row_profile = (
            (row.get("Normalized Profile URL") or "").strip().lower().rstrip("/")
        )
        if normalized and row_profile and normalized == row_profile:
            return row

    for row in reversed(rows):
        if (row.get("Run ID") or "").strip() == run_id:
            return row

    return rows[-1]


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


@app.post("/v1/runs/{run_id}/stop", response_model=RunStatusResponse)
def stop_run(run_id: str) -> RunStatusResponse:
    run = store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")

    try:
        stopped = orchestrator.request_stop(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not stop run: {exc}"
        ) from exc

    return RunStatusResponse(
        run_id=stopped.run_id,
        status=stopped.status,
        started_at_ist=stopped.started_at_ist,
        ended_at_ist=stopped.ended_at_ist,
        progress_message=stopped.progress_message,
        progress_pct=stopped.progress_pct,
        challenge_encountered=stopped.challenge_encountered,
        error_code=stopped.error_code,
        error_message=stopped.error_message,
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
    summary_csv_path = artifacts.get("profiles_rollup_csv") or artifacts.get(
        "master_summary_csv"
    )
    if summary_csv_path:
        summary_rows = _read_csv_rows(Path(summary_csv_path))
        picked = _pick_summary_row_for_run(
            summary_rows,
            run_id=run_id,
            normalized_profile_url=run.normalized_profile_url,
        )
        if picked:
            profile_row = _profile_from_summary_row(picked)

    content_csv_path = artifacts.get("profile_content_csv")
    if not content_csv_path:
        for key in sorted(artifacts.keys()):
            if key.startswith("profile_content_csv_"):
                content_csv_path = artifacts[key]
                break

    all_media_rows: list[dict[str, str]] = []
    if content_csv_path:
        all_media_rows = _read_csv_rows(Path(content_csv_path))
    else:
        posts_csv_path = artifacts.get("posts_csv")
        reels_csv_path = artifacts.get("reels_csv")
        if posts_csv_path:
            all_media_rows.extend(_read_csv_rows(Path(posts_csv_path)))
        if reels_csv_path:
            all_media_rows.extend(_read_csv_rows(Path(reels_csv_path)))

    posts_rows = [
        row
        for row in all_media_rows
        if (row.get("content_type") or "").strip().lower() == "post"
        or (
            (row.get("content_type") or "").strip() == ""
            and (row.get("media_type") or "").strip().lower() != "reel"
        )
    ]
    reels_rows = [
        row
        for row in all_media_rows
        if (row.get("content_type") or "").strip().lower() == "reel"
        or (row.get("media_type") or "").strip().lower() == "reel"
    ]

    external_links_rows: list[dict[str, str]] = []
    external_links_csv_path = artifacts.get("external_links_csv")
    if external_links_csv_path:
        external_links_rows = _read_csv_rows(Path(external_links_csv_path))

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
    output_mixed = [
        item
        for item in (_serialize_output_row(row) for row in all_media_rows)
        if item is not None
    ]

    samples = {
        "post": _serialize_sample_row(_pick_sample(all_media_rows, "posts")),
        "reel": _serialize_sample_row(
            _pick_sample(reels_rows, "reels") or _pick_sample(all_media_rows, "reels")
        ),
    }

    external_links = _serialize_external_links(external_links_rows, profile_row)

    return {
        "run_id": run_id,
        "status": run.status,
        "progress_pct": run.progress_pct,
        "progress_message": run.progress_message,
        "error_code": run.error_code,
        "error_message": run.error_message,
        "profile": profile_row,
        "external_links": external_links,
        "samples": samples,
        "outputs": {
            "mixed": output_mixed,
            "posts": output_posts,
            "reels": output_reels,
            "total_count": len(output_mixed),
        },
        "artifacts": report_artifacts,
    }


@app.post("/v1/reels/sync-counts")
def sync_existing_reel_counts(req: SyncReelsCountsRequest) -> dict:
    source_csv_path = req.source_csv_path
    if req.source_csv_text:
        persisted_source = _persist_uploaded_sync_csv(
            source_csv_text=req.source_csv_text,
            source_csv_filename=req.source_csv_filename,
        )
        source_csv_path = str(persisted_source)

    try:
        result = orchestrator.sync_existing_reels_counts(
            profile_url=req.profile_url,
            source_csv_path=source_csv_path,
            use_saved_session=req.use_saved_session,
            max_reels=req.max_reels,
        )
    except InvalidInstagramUrl as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not sync reel counts: {exc}"
        ) from exc

    synced_path = Path(result.get("synced_csv") or "")
    source_path = Path(result.get("source_csv") or "")

    return {
        **result,
        "synced_csv_output_url": _output_url_from_local_path(str(synced_path)),
        "source_csv_output_url": _output_url_from_local_path(str(source_path)),
    }
