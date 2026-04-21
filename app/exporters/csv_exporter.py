from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from app.core.models import (
    EXTERNAL_LINKS_COLUMNS,
    POSTS_COLUMNS,
    PROFILE_COLUMNS,
)


PROFILE_CONTENT_COLUMNS = [
    "scraped_at_ist",
    "run_id",
    "username",
    "shortcode",
    "post_url",
    "content_type",
    "media_type",
    "posted_at_ist",
    "likes_count",
    "comments_count",
    "views_count",
    "repost_count",
    "is_remix_repost",
    "is_tagged_post",
    "tagged_users_count",
    "hashtags_csv",
    "keywords_csv",
    "mentions_csv",
    "caption_text",
    "location_name",
    "media_asset_urls_csv",
    "media_asset_local_paths_csv",
    "sample_bucket",
    "missing_reason_post",
]


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c) for c in columns})


def _read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return [], []

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [{k: (v or "") for k, v in row.items()} for row in reader]
        return list(reader.fieldnames or []), rows


def _safe_slug(value: str | None, default: str) -> str:
    text = (value or "").strip().lower()
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in text)
    safe = safe.strip("_")
    return safe or default


def _profile_folder_name(username: str | None, full_name: str | None) -> str:
    base = (full_name or username or "unknown_profile").strip().lower()
    safe = "".join(ch if ch.isalnum() else "_" for ch in base)
    while "__" in safe:
        safe = safe.replace("__", "_")
    safe = safe.strip("_")
    return safe or "unknown_profile"


def _normalize_profile_content_row(row: dict[str, Any]) -> dict[str, Any]:
    media_type = str(row.get("media_type") or "").strip().lower()
    content_type = "reel" if media_type == "reel" else "post"

    normalized = {k: row.get(k) for k in POSTS_COLUMNS}
    normalized["content_type"] = content_type
    if content_type == "post":
        normalized["views_count"] = None
    return normalized


def _upsert_rollup_rows(
    path: Path,
    rows: list[dict[str, Any]],
    unique_keys: tuple[str, str] = ("Run ID", "Username"),
) -> None:
    if not rows:
        if not path.exists():
            _write_csv(path, ["Run ID", "Username"], [])
        return

    existing_columns, existing_rows = _read_csv_rows(path)
    incoming_columns = list(rows[0].keys())

    columns = list(existing_columns)
    if not columns:
        columns = incoming_columns
    else:
        for col in incoming_columns:
            if col not in columns:
                columns.append(col)

    merged: dict[tuple[str, str], dict[str, Any]] = {}
    ordered_keys: list[tuple[str, str]] = []

    def _row_key(item: dict[str, Any]) -> tuple[str, str]:
        return (
            str(item.get(unique_keys[0]) or "").strip(),
            str(item.get(unique_keys[1]) or "").strip(),
        )

    for row in existing_rows:
        key = _row_key(row)
        if key not in merged:
            ordered_keys.append(key)
        merged[key] = row

    for row in rows:
        key = _row_key(row)
        if key not in merged:
            ordered_keys.append(key)
        merged[key] = row

    out_rows = [merged[key] for key in ordered_keys]
    _write_csv(path, columns, out_rows)


def export_csv_artifacts(
    exports_dir: Path,
    media_dir: Path,
    base_name: str,
    run_log_rows: list[dict[str, Any]],
    profile_rows: list[dict[str, Any]],
    highlights_rows: list[dict[str, Any]],
    external_links_rows: list[dict[str, Any]],
    posts_rows: list[dict[str, Any]],
    aggregate_rows: list[dict[str, Any]],
    summary_flat_rows: list[dict[str, Any]],
) -> dict[str, str]:
    artifacts: dict[str, str] = {}

    profile_lookup: dict[str, dict[str, Any]] = {}
    for row in profile_rows:
        username = str(row.get("username") or "").strip()
        if username:
            profile_lookup[username] = row

    grouped_rows: dict[str, list[dict[str, Any]]] = {}
    for row in posts_rows:
        username = str(row.get("username") or "").strip() or "unknown"
        grouped_rows.setdefault(username, []).append(
            _normalize_profile_content_row(row)
        )

    if not grouped_rows and profile_rows:
        username = str(profile_rows[0].get("username") or "").strip() or "unknown"
        grouped_rows[username] = []

    first_content_key: str | None = None
    for username, rows in grouped_rows.items():
        profile_meta = profile_lookup.get(username, {})
        folder_name = _profile_folder_name(
            username=username,
            full_name=str(profile_meta.get("full_name") or "").strip() or None,
        )
        safe_username = _safe_slug(username, "unknown")
        content_csv = (
            media_dir / folder_name / f"instagram_{safe_username}_content_latest.csv"
        )
        _write_csv(content_csv, PROFILE_CONTENT_COLUMNS, rows)

        artifact_key = (
            "profile_content_csv"
            if first_content_key is None
            else f"profile_content_csv_{safe_username}"
        )
        artifacts[artifact_key] = str(content_csv.resolve())
        if first_content_key is None:
            first_content_key = artifact_key

    profiles_rollup_csv = exports_dir / "instagram_profiles_rollup.csv"
    _upsert_rollup_rows(profiles_rollup_csv, summary_flat_rows)

    _ = run_log_rows
    _ = highlights_rows
    _ = aggregate_rows
    _ = base_name
    _ = external_links_rows
    _ = PROFILE_COLUMNS
    _ = EXTERNAL_LINKS_COLUMNS

    artifacts["profiles_rollup_csv"] = str(profiles_rollup_csv.resolve())
    return artifacts
