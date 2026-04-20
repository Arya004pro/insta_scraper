from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from app.core.models import (
    EXTERNAL_LINKS_COLUMNS,
    POSTS_COLUMNS,
    PROFILE_COLUMNS,
)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c) for c in columns})


def export_csv_artifacts(
    exports_dir: Path,
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

    posts_csv = exports_dir / f"{base_name}_posts.csv"
    reels_csv = exports_dir / f"{base_name}_reels.csv"
    summary_csv = exports_dir / f"{base_name}_master_summary.csv"
    profile_csv = exports_dir / f"{base_name}_profile.csv"
    external_links_csv = exports_dir / f"{base_name}_external_links.csv"

    posts_only_rows = [
        row
        for row in posts_rows
        if (row.get("media_type") or "").strip().lower() != "reel"
    ]
    reels_rows = [
        row
        for row in posts_rows
        if (row.get("media_type") or "").strip().lower() == "reel"
    ]

    _write_csv(posts_csv, POSTS_COLUMNS, posts_only_rows)
    _write_csv(reels_csv, POSTS_COLUMNS, reels_rows)
    _write_csv(profile_csv, PROFILE_COLUMNS, profile_rows)
    _write_csv(external_links_csv, EXTERNAL_LINKS_COLUMNS, external_links_rows)
    _write_csv(
        summary_csv,
        list(summary_flat_rows[0].keys()) if summary_flat_rows else ["scraped_at_ist"],
        summary_flat_rows,
    )

    _ = run_log_rows
    _ = highlights_rows
    _ = aggregate_rows

    artifacts["posts_csv"] = str(posts_csv.resolve())
    artifacts["reels_csv"] = str(reels_csv.resolve())
    artifacts["profile_csv"] = str(profile_csv.resolve())
    artifacts["external_links_csv"] = str(external_links_csv.resolve())
    artifacts["master_summary_csv"] = str(summary_csv.resolve())
    return artifacts
