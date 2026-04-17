from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from app.core.models import (
    AGGREGATES_COLUMNS,
    EXTERNAL_LINKS_COLUMNS,
    HIGHLIGHTS_COLUMNS,
    POSTS_COLUMNS,
    PROFILE_COLUMNS,
    RUN_LOG_COLUMNS,
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
    summary_csv = exports_dir / f"{base_name}_master_summary.csv"
    run_log_csv = exports_dir / f"{base_name}_run_log.csv"
    profile_csv = exports_dir / f"{base_name}_profile.csv"
    highlights_csv = exports_dir / f"{base_name}_highlights.csv"
    links_csv = exports_dir / f"{base_name}_external_links.csv"
    aggregates_csv = exports_dir / f"{base_name}_aggregates.csv"

    _write_csv(posts_csv, POSTS_COLUMNS, posts_rows)
    _write_csv(
        summary_csv,
        list(summary_flat_rows[0].keys()) if summary_flat_rows else ["scraped_at_ist"],
        summary_flat_rows,
    )
    _write_csv(run_log_csv, RUN_LOG_COLUMNS, run_log_rows)
    _write_csv(profile_csv, PROFILE_COLUMNS, profile_rows)
    _write_csv(highlights_csv, HIGHLIGHTS_COLUMNS, highlights_rows)
    _write_csv(links_csv, EXTERNAL_LINKS_COLUMNS, external_links_rows)
    _write_csv(aggregates_csv, AGGREGATES_COLUMNS, aggregate_rows)

    artifacts["posts_csv"] = str(posts_csv.resolve())
    artifacts["summary_flat_csv"] = str(summary_csv.resolve())
    artifacts["master_summary_csv"] = str(summary_csv.resolve())
    artifacts["run_log_csv"] = str(run_log_csv.resolve())
    artifacts["profile_csv"] = str(profile_csv.resolve())
    artifacts["highlights_csv"] = str(highlights_csv.resolve())
    artifacts["external_links_csv"] = str(links_csv.resolve())
    artifacts["aggregates_csv"] = str(aggregates_csv.resolve())
    return artifacts
