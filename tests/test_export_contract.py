from pathlib import Path

import pytest

from app.core.models import (
    AGGREGATES_COLUMNS,
    EXTERNAL_LINKS_COLUMNS,
    HIGHLIGHTS_COLUMNS,
    POSTS_COLUMNS,
    PROFILE_COLUMNS,
    RUN_LOG_COLUMNS,
)
from app.exporters.csv_exporter import export_csv_artifacts
from app.exporters.xlsx_exporter import export_xlsx_artifacts


def test_csv_export_contract(tmp_path: Path):
    run_log = [{k: None for k in RUN_LOG_COLUMNS}]
    run_log[0]["scraped_at_ist"] = "2026-04-17T10:00:00+05:30"
    profile = [{k: None for k in PROFILE_COLUMNS}]
    highlights = [{k: None for k in HIGHLIGHTS_COLUMNS}]
    links = [{k: None for k in EXTERNAL_LINKS_COLUMNS}]
    posts = [{k: None for k in POSTS_COLUMNS}]
    aggs = [{k: None for k in AGGREGATES_COLUMNS}]
    summary = [{"scraped_at_ist": "2026-04-17T10:00:00+05:30", "run_id": "r1"}]

    artifacts = export_csv_artifacts(
        exports_dir=tmp_path,
        base_name="dataset_test",
        run_log_rows=run_log,
        profile_rows=profile,
        highlights_rows=highlights,
        external_links_rows=links,
        posts_rows=posts,
        aggregate_rows=aggs,
        summary_flat_rows=summary,
    )

    assert set(artifacts.keys()) == {"posts_csv", "reels_csv", "master_summary_csv"}

    posts_csv = Path(artifacts["posts_csv"])
    header = posts_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert header[0] == "scraped_at_ist"
    assert header == POSTS_COLUMNS

    reels_csv = Path(artifacts["reels_csv"])
    reels_header = reels_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert reels_header[0] == "scraped_at_ist"
    assert reels_header == POSTS_COLUMNS


def test_xlsx_export_contract(tmp_path: Path):
    openpyxl = pytest.importorskip("openpyxl")
    run_log = [{k: None for k in RUN_LOG_COLUMNS}]
    run_log[0]["scraped_at_ist"] = "2026-04-17T10:00:00+05:30"
    profile = [{k: None for k in PROFILE_COLUMNS}]
    highlights = [{k: None for k in HIGHLIGHTS_COLUMNS}]
    links = [{k: None for k in EXTERNAL_LINKS_COLUMNS}]
    posts = [{k: None for k in POSTS_COLUMNS}]
    aggs = [{k: None for k in AGGREGATES_COLUMNS}]
    summary = [{"scraped_at_ist": "2026-04-17T10:00:00+05:30", "run_id": "r1"}]

    artifacts = export_xlsx_artifacts(
        exports_dir=tmp_path,
        base_name="dataset_test",
        run_log_rows=run_log,
        profile_rows=profile,
        highlights_rows=highlights,
        external_links_rows=links,
        posts_rows=posts,
        aggregate_rows=aggs,
        summary_flat_rows=summary,
    )
    wb = openpyxl.load_workbook(artifacts["normalized_xlsx"])
    assert wb["posts"]["A1"].value == "scraped_at_ist"
    assert wb["run_log"]["A1"].value == "scraped_at_ist"
