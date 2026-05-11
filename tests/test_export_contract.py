from pathlib import Path

import pytest

from app.core.models import (
    AGGREGATES_COLUMNS,
    HIGHLIGHTS_COLUMNS,
    POSTS_COLUMNS,
    PROFILE_COLUMNS,
    RUN_LOG_COLUMNS,
)
from app.exporters.csv_exporter import (
    PROFILE_BIO_COLUMNS,
    PROFILE_CONTENT_COLUMNS,
    export_csv_artifacts,
)
from app.exporters.xlsx_exporter import export_xlsx_artifacts


def test_csv_export_contract(tmp_path: Path):
    run_log = [{k: None for k in RUN_LOG_COLUMNS}]
    run_log[0]["scraped_at_ist"] = "2026-04-17T10:00:00+05:30"
    profile = [{k: None for k in PROFILE_COLUMNS}]
    profile[0]["username"] = "indriyajewels"
    profile[0]["full_name"] = "indriyajewels"
    highlights = [{k: None for k in HIGHLIGHTS_COLUMNS}]
    links = []
    posts = [{k: None for k in POSTS_COLUMNS}]
    posts[0]["username"] = "indriyajewels"
    posts[0]["media_type"] = "image_post"
    aggs = [{k: None for k in AGGREGATES_COLUMNS}]
    summary = [
        {
            "Run ID": "r1",
            "Username": "indriyajewels",
            "Scraped At (IST)": "2026-04-17T10:00:00+05:30",
        }
    ]

    artifacts = export_csv_artifacts(
        exports_dir=tmp_path,
        media_dir=tmp_path / "media",
        base_name="dataset_test",
        run_log_rows=run_log,
        profile_rows=profile,
        highlights_rows=highlights,
        external_links_rows=links,
        posts_rows=posts,
        aggregate_rows=aggs,
        summary_flat_rows=summary,
    )

    assert set(artifacts.keys()) == {
        "profile_content_csv",
        "profiles_bio_csv",
        "profiles_rollup_csv",
    }

    content_csv = Path(artifacts["profile_content_csv"])
    header = content_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert header[0] == "scraped_at_ist"
    assert header == PROFILE_CONTENT_COLUMNS

    rollup_csv = Path(artifacts["profiles_rollup_csv"])
    rollup_header = rollup_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert "Run ID" in rollup_header
    assert "Username" in rollup_header

    bio_csv = Path(artifacts["profiles_bio_csv"])
    bio_header = bio_csv.read_text(encoding="utf-8").splitlines()[0].split(",")
    assert bio_header == PROFILE_BIO_COLUMNS


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
