from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


InputType = Literal["single_url", "csv_file"]
RunStatusLiteral = Literal[
    "queued",
    "running",
    "cancelling",
    "cancelled",
    "completed",
    "failed",
    "skipped_private",
    "needs_human",
    "resuming",
]


RUN_LOG_COLUMNS = [
    "scraped_at_ist",
    "run_id",
    "input_url",
    "normalized_profile_url",
    "status",
    "started_at_ist",
    "ended_at_ist",
    "duration_sec",
    "proxy_id",
    "session_mode",
    "challenge_encountered",
    "error_code",
    "error_message",
]

PROFILE_COLUMNS = [
    "scraped_at_ist",
    "run_id",
    "username",
    "profile_url",
    "full_name",
    "biography",
    "email_address",
    "external_url_primary",
    "followers_count",
    "following_count",
    "highlight_reel_count",
    "total_posts_count",
    "date_joined",
    "account_based_in",
    "active_ads_status",
    "active_ads_url",
    "time_verified",
    "is_verified",
    "is_private",
    "business_category",
    "profile_pic_url",
    "missing_reason_profile",
]

HIGHLIGHTS_COLUMNS = [
    "scraped_at_ist",
    "run_id",
    "username",
    "highlight_index",
    "highlight_name",
    "highlight_url",
    "missing_reason_highlight",
]

EXTERNAL_LINKS_COLUMNS = [
    "scraped_at_ist",
    "run_id",
    "username",
    "source_surface",
    "raw_url",
    "expanded_url",
    "final_url",
    "domain",
    "http_status",
    "is_linktree",
    "missing_reason_link",
]

POSTS_COLUMNS = [
    "scraped_at_ist",
    "run_id",
    "username",
    "shortcode",
    "post_url",
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
    "collaborators_csv",
    "caption_text",
    "location_name",
    "media_asset_urls_csv",
    "media_asset_local_paths_csv",
    "sample_bucket",
    "missing_reason_post",
]

AGGREGATES_COLUMNS = [
    "scraped_at_ist",
    "run_id",
    "username",
    "window_label",
    "window_days",
    "posts_total",
    "reels_total",
    "images_total",
    "reels_pct",
    "images_pct",
    "likes_total",
    "comments_total",
    "views_total",
    "remix_repost_total",
    "tagged_total",
    "avg_posts_per_day",
    "avg_posts_per_week",
    "avg_likes_per_post",
    "avg_comments_per_post",
    "top_liked_reel_url",
    "top_liked_post_url",
    "top_commented_reel_url",
    "top_commented_post_url",
    "top_viewed_reel_url",
    "top_viewed_post_url",
]


WINDOWS: list[tuple[str, int | None]] = [
    ("all_time", None),
    ("last_7_days", 7),
    ("last_15_days", 15),
    ("last_30_days", 30),
    ("last_90_days", 90),
    ("last_180_days", 180),
    ("last_365_days", 365),
]


class StartRunRequest(BaseModel):
    input_type: InputType = "single_url"
    input_value: str = Field(min_length=1)
    use_saved_session: bool = True
    proxy_pool_id: str | None = None
    max_entities: int | None = Field(default=None, ge=1, le=5000)
    fast_mode: bool = True
    reels_only: bool = True
    stats_only: bool = True

    @field_validator("input_value")
    @classmethod
    def strip_value(cls, value: str) -> str:
        return value.strip()


class SyncReelsCountsRequest(BaseModel):
    profile_url: str = Field(min_length=1)
    source_csv_path: str | None = None
    source_csv_filename: str | None = None
    source_csv_text: str | None = None
    use_saved_session: bool = True
    max_reels: int | None = Field(default=None, ge=1, le=5000)

    @field_validator("profile_url")
    @classmethod
    def strip_profile_url(cls, value: str) -> str:
        return value.strip()

    @field_validator("source_csv_path")
    @classmethod
    def strip_source_csv_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @field_validator("source_csv_filename")
    @classmethod
    def strip_source_csv_filename(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @field_validator("source_csv_text")
    @classmethod
    def validate_source_csv_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            return None
        return value


class RunStatusResponse(BaseModel):
    run_id: str
    status: RunStatusLiteral
    started_at_ist: str | None = None
    ended_at_ist: str | None = None
    progress_message: str | None = None
    progress_pct: float | None = None
    challenge_encountered: bool = False
    error_code: str | None = None
    error_message: str | None = None


class RunArtifactsResponse(BaseModel):
    run_id: str
    status: RunStatusLiteral
    artifacts: dict[str, str]


class ResumeRunRequest(BaseModel):
    notes: str | None = None


class RunContext(BaseModel):
    run_id: str
    input_url: str
    normalized_profile_url: str
    status: RunStatusLiteral = "queued"
    started_at_ist: str | None = None
    ended_at_ist: str | None = None
    duration_sec: float | None = None
    proxy_id: str | None = None
    session_mode: str = "anonymous_optional_saved_state"
    challenge_encountered: bool = False
    error_code: str | None = None
    error_message: str | None = None
    progress_message: str | None = None
    progress_pct: float = 0.0
    artifacts: dict[str, str] = Field(default_factory=dict)
    state: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_db(cls, row: dict[str, Any]) -> "RunContext":
        return cls(**row)


class PostRecord(BaseModel):
    scraped_at_ist: str
    run_id: str
    username: str
    shortcode: str
    post_url: str
    media_type: str | None = None
    posted_at_ist: str | None = None
    likes_count: int | None = None
    comments_count: int | None = None
    views_count: int | None = None
    repost_count: int | None = None
    is_remix_repost: bool | None = None
    is_tagged_post: bool | None = None
    tagged_users_count: int | None = None
    hashtags_csv: str | None = None
    keywords_csv: str | None = None
    mentions_csv: str | None = None
    collaborators_csv: str | None = None
    caption_text: str | None = None
    location_name: str | None = None
    media_asset_urls_csv: str | None = None
    media_asset_local_paths_csv: str | None = None
    sample_bucket: str | None = None
    missing_reason_post: str | None = None

    def to_row(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in POSTS_COLUMNS}


class ProfileRecord(BaseModel):
    scraped_at_ist: str
    run_id: str
    username: str | None = None
    profile_url: str | None = None
    full_name: str | None = None
    biography: str | None = None
    email_address: str | None = None
    external_url_primary: str | None = None
    followers_count: int | None = None
    following_count: int | None = None
    highlight_reel_count: int | None = None
    total_posts_count: int | None = None
    date_joined: str | None = None
    account_based_in: str | None = None
    active_ads_status: str | None = None
    active_ads_url: str | None = None
    time_verified: str | None = None
    is_verified: bool | None = None
    is_private: bool | None = None
    business_category: str | None = None
    profile_pic_url: str | None = None
    missing_reason_profile: str | None = None

    def to_row(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in PROFILE_COLUMNS}


def with_column_defaults(
    columns: list[str], rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append({col: row.get(col) for col in columns})
    return out


def parse_ist_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)
