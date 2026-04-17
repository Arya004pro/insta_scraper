from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.core.models import WINDOWS


def _to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _sum_int(rows: list[dict], key: str) -> int:
    total = 0
    for row in rows:
        value = row.get(key)
        if isinstance(value, int):
            total += value
    return total


def _top_metric_value(
    rows: list[dict], media_type: str, metric_key: str, value_key: str = "post_url"
) -> str | None:
    filtered = [
        r
        for r in rows
        if r.get("media_type") == media_type and isinstance(r.get(metric_key), int)
    ]
    if not filtered:
        return None
    best = max(filtered, key=lambda r: r.get(metric_key, 0))
    return best.get(value_key)


WINDOW_LABELS = {
    "all_time": "All Time",
    "last_7_days": "Last 7 Days",
    "last_15_days": "Last 15 Days",
    "last_30_days": "Last 30 Days",
    "last_90_days": "Last Quarter",
    "last_180_days": "Last 6 Months",
    "last_365_days": "Last Year",
}


WINDOW_METRIC_LABELS = {
    "window_days": "Window Days",
    "posts_total": "Posts Total",
    "reels_total": "Reels Total",
    "images_total": "Image Posts Total",
    "reels_pct": "Reel %",
    "images_pct": "Image Post %",
    "likes_total": "Likes Total",
    "comments_total": "Comments Total",
    "views_total": "Views Total",
    "remix_repost_total": "Remix/Repost Total",
    "tagged_total": "Tagged Total",
    "avg_posts_per_day": "Avg Posts / Day",
    "avg_posts_per_week": "Avg Posts / Week",
    "avg_likes_per_post": "Avg Likes / Post",
    "avg_comments_per_post": "Avg Comments / Post",
    "top_liked_reel_url": "Top Liked Reel URL",
    "top_liked_post_url": "Top Liked Post URL",
    "top_commented_reel_url": "Top Commented Reel URL",
    "top_commented_post_url": "Top Commented Post URL",
    "top_viewed_reel_url": "Top Viewed Reel URL",
    "top_viewed_post_url": "Top Viewed Post URL",
}


def _format_ist_readable(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return value
    return dt.strftime("%d %b %Y %I:%M:%S %p IST")


def _window_rows(rows: list[dict], now: datetime, days: int | None) -> list[dict]:
    if days is None:
        return rows
    cutoff = now - timedelta(days=days)
    return [
        r
        for r in rows
        if (_to_dt(r.get("posted_at_ist")) or datetime.min.replace(tzinfo=now.tzinfo))
        >= cutoff
    ]


def _avg_posts_per_day(rows: list[dict], days: int | None, now: datetime) -> float:
    if not rows:
        return 0.0
    if days is None:
        dts = sorted(
            [
                _to_dt(r.get("posted_at_ist"))
                for r in rows
                if _to_dt(r.get("posted_at_ist")) is not None
            ]
        )
        if not dts:
            span_days = 1
        else:
            span_days = max(1, (now - dts[0]).days + 1)
    else:
        span_days = max(1, days)
    return round(len(rows) / span_days, 4)


def build_aggregates(
    scraped_at_ist: str,
    run_id: str,
    username: str,
    posts_rows: list[dict],
    now: datetime,
) -> list[dict]:
    all_rows = list(posts_rows)
    output: list[dict] = []
    for window_label, window_days in WINDOWS:
        rows = _window_rows(all_rows, now, window_days)
        reels = [r for r in rows if r.get("media_type") == "reel"]
        images = [r for r in rows if r.get("media_type") != "reel"]
        total_posts = len(rows)
        reels_total = len(reels)
        images_total = len(images)

        likes_total = _sum_int(rows, "likes_count")
        comments_total = _sum_int(rows, "comments_count")
        views_total = _sum_int(rows, "views_count")
        remix_total = sum(1 for r in rows if r.get("is_remix_repost") is True)
        tagged_total = sum(
            (r.get("tagged_users_count") or 0)
            for r in rows
            if isinstance(r.get("tagged_users_count"), int)
        )

        avg_day = _avg_posts_per_day(rows, window_days, now)
        avg_week = round(avg_day * 7, 4)
        avg_likes = round(likes_total / total_posts, 4) if total_posts else 0.0
        avg_comments = round(comments_total / total_posts, 4) if total_posts else 0.0
        reels_pct = round((reels_total / total_posts) * 100, 2) if total_posts else 0.0
        images_pct = (
            round((images_total / total_posts) * 100, 2) if total_posts else 0.0
        )

        output.append(
            {
                "scraped_at_ist": scraped_at_ist,
                "run_id": run_id,
                "username": username,
                "window_label": window_label,
                "window_days": window_days,
                "posts_total": total_posts,
                "reels_total": reels_total,
                "images_total": images_total,
                "reels_pct": reels_pct,
                "images_pct": images_pct,
                "likes_total": likes_total,
                "comments_total": comments_total,
                "views_total": views_total,
                "remix_repost_total": remix_total,
                "tagged_total": tagged_total,
                "avg_posts_per_day": avg_day,
                "avg_posts_per_week": avg_week,
                "avg_likes_per_post": avg_likes,
                "avg_comments_per_post": avg_comments,
                "top_liked_reel_url": _top_metric_value(rows, "reel", "likes_count"),
                "top_liked_post_url": _top_metric_value(
                    rows, "image_post", "likes_count"
                )
                or _top_metric_value(rows, "video_post", "likes_count"),
                "top_commented_reel_url": _top_metric_value(
                    rows, "reel", "comments_count"
                ),
                "top_commented_post_url": _top_metric_value(
                    rows, "image_post", "comments_count"
                )
                or _top_metric_value(rows, "video_post", "comments_count"),
                "top_viewed_reel_url": _top_metric_value(rows, "reel", "views_count"),
                "top_viewed_post_url": _top_metric_value(
                    rows, "image_post", "views_count"
                )
                or _top_metric_value(rows, "video_post", "views_count"),
            }
        )
    return output


def build_summary_flat(
    run_log_row: dict[str, Any],
    profile_row: dict[str, Any],
    aggregate_rows: list[dict[str, Any]],
    highlights_rows: list[dict[str, Any]],
    external_links_rows: list[dict[str, Any]],
    posts_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    row: dict[str, Any] = {
        "Scraped At (IST)": _format_ist_readable(run_log_row.get("scraped_at_ist")),
        "Scraped At ISO": run_log_row.get("scraped_at_ist"),
        "Run ID": run_log_row.get("run_id"),
        "Run Status": run_log_row.get("status"),
        "Input URL": run_log_row.get("input_url"),
        "Normalized Profile URL": run_log_row.get("normalized_profile_url"),
        "Session Mode": run_log_row.get("session_mode"),
        "Proxy ID": run_log_row.get("proxy_id"),
        "Challenge Encountered": run_log_row.get("challenge_encountered"),
        "Started At (IST)": _format_ist_readable(run_log_row.get("started_at_ist")),
        "Ended At (IST)": _format_ist_readable(run_log_row.get("ended_at_ist")),
        "Duration (sec)": run_log_row.get("duration_sec"),
        "Username": profile_row.get("username"),
        "Profile URL": profile_row.get("profile_url"),
        "Full Name": profile_row.get("full_name"),
        "Biography": profile_row.get("biography"),
        "External URL (Primary)": profile_row.get("external_url_primary"),
        "Followers": profile_row.get("followers_count"),
        "Following": profile_row.get("following_count"),
        "Highlight Reels": profile_row.get("highlight_reel_count"),
        "Total Posts (Profile)": profile_row.get("total_posts_count"),
        "Date Joined": profile_row.get("date_joined"),
        "Active Ads": profile_row.get("active_ads_status"),
        "Time Verified": profile_row.get("time_verified"),
        "Is Verified": profile_row.get("is_verified"),
        "Is Private": profile_row.get("is_private"),
        "Business Category": profile_row.get("business_category"),
        "Profile Pic URL": profile_row.get("profile_pic_url"),
        "Profile Missing Reason": profile_row.get("missing_reason_profile"),
    }

    for agg in aggregate_rows:
        window_label = agg.get("window_label")
        if not window_label:
            continue
        prefix = WINDOW_LABELS.get(window_label, window_label.replace("_", " ").title())
        for key, value in agg.items():
            if key in ("scraped_at_ist", "run_id", "username", "window_label"):
                continue
            metric_label = WINDOW_METRIC_LABELS.get(key, key.replace("_", " ").title())
            row[f"{prefix} {metric_label}"] = value

    highlight_names = sorted(
        {
            (r.get("highlight_name") or "").strip()
            for r in highlights_rows
            if (r.get("highlight_name") or "").strip()
        }
    )
    final_urls = sorted(
        {
            (
                r.get("final_url") or r.get("expanded_url") or r.get("raw_url") or ""
            ).strip()
            for r in external_links_rows
            if (
                r.get("final_url") or r.get("expanded_url") or r.get("raw_url") or ""
            ).strip()
        }
    )
    domains = sorted(
        {
            (r.get("domain") or "").strip().lower()
            for r in external_links_rows
            if (r.get("domain") or "").strip()
        }
    )
    row["External Links Total"] = len(external_links_rows)
    row["External Domains (CSV)"] = ",".join(domains) if domains else None
    row["External URLs (CSV)"] = ",".join(final_urls) if final_urls else None

    row["Posts Rows Exported"] = len(posts_rows)
    row["Posts Parse Error Count"] = sum(
        1 for r in posts_rows if r.get("missing_reason_post") == "parse_error"
    )

    row["Highlights Total"] = len(highlights_rows)
    row["Highlight Names (CSV)"] = (
        ",".join(highlight_names) if highlight_names else None
    )

    return [row]
