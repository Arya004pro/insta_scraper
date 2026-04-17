from datetime import datetime
from zoneinfo import ZoneInfo

from app.metrics.aggregator import build_aggregates


def test_build_aggregates_windows_and_ratios():
    now = datetime(2026, 4, 17, 12, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
    rows = [
        {
            "media_type": "reel",
            "posted_at_ist": "2026-04-16T10:00:00+05:30",
            "likes_count": 100,
            "comments_count": 10,
            "views_count": 1000,
            "shortcode": "R1",
            "post_url": "https://www.instagram.com/reel/R1/",
            "is_remix_repost": True,
            "tagged_users_count": 2,
        },
        {
            "media_type": "image_post",
            "posted_at_ist": "2026-04-10T10:00:00+05:30",
            "likes_count": 50,
            "comments_count": 5,
            "views_count": 100,
            "shortcode": "P1",
            "post_url": "https://www.instagram.com/p/P1/",
            "is_remix_repost": False,
            "tagged_users_count": 1,
        },
    ]
    out = build_aggregates(
        "2026-04-17T12:00:00+05:30", "run1", "indriyajewels", rows, now=now
    )
    all_time = next(x for x in out if x["window_label"] == "all_time")
    assert all_time["posts_total"] == 2
    assert all_time["reels_total"] == 1
    assert all_time["images_total"] == 1
    assert all_time["reels_pct"] == 50.0
    assert all_time["likes_total"] == 150
    assert all_time["top_liked_reel_url"] == "https://www.instagram.com/reel/R1/"
    assert all_time["top_liked_post_url"] == "https://www.instagram.com/p/P1/"

    last_7 = next(x for x in out if x["window_label"] == "last_7_days")
    assert last_7["posts_total"] == 1
    assert last_7["reels_total"] == 1
    assert last_7["images_total"] == 0
