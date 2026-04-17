from __future__ import annotations

import re
from datetime import timezone

from dateutil import parser as date_parser

from app.core.config import IST


HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
MENTION_RE = re.compile(r"@([A-Za-z0-9._]+)")


def _to_int(raw: str | None) -> int | None:
    if not raw:
        return None
    cleaned = raw.strip().replace(",", "").lower()
    mult = 1
    if cleaned.endswith("k"):
        mult = 1_000
        cleaned = cleaned[:-1]
    elif cleaned.endswith("m"):
        mult = 1_000_000
        cleaned = cleaned[:-1]
    try:
        return int(float(cleaned) * mult)
    except ValueError:
        return None


def _extract_og_description(page: object) -> str | None:
    try:
        value = page.locator("meta[property='og:description']").first.get_attribute(
            "content", timeout=1500
        )
        return value or None
    except Exception:
        return None


def _parse_counts_from_text(text: str) -> tuple[int | None, int | None, int | None]:
    likes = None
    comments = None
    views = None

    m_likes = re.search(r"([\d,.]+[kmb]?)\s+likes", text, re.IGNORECASE)
    if m_likes:
        likes = _to_int(m_likes.group(1))
    m_comments = re.search(r"([\d,.]+[kmb]?)\s+comments?", text, re.IGNORECASE)
    if m_comments:
        comments = _to_int(m_comments.group(1))
    m_views = re.search(r"([\d,.]+[kmb]?)\s+views", text, re.IGNORECASE)
    if m_views:
        views = _to_int(m_views.group(1))

    return likes, comments, views


def _extract_caption(page: object) -> str | None:
    selectors = ["article h1", "main h1", "article ul li h1"]
    for selector in selectors:
        try:
            value = page.locator(selector).first.inner_text(timeout=1500).strip()
            if value:
                return value
        except Exception:
            continue
    return None


def _extract_posted_time_ist(page: object) -> str | None:
    try:
        dt = page.locator("time").first.get_attribute("datetime", timeout=1500)
    except Exception:
        dt = None
    if not dt:
        return None
    try:
        parsed = date_parser.parse(dt)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(IST).isoformat()
    except Exception:
        return None


def _extract_location(page: object) -> str | None:
    try:
        loc = (
            page.locator("a[href*='/explore/locations/']")
            .first.inner_text(timeout=1000)
            .strip()
        )
        return loc or None
    except Exception:
        return None


def _extract_keywords(caption: str | None) -> str | None:
    if not caption:
        return None
    words = re.findall(r"[A-Za-z]{4,}", caption.lower())
    stop = {
        "this",
        "that",
        "with",
        "your",
        "from",
        "have",
        "will",
        "about",
        "into",
        "when",
        "what",
    }
    freq: dict[str, int] = {}
    for word in words:
        if word in stop:
            continue
        freq[word] = freq.get(word, 0) + 1
    if not freq:
        return None
    top = sorted(freq.items(), key=lambda x: (-x[1], x[0]))[:15]
    return ",".join([k for k, _ in top])


def scrape_post_detail(
    page: object,
    post_url: str,
    media_type_hint: str | None = None,
    page_settle_ms: int = 700,
) -> dict:
    page.goto(post_url, wait_until="domcontentloaded")
    if page_settle_ms > 0:
        page.wait_for_timeout(page_settle_ms)

    media_type = media_type_hint or ("reel" if "/reel/" in post_url else "image_post")
    if media_type == "image_post":
        try:
            if page.locator("video").count() > 0:
                media_type = "video_post"
        except Exception:
            pass

    caption = _extract_caption(page)
    og_description = _extract_og_description(page)
    parse_text = " ".join([x for x in [caption, og_description] if x]) or ""
    likes_count, comments_count, views_count = _parse_counts_from_text(parse_text)

    if likes_count is None or comments_count is None:
        try:
            body = page.inner_text("body", timeout=1500)
            l2, c2, v2 = _parse_counts_from_text(body)
            likes_count = likes_count if likes_count is not None else l2
            comments_count = comments_count if comments_count is not None else c2
            views_count = views_count if views_count is not None else v2
        except Exception:
            pass

    hashtags = HASHTAG_RE.findall(caption or "")
    mentions = MENTION_RE.findall(caption or "")
    is_remix = bool(re.search(r"\bremix\b|\brepost\b", (caption or ""), re.IGNORECASE))
    tagged_count = len(set(mentions))
    is_tagged = tagged_count > 0

    missing_reason = None
    if (
        caption is None
        and likes_count is None
        and comments_count is None
        and views_count is None
    ):
        missing_reason = "parse_error"

    return {
        "shortcode": post_url.rstrip("/").split("/")[-1],
        "post_url": post_url,
        "media_type": media_type,
        "posted_at_ist": _extract_posted_time_ist(page),
        "likes_count": likes_count,
        "comments_count": comments_count,
        "views_count": views_count,
        "is_remix_repost": is_remix,
        "is_tagged_post": is_tagged,
        "tagged_users_count": tagged_count,
        "hashtags_csv": ",".join(sorted(set(hashtags))) if hashtags else None,
        "keywords_csv": _extract_keywords(caption),
        "mentions_csv": ",".join(sorted(set(mentions))) if mentions else None,
        "caption_text": caption,
        "location_name": _extract_location(page),
        "missing_reason_post": missing_reason,
    }
