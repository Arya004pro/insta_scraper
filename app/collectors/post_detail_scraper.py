from __future__ import annotations

import re
from datetime import timezone

from dateutil import parser as date_parser

from app.core.config import IST


HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
MENTION_RE = re.compile(r"@([A-Za-z0-9._]+)")


def _is_transient_error_or_blank(page: object) -> bool:
    title = ""
    body = ""
    try:
        title = str(page.title() or "")
    except Exception:
        title = ""
    try:
        body = str(page.inner_text("body", timeout=1200) or "")
    except Exception:
        body = ""

    body_clean = body.strip()
    if len(body_clean) < 20:
        return True

    text = f"{title}\n{body_clean}".lower()
    return (
        "something went wrong" in text
        or "page could not be loaded" in text
        or "reload page" in text
        or "please wait" in text
    )


def _best_src_from_srcset(srcset: str | None) -> str | None:
    if not srcset:
        return None
    best_url = None
    best_width = -1
    for chunk in srcset.split(","):
        part = chunk.strip()
        if not part:
            continue
        pieces = part.split()
        url = pieces[0]
        width = 0
        if len(pieces) > 1 and pieces[1].endswith("w"):
            try:
                width = int(pieces[1][:-1])
            except ValueError:
                width = 0
        if width > best_width:
            best_width = width
            best_url = url
    return best_url


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
    m_views = re.search(r"([\d,.]+[kmb]?)\s+(?:views?|plays?)\b", text, re.IGNORECASE)
    if m_views:
        views = _to_int(m_views.group(1))

    return likes, comments, views


def _extract_views_from_json_payload(page: object, shortcode: str) -> int | None:
    try:
        html = page.content()
    except Exception:
        return None

    if not html:
        return None

    scopes: list[str] = []
    token = f'"code":"{shortcode}"'
    idx = html.find(token)
    if idx >= 0:
        # Prefer metrics near the current shortcode to avoid picking unrelated cards.
        start = max(0, idx - 6000)
        end = min(len(html), idx + 20000)
        scopes.append(html[start:end])
    scopes.append(html)

    keys = ("video_view_count", "play_count", "video_play_count", "view_count")
    for scope in scopes:
        for key in keys:
            pattern = rf'"{key}"\s*:\s*(?:"([^"]+)"|([0-9][0-9,\.]*[kmbKMB]?)|null)'
            for match in re.finditer(pattern, scope):
                raw = match.group(1) or match.group(2)
                value = _to_int(raw) if raw else None
                if value is not None:
                    return value
    return None


def _extract_like_comment_from_json_payload(
    page: object, shortcode: str
) -> tuple[int | None, int | None]:
    try:
        html = page.content()
    except Exception:
        return None, None

    if not html:
        return None, None

    scopes: list[str] = []
    token = f'"code":"{shortcode}"'
    idx = html.find(token)
    if idx >= 0:
        # Prefer data around current shortcode to avoid counts from suggested cards.
        start = max(0, idx - 8000)
        end = min(len(html), idx + 30000)
        scopes.append(html[start:end])
    scopes.append(html)

    like_patterns = [
        r'"edge_media_preview_like"\s*:\s*\{[^{}]{0,300}?"count"\s*:\s*([0-9][0-9,\.]*)',
        r'"like_count"\s*:\s*([0-9][0-9,\.]*)',
    ]
    comment_patterns = [
        r'"edge_media_to_parent_comment"\s*:\s*\{[^{}]{0,300}?"count"\s*:\s*([0-9][0-9,\.]*)',
        r'"edge_media_to_comment"\s*:\s*\{[^{}]{0,300}?"count"\s*:\s*([0-9][0-9,\.]*)',
        r'"comment_count"\s*:\s*([0-9][0-9,\.]*)',
    ]

    likes: int | None = None
    comments: int | None = None
    for scope in scopes:
        if likes is None:
            for pattern in like_patterns:
                match = re.search(pattern, scope)
                if not match:
                    continue
                likes = _to_int(match.group(1))
                if likes is not None:
                    break
        if comments is None:
            for pattern in comment_patterns:
                match = re.search(pattern, scope)
                if not match:
                    continue
                comments = _to_int(match.group(1))
                if comments is not None:
                    break
        if likes is not None and comments is not None:
            break

    return likes, comments


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


def _extract_media_asset_urls(page: object) -> list[str]:
    def _collect_visible_article_assets() -> list[str]:
        out: list[str] = []
        seen_local: set[str] = set()
        try:
            image_nodes = page.locator("article img").all()
        except Exception:
            image_nodes = []
        for node in image_nodes:
            try:
                srcset = node.get_attribute("srcset")
                src = _best_src_from_srcset(srcset) or node.get_attribute("src")
            except Exception:
                src = None
            if not src or not src.startswith("http"):
                continue
            if src in seen_local:
                continue
            seen_local.add(src)
            out.append(src)

        try:
            video_nodes = page.locator(
                "article video[src], article video source[src]"
            ).all()
        except Exception:
            video_nodes = []
        for node in video_nodes:
            try:
                src = node.get_attribute("src")
            except Exception:
                src = None
            if not src or not src.startswith("http"):
                continue
            if src in seen_local:
                continue
            seen_local.add(src)
            out.append(src)
        return out

    def _click_carousel_next() -> bool:
        selectors = [
            "button[aria-label='Next']",
            "button:has(svg[aria-label='Next'])",
            "article button[aria-label='Next']",
        ]
        for selector in selectors:
            try:
                btn = page.locator(selector).first
                if btn.count() > 0:
                    btn.click(timeout=900)
                    return True
            except Exception:
                continue
        try:
            btn = page.get_by_role(
                "button", name=re.compile(r"^next$", re.IGNORECASE)
            ).first
            if btn.count() > 0:
                btn.click(timeout=900)
                return True
        except Exception:
            pass
        return False

    urls: list[str] = []
    seen: set[str] = set()

    for src in _collect_visible_article_assets():
        if src not in seen:
            seen.add(src)
            urls.append(src)

    # Traverse carousel cards to collect all media URLs when present.
    for _ in range(10):
        if not _click_carousel_next():
            break
        page.wait_for_timeout(350)
        added = 0
        for src in _collect_visible_article_assets():
            if src in seen:
                continue
            seen.add(src)
            urls.append(src)
            added += 1
        if added == 0:
            break

    if urls:
        return urls

    # Single media posts often expose a usable fallback via OG tags.
    for selector in [
        "meta[property='og:image']",
        "meta[property='og:video']",
    ]:
        try:
            content = page.locator(selector).first.get_attribute("content", timeout=800)
        except Exception:
            content = None
        if content and content.startswith("http") and content not in seen:
            seen.add(content)
            urls.append(content)
    return urls


def _extract_reel_video_urls(page: object) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    for selector in [
        "meta[property='og:video']",
        "meta[property='og:video:url']",
        "video[src]",
        "video source[src]",
    ]:
        try:
            nodes = page.locator(selector).all()
        except Exception:
            nodes = []
        for node in nodes:
            try:
                src = node.get_attribute("content") or node.get_attribute("src")
            except Exception:
                src = None
            if not src or not src.startswith("http"):
                continue
            if src in seen:
                continue
            seen.add(src)
            urls.append(src)

    try:
        html = page.content()
    except Exception:
        html = ""

    if html:
        for raw in re.findall(r'"video_url":"(https:[^"]+)"', html):
            decoded = raw.replace("\\/", "/").replace("\\u0026", "&")
            if decoded not in seen:
                seen.add(decoded)
                urls.append(decoded)

        for raw in re.findall(
            r'"video_versions":\[\{"type":\d+,"url":"(https:[^"]+)"', html
        ):
            decoded = raw.replace("\\/", "/").replace("\\u0026", "&")
            if decoded not in seen:
                seen.add(decoded)
                urls.append(decoded)

        # Sometimes JSON-LD contains contentUrl for videos.
        for raw in re.findall(r'"contentUrl"\s*:\s*"(https:[^"]+)"', html):
            decoded = raw.replace("\\/", "/").replace("\\u0026", "&")
            if decoded not in seen:
                seen.add(decoded)
                urls.append(decoded)

    # Guardrail: keep only URLs that look like video payloads for reels.
    filtered = []
    for url in urls:
        lower = url.lower()
        if ".mp4" in lower or "video" in lower:
            filtered.append(url)

    return filtered or urls


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
    for _ in range(2):
        page.goto(post_url, wait_until="domcontentloaded")
        if page_settle_ms > 0:
            page.wait_for_timeout(page_settle_ms)

        if not _is_transient_error_or_blank(page):
            break

        try:
            page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
            page.wait_for_timeout(max(600, page_settle_ms))
        except Exception:
            pass

    shortcode = post_url.rstrip("/").split("/")[-1]

    media_type = media_type_hint or ("reel" if "/reel/" in post_url else "image_post")
    if media_type == "image_post":
        try:
            if page.locator("video").count() > 0:
                media_type = "video_post"
        except Exception:
            pass

    if media_type == "reel":
        media_asset_urls = _extract_reel_video_urls(page)
    else:
        media_asset_urls = _extract_media_asset_urls(page)

    if media_type != "reel" and len(media_asset_urls) > 1:
        media_type = "carousel_post"

    if media_type == "reel" and media_asset_urls:
        # Reels should always prefer actual video URLs over thumbnails.
        media_asset_urls = [
            u for u in media_asset_urls if ".mp4" in u.lower() or "video" in u.lower()
        ] or media_asset_urls

    caption = _extract_caption(page)
    og_description = _extract_og_description(page)
    parse_text = " ".join([x for x in [caption, og_description] if x]) or ""
    likes_count, comments_count, views_count = _parse_counts_from_text(parse_text)

    json_likes, json_comments = _extract_like_comment_from_json_payload(page, shortcode)
    if json_likes is not None:
        likes_count = json_likes
    if json_comments is not None:
        comments_count = json_comments

    if likes_count is None or comments_count is None or views_count is None:
        try:
            body = page.inner_text("body", timeout=1500)
            l2, c2, v2 = _parse_counts_from_text(body)
            likes_count = likes_count if likes_count is not None else l2
            comments_count = comments_count if comments_count is not None else c2
            views_count = views_count if views_count is not None else v2
        except Exception:
            pass

    if views_count is None:
        views_count = _extract_views_from_json_payload(page, shortcode)

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
        "shortcode": shortcode,
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
        "media_asset_urls": media_asset_urls,
        "missing_reason_post": missing_reason,
    }
