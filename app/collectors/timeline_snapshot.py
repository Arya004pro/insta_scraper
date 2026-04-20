from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from app.core.config import IST


HASHTAG_RE = re.compile(r"#([A-Za-z0-9_]+)")
MENTION_RE = re.compile(r"@([A-Za-z0-9._]+)")


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _extract_repost_count(node: dict[str, Any]) -> int | None:
    candidates = [
        node.get("repost_count"),
        node.get("media_repost_count"),
        node.get("reshare_count"),
        node.get("share_count"),
    ]
    clips = node.get("clips_metadata")
    if isinstance(clips, dict):
        candidates.extend(
            [
                clips.get("repost_count"),
                clips.get("reshare_count"),
                clips.get("share_count"),
            ]
        )

    for candidate in candidates:
        value = _to_int(candidate)
        if value is not None:
            return value
    return None


def _is_pinned_node(node: dict[str, Any]) -> bool:
    return bool(
        node.get("is_pinned")
        or node.get("is_pinned_for_users")
        or node.get("pinned_for_users")
        or node.get("pinned_for_clips_tabs")
    )


def _to_iso_ist_from_epoch(value: Any) -> str | None:
    try:
        ts = int(value)
    except Exception:
        return None
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(IST)
    return dt.isoformat()


def _best_image_url(image_versions2: dict[str, Any] | None) -> str | None:
    if not isinstance(image_versions2, dict):
        return None
    candidates = image_versions2.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        return None

    def _score(item: Any) -> int:
        if not isinstance(item, dict):
            return 0
        w = int(item.get("width") or 0)
        h = int(item.get("height") or 0)
        return w * h

    best = max(candidates, key=_score)
    url = best.get("url") if isinstance(best, dict) else None
    return url if isinstance(url, str) and url.startswith("http") else None


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


def _media_type_from_node(node: dict[str, Any]) -> str | None:
    media_type = int(node.get("media_type") or 0)
    product_type = str(node.get("product_type") or "").lower()

    if media_type == 8:
        return "carousel_post"
    if media_type == 2:
        if product_type == "clips":
            return "reel"
        return "video_post"
    if media_type == 1:
        return "image_post"
    return None


def _extract_media_urls(node: dict[str, Any], media_type: str | None) -> list[str]:
    urls: list[str] = []

    if media_type == "reel":
        for item in node.get("video_versions") or []:
            if not isinstance(item, dict):
                continue
            url = item.get("url")
            if isinstance(url, str) and url.startswith("http") and url not in urls:
                urls.append(url)
        return urls

    if media_type == "carousel_post":
        children = node.get("carousel_media") or []
        for child in children:
            if not isinstance(child, dict):
                continue
            c_type = int(child.get("media_type") or 0)
            if c_type == 1:
                image_url = _best_image_url(child.get("image_versions2"))
                if image_url and image_url not in urls:
                    urls.append(image_url)
            elif c_type == 2:
                # Keep mixed-media support, but images are preferred for this bucket.
                for item in child.get("video_versions") or []:
                    if not isinstance(item, dict):
                        continue
                    url = item.get("url")
                    if (
                        isinstance(url, str)
                        and url.startswith("http")
                        and url not in urls
                    ):
                        urls.append(url)
        return urls

    if media_type in {"image_post", "video_post"}:
        if media_type == "image_post":
            image_url = _best_image_url(node.get("image_versions2"))
            if image_url:
                urls.append(image_url)
        else:
            for item in node.get("video_versions") or []:
                if not isinstance(item, dict):
                    continue
                url = item.get("url")
                if isinstance(url, str) and url.startswith("http") and url not in urls:
                    urls.append(url)
            if not urls:
                image_url = _best_image_url(node.get("image_versions2"))
                if image_url:
                    urls.append(image_url)
    return urls


def _sample_bucket(media_type: str | None, urls: list[str]) -> str | None:
    if media_type == "reel":
        return "reels"
    if media_type in {"image_post", "video_post", "carousel_post"}:
        return "posts"
    return None


def collect_recent_timeline_items(
    page: object,
    profile_url: str,
    wait_ms: int = 4500,
) -> list[dict[str, Any]]:
    timeline_payload: dict[str, Any] | None = None

    def _on_response(response: Any) -> None:
        nonlocal timeline_payload
        if timeline_payload is not None:
            return

        ctype = (response.headers.get("content-type") or "").lower()
        if "application/json" not in ctype:
            return
        if "/graphql/query" not in response.url:
            return

        try:
            obj = json.loads(response.text())
        except Exception:
            return

        data = obj.get("data") if isinstance(obj, dict) else None
        if not isinstance(data, dict):
            return
        payload = data.get("xdt_api__v1__feed__user_timeline_graphql_connection")
        if isinstance(payload, dict):
            timeline_payload = payload

    page.on("response", _on_response)
    page.goto(profile_url, wait_until="domcontentloaded")
    if wait_ms > 0:
        page.wait_for_timeout(wait_ms)

    edges = (timeline_payload or {}).get("edges") or []
    results: list[dict[str, Any]] = []

    for edge in edges:
        node = edge.get("node") if isinstance(edge, dict) else None
        if not isinstance(node, dict):
            continue

        shortcode = node.get("code")
        if not isinstance(shortcode, str) or not shortcode:
            continue

        media_type = _media_type_from_node(node)
        media_urls = _extract_media_urls(node, media_type)
        bucket = _sample_bucket(media_type, media_urls)

        caption = None
        cap = node.get("caption")
        if isinstance(cap, dict):
            cap_text = cap.get("text")
            if isinstance(cap_text, str):
                caption = cap_text

        hashtags = HASHTAG_RE.findall(caption or "")
        mentions = MENTION_RE.findall(caption or "")
        tagged_count = 0
        usertags = node.get("usertags")
        if isinstance(usertags, dict):
            tagged = usertags.get("in")
            if isinstance(tagged, list):
                tagged_count = len(tagged)

        post_url = (
            f"https://www.instagram.com/reel/{shortcode}/"
            if media_type == "reel"
            else f"https://www.instagram.com/p/{shortcode}/"
        )

        taken_at_epoch = _to_int(node.get("taken_at"))

        result = {
            "shortcode": shortcode,
            "post_url": post_url,
            "media_type": media_type,
            "sample_bucket": bucket,
            "taken_at_epoch": taken_at_epoch,
            "posted_at_ist": _to_iso_ist_from_epoch(node.get("taken_at")),
            "likes_count": node.get("like_count"),
            "comments_count": node.get("comment_count"),
            "views_count": node.get("view_count") or node.get("play_count"),
            "repost_count": _extract_repost_count(node),
            "is_pinned": _is_pinned_node(node),
            "is_remix_repost": bool(
                re.search(r"\bremix\b|\brepost\b", caption or "", re.IGNORECASE)
            ),
            "is_tagged_post": tagged_count > 0,
            "tagged_users_count": tagged_count,
            "hashtags_csv": ",".join(sorted(set(hashtags))) if hashtags else None,
            "keywords_csv": _extract_keywords(caption),
            "mentions_csv": ",".join(sorted(set(mentions))) if mentions else None,
            "caption_text": caption,
            "location_name": (
                node.get("location", {}).get("name")
                if isinstance(node.get("location"), dict)
                else None
            ),
            "media_asset_urls": media_urls,
        }
        results.append(result)

    return results


def collect_recent_reels_tab_items(
    page: object,
    profile_url: str,
    wait_ms: int = 4500,
) -> list[dict[str, Any]]:
    reels_payload: dict[str, Any] | None = None

    def _on_response(response: Any) -> None:
        nonlocal reels_payload
        if reels_payload is not None:
            return

        ctype = (response.headers.get("content-type") or "").lower()
        if "application/json" not in ctype:
            return
        if "/graphql/query" not in response.url:
            return

        try:
            obj = json.loads(response.text())
        except Exception:
            return

        data = obj.get("data") if isinstance(obj, dict) else None
        if not isinstance(data, dict):
            return

        payload = data.get("xdt_api__v1__clips__user__connection_v2")
        if isinstance(payload, dict):
            reels_payload = payload

    page.on("response", _on_response)
    page.goto(f"{profile_url.rstrip('/')}/reels/", wait_until="domcontentloaded")
    if wait_ms > 0:
        page.wait_for_timeout(wait_ms)

    edges = (reels_payload or {}).get("edges") or []
    results: list[dict[str, Any]] = []

    for edge in edges:
        node = edge.get("node") if isinstance(edge, dict) else None
        if not isinstance(node, dict):
            continue

        media = node.get("media") if isinstance(node.get("media"), dict) else node
        if not isinstance(media, dict):
            continue

        shortcode = media.get("code")
        if not isinstance(shortcode, str) or not shortcode:
            continue

        play_or_view = media.get("play_count") or media.get("view_count")
        try:
            views_count = int(play_or_view) if play_or_view is not None else None
        except Exception:
            views_count = None

        repost_count = _extract_repost_count(media)
        if repost_count is None:
            repost_count = _extract_repost_count(node)

        taken_at_epoch = _to_int(media.get("taken_at"))

        try:
            likes_count = int(media.get("like_count"))
        except Exception:
            likes_count = None

        try:
            comments_count = int(media.get("comment_count"))
        except Exception:
            comments_count = None

        image_url = None
        image_versions = media.get("image_versions2")
        if isinstance(image_versions, dict):
            image_url = _best_image_url(image_versions)

        results.append(
            {
                "shortcode": shortcode,
                "post_url": f"https://www.instagram.com/reel/{shortcode}/",
                "media_type": "reel",
                "sample_bucket": "reels",
                "taken_at_epoch": taken_at_epoch,
                "posted_at_ist": _to_iso_ist_from_epoch(media.get("taken_at")),
                "likes_count": likes_count,
                "comments_count": comments_count,
                "views_count": views_count,
                "repost_count": repost_count,
                "is_pinned": _is_pinned_node(media) or _is_pinned_node(node),
                "thumbnail_url": image_url,
                "media_asset_urls": [image_url] if image_url else [],
            }
        )

    return results
