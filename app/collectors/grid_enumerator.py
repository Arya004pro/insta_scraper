from __future__ import annotations

import random
import re
from collections import OrderedDict

from app.core.config import Settings


POST_LINK_RE = re.compile(
    r"^https://www\.instagram\.com/(?:[A-Za-z0-9._]+/)?(p|reel)/([A-Za-z0-9_-]+)/?(?:\?.*)?$"
)
MEDIA_HREF_RE = re.compile(
    r"/(?:[A-Za-z0-9._]+/)?(?P<kind>p|reel)/(?P<shortcode>[A-Za-z0-9_-]+)"
)


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
    elif cleaned.endswith("b"):
        mult = 1_000_000_000
        cleaned = cleaned[:-1]
    try:
        return int(float(cleaned) * mult)
    except ValueError:
        return None


def _extract_media_links_via_dom(
    page: object,
) -> list[tuple[str, str, str, str | None, int | None, int | None]]:
    try:
        rows = page.evaluate(
            r"""
                        () => {
                            const firstCount = (text) => {
                                if (!text) return null;
                                const m = String(text).match(/([0-9][0-9.,]*\s*[kmbKMB]?)/);
                                return m ? m[1] : null;
                            };

                            const links = Array.from(document.querySelectorAll("article a[href]"));
                            const out = [];
                            const seen = new Set();

                            for (const a of links) {
                                const href = a.getAttribute("href") || "";
                                const m = href.match(/\/(?:[A-Za-z0-9._]+\/)?(p|reel)\/([A-Za-z0-9_-]+)/);
                                if (!m) continue;

                                const pathKind = m[1];
                                const shortcode = m[2];
                                if (seen.has(shortcode)) continue;
                                seen.add(shortcode);

                                let mediaHint = pathKind === "reel" ? "reel" : "image_post";
                                if (pathKind !== "reel") {
                                    const ariaLabel = (
                                        (a.querySelector("svg[aria-label]") && a.querySelector("svg[aria-label]").getAttribute("aria-label")) || ""
                                    ).toLowerCase();
                                    const titleText = (
                                        (a.querySelector("svg title") && a.querySelector("svg title").textContent) || ""
                                    ).toLowerCase();
                                    const marker = `${ariaLabel} ${titleText}`;

                                    if (marker.includes("carousel") || marker.includes("album") || marker.includes("multiple")) {
                                        mediaHint = "carousel_post";
                                    } else if (marker.includes("reel") || marker.includes("clip")) {
                                        mediaHint = "reel";
                                    }
                                }

                                let thumb = null;
                                const img = a.querySelector("img");
                                if (img) {
                                    const srcset = (img.getAttribute("srcset") || "").trim();
                                    const firstSrcsetUrl = srcset
                                        ? srcset.split(",")[0].trim().split(/\s+/)[0]
                                        : null;
                                    thumb = img.currentSrc || img.getAttribute("src") || firstSrcsetUrl || null;
                                }

                                const blob = `${a.getAttribute("aria-label") || ""} ${a.innerText || a.textContent || ""}`;
                                let likesRaw = null;
                                let commentsRaw = null;

                                const mLikes = blob.match(/([0-9][0-9.,]*\s*[kmbKMB]?)\s+likes?/i);
                                if (mLikes) likesRaw = mLikes[1];
                                const mComments = blob.match(/([0-9][0-9.,]*\s*[kmbKMB]?)\s+comments?/i);
                                if (mComments) commentsRaw = mComments[1];

                                if (!likesRaw) {
                                    const likesIcon = a.querySelector("svg[aria-label='Likes'], svg[aria-label='likes']");
                                    if (likesIcon) {
                                        likesRaw = firstCount(likesIcon.closest("li")?.textContent || likesIcon.parentElement?.textContent || "");
                                    }
                                }
                                if (!commentsRaw) {
                                    const commentsIcon = a.querySelector("svg[aria-label='Comments'], svg[aria-label='Comment'], svg[aria-label='comments'], svg[aria-label='comment']");
                                    if (commentsIcon) {
                                        commentsRaw = firstCount(commentsIcon.closest("li")?.textContent || commentsIcon.parentElement?.textContent || "");
                                    }
                                }

                                out.push({ shortcode, href, mediaHint, thumbnailUrl: thumb, likesRaw, commentsRaw });
                            }

                            return out;
                        }
                        """
        )
    except Exception:
        return []

    found: list[tuple[str, str, str, str | None, int | None, int | None]] = []
    for row in rows or []:
        href = row.get("href") if isinstance(row, dict) else None
        normalized = _normalize_media_href(href)
        if not normalized:
            continue
        shortcode, post_url, media_kind = normalized
        media_hint = row.get("mediaHint") if isinstance(row, dict) else None
        thumbnail_url = row.get("thumbnailUrl") if isinstance(row, dict) else None
        likes_count = _to_int(row.get("likesRaw")) if isinstance(row, dict) else None
        comments_count = (
            _to_int(row.get("commentsRaw")) if isinstance(row, dict) else None
        )
        if media_hint in {"reel", "image_post", "carousel_post"}:
            media_kind = media_hint
        found.append(
            (
                shortcode,
                post_url,
                media_kind,
                thumbnail_url,
                likes_count,
                comments_count,
            )
        )
    return found


def _normalize_media_href(href: str | None) -> tuple[str, str, str] | None:
    if not href:
        return None
    match = MEDIA_HREF_RE.search(href)
    if not match:
        return None
    kind = match.group("kind")
    shortcode = match.group("shortcode")
    media_kind = "reel" if kind == "reel" else "image_post"
    post_url = f"https://www.instagram.com/{kind}/{shortcode}/"
    return shortcode, post_url, media_kind


def extract_media_links_from_html(html: str) -> list[tuple[str, str, str]]:
    found: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for m in re.finditer(r'href=["\'](?P<href>[^"\']+)["\']', html):
        normalized = _normalize_media_href(m.group("href"))
        if not normalized:
            continue
        shortcode, post_url, media_kind = normalized
        if shortcode in seen:
            continue
        seen.add(shortcode)
        found.append((shortcode, post_url, media_kind))
    return found


def _extract_media_links(
    page: object,
) -> list[tuple[str, str, str, str | None, int | None, int | None]]:
    dom_found = _extract_media_links_via_dom(page)
    if dom_found:
        return dom_found

    found: list[tuple[str, str, str, str | None, int | None, int | None]] = []
    try:
        html = page.locator("article").first.inner_html(timeout=3000)
        found = [
            (s, u, k, None, None, None)
            for s, u, k in extract_media_links_from_html(html)
        ]
    except Exception:
        found = []

    if found:
        return found

    seen: set[str] = set()
    try:
        links = page.locator("a[href]").all()
    except Exception:
        links = []
    for link in links:
        try:
            href = link.get_attribute("href")
        except Exception:
            href = None
        normalized = _normalize_media_href(href)
        if not normalized:
            continue
        shortcode, post_url, media_kind = normalized
        if shortcode in seen:
            continue
        seen.add(shortcode)
        found.append((shortcode, post_url, media_kind, None, None, None))
    return found


def enumerate_grid_posts(
    page: object,
    settings: Settings,
    resume_state: dict | None = None,
) -> list[dict]:
    ordered: OrderedDict[str, dict] = OrderedDict()
    resume_state = resume_state or {}
    for row in resume_state.get("discovered_posts", []):
        shortcode = row.get("shortcode")
        if shortcode:
            ordered[shortcode] = row

    for (
        shortcode,
        url,
        media_kind,
        thumbnail_url,
        likes_count,
        comments_count,
    ) in _extract_media_links(page):
        if shortcode not in ordered:
            ordered[shortcode] = {
                "shortcode": shortcode,
                "post_url": url,
                "media_type_hint": media_kind,
                "thumbnail_url": thumbnail_url,
                "likes_count": likes_count,
                "comments_count": comments_count,
            }
        else:
            existing = ordered[shortcode]
            if existing.get("media_type_hint") == "image_post" and media_kind in {
                "carousel_post",
                "reel",
            }:
                existing["media_type_hint"] = media_kind
            if existing.get("thumbnail_url") is None and thumbnail_url:
                existing["thumbnail_url"] = thumbnail_url
            if existing.get("likes_count") is None and likes_count is not None:
                existing["likes_count"] = likes_count
            if existing.get("comments_count") is None and comments_count is not None:
                existing["comments_count"] = comments_count

    if settings.scroll_idle_rounds <= 0:
        return list(ordered.values())

    idle_rounds = 0
    while idle_rounds < settings.scroll_idle_rounds:
        previous_count = len(ordered)
        for (
            shortcode,
            url,
            media_kind,
            thumbnail_url,
            likes_count,
            comments_count,
        ) in _extract_media_links(page):
            if shortcode not in ordered:
                ordered[shortcode] = {
                    "shortcode": shortcode,
                    "post_url": url,
                    "media_type_hint": media_kind,
                    "thumbnail_url": thumbnail_url,
                    "likes_count": likes_count,
                    "comments_count": comments_count,
                }
            else:
                existing = ordered[shortcode]
                if existing.get("media_type_hint") == "image_post" and media_kind in {
                    "carousel_post",
                    "reel",
                }:
                    existing["media_type_hint"] = media_kind
                if existing.get("thumbnail_url") is None and thumbnail_url:
                    existing["thumbnail_url"] = thumbnail_url
                if existing.get("likes_count") is None and likes_count is not None:
                    existing["likes_count"] = likes_count
                if (
                    existing.get("comments_count") is None
                    and comments_count is not None
                ):
                    existing["comments_count"] = comments_count

        if len(ordered) == previous_count:
            idle_rounds += 1
        else:
            idle_rounds = 0

        page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight * 0.9));")
        wait_ms = random.randint(
            settings.scroll_pause_min_ms, settings.scroll_pause_max_ms
        )
        page.wait_for_timeout(wait_ms)

        try:
            is_loading = page.locator("svg[aria-label='Loading...']").count() > 0
        except Exception:
            is_loading = False
        if is_loading:
            page.wait_for_timeout(1200)

    return list(ordered.values())
