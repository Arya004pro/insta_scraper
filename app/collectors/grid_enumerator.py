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


def _extract_media_links(page: object) -> list[tuple[str, str, str]]:
    found: list[tuple[str, str, str]] = []
    try:
        html = page.locator("article").first.inner_html(timeout=3000)
        found = extract_media_links_from_html(html)
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
        found.append((shortcode, post_url, media_kind))
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

    idle_rounds = 0
    while idle_rounds < settings.scroll_idle_rounds:
        previous_count = len(ordered)
        for shortcode, url, media_kind in _extract_media_links(page):
            if shortcode not in ordered:
                ordered[shortcode] = {
                    "shortcode": shortcode,
                    "post_url": url,
                    "media_type_hint": media_kind,
                }

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
