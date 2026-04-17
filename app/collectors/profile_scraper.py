from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urljoin


PRIVATE_PATTERNS = [
    re.compile(r"this account is private", re.IGNORECASE),
    re.compile(r"this profile is private", re.IGNORECASE),
]


def parse_metric_count(raw: str | None) -> int | None:
    if not raw:
        return None
    value = raw.strip().replace(",", "").lower()
    multiplier = 1
    if value.endswith("k"):
        multiplier = 1_000
        value = value[:-1]
    elif value.endswith("m"):
        multiplier = 1_000_000
        value = value[:-1]
    elif value.endswith("b"):
        multiplier = 1_000_000_000
        value = value[:-1]
    try:
        return int(float(value) * multiplier)
    except ValueError:
        return None


def detect_private_profile_from_text(text: str) -> bool:
    return any(p.search(text) for p in PRIVATE_PATTERNS)


def _extract_header_count(page: object, label_contains: str) -> int | None:
    try:
        el = page.locator(f"header li:has-text('{label_contains}')").first
        txt = el.inner_text(timeout=2000)
        match = re.search(r"([\d.,]+[kmb]?)", txt, re.IGNORECASE)
        return parse_metric_count(match.group(1) if match else None)
    except Exception:
        return None


def _extract_og_description(page: object) -> str | None:
    try:
        value = page.locator("meta[property='og:description']").first.get_attribute(
            "content", timeout=1500
        )
        return value or None
    except Exception:
        return None


def _parse_counts_from_og_description(text: str | None) -> dict[str, int | None]:
    if not text:
        return {
            "followers_count": None,
            "following_count": None,
            "total_posts_count": None,
        }

    followers_match = re.search(r"([\d.,]+[kmb]?)\s+followers", text, re.IGNORECASE)
    following_match = re.search(r"([\d.,]+[kmb]?)\s+following", text, re.IGNORECASE)
    posts_match = re.search(r"([\d.,]+[kmb]?)\s+posts", text, re.IGNORECASE)

    return {
        "followers_count": parse_metric_count(
            followers_match.group(1) if followers_match else None
        ),
        "following_count": parse_metric_count(
            following_match.group(1) if following_match else None
        ),
        "total_posts_count": parse_metric_count(
            posts_match.group(1) if posts_match else None
        ),
    }


def _extract_full_name_from_og_description(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(
        r"see instagram photos and videos from\s+(.+?)\s*\(@",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    value = match.group(1).strip()
    return value or None


def _extract_biography(page: object) -> str | None:
    selectors = [
        "header section div.-vDIg span",
        "header section span[dir='auto']",
        "header h1 + div span",
    ]
    for selector in selectors:
        try:
            txt = page.locator(selector).first.inner_text(timeout=1500).strip()
            if txt:
                return txt
        except Exception:
            continue
    return None


def _extract_full_name(page: object) -> str | None:
    for selector in ["header h2", "header h1", "main header section h2"]:
        try:
            value = page.locator(selector).first.inner_text(timeout=1500).strip()
            if value:
                return value
        except Exception:
            continue
    return None


def _extract_profile_pic(page: object) -> str | None:
    for selector in ["header img", "img[alt*='profile picture']"]:
        try:
            src = page.locator(selector).first.get_attribute("src", timeout=1500)
            if src:
                return src
        except Exception:
            continue
    return None


def _extract_primary_external_url(page: object) -> str | None:
    candidates = [
        "header a[href^='http']",
        "main header a[href^='http']",
    ]
    for selector in candidates:
        try:
            href = page.locator(selector).first.get_attribute("href", timeout=1500)
            if href:
                return href
        except Exception:
            continue
    return None


def _extract_all_external_urls(page: object, base_url: str) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    try:
        links = page.locator("header a[href]").all()
    except Exception:
        links = []
    for link in links:
        try:
            href = link.get_attribute("href")
        except Exception:
            href = None
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if absolute.startswith("https://www.instagram.com/"):
            continue
        if absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)
    return urls


@dataclass
class ProfileScrapeResult:
    profile_data: dict
    external_urls: list[str]
    is_private: bool


def scrape_profile_header(page: object, profile_url: str) -> ProfileScrapeResult:
    page.goto(profile_url, wait_until="domcontentloaded")
    page.wait_for_timeout(1500)

    try:
        page.get_by_role("button", name=re.compile(r"not now", re.IGNORECASE)).click(
            timeout=1200
        )
    except Exception:
        pass

    body_text = ""
    try:
        body_text = page.inner_text("body", timeout=2000)
    except Exception:
        pass
    is_private = detect_private_profile_from_text(body_text)

    username = profile_url.rstrip("/").split("/")[-1]
    og_description = _extract_og_description(page)
    og_counts = _parse_counts_from_og_description(og_description)

    followers_count = _extract_header_count(page, "followers")
    following_count = _extract_header_count(page, "following")
    total_posts_count = _extract_header_count(page, "posts")

    profile_data = {
        "username": username,
        "profile_url": profile_url,
        "full_name": _extract_full_name(page),
        "biography": _extract_biography(page),
        "external_url_primary": _extract_primary_external_url(page),
        "followers_count": followers_count
        if followers_count is not None
        else og_counts["followers_count"],
        "following_count": following_count
        if following_count is not None
        else og_counts["following_count"],
        "highlight_reel_count": None,
        "total_posts_count": total_posts_count
        if total_posts_count is not None
        else og_counts["total_posts_count"],
        "is_verified": False,
        "is_private": is_private,
        "business_category": None,
        "profile_pic_url": _extract_profile_pic(page),
        "missing_reason_profile": None,
    }

    try:
        profile_data["is_verified"] = (
            page.locator("header svg[aria-label='Verified']").count() > 0
        )
    except Exception:
        profile_data["is_verified"] = None

    try:
        profile_data["highlight_reel_count"] = page.locator("section ul li").count()
    except Exception:
        profile_data["highlight_reel_count"] = None

    if not profile_data["full_name"]:
        profile_data["full_name"] = _extract_full_name_from_og_description(
            og_description
        )

    external_urls = _extract_all_external_urls(page, profile_url)
    if (
        profile_data["external_url_primary"]
        and profile_data["external_url_primary"] not in external_urls
    ):
        external_urls.insert(0, profile_data["external_url_primary"])

    if not profile_data["full_name"] and not profile_data["biography"]:
        profile_data["missing_reason_profile"] = "parse_error"

    return ProfileScrapeResult(
        profile_data=profile_data, external_urls=external_urls, is_private=is_private
    )
