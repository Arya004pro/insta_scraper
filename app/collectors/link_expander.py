from __future__ import annotations

import re
from html import unescape
from urllib.parse import urlparse

import requests


LINKTREE_HOSTS = {"linktr.ee", "bio.site", "beacons.ai", "taplink.cc"}
HREF_RE = re.compile(r'href=[\'"]([^\'"]+)[\'"]', re.IGNORECASE)


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return (urlparse(url).hostname or "").lower() or None
    except Exception:
        return None


def _fetch(url: str, timeout_seconds: int) -> tuple[str | None, int | None, str | None]:
    try:
        resp = requests.get(url, allow_redirects=True, timeout=timeout_seconds, headers={"User-Agent": "Mozilla/5.0"})
        return resp.url, resp.status_code, resp.text[:250_000]
    except Exception:
        return None, None, None


def _extract_linktree_child_links(html: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in HREF_RE.findall(html):
        href = unescape(raw.strip())
        if not href.startswith("http"):
            continue
        if "instagram.com" in href:
            continue
        if href not in seen:
            seen.add(href)
            out.append(href)
    return out


def expand_external_links(external_urls: list[str], timeout_seconds: int = 20) -> list[dict]:
    rows: list[dict] = []
    for raw_url in external_urls:
        dom = _domain(raw_url)
        is_linktree = dom in LINKTREE_HOSTS if dom else False

        final_url, status, body = _fetch(raw_url, timeout_seconds=timeout_seconds)
        row = {
            "source_surface": "profile_bio",
            "raw_url": raw_url,
            "expanded_url": final_url or raw_url,
            "final_url": final_url,
            "domain": _domain(final_url or raw_url),
            "http_status": status,
            "is_linktree": is_linktree,
            "missing_reason_link": None if final_url else "parse_error",
        }
        rows.append(row)

        if is_linktree and body:
            for child_url in _extract_linktree_child_links(body):
                child_final, child_status, _ = _fetch(child_url, timeout_seconds=timeout_seconds)
                rows.append(
                    {
                        "source_surface": "linktree_child",
                        "raw_url": child_url,
                        "expanded_url": child_final or child_url,
                        "final_url": child_final,
                        "domain": _domain(child_final or child_url),
                        "http_status": child_status,
                        "is_linktree": False,
                        "missing_reason_link": None if child_final else "parse_error",
                    }
                )
    return rows

