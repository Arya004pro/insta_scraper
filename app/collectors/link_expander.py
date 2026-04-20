from __future__ import annotations

import re
from html import unescape
from urllib.parse import parse_qs, unquote, urlparse

import requests


LINKTREE_HOSTS = {"linktr.ee", "bio.site", "beacons.ai", "taplink.cc"}
HREF_RE = re.compile(r'href=[\'"]([^\'"]+)[\'"]', re.IGNORECASE)
HUB_HINT_RE = re.compile(
    r"link\s*in\s*bio|all\s+links|social\s+links|my\s+links|tap\s+to\s+open",
    re.IGNORECASE,
)


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return (urlparse(url).hostname or "").lower() or None
    except Exception:
        return None


def _fetch(url: str, timeout_seconds: int) -> tuple[str | None, int | None, str | None]:
    try:
        resp = requests.get(
            url,
            allow_redirects=True,
            timeout=timeout_seconds,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        return resp.url, resp.status_code, resp.text[:250_000]
    except Exception:
        return None, None, None


def _unwrap_instagram_redirect(url: str | None) -> str | None:
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    host = (parsed.hostname or "").lower()
    if host != "l.instagram.com":
        return None
    query = parse_qs(parsed.query)
    wrapped = (query.get("u") or [None])[0]
    if not wrapped:
        return None
    unwrapped = unquote(wrapped)
    return unwrapped if unwrapped.startswith(("http://", "https://")) else None


def _registrable_domain(hostname: str | None) -> str | None:
    if not hostname:
        return None
    labels = [x for x in hostname.lower().split(".") if x]
    if len(labels) < 2:
        return hostname.lower()
    return f"{labels[-2]}.{labels[-1]}"


def _looks_like_link_hub(
    body: str | None, page_domain: str | None, is_known_hub_host: bool
) -> bool:
    if not body:
        return False

    if not is_known_hub_host and not HUB_HINT_RE.search(body):
        return False

    child_links = _extract_linktree_child_links(body)
    if len(child_links) < 4:
        return False

    page_reg = _registrable_domain(page_domain)
    child_regs: set[str] = set()
    for link in child_links:
        dom = _domain(link)
        if not dom or "instagram.com" in dom:
            continue
        reg = _registrable_domain(dom)
        if page_reg and reg == page_reg:
            continue
        if reg:
            child_regs.add(reg)

    if len(child_regs) < 3:
        return False

    # Generic product sites usually have large nav footers. Limit auto-hub detection.
    return len(child_links) <= 40


def _is_resource_like_url(url: str) -> bool:
    lower = url.lower()
    if any(
        x in lower
        for x in (
            "fonts.googleapis.com",
            "fonts.gstatic.com",
            "doubleclick.net",
            "googletagmanager.com",
            "google-analytics.com",
        )
    ):
        return True
    return bool(re.search(r"\.(css|js|png|jpg|jpeg|svg|gif|webp|ico)(\?|$)", lower))


def _filter_child_links(
    child_links: list[str],
    page_domain: str | None,
    is_known_hub_host: bool,
) -> list[str]:
    page_reg = _registrable_domain(page_domain)
    out: list[str] = []
    seen: set[str] = set()

    for url in child_links:
        dom = _domain(url)
        if not dom or "instagram.com" in dom:
            continue
        if _is_resource_like_url(url):
            continue
        reg = _registrable_domain(dom)
        if not is_known_hub_host and page_reg and reg == page_reg:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)

    return out[:24]


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


def expand_external_links(
    external_urls: list[str], timeout_seconds: int = 20
) -> list[dict]:
    rows: list[dict] = []
    for raw_url in external_urls:
        normalized_raw = _unwrap_instagram_redirect(raw_url) or raw_url
        dom = _domain(normalized_raw)
        is_known_hub_host = dom in LINKTREE_HOSTS if dom else False
        is_link_hub = is_known_hub_host

        final_url, status, body = _fetch(
            normalized_raw, timeout_seconds=timeout_seconds
        )
        final_or_raw = final_url or normalized_raw
        final_domain = _domain(final_or_raw)
        if not is_link_hub and _looks_like_link_hub(
            body, final_domain, is_known_hub_host=is_known_hub_host
        ):
            is_link_hub = True

        row = {
            "source_surface": "profile_bio",
            "raw_url": normalized_raw,
            "expanded_url": final_or_raw,
            "final_url": final_url,
            "domain": final_domain,
            "http_status": status,
            "is_linktree": is_link_hub,
            "missing_reason_link": None if final_url else "parse_error",
        }
        rows.append(row)

        if is_link_hub and body:
            child_urls = _filter_child_links(
                _extract_linktree_child_links(body),
                final_domain,
                is_known_hub_host=is_known_hub_host,
            )
            for child_url in child_urls:
                child_final, child_status, _ = _fetch(
                    child_url, timeout_seconds=timeout_seconds
                )
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
