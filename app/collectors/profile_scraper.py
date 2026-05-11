from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse, urlunparse


PRIVATE_PATTERNS = [
    re.compile(r"this account is private", re.IGNORECASE),
    re.compile(r"this profile is private", re.IGNORECASE),
]

ERROR_PAGE_PATTERNS = [
    re.compile(r"something went wrong", re.IGNORECASE),
    re.compile(r"page could not be loaded", re.IGNORECASE),
    re.compile(r"reload page", re.IGNORECASE),
]

EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._%+-])([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})(?![A-Za-z0-9._%+-])",
    re.IGNORECASE,
)


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


def _is_instagram_error_page(page: object) -> bool:
    title = ""
    body = ""
    try:
        title = str(page.title() or "")
    except Exception:
        title = ""
    try:
        body = str(page.inner_text("body", timeout=1500) or "")
    except Exception:
        body = ""

    text = f"{title}\n{body}"
    return any(p.search(text) for p in ERROR_PAGE_PATTERNS)


def _extract_header_count(page: object, label_contains: str) -> int | None:
    label = (label_contains or "").strip().lower()
    selector_groups: dict[str, list[str]] = {
        "followers": [
            "header a[href$='/followers/']",
            "a[href$='/followers/']",
            "header li:has-text('followers')",
            "header section ul li",
            "header ul li",
        ],
        "following": [
            "header a[href$='/following/']",
            "a[href$='/following/']",
            "header li:has-text('following')",
            "header section ul li",
            "header ul li",
        ],
        "posts": [
            "header li:has-text('posts')",
            "header section ul li",
            "header ul li",
        ],
    }

    selectors = selector_groups.get(label, ["header section ul li", "header ul li"])

    for selector in selectors:
        try:
            nodes = page.locator(selector)
            count = min(nodes.count(), 30)
            for idx in range(count):
                txt = (nodes.nth(idx).inner_text(timeout=1200) or "").strip()
                if not txt:
                    continue
                if label not in txt.lower():
                    continue

                match = re.search(r"([\d.,]+\s*[kmb]?)", txt, re.IGNORECASE)
                value = parse_metric_count(match.group(1) if match else None)
                if value is not None:
                    return value
        except Exception:
            continue

    return None


def _extract_og_description(page: object) -> str | None:
    try:
        value = page.locator("meta[property='og:description']").first.get_attribute(
            "content", timeout=1500
        )
        return value or None
    except Exception:
        return None


def _extract_exact_profile_counts_from_json(
    page: object, username: str
) -> dict[str, int | None]:
    try:
        html = page.content()
    except Exception:
        html = ""

    if not html:
        return {
            "followers_count": None,
            "following_count": None,
            "total_posts_count": None,
        }

    scopes: list[str] = []
    if username:
        token = f'"username":"{username}"'
        idx = html.find(token)
        if idx >= 0:
            start = max(0, idx - 40000)
            end = min(len(html), idx + 120000)
            scopes.append(html[start:end])
    scopes.append(html)

    def _first_int(patterns: list[str]) -> int | None:
        for scope in scopes:
            for pattern in patterns:
                m = re.search(pattern, scope)
                if not m:
                    continue
                value = parse_metric_count(m.group(1))
                if value is not None:
                    return value
        return None

    return {
        "followers_count": _first_int(
            [
                r'"edge_followed_by"\s*:\s*\{[^{}]{0,300}?"count"\s*:\s*([0-9][0-9,\.]*)',
                r'"follower_count"\s*:\s*([0-9][0-9,\.]*)',
            ]
        ),
        "following_count": _first_int(
            [
                r'"edge_follow"\s*:\s*\{[^{}]{0,300}?"count"\s*:\s*([0-9][0-9,\.]*)',
                r'"following_count"\s*:\s*([0-9][0-9,\.]*)',
            ]
        ),
        "total_posts_count": _first_int(
            [
                r'"edge_owner_to_timeline_media"\s*:\s*\{[^{}]{0,300}?"count"\s*:\s*([0-9][0-9,\.]*)',
                r'"media_count"\s*:\s*([0-9][0-9,\.]*)',
            ]
        ),
    }


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


def _extract_emails(text: str | None) -> list[str]:
    if not text:
        return []
    found = [m.group(1).strip() for m in EMAIL_RE.finditer(text)]
    out: list[str] = []
    seen: set[str] = set()
    for email in found:
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(email)
    return out


def _extract_profile_emails_from_json_payload(page: object) -> list[str]:
    try:
        html = page.content()
    except Exception:
        html = ""

    if not html:
        return []

    out: list[str] = []
    patterns = (
        r'"business_email"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"',
        r'"public_email"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"',
        r'"contact_email"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"',
    )
    for pattern in patterns:
        for match in re.finditer(pattern, html):
            raw = match.group(1)
            try:
                decoded = bytes(raw, "utf-8").decode("unicode_escape")
            except Exception:
                decoded = raw
            out.extend(_extract_emails(decoded))
    return out


def _extract_profile_email(page: object, biography: str | None) -> str | None:
    collected: list[str] = []

    collected.extend(_extract_profile_emails_from_json_payload(page))

    collected.extend(_extract_emails(biography))

    try:
        header_text = page.locator("header").first.inner_text(timeout=1500)
    except Exception:
        header_text = None
    collected.extend(_extract_emails(header_text))

    try:
        body_text = page.inner_text("body", timeout=1500)
    except Exception:
        body_text = None
    collected.extend(_extract_emails(body_text))

    try:
        links = page.locator("header a[href^='mailto:']").all()
    except Exception:
        links = []

    for link in links:
        try:
            href = (link.get_attribute("href") or "").strip()
        except Exception:
            href = ""
        if not href:
            continue
        address = href.split("mailto:", 1)[-1].split("?", 1)[0].strip()
        collected.extend(_extract_emails(address))

    if not collected:
        return None

    unique: list[str] = []
    seen: set[str] = set()
    for email in collected:
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(email)

    return ",".join(unique) if unique else None


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

    # Some profiles expose links through a popup trigger ("... and N more").
    dialog_urls = _extract_external_urls_from_links_dialog(
        page, "https://www.instagram.com/"
    )
    return dialog_urls[0] if dialog_urls else None


def _unwrap_instagram_redirect(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    host = (parsed.hostname or "").lower()
    if host != "l.instagram.com":
        return url
    wrapped = (parse_qs(parsed.query).get("u") or [None])[0]
    if not wrapped:
        return url
    unwrapped = unquote(wrapped)
    return unwrapped if unwrapped.startswith(("http://", "https://")) else url


def _canonicalize_external_url(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return url

    scheme = (parsed.scheme or "https").lower()
    host = (parsed.hostname or "").lower()
    if not host:
        return url

    path = parsed.path or ""
    if path == "/":
        path = ""

    tracking_keys = {
        "fbclid",
        "gclid",
        "mc_cid",
        "mc_eid",
        "igshid",
        "ig_rid",
        "igsh",
    }
    filtered_pairs: list[tuple[str, str]] = []
    for key, values in parse_qs(parsed.query, keep_blank_values=False).items():
        k = (key or "").strip()
        if not k:
            continue
        if k.lower().startswith("utm_") or k.lower() in tracking_keys:
            continue
        for value in values:
            filtered_pairs.append((k, value))

    query = urlencode(filtered_pairs, doseq=True)
    return urlunparse((scheme, host, path, "", query, ""))


def _nudge_page_interaction(page: object) -> None:
    # Some anti-bot flows defer content until first input; send a harmless synthetic nudge.
    try:
        page.mouse.move(120, 140)
        page.mouse.click(120, 140, delay=35)
    except Exception:
        pass


def _is_nav_aborted_error(exc: Exception) -> bool:
    return "err_aborted" in str(exc).lower()


def _is_usable_instagram_page(page: object) -> bool:
    try:
        current = (page.url or "").lower()
    except Exception:
        return False
    return "instagram.com" in current and "about:blank" not in current


def _navigate_profile(page: object, profile_url: str) -> None:
    last_error: Exception | None = None

    for _ in range(4):
        try:
            page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(700)
            _nudge_page_interaction(page)
            if _is_usable_instagram_page(page):
                return
        except Exception as exc:
            last_error = exc

            if _is_nav_aborted_error(exc):
                try:
                    page.wait_for_timeout(900)
                    _nudge_page_interaction(page)
                    if _is_usable_instagram_page(page):
                        return
                except Exception:
                    pass

                try:
                    page.goto(profile_url, wait_until="commit", timeout=30_000)
                    page.wait_for_timeout(900)
                    _nudge_page_interaction(page)
                    if _is_usable_instagram_page(page):
                        return
                except Exception as commit_exc:
                    last_error = commit_exc

                try:
                    page.evaluate(
                        "(url) => { window.location.href = url; }", profile_url
                    )
                    page.wait_for_timeout(1200)
                    _nudge_page_interaction(page)
                    if _is_usable_instagram_page(page):
                        return
                except Exception as eval_exc:
                    last_error = eval_exc

        try:
            page.goto(
                "https://www.instagram.com/",
                wait_until="domcontentloaded",
                timeout=20_000,
            )
            page.wait_for_timeout(450)
            _nudge_page_interaction(page)
        except Exception:
            pass

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Could not navigate to profile URL: {profile_url}")


def _normalize_external_candidate(raw: str | None, base_url: str) -> str | None:
    if not raw:
        return None
    candidate = (raw or "").strip()
    if not candidate:
        return None

    if candidate.startswith("www."):
        candidate = f"https://{candidate}"

    if not candidate.startswith(("http://", "https://", "/")) and re.match(
        r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,}(/.*)?$", candidate
    ):
        candidate = f"https://{candidate}"

    candidate = _unwrap_instagram_redirect(candidate)
    absolute = urljoin(base_url, candidate)
    try:
        host = (urlparse(absolute).hostname or "").lower()
    except Exception:
        return None

    if not host:
        return None
    if host in {"www.instagram.com", "instagram.com", "m.instagram.com"}:
        return None
    return _canonicalize_external_url(absolute)


def _extract_external_urls_from_links_dialog(page: object, base_url: str) -> list[str]:
    try:
        clicked = bool(
            page.evaluate(
                r"""
                () => {
                  const nodes = Array.from(document.querySelectorAll("header button, header [role='button']"));
                  for (const node of nodes) {
                    const text = (node.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
                    if (!text) continue;
                    if ((text.includes(' and ') && text.includes('more')) || text.startsWith('www.') || text.includes('.com')) {
                      node.click();
                      return true;
                    }
                  }
                  return false;
                }
                """
            )
        )
    except Exception:
        clicked = False

    if not clicked:
        return []

    page.wait_for_timeout(600)

    candidates: list[str] = []
    try:
        payload = page.evaluate(
            r"""
            () => {
              const out = [];
              const dialogs = Array.from(document.querySelectorAll("div[role='dialog']"));
              const dialog = dialogs[dialogs.length - 1];
              if (!dialog) return out;

              for (const a of dialog.querySelectorAll('a[href]')) {
                const href = (a.getAttribute('href') || '').trim();
                if (href) out.push(href);
              }

              const text = (dialog.innerText || '').replace(/\u00a0/g, ' ');
              const lines = text.split(/\n+/).map(x => x.trim()).filter(Boolean);
              const urlLike = /((https?:\/\/)?(www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(\/[^\s]*)?)/g;
              for (const line of lines) {
                const matches = line.match(urlLike) || [];
                for (const m of matches) out.push(m);
              }

              return Array.from(new Set(out));
            }
            """
        )
        if isinstance(payload, list):
            candidates = [str(x) for x in payload if isinstance(x, str)]
    except Exception:
        candidates = []
    finally:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass

    urls: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        normalized = _normalize_external_candidate(raw, base_url)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        urls.append(normalized)

    return urls
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
        absolute = _normalize_external_candidate(href, base_url)
        if not absolute:
            continue
        if absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)

    for absolute in _extract_external_urls_from_links_dialog(page, base_url):
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
    page_error_detected = False
    for _ in range(2):
        _navigate_profile(page, profile_url)
        page.wait_for_timeout(800)
        if not _is_instagram_error_page(page):
            page_error_detected = False
            break

        page_error_detected = True
        try:
            page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
            page.wait_for_timeout(1200)
        except Exception:
            pass

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
    exact_counts = _extract_exact_profile_counts_from_json(page, username)

    followers_count = _extract_header_count(page, "followers")
    following_count = _extract_header_count(page, "following")
    total_posts_count = _extract_header_count(page, "posts")

    profile_data = {
        "username": username,
        "profile_url": profile_url,
        "full_name": _extract_full_name(page),
        "biography": _extract_biography(page),
        "email_address": None,
        "external_url_primary": _extract_primary_external_url(page),
        "followers_count": followers_count
        if followers_count is not None
        else exact_counts["followers_count"]
        if exact_counts["followers_count"] is not None
        else og_counts["followers_count"],
        "following_count": following_count
        if following_count is not None
        else exact_counts["following_count"]
        if exact_counts["following_count"] is not None
        else og_counts["following_count"],
        "highlight_reel_count": None,
        "total_posts_count": total_posts_count
        if total_posts_count is not None
        else exact_counts["total_posts_count"]
        if exact_counts["total_posts_count"] is not None
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

    profile_data["email_address"] = _extract_profile_email(
        page, profile_data.get("biography")
    )

    external_urls = _extract_all_external_urls(page, profile_url)
    if not profile_data["external_url_primary"] and external_urls:
        profile_data["external_url_primary"] = external_urls[0]

    if (
        profile_data["external_url_primary"]
        and profile_data["external_url_primary"] not in external_urls
    ):
        external_urls.insert(0, profile_data["external_url_primary"])

    if not profile_data["full_name"] and not profile_data["biography"]:
        profile_data["missing_reason_profile"] = "parse_error"
    if page_error_detected and not profile_data["full_name"]:
        profile_data["missing_reason_profile"] = "instagram_page_error"

    return ProfileScrapeResult(
        profile_data=profile_data, external_urls=external_urls, is_private=is_private
    )
