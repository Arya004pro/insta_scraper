from __future__ import annotations

import re


CHALLENGE_PATTERNS = [
    re.compile(r"challenge", re.IGNORECASE),
    re.compile(r"captcha", re.IGNORECASE),
    re.compile(r"suspicious activity", re.IGNORECASE),
    re.compile(r"log in to continue", re.IGNORECASE),
    re.compile(r"please log in", re.IGNORECASE),
    re.compile(r"confirm it'?s you", re.IGNORECASE),
    re.compile(r"http\s*error\s*429", re.IGNORECASE),
    re.compile(r"too many requests", re.IGNORECASE),
    re.compile(r"try again later", re.IGNORECASE),
    re.compile(r"this page isn'?t working", re.IGNORECASE),
    re.compile(r"temporarily blocked", re.IGNORECASE),
]

ACCOUNT_RESTRICTION_PATTERNS = [
    re.compile(r"your account has been temporarily blocked", re.IGNORECASE),
    re.compile(r"we restrict certain activity", re.IGNORECASE),
    re.compile(r"account (?:has been )?disabled", re.IGNORECASE),
    re.compile(r"account suspended", re.IGNORECASE),
    re.compile(r"violat(?:e|ed) our community guidelines", re.IGNORECASE),
    re.compile(r"appeal this decision", re.IGNORECASE),
]


def collect_page_diagnostics(page: object) -> dict[str, str | None]:
    try:
        body_text = page.inner_text("body", timeout=2500)
    except Exception:
        body_text = ""

    try:
        title = page.title()
    except Exception:
        title = None

    try:
        url = page.url
    except Exception:
        url = None

    combined = f"{title or ''}\n{body_text or ''}"
    http_error_match = re.search(r"http\s*error\s*(\d{3})", combined, re.IGNORECASE)
    error_code = http_error_match.group(1) if http_error_match else None

    snippet = re.sub(r"\s+", " ", body_text).strip()[:500] if body_text else None
    return {
        "url": url,
        "title": title,
        "body_snippet": snippet,
        "http_error_code": error_code,
    }


def detect_challenge(page: object) -> tuple[bool, str | None]:
    diagnostics = collect_page_diagnostics(page)
    text = diagnostics.get("body_snippet") or ""
    title = diagnostics.get("title") or ""
    combined_text = f"{title}\n{text}"

    if diagnostics.get("http_error_code") == "429":
        return True, "http_error_429"

    for pattern in CHALLENGE_PATTERNS:
        if pattern.search(combined_text):
            return True, pattern.pattern

    lowered = combined_text.lower()
    if "log in" in lowered and "sign up" in lowered:
        return True, "login_wall"

    try:
        has_login_form = (
            page.locator("input[name='username']").count() > 0
            and page.locator("input[name='password']").count() > 0
        )
        if has_login_form:
            return True, "login_form"
    except Exception:
        pass

    try:
        has_login_button = (
            page.get_by_role(
                "button", name=re.compile(r"^\s*log in\s*$", re.IGNORECASE)
            ).count()
            > 0
        )
        has_signup_button = (
            page.get_by_role(
                "button", name=re.compile(r"^\s*sign up\s*$", re.IGNORECASE)
            ).count()
            > 0
        )
        if has_login_button and has_signup_button:
            return True, "login_buttons"
    except Exception:
        pass
    return False, None


def detect_account_restriction(page: object) -> tuple[bool, str | None]:
    diagnostics = collect_page_diagnostics(page)
    text = diagnostics.get("body_snippet") or ""
    title = diagnostics.get("title") or ""
    url = (diagnostics.get("url") or "").lower()
    combined_text = f"{title}\n{text}"

    if "/accounts/suspended" in url or "/accounts/disabled" in url:
        return True, "account_restriction_url"

    for pattern in ACCOUNT_RESTRICTION_PATTERNS:
        if pattern.search(combined_text):
            return True, pattern.pattern

    return False, None
