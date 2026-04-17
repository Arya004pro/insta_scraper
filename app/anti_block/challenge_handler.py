from __future__ import annotations

import re


CHALLENGE_PATTERNS = [
    re.compile(r"challenge", re.IGNORECASE),
    re.compile(r"captcha", re.IGNORECASE),
    re.compile(r"suspicious activity", re.IGNORECASE),
    re.compile(r"log in to continue", re.IGNORECASE),
    re.compile(r"please log in", re.IGNORECASE),
    re.compile(r"confirm it'?s you", re.IGNORECASE),
]


def detect_challenge(page: object) -> tuple[bool, str | None]:
    try:
        text = page.inner_text("body", timeout=5000)
    except Exception:
        return False, None

    for pattern in CHALLENGE_PATTERNS:
        if pattern.search(text):
            return True, pattern.pattern

    lowered = text.lower()
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
