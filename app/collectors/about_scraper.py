from __future__ import annotations

import re


def _open_about_dialog(page: object) -> bool:
    try:
        direct = page.get_by_text(
            re.compile(r"about this account", re.IGNORECASE)
        ).first
        if direct.count() > 0:
            direct.click(timeout=1500)
            return True
    except Exception:
        pass

    button_selectors = [
        "button:has(svg[aria-label='Options'])",
        "button:has(svg[aria-label='More options'])",
        "button[aria-label='Options']",
        "button[aria-label='More options']",
        "svg[aria-label='Options']",
    ]
    clicked = False
    for selector in button_selectors:
        try:
            page.locator(selector).first.click(timeout=1500)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        return False
    try:
        page.get_by_role(
            "menuitem", name=re.compile(r"about this account", re.IGNORECASE)
        ).first.click(timeout=2000)
        return True
    except Exception:
        pass

    try:
        page.get_by_text(re.compile(r"about this account", re.IGNORECASE)).first.click(
            timeout=2000
        )
        return True
    except Exception:
        return False


def _extract_by_label(text: str, label: str) -> str | None:
    pattern = re.compile(rf"{re.escape(label)}\s*[:\n]\s*(.+)", re.IGNORECASE)
    match = pattern.search(text)
    if match:
        return match.group(1).strip()
    return None


def _extract_following_line(text: str, labels: list[str]) -> str | None:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    label_regex = re.compile("|".join(re.escape(x) for x in labels), re.IGNORECASE)
    for idx, line in enumerate(lines):
        if not label_regex.search(line):
            continue

        same_line = re.search(r":\s*(.+)$", line)
        if same_line and same_line.group(1).strip():
            return same_line.group(1).strip()

        for j in range(idx + 1, min(idx + 4, len(lines))):
            candidate = lines[j]
            if label_regex.search(candidate):
                continue
            if candidate.lower() in {"active ads", "verified", "date joined"}:
                continue
            return candidate
    return None


def scrape_about_section(page: object) -> dict:
    default = {
        "date_joined": None,
        "active_ads_status": None,
        "time_verified": None,
    }
    opened = _open_about_dialog(page)
    if not opened:
        return default

    try:
        modal_text = page.locator("div[role='dialog']").first.inner_text(timeout=2500)
    except Exception:
        modal_text = ""

    date_joined = _extract_by_label(
        modal_text, "Date joined"
    ) or _extract_following_line(modal_text, ["Date joined", "Joined"])
    time_verified = _extract_by_label(
        modal_text, "Verified"
    ) or _extract_following_line(
        modal_text, ["Time verified", "Date verified", "Verified"]
    )

    active_ads_status: str | None
    if re.search(r"active ads|running ads", modal_text, re.IGNORECASE):
        if re.search(r"no active ads|not running ads", modal_text, re.IGNORECASE):
            active_ads_status = "no"
        else:
            active_ads_status = "yes"
    else:
        active_ads_status = None

    try:
        page.keyboard.press("Escape")
    except Exception:
        pass

    return {
        "date_joined": date_joined,
        "active_ads_status": active_ads_status,
        "time_verified": time_verified,
    }
