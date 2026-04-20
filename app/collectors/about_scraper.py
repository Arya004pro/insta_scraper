from __future__ import annotations

import re
from urllib.parse import parse_qs, quote_plus, unquote, urlparse


def _is_about_details_text(text: str) -> bool:
    return bool(
        re.search(
            r"date joined|account based in|active ads|verified",
            text or "",
            re.IGNORECASE,
        )
    )


def _extract_about_dialog_text(page: object) -> str:
    dialogs = page.locator("div[role='dialog']")
    try:
        count = dialogs.count()
    except Exception:
        count = 0

    best = ""
    for idx in range(max(0, count - 1), -1, -1):
        try:
            text = dialogs.nth(idx).inner_text(timeout=1200)
        except Exception:
            continue
        if _is_about_details_text(text):
            return text
        if len(text) > len(best):
            best = text
    return best


def _click_about_from_menu(page: object) -> bool:
    text_pattern = re.compile(r"about this account", re.IGNORECASE)
    locator_factories = [
        lambda: page.get_by_role("menuitem", name=text_pattern).first,
        lambda: (
            page.locator("div[role='dialog'] button")
            .filter(has_text=text_pattern)
            .first
        ),
        lambda: (
            page.locator("div[role='dialog'] a").filter(has_text=text_pattern).first
        ),
        lambda: page.get_by_text(text_pattern).first,
    ]

    for make_locator in locator_factories:
        try:
            locator = make_locator()
            if locator.count() <= 0:
                continue

            clicked = False
            for force_click in (False, True):
                try:
                    locator.click(timeout=1800, force=force_click)
                    clicked = True
                    break
                except Exception:
                    continue

            if not clicked:
                continue

            page.wait_for_timeout(700)
            if _is_about_details_text(_extract_about_dialog_text(page)):
                return True
        except Exception:
            continue

    try:
        clicked_by_js = bool(
            page.evaluate(
                """
                () => {
                  const dialogs = Array.from(document.querySelectorAll("div[role='dialog']"));
                  for (let d = dialogs.length - 1; d >= 0; d--) {
                    const buttons = Array.from(dialogs[d].querySelectorAll('button, a, [role="button"]'));
                    for (const node of buttons) {
                      const text = (node.textContent || '').trim().toLowerCase();
                      if (text === 'about this account' || text.includes('about this account')) {
                        node.click();
                        return true;
                      }
                    }
                  }
                  return false;
                }
                """
            )
        )
        if clicked_by_js:
            page.wait_for_timeout(700)
            if _is_about_details_text(_extract_about_dialog_text(page)):
                return True
    except Exception:
        pass

    return False


def _open_about_dialog(page: object) -> bool:
    try:
        direct = page.get_by_text(
            re.compile(r"about this account", re.IGNORECASE)
        ).first
        if direct.count() > 0:
            try:
                direct.click(timeout=1500)
            except Exception:
                direct.click(timeout=1500, force=True)
            page.wait_for_timeout(600)
            if _is_about_details_text(_extract_about_dialog_text(page)):
                return True
    except Exception:
        pass

    # Open the 3-dots profile menu first.
    button_selectors = [
        "button:has(svg[aria-label='Options'])",
        "button:has(svg[aria-label='More options'])",
        "button[aria-label='Options']",
        "button[aria-label='More options']",
        "svg[aria-label='Options']",
        "svg[aria-label='More options']",
    ]
    for selector in button_selectors:
        try:
            target = page.locator(selector).first
            if target.count() <= 0:
                continue

            opened = False
            for force_click in (False, True):
                try:
                    target.click(timeout=1500, force=force_click)
                    opened = True
                    break
                except Exception:
                    continue

            if not opened and selector.startswith("svg"):
                try:
                    opened = bool(
                        target.evaluate(
                            """
                            el => {
                              const buttonLike = el.closest('[role="button"]') || el.parentElement;
                              if (!buttonLike) return false;
                              buttonLike.click();
                              return true;
                            }
                            """
                        )
                    )
                except Exception:
                    opened = False

            if not opened:
                continue

            page.wait_for_timeout(500)
            if _click_about_from_menu(page):
                return True
            try:
                page.keyboard.press("Escape")
                page.wait_for_timeout(300)
            except Exception:
                pass
        except Exception:
            continue

    # JS fallback: click any visible options icon container in the header area.
    try:
        clicked_by_js = bool(
            page.evaluate(
                """
                () => {
                  const nodes = Array.from(
                    document.querySelectorAll(
                      "header svg[aria-label='Options'], header svg[aria-label='More options'], svg[aria-label='Options'], svg[aria-label='More options']"
                    )
                  );
                  for (const icon of nodes) {
                    const clickTarget = icon.closest('[role="button"]') || icon.parentElement;
                    if (!clickTarget) continue;
                    clickTarget.click();
                    return true;
                  }
                  return false;
                }
                """
            )
        )
        if clicked_by_js:
            page.wait_for_timeout(500)
            if _click_about_from_menu(page):
                return True
    except Exception:
        pass

    # Last attempt: if about text is visible now, click it once more.
    try:
        about = page.get_by_text(re.compile(r"about this account", re.IGNORECASE)).first
        try:
            about.click(timeout=1500)
        except Exception:
            about.click(timeout=1500, force=True)
        page.wait_for_timeout(600)
        if _is_about_details_text(_extract_about_dialog_text(page)):
            return True
    except Exception:
        pass
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
            if candidate.lower() in {
                "active ads",
                "verified",
                "date joined",
                "account based in",
            }:
                continue
            return candidate
    return None


def _extract_ads_library_url(page: object) -> str | None:
    selectors = [
        "a[href*='ads/library']",
        "a[href*='facebook.com/ads/library']",
        "a[href*='Ad Library']",
        "a[href*='ad library']",
        "a[href]",
    ]
    dialogs = page.locator("div[role='dialog']")
    try:
        count = dialogs.count()
    except Exception:
        count = 0

    for idx in range(max(0, count - 1), -1, -1):
        dialog = dialogs.nth(idx)
        for selector in selectors:
            try:
                nodes = dialog.locator(selector)
                node_count = nodes.count()
            except Exception:
                node_count = 0

            for n in range(node_count):
                try:
                    href = nodes.nth(n).get_attribute("href", timeout=1200)
                except Exception:
                    href = None
                if not href:
                    continue

                candidate = href.strip()
                if "ads/library" in candidate.lower():
                    return candidate

                try:
                    parsed = urlparse(candidate)
                    query = parse_qs(parsed.query)
                    wrapped = (query.get("u") or [None])[0]
                    if wrapped:
                        unwrapped = unquote(wrapped)
                        if "ads/library" in unwrapped.lower():
                            return unwrapped
                except Exception:
                    continue
    return None


def scrape_about_section(page: object) -> dict:
    default = {
        "date_joined": None,
        "account_based_in": None,
        "active_ads_status": None,
        "active_ads_url": None,
        "time_verified": None,
    }
    opened = _open_about_dialog(page)
    if not opened:
        return default

    modal_text = _extract_about_dialog_text(page)

    date_joined = _extract_by_label(
        modal_text, "Date joined"
    ) or _extract_following_line(modal_text, ["Date joined", "Joined"])
    account_based_in = _extract_by_label(
        modal_text, "Account based in"
    ) or _extract_following_line(modal_text, ["Account based in", "Based in"])
    time_verified = _extract_by_label(
        modal_text, "Verified"
    ) or _extract_following_line(
        modal_text, ["Time verified", "Date verified", "Verified"]
    )

    active_ads_url = _extract_ads_library_url(page)

    active_ads_status: str | None
    if re.search(r"active ads|running ads", modal_text, re.IGNORECASE):
        if re.search(r"no active ads|not running ads", modal_text, re.IGNORECASE):
            active_ads_status = "no"
        else:
            active_ads_status = "yes"
    else:
        active_ads_status = None

    if active_ads_status == "yes" and not active_ads_url:
        username = page.url.rstrip("/").split("/")[-1]
        if username:
            active_ads_url = (
                "https://www.facebook.com/ads/library/"
                f"?active_status=all&ad_type=all&country=ALL&q={quote_plus(username)}"
            )

    try:
        page.keyboard.press("Escape")
    except Exception:
        pass

    return {
        "date_joined": date_joined,
        "account_based_in": account_based_in,
        "active_ads_status": active_ads_status,
        "active_ads_url": active_ads_url,
        "time_verified": time_verified,
    }
