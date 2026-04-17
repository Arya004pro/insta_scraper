from __future__ import annotations

def scrape_highlights(page: object, username: str) -> list[dict]:
    rows: list[dict] = []
    locators = [
        "section ul li button",
        "section ul li a",
    ]
    seen: set[str] = set()
    idx = 0
    for selector in locators:
        try:
            elems = page.locator(selector).all()
        except Exception:
            elems = []
        for elem in elems:
            name = None
            href = None
            try:
                name = elem.inner_text(timeout=1000).strip()
            except Exception:
                name = None
            try:
                href = elem.get_attribute("href")
            except Exception:
                href = None

            key = (name or "") + "|" + (href or "")
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "username": username,
                    "highlight_index": idx,
                    "highlight_name": name or None,
                    "highlight_url": f"https://www.instagram.com{href}" if href and href.startswith("/") else href,
                    "missing_reason_highlight": None if name or href else "not_visible",
                }
            )
            idx += 1
    return rows
