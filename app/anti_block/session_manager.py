from __future__ import annotations

from pathlib import Path


class SessionManager:
    def __init__(self, browser_state_dir: Path):
        self.browser_state_dir = browser_state_dir
        self.browser_state_dir.mkdir(parents=True, exist_ok=True)

    def storage_state_path(self, username: str = "default") -> Path:
        safe = "".join(ch for ch in username if ch.isalnum() or ch in ("-", "_", ".")).strip(".")
        if not safe:
            safe = "default"
        return self.browser_state_dir / f"{safe}_storage_state.json"

    def has_saved_state(self, username: str = "default") -> bool:
        return self.storage_state_path(username).exists()

