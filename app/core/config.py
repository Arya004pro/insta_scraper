from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")


def now_ist() -> datetime:
    return datetime.now(tz=IST)


def iso_ist(dt: datetime | None = None) -> str:
    value = dt or now_ist()
    return value.isoformat()


@dataclass(frozen=True)
class ProxyConfig:
    proxy_id: str
    server: str
    username: str | None = None
    password: str | None = None


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    project_root: Path
    data_dir: Path
    runs_dir: Path
    browser_state_dir: Path
    exports_dir: Path
    media_dir: Path
    sqlite_path: Path
    brave_executable_path: str | None
    brave_user_data_dir: str | None
    brave_profile_directory: str
    brave_clone_profile_when_running: bool
    browser_headless: bool
    browser_viewport_width: int
    browser_viewport_height: int
    proxy_rotation_every_n_requests: int
    scroll_idle_rounds: int
    scroll_pause_min_ms: int
    scroll_pause_max_ms: int
    post_detail_wait_ms: int
    request_timeout_seconds: int
    retry_max_attempts: int
    retry_base_delay_seconds: float
    challenge_auto_retry_attempts: int
    challenge_auto_retry_wait_seconds: float
    rate_limit_cooldown_seconds: float
    sample_collection_mode: bool
    max_posts_per_profile: int | None
    proxies: list[ProxyConfig]

    @property
    def has_proxy_pool(self) -> bool:
        return bool(self.proxies)


def _parse_proxy_pool(raw: str) -> list[ProxyConfig]:
    if not raw.strip():
        return []
    payload = json.loads(raw)
    result: list[ProxyConfig] = []
    for item in payload:
        result.append(
            ProxyConfig(
                proxy_id=str(item["proxy_id"]),
                server=str(item["server"]),
                username=item.get("username"),
                password=item.get("password"),
            )
        )
    return result


def _auto_detect_brave_executable_path() -> str | None:
    configured = os.getenv("BRAVE_EXECUTABLE_PATH", "").strip()
    if configured:
        return configured

    candidates: list[Path] = []
    if os.name == "nt":
        program_files = Path(os.getenv("ProgramFiles", ""))
        program_files_x86 = Path(os.getenv("ProgramFiles(x86)", ""))
        candidates.extend(
            [
                program_files
                / "BraveSoftware"
                / "Brave-Browser"
                / "Application"
                / "brave.exe",
                program_files_x86
                / "BraveSoftware"
                / "Brave-Browser"
                / "Application"
                / "brave.exe",
            ]
        )
    elif sys.platform == "darwin":
        candidates.append(
            Path("/Applications/Brave Browser.app/Contents/MacOS/Brave Browser")
        )
    else:
        candidates.extend(
            [
                Path("/usr/bin/brave-browser"),
                Path("/usr/bin/brave"),
                Path("/snap/bin/brave"),
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def _auto_detect_brave_user_data_dir() -> str | None:
    configured = os.getenv("BRAVE_USER_DATA_DIR", "").strip()
    if configured:
        return configured

    home = Path.home()
    candidates: list[Path] = []
    if os.name == "nt":
        candidates.append(
            home / "AppData" / "Local" / "BraveSoftware" / "Brave-Browser" / "User Data"
        )
    elif sys.platform == "darwin":
        candidates.append(
            home / "Library" / "Application Support" / "BraveSoftware" / "Brave-Browser"
        )
    else:
        candidates.extend(
            [
                home / ".config" / "BraveSoftware" / "Brave-Browser",
                home
                / "snap"
                / "brave"
                / "current"
                / ".config"
                / "BraveSoftware"
                / "Brave-Browser",
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def load_settings() -> Settings:
    root = Path(__file__).resolve().parents[2]
    data_dir = root / "data"
    runs_dir = data_dir / "runs"
    browser_state_dir = data_dir / "browser_state"
    exports_dir = root / "exports"
    media_dir = Path(os.getenv("SCRAPED_MEDIA_DIR", str(root / "Output")))
    sqlite_path = data_dir / "state.sqlite3"

    for p in (data_dir, runs_dir, browser_state_dir, exports_dir, media_dir):
        p.mkdir(parents=True, exist_ok=True)

    proxy_pool = _parse_proxy_pool(os.getenv("PROXY_POOL_JSON", "[]"))
    headless_raw = os.getenv("BROWSER_HEADLESS", "0").strip().lower()
    browser_headless = headless_raw in {"1", "true", "yes", "on"}
    clone_raw = os.getenv("BRAVE_CLONE_PROFILE_WHEN_RUNNING", "1").strip().lower()
    brave_clone_profile_when_running = clone_raw in {"1", "true", "yes", "on"}
    sample_mode_raw = os.getenv("SAMPLE_COLLECTION_MODE", "1").strip().lower()
    sample_collection_mode = sample_mode_raw in {"1", "true", "yes", "on"}

    brave_executable_path = _auto_detect_brave_executable_path()
    brave_user_data_dir = _auto_detect_brave_user_data_dir()

    return Settings(
        app_name="insta-scraper",
        app_env=os.getenv("APP_ENV", "dev"),
        project_root=root,
        data_dir=data_dir,
        runs_dir=runs_dir,
        browser_state_dir=browser_state_dir,
        exports_dir=exports_dir,
        media_dir=media_dir,
        sqlite_path=sqlite_path,
        brave_executable_path=brave_executable_path,
        brave_user_data_dir=brave_user_data_dir,
        brave_profile_directory=os.getenv("BRAVE_PROFILE_DIRECTORY", "Default"),
        brave_clone_profile_when_running=brave_clone_profile_when_running,
        browser_headless=browser_headless,
        browser_viewport_width=int(os.getenv("BROWSER_VIEWPORT_WIDTH", "1100")),
        browser_viewport_height=int(os.getenv("BROWSER_VIEWPORT_HEIGHT", "750")),
        proxy_rotation_every_n_requests=int(os.getenv("PROXY_ROTATE_EVERY_N", "20")),
        scroll_idle_rounds=int(os.getenv("SCROLL_IDLE_ROUNDS", "4")),
        scroll_pause_min_ms=int(os.getenv("SCROLL_PAUSE_MIN_MS", "450")),
        scroll_pause_max_ms=int(os.getenv("SCROLL_PAUSE_MAX_MS", "900")),
        post_detail_wait_ms=int(os.getenv("POST_DETAIL_WAIT_MS", "300")),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
        retry_max_attempts=int(os.getenv("RETRY_MAX_ATTEMPTS", "3")),
        retry_base_delay_seconds=float(os.getenv("RETRY_BASE_DELAY_SECONDS", "2.0")),
        challenge_auto_retry_attempts=int(
            os.getenv("CHALLENGE_AUTO_RETRY_ATTEMPTS", "3")
        ),
        challenge_auto_retry_wait_seconds=float(
            os.getenv("CHALLENGE_AUTO_RETRY_WAIT_SECONDS", "8.0")
        ),
        rate_limit_cooldown_seconds=float(
            os.getenv("RATE_LIMIT_COOLDOWN_SECONDS", "30")
        ),
        sample_collection_mode=sample_collection_mode,
        max_posts_per_profile=(
            None
            if int(os.getenv("MAX_POSTS_PER_PROFILE", "0")) <= 0
            else int(os.getenv("MAX_POSTS_PER_PROFILE", "0"))
        ),
        proxies=proxy_pool,
    )
