from __future__ import annotations

import json
import os
import re
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
    browser_engine: str
    project_root: Path
    data_dir: Path
    runs_dir: Path
    browser_state_dir: Path
    exports_dir: Path
    media_dir: Path
    sqlite_path: Path
    opera_gx_executable_path: str | None
    opera_gx_user_data_dir: str | None
    opera_gx_cdp_url: str | None
    opera_gx_attach_existing: bool
    opera_gx_clone_profile_when_running: bool
    opera_gx_use_fresh_profile: bool
    download_media_assets: bool
    vpn_rotate_every_n: int
    vpn_rotate_command: str | None
    vpn_rotate_wait_seconds: float
    reels_tab_max_items: int
    reels_tab_max_scroll_rounds: int
    skip_media_shortcodes: frozenset[str]
    skip_media_urls: frozenset[str]
    browser_headless: bool
    browser_start_maximized: bool
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


def _parse_csv_set(raw: str) -> frozenset[str]:
    if not raw or not raw.strip():
        return frozenset()
    parts = re.split(r"[\n,]+", raw)
    values = [p.strip() for p in parts if p and p.strip()]
    return frozenset(values)


def _auto_detect_opera_gx_executable_path() -> str | None:
    configured = os.getenv("OPERA_GX_EXECUTABLE_PATH", "").strip()
    if configured:
        return configured

    candidates: list[Path] = []
    if os.name == "nt":
        local_app_data = Path(os.getenv("LOCALAPPDATA", ""))
        program_files = Path(os.getenv("ProgramFiles", ""))
        program_files_x86 = Path(os.getenv("ProgramFiles(x86)", ""))
        candidates.extend(
            [
                local_app_data / "Programs" / "Opera GX" / "opera.exe",
                program_files / "Opera GX" / "opera.exe",
                program_files_x86 / "Opera GX" / "opera.exe",
            ]
        )
    else:
        candidates.extend([Path("/usr/bin/opera"), Path("/usr/local/bin/opera")])

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def _auto_detect_opera_gx_user_data_dir(fallback_dir: Path) -> str:
    configured = os.getenv("OPERA_GX_USER_DATA_DIR", "").strip()
    if configured:
        return configured

    fallback_dir.mkdir(parents=True, exist_ok=True)
    return str(fallback_dir)


def load_settings() -> Settings:
    root = Path(__file__).resolve().parents[2]
    output_root = Path(os.getenv("OUTPUT_ROOT_DIR", r"D:\Insta-scraper-opera-gx"))
    data_dir = root / "data"
    runs_dir = data_dir / "runs"
    browser_state_dir = data_dir / "browser_state"
    exports_dir = Path(os.getenv("EXPORTS_DIR", str(output_root / "exports")))
    media_dir = Path(os.getenv("SCRAPED_MEDIA_DIR", str(output_root / "media")))
    sqlite_path = data_dir / "state.sqlite3"

    for p in (data_dir, runs_dir, browser_state_dir, exports_dir, media_dir):
        p.mkdir(parents=True, exist_ok=True)

    proxy_pool = _parse_proxy_pool(os.getenv("PROXY_POOL_JSON", "[]"))
    headless_raw = os.getenv("BROWSER_HEADLESS", "0").strip().lower()
    browser_headless = headless_raw in {"1", "true", "yes", "on"}
    browser_start_maximized_raw = (
        os.getenv("BROWSER_START_MAXIMIZED", "1").strip().lower()
    )
    browser_start_maximized = browser_start_maximized_raw in {
        "1",
        "true",
        "yes",
        "on",
    }
    browser_engine = "opera_gx"

    opera_gx_clone_raw = (
        os.getenv("OPERA_GX_CLONE_PROFILE_WHEN_RUNNING", "0").strip().lower()
    )
    opera_gx_clone_profile_when_running = opera_gx_clone_raw in {
        "1",
        "true",
        "yes",
        "on",
    }
    opera_gx_fresh_raw = os.getenv("OPERA_GX_USE_FRESH_PROFILE", "0").strip().lower()
    opera_gx_use_fresh_profile = opera_gx_fresh_raw in {"1", "true", "yes", "on"}
    download_media_raw = os.getenv("DOWNLOAD_MEDIA_ASSETS", "0").strip().lower()
    download_media_assets = download_media_raw in {"1", "true", "yes", "on"}
    vpn_rotate_command = os.getenv("VPN_ROTATE_CMD", "").strip() or None
    if not vpn_rotate_command:
        default_vpn_script = root / "scripts" / "vpn" / "vpn_rotate.bat"
        if default_vpn_script.exists():
            vpn_rotate_command = f'"{default_vpn_script}"'
    vpn_rotate_every_n_raw = int(os.getenv("VPN_ROTATE_EVERY_N", "0"))
    vpn_rotate_every_n = max(0, vpn_rotate_every_n_raw)
    if vpn_rotate_every_n <= 0 and vpn_rotate_command:
        vpn_rotate_every_n = 50
    vpn_rotate_wait_seconds = float(os.getenv("VPN_ROTATE_WAIT_SECONDS", "6"))
    reels_tab_max_items = max(0, int(os.getenv("REELS_TAB_MAX_ITEMS", "0")))
    reels_tab_max_scroll_rounds = max(
        0, int(os.getenv("REELS_TAB_MAX_SCROLL_ROUNDS", "0"))
    )
    skip_media_shortcodes = _parse_csv_set(os.getenv("SKIP_MEDIA_SHORTCODES", ""))
    skip_media_urls = _parse_csv_set(os.getenv("SKIP_MEDIA_URLS", ""))
    sample_mode_raw = os.getenv("SAMPLE_COLLECTION_MODE", "0").strip().lower()
    sample_collection_mode = sample_mode_raw in {"1", "true", "yes", "on"}
    max_posts_raw = int(os.getenv("MAX_POSTS_PER_PROFILE", "0"))

    opera_gx_executable_path = _auto_detect_opera_gx_executable_path()
    opera_gx_user_data_dir = _auto_detect_opera_gx_user_data_dir(
        browser_state_dir / "opera_gx_user_data"
    )
    opera_gx_cdp_url = os.getenv("OPERA_GX_CDP_URL", "http://127.0.0.1:9222").strip()
    if not opera_gx_cdp_url:
        opera_gx_cdp_url = None
    opera_gx_attach_existing_raw = (
        os.getenv("OPERA_GX_ATTACH_EXISTING", "1").strip().lower()
    )
    opera_gx_attach_existing = opera_gx_attach_existing_raw in {
        "1",
        "true",
        "yes",
        "on",
    }

    return Settings(
        app_name="insta-scraper",
        app_env=os.getenv("APP_ENV", "dev"),
        browser_engine=browser_engine,
        project_root=root,
        data_dir=data_dir,
        runs_dir=runs_dir,
        browser_state_dir=browser_state_dir,
        exports_dir=exports_dir,
        media_dir=media_dir,
        sqlite_path=sqlite_path,
        opera_gx_executable_path=opera_gx_executable_path,
        opera_gx_user_data_dir=opera_gx_user_data_dir,
        opera_gx_cdp_url=opera_gx_cdp_url,
        opera_gx_attach_existing=opera_gx_attach_existing,
        opera_gx_clone_profile_when_running=opera_gx_clone_profile_when_running,
        opera_gx_use_fresh_profile=opera_gx_use_fresh_profile,
        download_media_assets=download_media_assets,
        vpn_rotate_every_n=vpn_rotate_every_n,
        vpn_rotate_command=vpn_rotate_command,
        vpn_rotate_wait_seconds=vpn_rotate_wait_seconds,
        reels_tab_max_items=reels_tab_max_items,
        reels_tab_max_scroll_rounds=reels_tab_max_scroll_rounds,
        skip_media_shortcodes=skip_media_shortcodes,
        skip_media_urls=skip_media_urls,
        browser_headless=browser_headless,
        browser_start_maximized=browser_start_maximized,
        browser_viewport_width=int(os.getenv("BROWSER_VIEWPORT_WIDTH", "1100")),
        browser_viewport_height=int(os.getenv("BROWSER_VIEWPORT_HEIGHT", "750")),
        proxy_rotation_every_n_requests=int(os.getenv("PROXY_ROTATE_EVERY_N", "20")),
        scroll_idle_rounds=int(os.getenv("SCROLL_IDLE_ROUNDS", "8")),
        scroll_pause_min_ms=int(os.getenv("SCROLL_PAUSE_MIN_MS", "450")),
        scroll_pause_max_ms=int(os.getenv("SCROLL_PAUSE_MAX_MS", "900")),
        post_detail_wait_ms=int(os.getenv("POST_DETAIL_WAIT_MS", "300")),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
        retry_max_attempts=int(os.getenv("RETRY_MAX_ATTEMPTS", "3")),
        retry_base_delay_seconds=float(os.getenv("RETRY_BASE_DELAY_SECONDS", "2.0")),
        challenge_auto_retry_attempts=int(
            os.getenv("CHALLENGE_AUTO_RETRY_ATTEMPTS", "0")
        ),
        challenge_auto_retry_wait_seconds=float(
            os.getenv("CHALLENGE_AUTO_RETRY_WAIT_SECONDS", "8.0")
        ),
        rate_limit_cooldown_seconds=float(
            os.getenv("RATE_LIMIT_COOLDOWN_SECONDS", "30")
        ),
        sample_collection_mode=sample_collection_mode,
        max_posts_per_profile=(None if max_posts_raw <= 0 else max_posts_raw),
        proxies=proxy_pool,
    )
