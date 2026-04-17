from __future__ import annotations

import csv
import mimetypes
import os
import random
import re
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from app.anti_block.challenge_handler import collect_page_diagnostics, detect_challenge
from app.anti_block.proxy_manager import ProxyManager
from app.anti_block.session_manager import SessionManager
from app.collectors.about_scraper import scrape_about_section
from app.collectors.grid_enumerator import enumerate_grid_posts
from app.collectors.highlight_scraper import scrape_highlights
from app.collectors.link_expander import expand_external_links
from app.collectors.post_detail_scraper import scrape_post_detail
from app.collectors.profile_scraper import scrape_profile_header
from app.collectors.timeline_snapshot import collect_recent_timeline_items
from app.core.config import Settings, iso_ist, now_ist
from app.core.models import (
    AGGREGATES_COLUMNS,
    EXTERNAL_LINKS_COLUMNS,
    HIGHLIGHTS_COLUMNS,
    POSTS_COLUMNS,
    PROFILE_COLUMNS,
    RUN_LOG_COLUMNS,
    RunContext,
    StartRunRequest,
)
from app.core.url_validator import InvalidInstagramUrl, normalize_instagram_profile_url
from app.exporters.csv_exporter import export_csv_artifacts
from app.metrics.aggregator import build_aggregates, build_summary_flat
from app.storage.sqlite_store import SQLiteStore


class ChallengeRequired(Exception):
    def __init__(self, message: str, state: dict[str, Any] | None = None):
        super().__init__(message)
        self.state = state or {}


@dataclass
class ProfileRunResult:
    profile_row: dict[str, Any]
    highlights_rows: list[dict[str, Any]]
    links_rows: list[dict[str, Any]]
    posts_rows: list[dict[str, Any]]
    aggregates_rows: list[dict[str, Any]]
    status: str


class RunOrchestrator:
    def __init__(self, settings: Settings, store: SQLiteStore):
        self.settings = settings
        self.store = store
        self._session_manager = SessionManager(settings.browser_state_dir)
        self._threads: dict[str, threading.Thread] = {}
        self._thread_lock = threading.Lock()

    def submit_run(self, req: StartRunRequest) -> str:
        run_id = uuid.uuid4().hex
        if req.input_type == "single_url":
            normalized = normalize_instagram_profile_url(req.input_value).normalized_url
        else:
            normalized = "batch_input"
        context = RunContext(
            run_id=run_id,
            input_url=req.input_value,
            normalized_profile_url=normalized,
            status="queued",
            session_mode="anonymous_optional_saved_state"
            if req.use_saved_session
            else "anonymous_only",
        )
        self.store.create_run(context)
        self._spawn_thread(run_id, req, is_resume=False)
        return run_id

    def resume_run(self, run_id: str) -> RunContext:
        run = self.store.get_run(run_id)
        if run is None:
            raise KeyError(f"Run not found: {run_id}")
        self.store.update_run(run_id, status="resuming")
        req = StartRunRequest(
            input_type="single_url",
            input_value=run.input_url,
            use_saved_session=True,
        )
        self._spawn_thread(run_id, req, is_resume=True)
        updated = self.store.get_run(run_id)
        assert updated is not None
        return updated

    def _spawn_thread(self, run_id: str, req: StartRunRequest, is_resume: bool) -> None:
        thread = threading.Thread(
            target=self._execute_run,
            args=(run_id, req, is_resume),
            daemon=True,
            name=f"run-{run_id[:8]}",
        )
        with self._thread_lock:
            self._threads[run_id] = thread
        thread.start()

    def _execute_run(self, run_id: str, req: StartRunRequest, is_resume: bool) -> None:
        started = now_ist()
        self.store.update_run(run_id, status="running", started_at_ist=iso_ist(started))
        self.store.set_progress(run_id, "Run started", 1.0)

        try:
            targets = self._resolve_targets(req.input_type, req.input_value)
            if not targets:
                raise ValueError("No valid target URLs found")

            all_profile_rows: list[dict[str, Any]] = []
            all_highlights_rows: list[dict[str, Any]] = []
            all_external_link_rows: list[dict[str, Any]] = []
            all_posts_rows: list[dict[str, Any]] = []
            all_aggregate_rows: list[dict[str, Any]] = []

            current_state = (
                self.store.get_run(run_id).state if self.store.get_run(run_id) else {}
            )
            start_index = int(current_state.get("target_index", 0)) if is_resume else 0

            for idx, target in enumerate(targets[start_index:], start=start_index):
                self.store.update_run(run_id, state={"target_index": idx})
                self.store.set_progress(
                    run_id,
                    f"Scraping profile {idx + 1}/{len(targets)}",
                    5 + (idx / len(targets)) * 85,
                )
                profile_result = self._scrape_single_profile(
                    run_id=run_id,
                    profile_url=target,
                    use_saved_session=req.use_saved_session,
                    resume_state=current_state.get("profile_state")
                    if is_resume
                    else None,
                )
                all_profile_rows.append(profile_result.profile_row)
                all_highlights_rows.extend(profile_result.highlights_rows)
                all_external_link_rows.extend(profile_result.links_rows)
                all_posts_rows.extend(profile_result.posts_rows)
                all_aggregate_rows.extend(profile_result.aggregates_rows)

                if profile_result.status == "skipped_private":
                    self.store.update_run(run_id, status="skipped_private")

            self.store.set_progress(run_id, "Exporting artifacts", 93)
            artifacts = self._export(
                run_id=run_id,
                input_url=req.input_value,
                profile_rows=all_profile_rows,
                highlights_rows=all_highlights_rows,
                external_links_rows=all_external_link_rows,
                posts_rows=all_posts_rows,
                aggregate_rows=all_aggregate_rows,
            )

            ended = now_ist()
            started_at = (
                self.store.get_run(run_id).started_at_ist
                if self.store.get_run(run_id)
                else iso_ist(started)
            )
            duration = (
                (ended - datetime.fromisoformat(started_at)).total_seconds()
                if started_at
                else None
            )
            status_value = (
                self.store.get_run(run_id).status
                if self.store.get_run(run_id)
                else "running"
            )
            final_status = (
                status_value if status_value == "skipped_private" else "completed"
            )
            self.store.update_run(
                run_id,
                status=final_status,
                ended_at_ist=iso_ist(ended),
                duration_sec=duration,
                artifacts=artifacts,
                state={},
            )
            self.store.set_progress(run_id, "Run completed", 100.0)

        except ChallengeRequired as challenge:
            run = self.store.get_run(run_id)
            state = run.state if run else {}
            state.update({"profile_state": challenge.state})
            self.store.update_run(
                run_id,
                status="needs_human",
                challenge_encountered=True,
                error_code="challenge_required",
                error_message=str(challenge),
                state=state,
            )
            self.store.add_event(
                run_id, "Run paused for human challenge resolution", level="warning"
            )
        except Exception as exc:
            self.store.fail_run(run_id, "run_error", str(exc))

    def _resolve_targets(self, input_type: str, input_value: str) -> list[str]:
        if input_type == "single_url":
            return [normalize_instagram_profile_url(input_value).normalized_url]

        path = Path(input_value)
        if not path.exists():
            raise FileNotFoundError(f"CSV/XLSX file not found: {path}")
        urls: list[str] = []
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                fields = reader.fieldnames or []
                first_col = fields[0] if fields else None
                for row in reader:
                    candidate = (
                        row.get("url")
                        or row.get("instagram_url")
                        or (row.get(first_col) if first_col else None)
                    )
                    if not candidate:
                        continue
                    try:
                        urls.append(
                            normalize_instagram_profile_url(candidate).normalized_url
                        )
                    except InvalidInstagramUrl:
                        continue
        else:
            raise ValueError("Only CSV batch input is supported in v1")
        return urls

    def _is_brave_running(self) -> bool:
        try:
            if os.name == "nt":
                result = subprocess.run(
                    ["tasklist", "/FI", "IMAGENAME eq brave.exe"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                return "brave.exe" in result.stdout.lower()

            result = subprocess.run(
                ["pgrep", "-f", "brave"],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _terminate_brave(self) -> None:
        try:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/IM", "brave.exe", "/F"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            else:
                subprocess.run(
                    ["pkill", "-f", "brave"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
        except Exception:
            pass

    def _clone_brave_profile_snapshot(
        self, run_id: str, source_user_data_dir: Path
    ) -> Path:
        clone_root = (
            self.settings.browser_state_dir
            / "brave_profile_clones"
            / f"{run_id[:8]}_{int(time.time())}"
        )
        clone_root.mkdir(parents=True, exist_ok=True)

        profile_name = self.settings.brave_profile_directory
        source_profile_dir = source_user_data_dir / profile_name
        if not source_profile_dir.exists():
            raise RuntimeError(
                f"Brave profile directory '{profile_name}' not found under {source_user_data_dir}"
            )

        local_state = source_user_data_dir / "Local State"
        if local_state.exists():
            shutil.copy2(local_state, clone_root / "Local State")

        first_run = source_user_data_dir / "First Run"
        if first_run.exists():
            shutil.copy2(first_run, clone_root / "First Run")

        ignore_names = {
            "Cache",
            "Code Cache",
            "GPUCache",
            "ShaderCache",
            "Service Worker",
            "GrShaderCache",
            "DawnCache",
            "Blob Storage",
            "SingletonLock",
            "SingletonCookie",
            "SingletonSocket",
        }

        def _ignore(_: str, names: list[str]) -> set[str]:
            return {name for name in names if name in ignore_names}

        def _safe_copy(src: str, dst: str) -> str:
            try:
                return shutil.copy2(src, dst)
            except OSError:
                # Locked files (cookies/session journals) can be skipped in live clones.
                return dst

        shutil.copytree(
            source_profile_dir,
            clone_root / profile_name,
            dirs_exist_ok=True,
            ignore=_ignore,
            copy_function=_safe_copy,
        )
        return clone_root

    def _launch_context(
        self,
        run_id: str,
        proxy_manager: ProxyManager,
        use_saved_session: bool,
        username_hint: str,
    ) -> tuple[Any, Any, Path | None]:
        from playwright.sync_api import sync_playwright

        _ = use_saved_session
        _ = username_hint

        if not self.settings.brave_executable_path:
            raise RuntimeError(
                "Brave-only mode requires BRAVE_EXECUTABLE_PATH to be configured"
            )
        if not self.settings.brave_user_data_dir:
            raise RuntimeError(
                "Brave-only mode requires BRAVE_USER_DATA_DIR to be configured"
            )

        source_user_data_dir = Path(self.settings.brave_user_data_dir)
        if not source_user_data_dir.exists():
            raise RuntimeError(f"BRAVE_USER_DATA_DIR not found: {source_user_data_dir}")

        playwright = sync_playwright().start()
        proxy = proxy_manager.active
        temp_profile_dir: Path | None = None
        effective_user_data_dir = source_user_data_dir

        if self._is_brave_running():
            if not self.settings.brave_clone_profile_when_running:
                raise RuntimeError(
                    "Brave is currently running. Close Brave and retry, or enable BRAVE_CLONE_PROFILE_WHEN_RUNNING=1"
                )
            try:
                self.store.add_event(
                    run_id,
                    "Brave appears to be open. Cloning profile snapshot for scraping session.",
                )
                temp_profile_dir = self._clone_brave_profile_snapshot(
                    run_id, source_user_data_dir
                )
                effective_user_data_dir = temp_profile_dir
            except Exception:
                self.store.add_event(
                    run_id,
                    "Profile clone failed while Brave is open. Closing Brave and retrying with original profile.",
                    level="warning",
                )
                self._terminate_brave()
                time.sleep(2)
                if self._is_brave_running():
                    raise RuntimeError(
                        "Brave is still running after termination attempt. Close Brave manually and retry."
                    )

        launch_kwargs: dict[str, Any] = {"headless": self.settings.browser_headless}
        launch_kwargs["executable_path"] = self.settings.brave_executable_path
        launch_kwargs["channel"] = None
        launch_kwargs["args"] = [
            f"--profile-directory={self.settings.brave_profile_directory}",
            f"--window-size={self.settings.browser_viewport_width},{self.settings.browser_viewport_height}",
        ]
        launch_kwargs["viewport"] = {
            "width": self.settings.browser_viewport_width,
            "height": self.settings.browser_viewport_height,
        }
        if proxy:
            launch_kwargs["proxy"] = proxy.as_playwright_proxy()

        try:
            context = playwright.chromium.launch_persistent_context(
                str(effective_user_data_dir),
                **launch_kwargs,
            )
            context.set_default_timeout(15_000)
            return playwright, context, temp_profile_dir
        except Exception as exc:
            try:
                playwright.stop()
            except Exception:
                pass
            if temp_profile_dir:
                shutil.rmtree(temp_profile_dir, ignore_errors=True)
            raise RuntimeError(
                "Could not launch Brave session. Ensure BRAVE_EXECUTABLE_PATH and BRAVE_USER_DATA_DIR are valid."
            ) from exc

    def _is_closed_context_error(self, exc: Exception) -> bool:
        text = str(exc).lower()
        needles = (
            "target page, context or browser has been closed",
            "browser has been closed",
            "context has been closed",
            "page has been closed",
        )
        return any(n in text for n in needles)

    def _is_rate_limited_error(self, exc: Exception, page: object) -> bool:
        text = str(exc).lower()
        if any(
            needle in text
            for needle in (
                "err_http_response_code_failure",
                "http error 429",
                "too many requests",
                "temporarily blocked",
            )
        ):
            return True
        try:
            diagnostics = collect_page_diagnostics(page)
            return diagnostics.get("http_error_code") == "429"
        except Exception:
            return False

    def _sample_bucket_for_media_type(self, media_type: str | None) -> str | None:
        if media_type == "reel":
            return "reels"
        if media_type == "carousel_post":
            return "multi_image_posts"
        if media_type in {"image_post", "video_post"}:
            return "posts"
        return None

    def _profile_media_folder_name(
        self, username: str, full_name: str | None = None
    ) -> str:
        base = (full_name or username or "unknown_profile").strip().lower()
        safe = "".join(ch if ch.isalnum() else "_" for ch in base)
        safe = re.sub(r"_+", "_", safe).strip("_")
        return safe or "unknown_profile"

    def _guess_asset_extension(self, url: str, content_type: str | None = None) -> str:
        parsed = urlparse(url)
        suffix = Path(parsed.path).suffix.lower()
        if suffix in {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".m4v"}:
            return suffix
        if content_type:
            guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
            if guessed:
                return guessed
        guess, _ = mimetypes.guess_type(url)
        if guess:
            ext = mimetypes.guess_extension(guess)
            if ext:
                return ext
        return ".bin"

    def _download_sample_media_assets(
        self,
        run_id: str,
        username: str,
        full_name: str | None,
        sample_bucket: str,
        shortcode: str,
        media_asset_urls: list[str],
    ) -> list[str]:
        if not media_asset_urls:
            return []

        profile_dir_name = self._profile_media_folder_name(username, full_name)
        base_dir = self.settings.media_dir / profile_dir_name / sample_bucket
        base_dir.mkdir(parents=True, exist_ok=True)
        saved_paths: list[str] = []
        ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
        run_short = run_id[:8]

        for idx, asset_url in enumerate(media_asset_urls, start=1):
            try:
                response = requests.get(
                    asset_url,
                    timeout=self.settings.request_timeout_seconds,
                    stream=True,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                response.raise_for_status()

                content_type = (response.headers.get("Content-Type") or "").lower()
                ext = self._guess_asset_extension(asset_url, content_type)

                is_video = content_type.startswith("video/") or ext in {
                    ".mp4",
                    ".mov",
                    ".m4v",
                    ".webm",
                }
                if sample_bucket == "reels" and not is_video:
                    continue

                out_path = base_dir / f"{ts}_{run_short}_{shortcode}_{idx:02d}{ext}"
                with out_path.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            f.write(chunk)
                saved_paths.append(str(out_path.resolve()))
            except Exception:
                continue

        return saved_paths

    def _append_sample_manifest(
        self,
        username: str,
        full_name: str | None,
        sample_bucket: str,
        shortcode: str,
        post_url: str,
        media_asset_urls: list[str],
        media_asset_local_paths: list[str],
    ) -> None:
        profile_dir_name = self._profile_media_folder_name(username, full_name)
        profile_root = self.settings.media_dir / profile_dir_name
        profile_root.mkdir(parents=True, exist_ok=True)
        manifest_path = profile_root / "sample_index.csv"
        columns = [
            "captured_at_ist",
            "sample_type",
            "shortcode",
            "post_url",
            "remote_asset_urls_csv",
            "local_asset_paths_csv",
        ]
        row = {
            "captured_at_ist": iso_ist(now_ist()),
            "sample_type": sample_bucket,
            "shortcode": shortcode,
            "post_url": post_url,
            "remote_asset_urls_csv": ",".join(media_asset_urls)
            if media_asset_urls
            else None,
            "local_asset_paths_csv": ",".join(media_asset_local_paths)
            if media_asset_local_paths
            else None,
        }

        exists = manifest_path.exists()
        with manifest_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            if not exists:
                writer.writeheader()
            writer.writerow(row)

    def _scrape_single_profile(
        self,
        run_id: str,
        profile_url: str,
        use_saved_session: bool,
        resume_state: dict[str, Any] | None = None,
    ) -> ProfileRunResult:
        resume_state = resume_state or {}
        proxy_manager = ProxyManager(self.settings)
        proxy = proxy_manager.active
        username_hint = profile_url.rstrip("/").split("/")[-1]

        playwright, context, temp_profile_dir = self._launch_context(
            run_id=run_id,
            proxy_manager=proxy_manager,
            use_saved_session=use_saved_session,
            username_hint=username_hint,
        )
        page = context.new_page()
        if proxy:
            self.store.update_run(run_id, proxy_id=proxy.proxy_id)
        self.store.add_event(run_id, f"Browser started for {profile_url}")

        def _save_page_debug_artifacts(stage: str) -> dict[str, str | None]:
            debug_dir = self.settings.runs_dir / run_id / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            safe_stage = re.sub(r"[^a-zA-Z0-9._-]+", "_", stage).strip("_") or "stage"
            stamp = now_ist().strftime("%Y%m%d_%H%M%S_%f")
            html_path = debug_dir / f"{safe_stage}_{stamp}.html"
            screenshot_path = debug_dir / f"{safe_stage}_{stamp}.png"

            try:
                html = page.content()
                html_path.write_text(html, encoding="utf-8")
            except Exception:
                html_path = None

            try:
                page.screenshot(path=str(screenshot_path), full_page=True)
            except Exception:
                screenshot_path = None

            return {
                "html_path": str(html_path.resolve()) if html_path else None,
                "screenshot_path": str(screenshot_path.resolve())
                if screenshot_path
                else None,
            }

        def _log_problem_diagnostics(stage: str, reason: str) -> None:
            diag = collect_page_diagnostics(page)
            artifacts = _save_page_debug_artifacts(stage)
            message = (
                f"Diagnostics [{stage}] reason={reason}; "
                f"url={diag.get('url')}; title={diag.get('title')}; "
                f"http_error={diag.get('http_error_code')}; "
                f"body_snippet={diag.get('body_snippet')}; "
                f"html={artifacts.get('html_path')}; screenshot={artifacts.get('screenshot_path')}"
            )
            self.store.add_event(run_id, message, level="warning")

        def _check_challenge_with_recovery(
            state: dict[str, Any],
            recovery_url: str | None = None,
        ) -> bool:
            hit, pattern = detect_challenge(page)
            if not hit:
                return True

            _log_problem_diagnostics(
                stage=str(state.get("stage") or "challenge"),
                reason=f"challenge_detected:{pattern}",
            )

            attempts = max(0, self.settings.challenge_auto_retry_attempts)
            for attempt in range(1, attempts + 1):
                self.store.add_event(
                    run_id,
                    f"Challenge detected ({pattern}). Auto-recovery attempt {attempt}/{attempts}",
                    level="warning",
                )
                wait_seconds = self.settings.challenge_auto_retry_wait_seconds
                if wait_seconds > 0:
                    time.sleep(wait_seconds)

                try:
                    _relaunch_browser_session(
                        "Relaunching browser session for challenge auto-recovery"
                    )
                    if recovery_url:
                        page.goto(recovery_url, wait_until="domcontentloaded")
                        page.wait_for_timeout(
                            max(500, self.settings.post_detail_wait_ms)
                        )
                except Exception:
                    pass

                hit, pattern = detect_challenge(page)
                if not hit:
                    self.store.add_event(
                        run_id,
                        "Challenge auto-recovery succeeded; continuing run.",
                    )
                    return True

            self.store.add_event(
                run_id,
                f"Challenge persisted after auto-recovery ({pattern}); continuing with partial data.",
                level="warning",
            )
            _log_problem_diagnostics(
                stage=str(state.get("stage") or "challenge_persisted"),
                reason=f"challenge_persisted:{pattern}",
            )
            self.store.update_run(run_id, challenge_encountered=True)
            return False

        def _relaunch_browser_session(reason: str) -> None:
            nonlocal playwright, context, page, temp_profile_dir
            self.store.add_event(run_id, reason)

            try:
                context.close()
            except Exception:
                pass
            try:
                playwright.stop()
            except Exception:
                pass
            if temp_profile_dir:
                shutil.rmtree(temp_profile_dir, ignore_errors=True)
                temp_profile_dir = None

            playwright, context, temp_profile_dir = self._launch_context(
                run_id=run_id,
                proxy_manager=proxy_manager,
                use_saved_session=use_saved_session,
                username_hint=username_hint,
            )
            page = context.new_page()
            active_proxy = proxy_manager.active
            if active_proxy:
                self.store.update_run(run_id, proxy_id=active_proxy.proxy_id)

        try:
            profile = scrape_profile_header(page, profile_url)
            profile_challenge_cleared = _check_challenge_with_recovery(
                {"stage": "profile_header", "profile_url": profile_url},
                recovery_url=profile_url,
            )

            scraped_at = iso_ist(now_ist())
            profile_row = {
                "scraped_at_ist": scraped_at,
                "run_id": run_id,
                **profile.profile_data,
            }

            if not profile_challenge_cleared:
                profile_row["missing_reason_profile"] = "challenge_blocked"
                return ProfileRunResult(
                    profile_row={k: profile_row.get(k) for k in PROFILE_COLUMNS},
                    highlights_rows=[],
                    links_rows=[],
                    posts_rows=[],
                    aggregates_rows=[],
                    status="completed",
                )

            if profile.is_private:
                profile_row["missing_reason_profile"] = (
                    profile_row.get("missing_reason_profile") or "not_applicable"
                )
                return ProfileRunResult(
                    profile_row=profile_row,
                    highlights_rows=[],
                    links_rows=[],
                    posts_rows=[],
                    aggregates_rows=[],
                    status="skipped_private",
                )

            about = scrape_about_section(page)
            profile_row.update(about)

            highlights_raw = scrape_highlights(
                page, profile_row.get("username") or username_hint
            )
            highlights_rows = [
                {"scraped_at_ist": scraped_at, "run_id": run_id, **row}
                for row in highlights_raw
            ]

            links_raw = expand_external_links(
                profile.external_urls,
                timeout_seconds=self.settings.request_timeout_seconds,
            )
            links_rows = [
                {
                    "scraped_at_ist": scraped_at,
                    "run_id": run_id,
                    "username": profile_row.get("username"),
                    **row,
                }
                for row in links_raw
            ]

            processed: set[str] = set(resume_state.get("processed_shortcodes", []))
            posts_rows: list[dict[str, Any]] = list(
                resume_state.get("partial_posts_rows", [])
            )
            username = profile_row.get("username") or username_hint
            sample_captured = {
                "posts": False,
                "reels": False,
                "multi_image_posts": False,
            }
            required_samples = {"posts", "reels", "multi_image_posts"}
            consecutive_rate_limits = 0
            stop_post_loop_due_rate_limit = False

            for row in posts_rows:
                bucket = row.get("sample_bucket")
                if bucket in sample_captured:
                    sample_captured[bucket] = True

            discovered: list[dict[str, Any]] = []
            if self.settings.sample_collection_mode:
                self.store.add_event(
                    run_id,
                    "Sample mode enabled: collecting timeline snapshot first to avoid deep scrolling.",
                )
                timeline_items: list[dict[str, Any]] = []
                try:
                    timeline_items = collect_recent_timeline_items(
                        page,
                        profile_url,
                        wait_ms=max(1500, self.settings.post_detail_wait_ms * 4),
                    )
                except Exception as exc:
                    self.store.add_event(
                        run_id,
                        f"Timeline snapshot unavailable ({type(exc).__name__}). Using visible grid fallback.",
                        level="warning",
                    )

                for item in timeline_items:
                    if all(sample_captured[b] for b in required_samples):
                        break
                    shortcode = item.get("shortcode")
                    if not shortcode or shortcode in processed:
                        continue

                    sample_bucket = item.get(
                        "sample_bucket"
                    ) or self._sample_bucket_for_media_type(item.get("media_type"))
                    if sample_bucket not in sample_captured or sample_captured.get(
                        sample_bucket
                    ):
                        continue

                    media_asset_urls = [
                        x
                        for x in (item.get("media_asset_urls") or [])
                        if isinstance(x, str) and x
                    ]
                    if not media_asset_urls:
                        continue

                    media_asset_local_paths = self._download_sample_media_assets(
                        run_id=run_id,
                        username=username,
                        full_name=profile_row.get("full_name"),
                        sample_bucket=sample_bucket,
                        shortcode=shortcode,
                        media_asset_urls=media_asset_urls,
                    )
                    if media_asset_local_paths:
                        self._append_sample_manifest(
                            username=username,
                            full_name=profile_row.get("full_name"),
                            sample_bucket=sample_bucket,
                            shortcode=shortcode,
                            post_url=item.get("post_url") or "",
                            media_asset_urls=media_asset_urls,
                            media_asset_local_paths=media_asset_local_paths,
                        )

                    row = {
                        "scraped_at_ist": scraped_at,
                        "run_id": run_id,
                        "username": username,
                        "shortcode": shortcode,
                        "post_url": item.get("post_url"),
                        "media_type": item.get("media_type"),
                        "posted_at_ist": item.get("posted_at_ist"),
                        "likes_count": item.get("likes_count"),
                        "comments_count": item.get("comments_count"),
                        "views_count": item.get("views_count"),
                        "is_remix_repost": item.get("is_remix_repost"),
                        "is_tagged_post": item.get("is_tagged_post"),
                        "tagged_users_count": item.get("tagged_users_count"),
                        "hashtags_csv": item.get("hashtags_csv"),
                        "keywords_csv": item.get("keywords_csv"),
                        "mentions_csv": item.get("mentions_csv"),
                        "caption_text": item.get("caption_text"),
                        "location_name": item.get("location_name"),
                        "media_asset_urls_csv": ",".join(media_asset_urls),
                        "media_asset_local_paths_csv": ",".join(media_asset_local_paths)
                        if media_asset_local_paths
                        else None,
                        "sample_bucket": sample_bucket,
                        "missing_reason_post": None,
                    }
                    posts_rows.append({k: row.get(k) for k in POSTS_COLUMNS})
                    processed.add(shortcode)
                    sample_captured[sample_bucket] = True
                    self.store.add_event(
                        run_id,
                        f"Sample captured from timeline: {sample_bucket} ({shortcode})",
                    )

                if not all(sample_captured[b] for b in required_samples):
                    self.store.add_event(
                        run_id,
                        "Sample mode fallback: scanning only currently visible grid posts (no deep scroll).",
                        level="warning",
                    )
                    visible_scan_settings = replace(self.settings, scroll_idle_rounds=0)
                    discovered = enumerate_grid_posts(
                        page, visible_scan_settings, resume_state=resume_state
                    )
                    if (
                        self.settings.max_posts_per_profile
                        and self.settings.max_posts_per_profile > 0
                    ):
                        discovered = discovered[: self.settings.max_posts_per_profile]
                    else:
                        discovered = discovered[:30]
                else:
                    discovered = []
            else:
                enum_attempts = 0
                last_enum_error: Exception | None = None
                discovered_or_none: list[dict[str, Any]] | None = None
                while enum_attempts < self.settings.retry_max_attempts:
                    enum_attempts += 1
                    try:
                        discovered_or_none = enumerate_grid_posts(
                            page, self.settings, resume_state=resume_state
                        )
                        break
                    except Exception as exc:
                        last_enum_error = exc
                        if not self._is_closed_context_error(exc):
                            raise
                        self.store.add_event(
                            run_id,
                            "Browser context closed during grid scan; relaunching and retrying.",
                            level="warning",
                        )
                        _relaunch_browser_session(
                            "Relaunching browser session after closed context during grid scan"
                        )
                        page.goto(profile_url, wait_until="domcontentloaded")
                        page.wait_for_timeout(
                            max(300, self.settings.post_detail_wait_ms)
                        )

                if discovered_or_none is None:
                    if last_enum_error is not None:
                        raise last_enum_error
                    raise RuntimeError("Failed to enumerate posts/reels")
                discovered = discovered_or_none

                if (
                    self.settings.max_posts_per_profile
                    and self.settings.max_posts_per_profile > 0
                ):
                    discovered = discovered[: self.settings.max_posts_per_profile]
                    self.store.add_event(
                        run_id,
                        f"Post discovery limited to first {self.settings.max_posts_per_profile} items",
                    )

            self.store.add_event(run_id, f"Discovered {len(discovered)} posts/reels")
            grid_challenge_cleared = _check_challenge_with_recovery(
                {
                    "stage": "grid_enumeration",
                    "profile_url": profile_url,
                    "discovered_posts": discovered,
                },
                recovery_url=profile_url,
            )
            if not grid_challenge_cleared:
                discovered = []

            for i, post in enumerate(discovered):
                shortcode = post.get("shortcode")
                if not shortcode or shortcode in processed:
                    continue

                previous_proxy_id = proxy_manager.current_proxy_id()
                active_proxy = proxy_manager.mark_request()
                current_proxy_id = active_proxy.proxy_id if active_proxy else None
                if (
                    current_proxy_id != previous_proxy_id
                    and current_proxy_id is not None
                ):
                    _relaunch_browser_session(
                        f"Rotating proxy to {current_proxy_id} before post {i + 1}"
                    )

                if i == 0 or i % 5 == 0:
                    total = max(1, len(discovered))
                    pct = 10 + (i / total) * 80
                    self.store.set_progress(
                        run_id, f"Scraping post {i + 1}/{total}", pct
                    )

                attempts = 0
                last_error: Exception | None = None
                while attempts < self.settings.retry_max_attempts:
                    attempts += 1
                    try:
                        detail = scrape_post_detail(
                            page,
                            post["post_url"],
                            media_type_hint=post.get("media_type_hint"),
                            page_settle_ms=self.settings.post_detail_wait_ms,
                        )
                        post_challenge_cleared = _check_challenge_with_recovery(
                            {
                                "stage": "post_detail",
                                "profile_url": profile_url,
                                "discovered_posts": discovered,
                                "processed_shortcodes": list(processed),
                                "partial_posts_rows": posts_rows,
                                "current_post_shortcode": shortcode,
                                "current_post_index": i,
                            },
                            recovery_url=post["post_url"],
                        )
                        if not post_challenge_cleared:
                            raise RuntimeError("challenge_persisted")

                        media_asset_urls = list(detail.get("media_asset_urls") or [])
                        sample_bucket = self._sample_bucket_for_media_type(
                            detail.get("media_type")
                        )
                        media_asset_local_paths: list[str] = []
                        if (
                            sample_bucket
                            and not sample_captured[sample_bucket]
                            and media_asset_urls
                        ):
                            media_asset_local_paths = (
                                self._download_sample_media_assets(
                                    run_id=run_id,
                                    username=username,
                                    full_name=profile_row.get("full_name"),
                                    sample_bucket=sample_bucket,
                                    shortcode=shortcode,
                                    media_asset_urls=media_asset_urls,
                                )
                            )
                            if media_asset_local_paths:
                                sample_captured[sample_bucket] = True
                                self.store.add_event(
                                    run_id,
                                    f"Stored {len(media_asset_local_paths)} files for {sample_bucket} ({shortcode})",
                                )
                                self._append_sample_manifest(
                                    username=username,
                                    full_name=profile_row.get("full_name"),
                                    sample_bucket=sample_bucket,
                                    shortcode=shortcode,
                                    post_url=post.get("post_url")
                                    or detail.get("post_url")
                                    or "",
                                    media_asset_urls=media_asset_urls,
                                    media_asset_local_paths=media_asset_local_paths,
                                )

                        row = {
                            "scraped_at_ist": scraped_at,
                            "run_id": run_id,
                            "username": username,
                            **detail,
                            "media_asset_urls_csv": ",".join(media_asset_urls)
                            if media_asset_urls
                            else None,
                            "media_asset_local_paths_csv": ",".join(
                                media_asset_local_paths
                            )
                            if media_asset_local_paths
                            else None,
                            "sample_bucket": sample_bucket,
                        }
                        posts_rows.append({k: row.get(k) for k in POSTS_COLUMNS})
                        processed.add(shortcode)
                        consecutive_rate_limits = 0
                        self.store.update_run(
                            run_id,
                            state={
                                "profile_state": {
                                    "stage": "post_detail",
                                    "profile_url": profile_url,
                                    "discovered_posts": discovered,
                                    "processed_shortcodes": list(processed),
                                    "partial_posts_rows": posts_rows,
                                    "current_post_index": i,
                                }
                            },
                        )
                        if self.settings.sample_collection_mode and all(
                            sample_captured[b] for b in required_samples
                        ):
                            self.store.add_event(
                                run_id,
                                "Sample collection mode: captured posts/reels/multi_image_posts; stopping early.",
                            )
                            break
                        break
                    except Exception as exc:
                        last_error = exc
                        is_rate_limited = self._is_rate_limited_error(exc, page)
                        if is_rate_limited:
                            consecutive_rate_limits += 1
                            cooldown = max(
                                1.0,
                                self.settings.rate_limit_cooldown_seconds * attempts,
                            )
                            jitter = random.uniform(0.15, 0.45) * cooldown
                            cooldown = cooldown + jitter
                            self.store.add_event(
                                run_id,
                                f"Rate limit detected on post {i + 1}. Cooling down {cooldown:.0f}s before retry.",
                                level="warning",
                            )
                            _log_problem_diagnostics(
                                stage=f"post_detail_{i + 1}",
                                reason=f"rate_limit:{type(exc).__name__}:{exc}",
                            )

                            rotated_proxy = proxy_manager.rotate_now()
                            if rotated_proxy:
                                self.store.add_event(
                                    run_id,
                                    f"Rate limit response: rotating proxy to {rotated_proxy.proxy_id}",
                                    level="warning",
                                )

                            time.sleep(cooldown)
                            _relaunch_browser_session(
                                f"Relaunching browser session after rate-limit cooldown on post {i + 1}"
                            )

                            # Prevent hammering Instagram when the account/session is hard-limited.
                            if (
                                consecutive_rate_limits >= 3
                                and not self.settings.has_proxy_pool
                            ):
                                self.store.add_event(
                                    run_id,
                                    "Repeated rate limits detected without proxy pool; pausing post loop to avoid further blocking.",
                                    level="warning",
                                )
                                stop_post_loop_due_rate_limit = True
                                break
                            continue
                        if attempts == self.settings.retry_max_attempts:
                            _log_problem_diagnostics(
                                stage=f"post_detail_{i + 1}",
                                reason=f"exception:{type(exc).__name__}:{exc}",
                            )
                        if self._is_closed_context_error(exc):
                            _relaunch_browser_session(
                                f"Browser context closed while scraping post {i + 1}; retrying in fresh session"
                            )
                        wait_seconds = self.settings.retry_base_delay_seconds * attempts
                        time.sleep(wait_seconds)
                        rotated_proxy = proxy_manager.rotate_now()
                        if rotated_proxy:
                            _relaunch_browser_session(
                                f"Retry rotating proxy to {rotated_proxy.proxy_id} for post {i + 1}"
                            )
                if last_error and shortcode not in processed:
                    posts_rows.append(
                        {
                            "scraped_at_ist": scraped_at,
                            "run_id": run_id,
                            "username": username,
                            "shortcode": shortcode,
                            "post_url": post["post_url"],
                            "media_type": post.get("media_type_hint"),
                            "posted_at_ist": None,
                            "likes_count": None,
                            "comments_count": None,
                            "views_count": None,
                            "is_remix_repost": None,
                            "is_tagged_post": None,
                            "tagged_users_count": None,
                            "hashtags_csv": None,
                            "keywords_csv": None,
                            "mentions_csv": None,
                            "caption_text": None,
                            "location_name": None,
                            "missing_reason_post": "parse_error",
                        }
                    )
                    processed.add(shortcode)

                if self.settings.sample_collection_mode and all(
                    sample_captured[b] for b in required_samples
                ):
                    break

                if stop_post_loop_due_rate_limit:
                    break

            aggregates_rows = build_aggregates(
                scraped_at_ist=scraped_at,
                run_id=run_id,
                username=username,
                posts_rows=posts_rows,
                now=now_ist(),
            )

            return ProfileRunResult(
                profile_row={k: profile_row.get(k) for k in PROFILE_COLUMNS},
                highlights_rows=highlights_rows,
                links_rows=[
                    {k: r.get(k) for k in EXTERNAL_LINKS_COLUMNS} for r in links_rows
                ],
                posts_rows=posts_rows,
                aggregates_rows=[
                    {k: r.get(k) for k in AGGREGATES_COLUMNS} for r in aggregates_rows
                ],
                status="completed",
            )
        finally:
            try:
                if use_saved_session:
                    state_path = self._session_manager.storage_state_path(username_hint)
                    context.storage_state(path=str(state_path))
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
            try:
                playwright.stop()
            except Exception:
                pass
            if temp_profile_dir:
                shutil.rmtree(temp_profile_dir, ignore_errors=True)

    def _check_challenge_or_raise(
        self, page: object, run_id: str, state: dict[str, Any]
    ) -> None:
        hit, pattern = detect_challenge(page)
        if not hit:
            return
        self.store.add_event(run_id, f"Challenge detected ({pattern})", level="warning")
        raise ChallengeRequired("Instagram challenge/login wall detected", state=state)

    def _export(
        self,
        run_id: str,
        input_url: str,
        profile_rows: list[dict[str, Any]],
        highlights_rows: list[dict[str, Any]],
        external_links_rows: list[dict[str, Any]],
        posts_rows: list[dict[str, Any]],
        aggregate_rows: list[dict[str, Any]],
    ) -> dict[str, str]:
        run = self.store.get_run(run_id)
        if run is None:
            raise RuntimeError(f"Run missing while exporting: {run_id}")
        run_log_row = {
            "scraped_at_ist": iso_ist(now_ist()),
            "run_id": run_id,
            "input_url": input_url,
            "normalized_profile_url": run.normalized_profile_url,
            "status": "completed" if run.status == "running" else run.status,
            "started_at_ist": run.started_at_ist,
            "ended_at_ist": run.ended_at_ist,
            "duration_sec": run.duration_sec,
            "proxy_id": run.proxy_id,
            "session_mode": run.session_mode,
            "challenge_encountered": run.challenge_encountered,
            "error_code": run.error_code,
            "error_message": run.error_message,
        }
        run_log_rows = [{k: run_log_row.get(k) for k in RUN_LOG_COLUMNS}]
        profile_row = (
            profile_rows[0]
            if profile_rows
            else {"scraped_at_ist": run_log_row["scraped_at_ist"], "run_id": run_id}
        )
        summary_flat_rows = build_summary_flat(
            run_log_row=run_log_row,
            profile_row=profile_row,
            aggregate_rows=aggregate_rows,
            highlights_rows=highlights_rows,
            external_links_rows=external_links_rows,
            posts_rows=posts_rows,
        )

        ts = now_ist().strftime("%Y-%m-%d_%H-%M-%S")
        profile_username = (profile_row.get("username") or "").strip().lower()
        safe_username = "".join(
            ch for ch in profile_username if ch.isalnum() or ch in ("_", "-", ".")
        )
        subject = safe_username or "batch"
        base_name = f"instagram_{subject}_{ts}_{run_id[:8]}"

        artifacts = {}
        artifacts.update(
            export_csv_artifacts(
                exports_dir=self.settings.exports_dir,
                base_name=base_name,
                run_log_rows=run_log_rows,
                profile_rows=profile_rows,
                highlights_rows=highlights_rows,
                external_links_rows=external_links_rows,
                posts_rows=posts_rows,
                aggregate_rows=aggregate_rows,
                summary_flat_rows=summary_flat_rows,
            )
        )
        return artifacts
