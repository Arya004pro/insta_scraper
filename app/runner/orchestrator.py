from __future__ import annotations

import csv
import os
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from app.anti_block.challenge_handler import detect_challenge
from app.anti_block.proxy_manager import ProxyManager
from app.anti_block.session_manager import SessionManager
from app.collectors.about_scraper import scrape_about_section
from app.collectors.grid_enumerator import enumerate_grid_posts
from app.collectors.highlight_scraper import scrape_highlights
from app.collectors.link_expander import expand_external_links
from app.collectors.post_detail_scraper import scrape_post_detail
from app.collectors.profile_scraper import scrape_profile_header
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
from app.exporters.xlsx_exporter import export_xlsx_artifacts
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
            self._check_challenge_or_raise(
                page, run_id, {"stage": "profile_header", "profile_url": profile_url}
            )

            scraped_at = iso_ist(now_ist())
            profile_row = {
                "scraped_at_ist": scraped_at,
                "run_id": run_id,
                **profile.profile_data,
            }

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

            discovered: list[dict[str, Any]] | None = None
            enum_attempts = 0
            last_enum_error: Exception | None = None
            while enum_attempts < self.settings.retry_max_attempts:
                enum_attempts += 1
                try:
                    discovered = enumerate_grid_posts(
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
                    page.wait_for_timeout(max(300, self.settings.post_detail_wait_ms))

            if discovered is None:
                if last_enum_error is not None:
                    raise last_enum_error
                raise RuntimeError("Failed to enumerate posts/reels")
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
            self._check_challenge_or_raise(
                page,
                run_id,
                {
                    "stage": "grid_enumeration",
                    "profile_url": profile_url,
                    "discovered_posts": discovered,
                },
            )

            processed: set[str] = set(resume_state.get("processed_shortcodes", []))
            posts_rows: list[dict[str, Any]] = list(
                resume_state.get("partial_posts_rows", [])
            )
            username = profile_row.get("username") or username_hint

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
                        self._check_challenge_or_raise(
                            page,
                            run_id,
                            {
                                "stage": "post_detail",
                                "profile_url": profile_url,
                                "discovered_posts": discovered,
                                "processed_shortcodes": list(processed),
                                "partial_posts_rows": posts_rows,
                                "current_post_shortcode": shortcode,
                                "current_post_index": i,
                            },
                        )
                        row = {
                            "scraped_at_ist": scraped_at,
                            "run_id": run_id,
                            "username": username,
                            **detail,
                        }
                        posts_rows.append({k: row.get(k) for k in POSTS_COLUMNS})
                        processed.add(shortcode)
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
                        break
                    except ChallengeRequired:
                        raise
                    except Exception as exc:
                        last_error = exc
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
        artifacts.update(
            export_xlsx_artifacts(
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
