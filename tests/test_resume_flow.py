from pathlib import Path

from app.core.config import Settings
from app.core.models import StartRunRequest
from app.runner.orchestrator import RunOrchestrator
from app.storage.sqlite_store import SQLiteStore


class NoThreadOrchestrator(RunOrchestrator):
    def __init__(self, settings: Settings, store: SQLiteStore):
        super().__init__(settings=settings, store=store)
        self.spawn_calls: list[tuple[str, bool]] = []

    def _spawn_thread(self, run_id: str, req: StartRunRequest, is_resume: bool) -> None:  # type: ignore[override]
        self.spawn_calls.append((run_id, is_resume))


def make_settings(tmp_path: Path) -> Settings:
    data_dir = tmp_path / "data"
    runs_dir = data_dir / "runs"
    browser_state_dir = data_dir / "browser_state"
    exports_dir = tmp_path / "exports"
    for p in [data_dir, runs_dir, browser_state_dir, exports_dir]:
        p.mkdir(parents=True, exist_ok=True)
    return Settings(
        app_name="test",
        app_env="test",
        project_root=tmp_path,
        data_dir=data_dir,
        runs_dir=runs_dir,
        browser_state_dir=browser_state_dir,
        exports_dir=exports_dir,
        sqlite_path=data_dir / "state.sqlite3",
        brave_executable_path=None,
        brave_user_data_dir=None,
        brave_profile_directory="Default",
        brave_clone_profile_when_running=True,
        browser_headless=True,
        browser_viewport_width=1100,
        browser_viewport_height=750,
        proxy_rotation_every_n_requests=20,
        scroll_idle_rounds=3,
        scroll_pause_min_ms=10,
        scroll_pause_max_ms=20,
        post_detail_wait_ms=50,
        request_timeout_seconds=10,
        retry_max_attempts=2,
        retry_base_delay_seconds=0.1,
        max_posts_per_profile=None,
        proxies=[],
    )


def test_resume_flow_state_and_status(tmp_path: Path):
    settings = make_settings(tmp_path)
    store = SQLiteStore(settings.sqlite_path)
    orch = NoThreadOrchestrator(settings=settings, store=store)

    req = StartRunRequest(
        input_type="single_url", input_value="https://www.instagram.com/indriyajewels/"
    )
    run_id = orch.submit_run(req)
    run = store.get_run(run_id)
    assert run is not None
    assert run.status == "queued"
    assert orch.spawn_calls == [(run_id, False)]

    store.update_run(
        run_id, status="needs_human", state={"profile_state": {"stage": "post_detail"}}
    )
    resumed = orch.resume_run(run_id)
    assert resumed.status == "resuming"
    assert orch.spawn_calls[-1] == (run_id, True)
