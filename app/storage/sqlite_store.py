from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from app.core.config import iso_ist, now_ist
from app.core.models import RunContext


class SQLiteStore:
    def __init__(self, path: Path):
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    input_url TEXT NOT NULL,
                    normalized_profile_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at_ist TEXT,
                    ended_at_ist TEXT,
                    duration_sec REAL,
                    proxy_id TEXT,
                    session_mode TEXT,
                    challenge_encountered INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT,
                    error_message TEXT,
                    progress_message TEXT,
                    progress_pct REAL NOT NULL DEFAULT 0,
                    artifacts_json TEXT NOT NULL DEFAULT '{}',
                    state_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    at_ist TEXT NOT NULL,
                    message TEXT NOT NULL,
                    level TEXT NOT NULL DEFAULT 'info'
                );
                """
            )
            conn.commit()

    def create_run(self, run: RunContext) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    run_id, input_url, normalized_profile_url, status, started_at_ist,
                    ended_at_ist, duration_sec, proxy_id, session_mode, challenge_encountered,
                    error_code, error_message, progress_message, progress_pct, artifacts_json, state_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.input_url,
                    run.normalized_profile_url,
                    run.status,
                    run.started_at_ist,
                    run.ended_at_ist,
                    run.duration_sec,
                    run.proxy_id,
                    run.session_mode,
                    int(run.challenge_encountered),
                    run.error_code,
                    run.error_message,
                    run.progress_message,
                    run.progress_pct,
                    json.dumps(run.artifacts, ensure_ascii=False),
                    json.dumps(run.state, ensure_ascii=False),
                ),
            )
            conn.commit()
            self.add_event(run.run_id, f"Run created with status={run.status}")

    def get_run(self, run_id: str) -> RunContext | None:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if not row:
                return None
            return self._row_to_context(row)

    def update_run(self, run_id: str, **kwargs: Any) -> RunContext:
        allowed = {
            "status",
            "started_at_ist",
            "ended_at_ist",
            "duration_sec",
            "proxy_id",
            "session_mode",
            "challenge_encountered",
            "error_code",
            "error_message",
            "progress_message",
            "progress_pct",
            "artifacts",
            "state",
        }
        update_payload = {k: v for k, v in kwargs.items() if k in allowed}
        if not update_payload:
            current = self.get_run(run_id)
            if current is None:
                raise KeyError(f"Run not found: {run_id}")
            return current

        if "artifacts" in update_payload:
            update_payload["artifacts_json"] = json.dumps(update_payload.pop("artifacts"), ensure_ascii=False)
        if "state" in update_payload:
            update_payload["state_json"] = json.dumps(update_payload.pop("state"), ensure_ascii=False)
        if "challenge_encountered" in update_payload:
            update_payload["challenge_encountered"] = int(bool(update_payload["challenge_encountered"]))

        set_sql = ", ".join(f"{k} = ?" for k in update_payload.keys())
        values = list(update_payload.values())

        with self._lock, self._connect() as conn:
            cursor = conn.execute(f"UPDATE runs SET {set_sql} WHERE run_id = ?", (*values, run_id))
            if cursor.rowcount == 0:
                raise KeyError(f"Run not found: {run_id}")
            conn.commit()
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            assert row is not None
            return self._row_to_context(row)

    def set_progress(self, run_id: str, message: str, pct: float) -> RunContext:
        pct = max(0.0, min(100.0, pct))
        self.add_event(run_id, message)
        return self.update_run(run_id, progress_message=message, progress_pct=pct)

    def fail_run(self, run_id: str, error_code: str, error_message: str) -> RunContext:
        self.add_event(run_id, f"Run failed: {error_code} - {error_message}", level="error")
        return self.update_run(
            run_id,
            status="failed",
            ended_at_ist=iso_ist(now_ist()),
            error_code=error_code,
            error_message=error_message,
        )

    def add_event(self, run_id: str, message: str, level: str = "info") -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO run_events(run_id, at_ist, message, level) VALUES (?, ?, ?, ?)",
                (run_id, iso_ist(now_ist()), message, level),
            )
            conn.commit()

    def get_events(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT at_ist, message, level FROM run_events WHERE run_id = ? ORDER BY id ASC", (run_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def _row_to_context(row: sqlite3.Row) -> RunContext:
        payload = dict(row)
        payload["challenge_encountered"] = bool(payload.get("challenge_encountered"))
        payload["artifacts"] = json.loads(payload.pop("artifacts_json") or "{}")
        payload["state"] = json.loads(payload.pop("state_json") or "{}")
        return RunContext.from_db(payload)
