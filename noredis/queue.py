import json
import time
import threading
import traceback
from typing import Any, Callable, Optional
from .db import get_conn, get_lock

_registry: dict[str, Callable] = {}

def task(fn: Callable) -> Callable:
    """Decorator to register a function as a callable task."""
    key = f"{fn.__module__}.{fn.__qualname__}"
    _registry[key] = fn
    fn._task_name = key
    return fn

def register(fn: Callable, name: str) -> Callable:
    """
    Manually register a function with an explicit name.
    Use this when @task is defined inside a function/class
    and the qualname would be unstable across processes.

    Example:
        def send_email(payload): ...
        register(send_email, "send_email")
    """
    _registry[name] = fn
    fn._task_name = name
    return fn


class Queue:
    """
    Background job queue backed by SQLite.

    Usage:
        queue = Queue("app.db")

        @task
        def send_email(payload):
            ...

        queue.enqueue(send_email, {"to": "x@y.com"})
        queue.start_worker(concurrency=2)
    """

    def __init__(self, db_path: str = "noredis.db"):
        self.db_path = db_path
        self._running = False
        self._threads: list[threading.Thread] = []
        get_conn(db_path)  # ensure schema created

    # ── Public API ──────────────────────────────────────────────

    def enqueue(self, func: Callable, payload: Any = None, *,
                priority: int = 0,
                delay: float = 0,
                max_attempts: int = 3,
                run_at: Optional[float] = None) -> int:
        func_name = getattr(func, "_task_name", None)
        if func_name is None:
            raise ValueError(
                f"{func.__name__} must be decorated with @task or registered via noredis.register()"
            )
        scheduled_at = run_at or (time.time() + delay)
        conn = get_conn(self.db_path)
        with get_lock(self.db_path):
            cur = conn.execute("""
                INSERT INTO jobs (func_name, payload, priority, run_at, max_attempts)
                VALUES (?, ?, ?, ?, ?)
            """, (func_name, json.dumps(payload or {}), priority, scheduled_at, max_attempts))
            conn.commit()
        return cur.lastrowid

    def enqueue_at(self, func: Callable, run_at: float, payload: Any = None, **kwargs) -> int:
        return self.enqueue(func, payload, run_at=run_at, **kwargs)

    def enqueue_in(self, func: Callable, seconds: float, payload: Any = None, **kwargs) -> int:
        return self.enqueue(func, payload, delay=seconds, **kwargs)

    def start_worker(self, concurrency: int = 2, poll_interval: float = 0.5):
        if self._running:
            return
        self._running = True
        for _ in range(concurrency):
            t = threading.Thread(target=self._worker_loop,
                                 args=(poll_interval,), daemon=True)
            t.start()
            self._threads.append(t)
        w = threading.Thread(target=self._watchdog_loop, daemon=True)
        w.start()

    def stop_worker(self):
        self._running = False

    def job_status(self, job_id: int) -> Optional[dict]:
        conn = get_conn(self.db_path)
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else None

    def stats(self) -> dict:
        conn = get_conn(self.db_path)
        rows = conn.execute(
            "SELECT status, COUNT(*) as count FROM jobs GROUP BY status"
        ).fetchall()
        return {row["status"]: row["count"] for row in rows}

    def dead_letters(self) -> list[dict]:
        conn = get_conn(self.db_path)
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status = 'failed' ORDER BY updated_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def retry(self, job_id: int) -> bool:
        conn = get_conn(self.db_path)
        with get_lock(self.db_path):
            cur = conn.execute("""
                UPDATE jobs SET status = 'pending', attempts = 0, error = NULL,
                    run_at = ?, updated_at = ?
                WHERE id = ? AND status = 'failed'
            """, (time.time(), time.time(), job_id))
            conn.commit()
        return cur.rowcount > 0

    # ── Worker internals ────────────────────────────────────────

    def _claim_job(self) -> Optional[dict]:
        """Atomically claim next pending job using a Python-level lock."""
        conn = get_conn(self.db_path)
        with get_lock(self.db_path):
            row = conn.execute("""
                SELECT * FROM jobs
                WHERE status = 'pending' AND run_at <= ?
                ORDER BY priority DESC, run_at ASC
                LIMIT 1
            """, (time.time(),)).fetchone()
            if row is None:
                return None
            conn.execute("""
                UPDATE jobs SET status = 'running', updated_at = ?
                WHERE id = ?
            """, (time.time(), row["id"]))
            conn.commit()
        return dict(row)

    def _worker_loop(self, poll_interval: float):
        while self._running:
            job = self._claim_job()
            if job is None:
                time.sleep(poll_interval)
                continue
            self._execute_job(job)

    def _execute_job(self, job: dict):
        func = _registry.get(job["func_name"])
        conn = get_conn(self.db_path)

        if func is None:
            with get_lock(self.db_path):
                conn.execute("""
                    UPDATE jobs SET status = 'failed', error = ?, updated_at = ?
                    WHERE id = ?
                """, (f"Unknown task: {job['func_name']}", time.time(), job["id"]))
                conn.commit()
            return

        try:
            payload = json.loads(job["payload"])
            func(payload)
            with get_lock(self.db_path):
                conn.execute("""
                    UPDATE jobs SET status = 'done', updated_at = ? WHERE id = ?
                """, (time.time(), job["id"]))
                conn.commit()

        except Exception as e:
            attempts = job["attempts"] + 1
            with get_lock(self.db_path):
                if attempts >= job["max_attempts"]:
                    conn.execute("""
                        UPDATE jobs SET status = 'failed', attempts = ?,
                            error = ?, updated_at = ?
                        WHERE id = ?
                    """, (attempts, traceback.format_exc(), time.time(), job["id"]))
                else:
                    backoff = 2 ** attempts
                    conn.execute("""
                        UPDATE jobs SET status = 'pending', attempts = ?,
                            error = ?, run_at = ?, updated_at = ?
                        WHERE id = ?
                    """, (attempts, str(e), time.time() + backoff, time.time(), job["id"]))
                conn.commit()

    def _watchdog_loop(self):
        while self._running:
            time.sleep(60)
            try:
                conn = get_conn(self.db_path)
                stuck_threshold = time.time() - 300
                with get_lock(self.db_path):
                    conn.execute("""
                        UPDATE jobs SET status = 'pending', updated_at = ?
                        WHERE status = 'running' AND updated_at <= ?
                    """, (time.time(), stuck_threshold))
                    conn.commit()
            except Exception:
                pass