import sqlite3
import threading
from pathlib import Path

_connections: dict[str, sqlite3.Connection] = {}
_locks: dict[str, threading.Lock] = {}
_meta_lock = threading.Lock()


def get_conn(path: str) -> sqlite3.Connection:
    """
    Get a shared SQLite connection for the given path.
    SQLite with check_same_thread=False + WAL mode handles concurrent reads fine.
    Serialized writes are handled by SQLite's internal locking.
    """
    with _meta_lock:
        if path not in _connections:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA busy_timeout=5000")
            _connections[path] = conn
            _locks[path] = threading.Lock()
            _create_tables(conn)
    return _connections[path]


def get_lock(path: str) -> threading.Lock:
    get_conn(path)  # ensure initialised
    return _locks[path]


def _create_tables(conn: sqlite3.Connection):
    """Create all tables if they don't exist (idempotent)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cache_entries (
            key         TEXT NOT NULL,
            namespace   TEXT NOT NULL DEFAULT 'default',
            value       BLOB NOT NULL,
            expires_at  REAL,
            created_at  REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
            PRIMARY KEY (key, namespace)
        );

        CREATE INDEX IF NOT EXISTS idx_cache_expires
            ON cache_entries(expires_at)
            WHERE expires_at IS NOT NULL;

        CREATE TABLE IF NOT EXISTS jobs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            func_name    TEXT NOT NULL,
            payload      TEXT NOT NULL DEFAULT '{}',
            status       TEXT NOT NULL DEFAULT 'pending',
            priority     INTEGER NOT NULL DEFAULT 0,
            attempts     INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            run_at       REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
            created_at   REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
            updated_at   REAL NOT NULL DEFAULT (unixepoch('now', 'subsec')),
            error        TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at
            ON jobs(status, priority DESC, run_at)
            WHERE status = 'pending';
    """)
    conn.commit()
