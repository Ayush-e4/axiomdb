import contextlib
import pickle
import threading
import time
from collections import OrderedDict
from typing import Any

from .db import get_conn


class Cache:
    """
    Redis-like cache backed by SQLite.

    Usage:
        cache = Cache("app.db")
        cache.set("key", {"any": "value"}, ttl=60)
        cache.get("key")
    """

    def __init__(
        self,
        db_path: str = "axiomdb.db",
        namespace: str = "default",
        max_size: int = 10_000,
        cleanup_interval: int = 300,
    ):
        self.db_path = db_path
        self.namespace = namespace
        self.max_size = max_size
        self._l1: OrderedDict[tuple[str, str], tuple[Any, float | None]] = OrderedDict()
        self._l1_lock = threading.Lock()
        get_conn(db_path)
        self._start_cleanup(cleanup_interval)

    # ── Public API ──────────────────────────────────────────────

    def set(self, key: str, value: Any, ttl: int | None = None, ex: int | None = None) -> None:
        """Set a key. ttl/ex = seconds until expiry."""
        ttl = ttl or ex
        expires_at = time.time() + ttl if ttl else None
        blob = pickle.dumps(value)

        conn = get_conn(self.db_path)
        conn.execute(
            """
            INSERT INTO cache_entries (key, namespace, value, expires_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(key, namespace) DO UPDATE SET
                value = excluded.value,
                expires_at = excluded.expires_at
        """,
            (key, self.namespace, blob, expires_at),
        )
        conn.commit()

        with self._l1_lock:
            self._l1[(key, self.namespace)] = (value, expires_at)
            self._l1.move_to_end((key, self.namespace))
            self._evict_l1_if_needed()

    def get(self, key: str, default: Any = None) -> Any:
        """Get a key. Returns default if missing or expired."""
        with self._l1_lock:
            entry = self._l1.get((key, self.namespace))
            if entry is not None:
                value, expires_at = entry
                if expires_at is None or expires_at > time.time():
                    self._l1.move_to_end((key, self.namespace))
                    return value
                del self._l1[(key, self.namespace)]

        conn = get_conn(self.db_path)
        row = conn.execute(
            """
            SELECT value, expires_at FROM cache_entries
            WHERE key = ? AND namespace = ?
        """,
            (key, self.namespace),
        ).fetchone()

        if row is None:
            return default

        if row["expires_at"] and row["expires_at"] <= time.time():
            self.delete(key)
            return default

        value = pickle.loads(row["value"])
        with self._l1_lock:
            self._l1[(key, self.namespace)] = (value, row["expires_at"])
            self._l1.move_to_end((key, self.namespace))
            self._evict_l1_if_needed()
        return value

    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if it existed."""
        with self._l1_lock:
            self._l1.pop((key, self.namespace), None)
        conn = get_conn(self.db_path)
        cur = conn.execute(
            """
            DELETE FROM cache_entries WHERE key = ? AND namespace = ?
        """,
            (key, self.namespace),
        )
        conn.commit()
        return cur.rowcount > 0

    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        sentinel = object()
        return self.get(key, sentinel) is not sentinel

    def ttl(self, key: str) -> float | None:
        """Remaining TTL in seconds. None = no expiry. -1 = not found."""
        conn = get_conn(self.db_path)
        row = conn.execute(
            """
            SELECT expires_at FROM cache_entries
            WHERE key = ? AND namespace = ?
        """,
            (key, self.namespace),
        ).fetchone()
        if row is None:
            return -1
        if row["expires_at"] is None:
            return None
        remaining = row["expires_at"] - time.time()
        return max(0.0, remaining)

    def flush(self, namespace: str | None = None) -> int:
        """Clear all keys in namespace (or current namespace)."""
        ns = namespace or self.namespace
        with self._l1_lock:
            keys_to_del = [k for k in self._l1 if k[1] == ns]
            for k in keys_to_del:
                del self._l1[k]
        conn = get_conn(self.db_path)
        cur = conn.execute("DELETE FROM cache_entries WHERE namespace = ?", (ns,))
        conn.commit()
        return cur.rowcount

    def stats(self) -> dict:
        """Return cache statistics."""
        conn = get_conn(self.db_path)
        total = conn.execute(
            "SELECT COUNT(*) FROM cache_entries WHERE namespace = ?", (self.namespace,)
        ).fetchone()[0]
        expired = conn.execute(
            "SELECT COUNT(*) FROM cache_entries WHERE namespace = ? AND expires_at <= ?",
            (self.namespace, time.time()),
        ).fetchone()[0]
        return {
            "namespace": self.namespace,
            "total_keys": total,
            "expired_keys": expired,
            "active_keys": total - expired,
            "l1_size": len(self._l1),
        }

    def namespace_scope(self, namespace: str) -> "Cache":
        """Return a Cache instance scoped to a different namespace."""
        c = Cache.__new__(Cache)
        c.db_path = self.db_path
        c.namespace = namespace
        c.max_size = self.max_size
        c._l1 = self._l1
        c._l1_lock = self._l1_lock
        return c

    # ── Internals ───────────────────────────────────────────────

    def _evict_l1_if_needed(self):
        """Evict least recently used items until the cache is back under max_size."""
        while len(self._l1) > self.max_size:
            self._l1.popitem(last=False)

    def _cleanup_expired(self):
        """Delete expired entries from SQLite."""
        conn = get_conn(self.db_path)
        conn.execute("DELETE FROM cache_entries WHERE expires_at <= ?", (time.time(),))
        conn.commit()

    def _start_cleanup(self, interval: int):
        """Run cleanup in background thread."""

        def loop():
            while True:
                time.sleep(interval)
                with contextlib.suppress(Exception):
                    self._cleanup_expired()

        t = threading.Thread(target=loop, daemon=True)
        t.start()
