import os
import time

import pytest

from noredis import Cache

DB = "test_cache.db"

@pytest.fixture(autouse=True)
def cleanup():
    yield
    for f in [DB, DB + "-wal", DB + "-shm"]:
        if os.path.exists(f):
            os.remove(f)

@pytest.fixture
def cache():
    return Cache(DB)


# ── Basic get/set ────────────────────────────────────────────────

def test_set_and_get(cache):
    cache.set("hello", "world")
    assert cache.get("hello") == "world"

def test_get_missing_returns_default(cache):
    assert cache.get("nope") is None
    assert cache.get("nope", "fallback") == "fallback"

def test_overwrite(cache):
    cache.set("k", "v1")
    cache.set("k", "v2")
    assert cache.get("k") == "v2"

def test_complex_value(cache):
    data = {"name": "Ayush", "scores": [1, 2, 3], "nested": {"x": True}}
    cache.set("data", data)
    assert cache.get("data") == data


# ── TTL / expiry ─────────────────────────────────────────────────

def test_ttl_expiry(cache):
    cache.set("temp", "value", ttl=1)
    assert cache.get("temp") == "value"
    time.sleep(1.1)
    assert cache.get("temp") is None

def test_ttl_alias_ex(cache):
    cache.set("temp", "value", ex=1)
    time.sleep(1.1)
    assert cache.get("temp") is None

def test_ttl_returns_remaining(cache):
    cache.set("k", "v", ttl=60)
    remaining = cache.ttl("k")
    assert 58 < remaining <= 60

def test_ttl_no_expiry(cache):
    cache.set("k", "v")
    assert cache.ttl("k") is None

def test_ttl_missing_key(cache):
    assert cache.ttl("ghost") == -1

def test_no_ttl_persists(cache):
    cache.set("permanent", "stays")
    time.sleep(0.1)
    assert cache.get("permanent") == "stays"


# ── exists / delete ──────────────────────────────────────────────

def test_exists(cache):
    cache.set("k", "v")
    assert cache.exists("k") is True
    assert cache.exists("missing") is False

def test_delete(cache):
    cache.set("k", "v")
    assert cache.delete("k") is True
    assert cache.get("k") is None

def test_delete_missing(cache):
    assert cache.delete("ghost") is False

def test_exists_after_expiry(cache):
    cache.set("k", "v", ttl=1)
    time.sleep(1.1)
    assert cache.exists("k") is False


# ── Namespaces ───────────────────────────────────────────────────

def test_namespace_isolation(cache):
    cache.set("k", "default_val")
    ns = cache.namespace_scope("sessions")
    ns.set("k", "session_val")
    assert cache.get("k") == "default_val"
    assert ns.get("k") == "session_val"

def test_flush_namespace(cache):
    cache.set("a", 1)
    cache.set("b", 2)
    ns = cache.namespace_scope("other")
    ns.set("c", 3)
    deleted = cache.flush()
    assert cache.get("a") is None
    assert cache.get("b") is None
    assert ns.get("c") == 3   # other namespace untouched


# ── Stats ────────────────────────────────────────────────────────

def test_stats(cache):
    cache.set("a", 1)
    cache.set("b", 2, ttl=1)
    time.sleep(1.1)
    stats = cache.stats()
    assert stats["total_keys"] >= 1
    assert stats["namespace"] == "default"