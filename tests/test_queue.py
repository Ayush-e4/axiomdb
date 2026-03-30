import os
import time

import pytest

from axiom import Queue, task
from axiom.queue import register

DB = "test_queue.db"

@pytest.fixture(autouse=True)
def cleanup():
    yield
    for f in [DB, DB + "-wal", DB + "-shm"]:
        if os.path.exists(f):
            if os.path.exists(f): os.remove(f)

@pytest.fixture
def queue():
    q = Queue(DB)
    q.start_worker(concurrency=2, poll_interval=0.1)
    return q


# ── Task registration ────────────────────────────────────────────

def test_task_requires_decorator():
    q = Queue(DB)
    def bare_fn(p): pass
    with pytest.raises(ValueError):
        q.enqueue(bare_fn, {})


# ── Basic enqueue + execute ──────────────────────────────────────

def test_job_executes(queue):
    results = []
    def collect(payload):
        results.append(payload["val"])
    register(collect, "test_collect")

    queue.enqueue(collect, {"val": 42})
    time.sleep(1)
    assert 42 in results

def test_job_status_done(queue):
    def noop(p): pass
    register(noop, "test_noop")

    jid = queue.enqueue(noop, {})
    time.sleep(1)
    assert queue.job_status(jid)["status"] == "done"

def test_job_status_unknown(queue):
    assert queue.job_status(99999) is None


# ── Priority ─────────────────────────────────────────────────────

def test_priority_order(queue):
    order = []
    def record(payload):
        order.append(payload["n"])
    register(record, "test_record")

    queue.enqueue(record, {"n": "low"}, priority=0)
    queue.enqueue(record, {"n": "high"}, priority=10)
    time.sleep(1)
    assert "high" in order
    assert "low" in order


# ── Delay ────────────────────────────────────────────────────────

def test_delayed_job(queue):
    results = []
    def delayed(payload):
        results.append(time.time())
    register(delayed, "test_delayed")

    start = time.time()
    queue.enqueue(delayed, {}, delay=1)
    time.sleep(0.5)
    assert len(results) == 0
    time.sleep(1)
    assert len(results) == 1
    assert results[0] >= start + 1


# ── Retry + backoff ──────────────────────────────────────────────

def test_retry_on_failure(queue):
    attempts = []
    def flaky(payload):
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("not yet")
    register(flaky, "test_flaky")

    queue.enqueue(flaky, {}, max_attempts=3)
    time.sleep(10)  # backoff: 2s + 4s
    assert len(attempts) == 3

def test_dead_letter_after_max_attempts(queue):
    def always_fails(payload):
        raise RuntimeError("always")
    register(always_fails, "test_always_fails")

    jid = queue.enqueue(always_fails, {}, max_attempts=2)
    time.sleep(6)  # backoff: 2^1=2s, then fail
    assert queue.job_status(jid)["status"] == "failed"
    assert len(queue.dead_letters()) >= 1


# ── Manual retry ────────────────────────────────────────────────

def test_manual_retry(queue):
    ran = []
    def retriable(payload):
        ran.append(1)
        if len(ran) == 1:
            raise ValueError("first run fails")
    register(retriable, "test_retriable")

    jid = queue.enqueue(retriable, {}, max_attempts=1)
    time.sleep(3)
    assert queue.job_status(jid)["status"] == "failed"

    queue.retry(jid)
    time.sleep(1)
    assert queue.job_status(jid)["status"] == "done"


# ── Stats ────────────────────────────────────────────────────────

def test_stats(queue):
    def stat_job(p): pass
    register(stat_job, "test_stat_job")

    queue.enqueue(stat_job, {})
    time.sleep(1)
    stats = queue.stats()
    assert "done" in stats