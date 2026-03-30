import os
import time

import pytest

from noredis import Queue, Scheduler, task

DB = "test_scheduler.db"

@pytest.fixture(autouse=True)
def cleanup():
    yield
    for f in [DB, DB + "-wal", DB + "-shm"]:
        if os.path.exists(f):
            os.remove(f)


def test_every_fires_multiple_times():
    fired = []

    @task
    def tick(payload):
        fired.append(time.time())

    queue = Queue(DB)
    queue.start_worker(concurrency=1, poll_interval=0.1)

    scheduler = Scheduler(queue)
    scheduler.every(tick, seconds=0.5)
    scheduler.start()

    time.sleep(2.5)
    # should fire at ~0.5s, ~1s, ~1.5s, ~2s — at least 3 times
    assert len(fired) >= 3

def test_scheduler_stop():
    fired = []

    @task
    def stoppable(payload):
        fired.append(1)

    queue = Queue(DB)
    queue.start_worker(concurrency=1, poll_interval=0.1)

    scheduler = Scheduler(queue)
    scheduler.every(stoppable, seconds=0.3)
    scheduler.start()
    time.sleep(1)
    count_before = len(fired)
    scheduler.stop()
    time.sleep(1)
    # after stop, no new jobs should be enqueued
    assert len(fired) == count_before or len(fired) <= count_before + 1