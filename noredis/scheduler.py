import time
import threading
from typing import Callable, Optional
from .queue import Queue

class Scheduler:
    """
    Cron-like scheduler. Runs jobs on a fixed interval or at specific times.

    Usage:
        scheduler = Scheduler(queue)

        @task
        def send_newsletter(payload):
            ...

        scheduler.every(seconds=3600, func=send_newsletter)
        scheduler.daily(hour=9, minute=0, func=send_newsletter)
        scheduler.start()
    """

    def __init__(self, queue: Queue):
        self.queue = queue
        self._jobs: list[dict] = []
        self._running = False

    def every(self, func: Callable, seconds: float, payload: dict = None):
        """Run func every N seconds."""
        self._jobs.append({
            "func": func,
            "type": "interval",
            "seconds": seconds,
            "payload": payload or {},
            "last_run": 0,
        })

    def daily(self, func: Callable, hour: int = 0, minute: int = 0, payload: dict = None):
        """Run func every day at HH:MM."""
        self._jobs.append({
            "func": func,
            "type": "daily",
            "hour": hour,
            "minute": minute,
            "payload": payload or {},
            "last_run": 0,
        })

    def start(self):
        """Start scheduler in background thread."""
        self._running = True
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def stop(self):
        self._running = False

    def _loop(self):
        import datetime
        while self._running:
            now = time.time()
            dt = datetime.datetime.now()

            for job in self._jobs:
                if job["type"] == "interval":
                    if now - job["last_run"] >= job["seconds"]:
                        self.queue.enqueue(job["func"], job["payload"])
                        job["last_run"] = now

                elif job["type"] == "daily":
                    if dt.hour == job["hour"] and dt.minute == job["minute"]:
                        minute_key = dt.hour * 60 + dt.minute
                        if job["last_run"] != minute_key:
                            self.queue.enqueue(job["func"], job["payload"])
                            job["last_run"] = minute_key

            time.sleep(0.1)  # tight loop — CPU cost is negligible