"""
Axiom + FastAPI example
=======================

A complete API server with cached responses and background email jobs.

Run:
    pip install fastapi uvicorn axiom
    uvicorn examples.fastapi_example:app --reload
"""

from fastapi import FastAPI, HTTPException
from axiom import Cache, Queue, task

app = FastAPI(title="Axiom FastAPI Demo")

# ── Initialize axiom (one file does it all) ─────────────────────
cache = Cache("app.db")
queue = Queue("app.db")
queue.start_worker(concurrency=4)

# ── Fake database ───────────────────────────────────────────────
USERS_DB = {
    "1": {"id": "1", "name": "Ayush", "email": "ayush@example.com"},
    "2": {"id": "2", "name": "Alex", "email": "alex@example.com"},
}


# ── Background tasks ───────────────────────────────────────────
@task
def send_welcome_email(payload):
    """Simulates sending a welcome email."""
    print(f"📧 Sending welcome email to {payload['email']}")
    # In production: call your email provider here


@task
def log_analytics(payload):
    """Simulates logging an analytics event."""
    print(f"📊 Analytics: {payload['event']} for user {payload['user_id']}")


# ── Routes ──────────────────────────────────────────────────────
@app.get("/users/{user_id}")
def get_user(user_id: str):
    """Get user by ID — cached for 5 minutes."""
    cached = cache.get(f"user:{user_id}")
    if cached:
        return {"source": "cache", "user": cached}

    user = USERS_DB.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    cache.set(f"user:{user_id}", user, ttl=300)

    # Fire-and-forget analytics event
    queue.enqueue(log_analytics, {"event": "user_viewed", "user_id": user_id})

    return {"source": "database", "user": user}


@app.post("/users/{user_id}/welcome")
def send_welcome(user_id: str):
    """Send a welcome email in the background."""
    user = USERS_DB.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    job_id = queue.enqueue(send_welcome_email, {"email": user["email"]})
    return {"message": "Welcome email queued", "job_id": job_id}


@app.delete("/cache/{key}")
def invalidate_cache(key: str):
    """Manually invalidate a cache key."""
    deleted = cache.delete(key)
    return {"deleted": deleted}


@app.get("/admin/stats")
def admin_stats():
    """Dashboard endpoint — cache and queue stats."""
    return {
        "cache": cache.stats(),
        "queue": queue.stats(),
    }
