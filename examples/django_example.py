"""
AxiomDB + Django example
======================

Drop-in caching and background jobs for any Django project.

Setup:
    1. pip install django axiomdb
    2. Add this to your Django app's views.py (or a dedicated tasks.py)
    3. Initialize axiomdb in your AppConfig.ready() method

This example shows the patterns — adapt paths and imports to your project structure.
"""

# ── tasks.py ────────────────────────────────────────────────────
# Create this file in your Django app directory.

from axiomdb import Cache, Queue, task

# Single database file, shared across your entire Django project.
cache = Cache("axiomdb.db")
queue = Queue("axiomdb.db")


@task
def send_welcome_email(payload):
    """Background task: send welcome email to new user."""
    from django.core.mail import send_mail

    send_mail(
        subject="Welcome!",
        message=f"Hi {payload['name']}, welcome aboard!",
        from_email="noreply@example.com",
        recipient_list=[payload["email"]],
    )


@task
def generate_report(payload):
    """Background task: generate and cache an expensive report."""
    # Simulate heavy computation
    result = {"total_users": 1234, "revenue": 56789}
    cache.set(f"report:{payload['type']}", result, ttl=3600)


# ── apps.py ─────────────────────────────────────────────────────
# In your Django app's AppConfig, start the worker on server boot.
#
# class MyAppConfig(AppConfig):
#     name = "myapp"
#
#     def ready(self):
#         from myapp.tasks import queue, cache
#         queue.start_worker(concurrency=4)
#
#         # Optional: schedule recurring tasks
#         from axiomdb import Scheduler
#         from myapp.tasks import generate_report
#         scheduler = Scheduler(queue)
#         scheduler.every(generate_report, seconds=3600, payload={"type": "daily"})
#         scheduler.start()


# ── views.py ────────────────────────────────────────────────────
# Example views using axiomdb for caching and job dispatch.

# from django.http import JsonResponse
# from myapp.tasks import cache, queue, send_welcome_email, generate_report
#
#
# def user_profile(request, user_id):
#     """View with cached response."""
#     cached = cache.get(f"user:{user_id}")
#     if cached:
#         return JsonResponse({"source": "cache", "user": cached})
#
#     user = User.objects.get(id=user_id)
#     data = {"id": user.id, "name": user.name, "email": user.email}
#     cache.set(f"user:{user_id}", data, ttl=300)
#     return JsonResponse({"source": "database", "user": data})
#
#
# def register(request):
#     """Registration with background welcome email."""
#     user = User.objects.create(
#         name=request.POST["name"],
#         email=request.POST["email"],
#     )
#     queue.enqueue(send_welcome_email, {
#         "name": user.name,
#         "email": user.email,
#     })
#     return JsonResponse({"message": "Registered", "id": user.id})
#
#
# def dashboard_stats(request):
#     """Return cached report, or trigger generation."""
#     report = cache.get("report:daily")
#     if report:
#         return JsonResponse(report)
#
#     queue.enqueue(generate_report, {"type": "daily"})
#     return JsonResponse({"message": "Report generation started"}, status=202)
