import argparse
import json
import sys
import time
from .db import get_conn

def cmd_stats(args):
    conn = get_conn(args.db)
    
    # cache stats
    cache_rows = conn.execute("""
        SELECT namespace, COUNT(*) as total,
               SUM(CASE WHEN expires_at IS NOT NULL AND expires_at <= ? THEN 1 ELSE 0 END) as expired
        FROM cache_entries GROUP BY namespace
    """, (time.time(),)).fetchall()
    
    # job stats
    job_rows = conn.execute("""
        SELECT status, COUNT(*) as count FROM jobs GROUP BY status
    """).fetchall()

    print("\n── Cache ──────────────────────────────")
    if not cache_rows:
        print("  (empty)")
    for r in cache_rows:
        active = r["total"] - r["expired"]
        print(f"  {r['namespace']:<20} {active} active  {r['expired']} expired")

    print("\n── Queue ──────────────────────────────")
    if not job_rows:
        print("  (empty)")
    for r in job_rows:
        print(f"  {r['status']:<12} {r['count']}")
    print()


def cmd_flush(args):
    conn = get_conn(args.db)
    if args.namespace:
        cur = conn.execute(
            "DELETE FROM cache_entries WHERE namespace = ?", (args.namespace,)
        )
        conn.commit()
        print(f"Flushed {cur.rowcount} keys from namespace '{args.namespace}'")
    else:
        cur = conn.execute("DELETE FROM cache_entries")
        conn.commit()
        print(f"Flushed {cur.rowcount} keys from all namespaces")


def cmd_jobs(args):
    conn = get_conn(args.db)
    status_filter = args.status or "pending"
    rows = conn.execute("""
        SELECT id, func_name, status, priority, attempts, max_attempts,
               run_at, updated_at, error
        FROM jobs WHERE status = ?
        ORDER BY priority DESC, run_at ASC
        LIMIT 50
    """, (status_filter,)).fetchall()

    if not rows:
        print(f"No jobs with status '{status_filter}'")
        return

    print(f"\n── Jobs ({status_filter}) ────────────────────────")
    for r in rows:
        run_at = time.strftime("%H:%M:%S", time.localtime(r["run_at"]))
        print(f"  #{r['id']:<6} {r['func_name']:<40} priority={r['priority']}  attempts={r['attempts']}/{r['max_attempts']}  run_at={run_at}")
        if r["error"] and args.verbose:
            print(f"         error: {r['error'][:120]}")
    print()


def cmd_retry(args):
    conn = get_conn(args.db)
    if args.all:
        cur = conn.execute("""
            UPDATE jobs SET status = 'pending', attempts = 0, error = NULL,
                run_at = ?, updated_at = ?
            WHERE status = 'failed'
        """, (time.time(), time.time()))
        conn.commit()
        print(f"Retried {cur.rowcount} failed jobs")
    elif args.id:
        cur = conn.execute("""
            UPDATE jobs SET status = 'pending', attempts = 0, error = NULL,
                run_at = ?, updated_at = ?
            WHERE id = ? AND status = 'failed'
        """, (time.time(), time.time(), args.id))
        conn.commit()
        if cur.rowcount:
            print(f"Job #{args.id} queued for retry")
        else:
            print(f"Job #{args.id} not found or not in failed state")
        conn.commit()
    else:
        print("Specify --id <job_id> or --all")


def cmd_purge(args):
    conn = get_conn(args.db)
    if not args.yes:
        confirm = input("This will delete all completed/failed jobs. Type 'yes' to confirm: ")
        if confirm.strip().lower() != "yes":
            print("Aborted.")
            return
    cur = conn.execute("DELETE FROM jobs WHERE status IN ('done', 'failed')")
    conn.commit()
    print(f"Purged {cur.rowcount} jobs")


def cmd_inspect(args):
    conn = get_conn(args.db)
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (args.id,)).fetchone()
    if not row:
        print(f"Job #{args.id} not found")
        return
    r = dict(row)
    print(f"\n── Job #{args.id} ────────────────────────────")
    print(f"  func      : {r['func_name']}")
    print(f"  status    : {r['status']}")
    print(f"  priority  : {r['priority']}")
    print(f"  attempts  : {r['attempts']} / {r['max_attempts']}")
    print(f"  payload   : {r['payload']}")
    print(f"  run_at    : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['run_at']))}")
    print(f"  created   : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['created_at']))}")
    print(f"  updated   : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['updated_at']))}")
    if r["error"]:
        print(f"\n  error:\n{r['error']}")
    print()


def main():
    parser = argparse.ArgumentParser(
        prog="axiom",
        description="Axiom — SQLite-backed cache & job queue"
    )
    parser.add_argument("--db", default="axiom.db", help="Path to SQLite db (default: axiom.db)")
    sub = parser.add_subparsers(dest="command", required=True)

    # stats
    sub.add_parser("stats", help="Show cache and queue statistics")

    # flush
    p_flush = sub.add_parser("flush", help="Clear cache entries")
    p_flush.add_argument("--namespace", "-n", help="Only flush this namespace")

    # jobs
    p_jobs = sub.add_parser("jobs", help="List jobs")
    p_jobs.add_argument("--status", "-s",
                        choices=["pending", "running", "done", "failed"],
                        default="pending", help="Filter by status (default: pending)")
    p_jobs.add_argument("--verbose", "-v", action="store_true", help="Show error traces")

    # retry
    p_retry = sub.add_parser("retry", help="Retry failed jobs")
    p_retry.add_argument("--id", type=int, help="Retry a specific job by ID")
    p_retry.add_argument("--all", action="store_true", help="Retry all failed jobs")

    # purge
    p_purge = sub.add_parser("purge", help="Delete all done/failed jobs")
    p_purge.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")

    # inspect
    p_inspect = sub.add_parser("inspect", help="Inspect a specific job")
    p_inspect.add_argument("id", type=int, help="Job ID")

    args = parser.parse_args()

    dispatch = {
        "stats":   cmd_stats,
        "flush":   cmd_flush,
        "jobs":    cmd_jobs,
        "retry":   cmd_retry,
        "purge":   cmd_purge,
        "inspect": cmd_inspect,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()