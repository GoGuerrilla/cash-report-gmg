#!/usr/bin/env python3
"""
C.A.S.H. Rate Limit Manager — command-line admin tool

Usage
-----
  python3 manage_rate_limits.py status
  python3 manage_rate_limits.py whitelist on
  python3 manage_rate_limits.py whitelist off
  python3 manage_rate_limits.py whitelist add  email@example.com  [--notes "beta tester"]
  python3 manage_rate_limits.py whitelist remove email@example.com
  python3 manage_rate_limits.py whitelist list
  python3 manage_rate_limits.py log          [--limit N]
  python3 manage_rate_limits.py clear-log    (wipes rate_limit_log — use with caution)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

from intake.rate_limiter import RateLimiter, ensure_rate_limit_schema, _connect


def _usage():
    print(__doc__)
    sys.exit(0)


def cmd_status(_args):
    rl = RateLimiter(bypass=True)
    rl.print_status()


def cmd_whitelist(args):
    if not args:
        _usage()
    rl = RateLimiter(bypass=True)
    sub = args[0].lower()

    if sub == "on":
        rl.set_whitelist_mode(True)
        print("  Whitelist mode ENABLED — only approved emails can run audits.")

    elif sub == "off":
        rl.set_whitelist_mode(False)
        print("  Whitelist mode DISABLED — open access restored.")

    elif sub == "add":
        if len(args) < 2:
            print("  Usage: whitelist add email@example.com [--notes 'note text']")
            sys.exit(1)
        email = args[1]
        notes = ""
        if "--notes" in args:
            ni = args.index("--notes")
            notes = args[ni + 1] if ni + 1 < len(args) else ""
        rl.add_to_whitelist(email, added_by="admin", notes=notes)

    elif sub == "remove":
        if len(args) < 2:
            print("  Usage: whitelist remove email@example.com")
            sys.exit(1)
        rl.remove_from_whitelist(args[1])

    elif sub == "list":
        entries = rl.list_whitelist()
        if not entries:
            print("  Whitelist is empty.")
        else:
            print(f"\n  {'EMAIL':<35} {'ADDED':<12}  NOTES")
            print("  " + "─" * 70)
            for e in entries:
                print(f"  {e['email']:<35} {e['added_at'][:10]:<12}  {e['notes']}")
        print()

    else:
        print(f"  Unknown whitelist subcommand: {sub}")
        _usage()


def cmd_log(args):
    ensure_rate_limit_schema()
    limit = 20
    if "--limit" in args:
        li = args.index("--limit")
        try:
            limit = int(args[li + 1])
        except (IndexError, ValueError):
            pass
    with _connect() as conn:
        rows = conn.execute(
            "SELECT ran_at, email, website_url, ip_address FROM rate_limit_log "
            "ORDER BY ran_at DESC LIMIT ?", (limit,)
        ).fetchall()
    if not rows:
        print("  No audit log entries found.")
        return
    print(f"\n  {'TIMESTAMP':<20} {'EMAIL':<30} {'WEBSITE':<30}  IP")
    print("  " + "─" * 100)
    for r in rows:
        print(f"  {r['ran_at'][:19]:<20} {(r['email'] or '—'):<30} {(r['website_url'] or '—'):<30}  {r['ip_address'] or '—'}")
    print()


def cmd_clear_log(_args):
    confirm = input("  ⚠️  This will delete ALL rate limit log entries. Type YES to confirm: ").strip()
    if confirm != "YES":
        print("  Cancelled.")
        return
    with _connect() as conn:
        conn.execute("DELETE FROM rate_limit_log")
        conn.commit()
    print("  ✅ Rate limit log cleared.")


def main():
    args = sys.argv[1:]
    if not args:
        _usage()

    cmd = args[0].lower()
    rest = args[1:]

    if cmd == "status":
        cmd_status(rest)
    elif cmd == "whitelist":
        cmd_whitelist(rest)
    elif cmd == "log":
        cmd_log(rest)
    elif cmd == "clear-log":
        cmd_clear_log(rest)
    else:
        print(f"  Unknown command: {cmd}")
        _usage()


if __name__ == "__main__":
    main()
