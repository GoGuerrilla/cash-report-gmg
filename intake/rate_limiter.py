"""
C.A.S.H. Report — Rate Limiter

Rules (configurable at top of file)
-------------------------------------
  IP address  : 1 audit per 7 days per detected public IP
  Email       : 1 audit per 7 days per email address
  Website URL : 1 audit per 30 days per website (protocol/www-normalized)
  Daily total : max 10 audits per calendar day (system-wide)
  Whitelist   : when enabled, only approved emails may run audits

Storage — all three tables live in cash_clients.db
---------------------------------------------------
  rate_limit_log       — one row logged per completed audit
  audit_whitelist      — approved beta-tester emails
  rate_limit_settings  — key/value config (whitelist_mode on/off)

Bypass
------
  Set env var  BYPASS_RATE_LIMIT=1  (or pass bypass=True) to skip all
  checks. The audit is still logged so totals stay accurate.

Usage
-----
  from intake.rate_limiter import RateLimiter

  rl = RateLimiter()
  allowed, reason = rl.check(email="x@x.com", website_url="https://acme.com")
  if not allowed:
      print(reason)
      sys.exit(0)
  # ... run audit ...
  rl.log(email="x@x.com", website_url="https://acme.com")
"""
import os
import sqlite3
from datetime import datetime, timedelta, date
from typing import Optional, Tuple

# ── Configurable limits ────────────────────────────────────────
IP_COOLDOWN_DAYS      = 7
EMAIL_COOLDOWN_DAYS   = 7
WEBSITE_COOLDOWN_DAYS = 30
DAILY_MAX_AUDITS      = 10

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cash_clients.db")


# ── DB helpers ─────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_rate_limit_schema():
    """Create rate-limit tables if they don't exist. Safe to call repeatedly."""
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_limit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_address  TEXT    DEFAULT '',
                email       TEXT    DEFAULT '',
                website_url TEXT    DEFAULT '',
                ran_at      TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_whitelist (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                email     TEXT UNIQUE NOT NULL COLLATE NOCASE,
                added_by  TEXT DEFAULT '',
                added_at  TEXT NOT NULL,
                notes     TEXT DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_limit_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        # Default: whitelist mode OFF
        conn.execute("""
            INSERT OR IGNORE INTO rate_limit_settings (key, value)
            VALUES ('whitelist_mode', 'off')
        """)
        conn.commit()


# ── Utilities ──────────────────────────────────────────────────

def _normalize_url(url: str) -> str:
    """Strip protocol, www prefix, and trailing slash for consistent matching."""
    u = url.lower().strip()
    for prefix in ("https://www.", "http://www.", "https://", "http://"):
        if u.startswith(prefix):
            u = u[len(prefix):]
            break
    return u.rstrip("/")


def _next_date(ran_at_str: str, days: int) -> str:
    """Return human-readable 'Month DD, YYYY' for the next allowed date."""
    d = datetime.strptime(ran_at_str[:10], "%Y-%m-%d")
    return (d + timedelta(days=days)).strftime("%B %d, %Y")


def get_public_ip() -> Optional[str]:
    """
    Fetch the machine's public IP via api.ipify.org.
    Returns None silently on any network failure — IP check is then skipped.
    """
    try:
        import urllib.request
        with urllib.request.urlopen("https://api.ipify.org", timeout=5) as r:
            return r.read().decode().strip()
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  RateLimiter class
# ══════════════════════════════════════════════════════════════

class RateLimiter:

    def __init__(self, bypass: bool = False):
        """
        bypass=True  — skips all checks (still logs the run).
        Also activated by env var  BYPASS_RATE_LIMIT=1.
        """
        self.bypass = bypass or (os.environ.get("BYPASS_RATE_LIMIT", "").strip() == "1")
        ensure_rate_limit_schema()

    # ── Main check ─────────────────────────────────────────────

    def check(
        self,
        email:       str = "",
        website_url: str = "",
        ip_address:  Optional[str] = None,
    ) -> Tuple[bool, str]:
        """
        Run all active rate limit rules in priority order.

        Returns
        -------
        (True,  "")        — audit is allowed
        (False, message)   — audit blocked; message is user-friendly
        """
        if self.bypass:
            return True, ""

        email       = (email or "").strip().lower()
        norm_url    = _normalize_url(website_url) if website_url else ""
        now_utc     = datetime.utcnow()
        now_str     = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        today_str   = date.today().isoformat()

        with _connect() as conn:

            # ── 1. Whitelist mode ──────────────────────────────
            wl_mode = conn.execute(
                "SELECT value FROM rate_limit_settings WHERE key='whitelist_mode'"
            ).fetchone()
            if wl_mode and wl_mode["value"] == "on":
                if not email:
                    return False, (
                        "Beta access is required to run a C.A.S.H. audit.\n"
                        "Please contact GMG to request access: gmg@goguerrilla.xyz"
                    )
                approved = conn.execute(
                    "SELECT id FROM audit_whitelist WHERE email = ? COLLATE NOCASE",
                    (email,)
                ).fetchone()
                if not approved:
                    return False, (
                        f"Beta access required — {email} is not on the approved list.\n"
                        "Contact GMG to request access: gmg@goguerrilla.xyz"
                    )

            # ── 2. Daily system limit ──────────────────────────
            daily_count = conn.execute(
                "SELECT COUNT(*) FROM rate_limit_log WHERE DATE(ran_at) = ?",
                (today_str,)
            ).fetchone()[0]
            if daily_count >= DAILY_MAX_AUDITS:
                return False, (
                    f"The C.A.S.H. Report system has reached its daily limit "
                    f"({DAILY_MAX_AUDITS} audits/day).\n"
                    "Please try again tomorrow, or contact GMG directly: gmg@goguerrilla.xyz"
                )

            # ── 3. IP address check ────────────────────────────
            if ip_address:
                cutoff = (now_utc - timedelta(days=IP_COOLDOWN_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
                ip_row = conn.execute(
                    "SELECT ran_at FROM rate_limit_log "
                    "WHERE ip_address = ? AND ran_at > ? "
                    "ORDER BY ran_at DESC LIMIT 1",
                    (ip_address, cutoff)
                ).fetchone()
                if ip_row:
                    ran     = ip_row["ran_at"]
                    next_ok = _next_date(ran, IP_COOLDOWN_DAYS)
                    return False, (
                        f"A C.A.S.H. audit was already run from your location on {ran[:10]}.\n"
                        f"Your next audit from this location is available after {next_ok}.\n"
                        "Questions? Contact GMG: gmg@goguerrilla.xyz"
                    )

            # ── 4. Email address check ─────────────────────────
            if email:
                cutoff = (now_utc - timedelta(days=EMAIL_COOLDOWN_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
                em_row = conn.execute(
                    "SELECT ran_at FROM rate_limit_log "
                    "WHERE email = ? COLLATE NOCASE AND ran_at > ? "
                    "ORDER BY ran_at DESC LIMIT 1",
                    (email, cutoff)
                ).fetchone()
                if em_row:
                    ran     = em_row["ran_at"]
                    next_ok = _next_date(ran, EMAIL_COOLDOWN_DAYS)
                    return False, (
                        f"A C.A.S.H. audit for {email} was already completed on {ran[:10]}.\n"
                        f"You can request another audit after {next_ok}.\n"
                        "Need help sooner? Contact GMG: gmg@goguerrilla.xyz"
                    )

            # ── 5. Website URL check ───────────────────────────
            if norm_url:
                cutoff = (now_utc - timedelta(days=WEBSITE_COOLDOWN_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
                url_row = conn.execute(
                    "SELECT ran_at FROM rate_limit_log "
                    "WHERE website_url = ? AND ran_at > ? "
                    "ORDER BY ran_at DESC LIMIT 1",
                    (norm_url, cutoff)
                ).fetchone()
                if url_row:
                    ran     = url_row["ran_at"]
                    next_ok = _next_date(ran, WEBSITE_COOLDOWN_DAYS)
                    return False, (
                        f"A C.A.S.H. audit for {website_url} was already completed on {ran[:10]}.\n"
                        f"Website audits refresh every {WEBSITE_COOLDOWN_DAYS} days.\n"
                        f"Your next audit will be available after {next_ok}.\n"
                        "Questions? Contact GMG: gmg@goguerrilla.xyz"
                    )

        return True, ""

    # ── Log a completed audit ──────────────────────────────────

    def log(
        self,
        email:       str = "",
        website_url: str = "",
        ip_address:  Optional[str] = None,
    ):
        """
        Record a completed audit in rate_limit_log.
        Always called after a successful audit, even when bypass=True,
        so daily totals and historical records stay accurate.
        """
        now     = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        norm_url = _normalize_url(website_url) if website_url else ""
        with _connect() as conn:
            conn.execute(
                "INSERT INTO rate_limit_log (ip_address, email, website_url, ran_at) "
                "VALUES (?, ?, ?, ?)",
                (ip_address or "", (email or "").strip().lower(), norm_url, now)
            )
            conn.commit()

    # ── Whitelist management ───────────────────────────────────

    def set_whitelist_mode(self, enabled: bool):
        """Enable or disable whitelist-only mode."""
        with _connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO rate_limit_settings (key, value) "
                "VALUES ('whitelist_mode', ?)",
                ("on" if enabled else "off",)
            )
            conn.commit()
        state = "ENABLED" if enabled else "DISABLED"
        print(f"  Whitelist mode: {state}")

    def is_whitelist_mode(self) -> bool:
        with _connect() as conn:
            row = conn.execute(
                "SELECT value FROM rate_limit_settings WHERE key='whitelist_mode'"
            ).fetchone()
        return bool(row and row["value"] == "on")

    def add_to_whitelist(self, email: str, added_by: str = "", notes: str = ""):
        """Add an email to the approved beta-tester list."""
        ensure_rate_limit_schema()
        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        with _connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO audit_whitelist (email, added_by, added_at, notes) "
                "VALUES (?, ?, ?, ?)",
                (email.strip().lower(), added_by, now, notes)
            )
            conn.commit()
        print(f"  ✅ Whitelist: added {email}")

    def remove_from_whitelist(self, email: str):
        """Remove an email from the approved list."""
        with _connect() as conn:
            conn.execute(
                "DELETE FROM audit_whitelist WHERE email = ? COLLATE NOCASE",
                (email.strip(),)
            )
            conn.commit()
        print(f"  ✅ Whitelist: removed {email}")

    def list_whitelist(self) -> list:
        ensure_rate_limit_schema()
        with _connect() as conn:
            rows = conn.execute(
                "SELECT email, added_by, added_at, notes "
                "FROM audit_whitelist ORDER BY added_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Status & reporting ─────────────────────────────────────

    def get_daily_count(self) -> int:
        ensure_rate_limit_schema()
        today_str = date.today().isoformat()
        with _connect() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM rate_limit_log WHERE DATE(ran_at) = ?",
                (today_str,)
            ).fetchone()[0]

    def get_status(self) -> dict:
        """Return a snapshot of current rate limit state."""
        ensure_rate_limit_schema()
        wl_list = self.list_whitelist()
        today = date.today().isoformat()
        with _connect() as conn:
            total_logged = conn.execute(
                "SELECT COUNT(*) FROM rate_limit_log"
            ).fetchone()[0]
            recent = conn.execute(
                "SELECT ip_address, email, website_url, ran_at "
                "FROM rate_limit_log ORDER BY ran_at DESC LIMIT 5"
            ).fetchall()
        return {
            "whitelist_mode":         self.is_whitelist_mode(),
            "whitelist_count":        len(wl_list),
            "whitelist_emails":       [w["email"] for w in wl_list],
            "audits_today":           self.get_daily_count(),
            "daily_limit":            DAILY_MAX_AUDITS,
            "total_logged":           total_logged,
            "recent_audits":          [dict(r) for r in recent],
            "ip_cooldown_days":       IP_COOLDOWN_DAYS,
            "email_cooldown_days":    EMAIL_COOLDOWN_DAYS,
            "website_cooldown_days":  WEBSITE_COOLDOWN_DAYS,
        }

    def print_status(self):
        """Print a formatted status report to stdout."""
        s = self.get_status()
        line = "─" * 52
        print(f"\n{line}")
        print("  C.A.S.H. RATE LIMITER STATUS")
        print(line)
        print(f"  Whitelist mode : {'ON  ⚠️  (only approved emails)' if s['whitelist_mode'] else 'OFF (open access)'}")
        print(f"  Whitelist size : {s['whitelist_count']} email(s)")
        if s["whitelist_emails"]:
            for e in s["whitelist_emails"]:
                print(f"    · {e}")
        print(f"  Audits today   : {s['audits_today']} / {s['daily_limit']}")
        print(f"  Total logged   : {s['total_logged']} audits all time")
        print(f"  IP cooldown    : {s['ip_cooldown_days']} days")
        print(f"  Email cooldown : {s['email_cooldown_days']} days")
        print(f"  URL cooldown   : {s['website_cooldown_days']} days")
        if s["recent_audits"]:
            print(f"\n  Recent audits:")
            for r in s["recent_audits"]:
                print(f"    {r['ran_at'][:16]}  {r['email'] or '—':30s}  {r['website_url'] or '—'}")
        print(line + "\n")
