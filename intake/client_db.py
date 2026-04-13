"""
C.A.S.H. Report by GMG — Client Database
Saves audit results to a local SQLite database (cash_clients.db).

Schema
------
clients
  id                 INTEGER PRIMARY KEY AUTOINCREMENT
  client_name        TEXT NOT NULL
  email              TEXT
  phone_number       TEXT
  marketing_consent  INTEGER     -- 1=yes, 0=no
  business_type      TEXT
  website            TEXT
  audit_score        INTEGER
  audit_grade        TEXT
  audit_date         TEXT        -- ISO-8601: YYYY-MM-DD
  report_path        TEXT        -- path to generated .docx
  cash_c             INTEGER
  cash_a             INTEGER
  cash_s             INTEGER
  cash_h             INTEGER
  icp_score          INTEGER
  brand_score        INTEGER
  seo_score          INTEGER
  geo_score          INTEGER
  created_at         TEXT        -- ISO-8601 datetime
"""
import sqlite3
import os
from datetime import datetime, date
from typing import Optional, Dict, Any

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cash_clients.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema():
    """Create the clients table if it doesn't exist, and migrate any missing columns."""
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name        TEXT NOT NULL,
                email              TEXT,
                phone_number       TEXT,
                marketing_consent  INTEGER,
                business_type      TEXT,
                website            TEXT,
                audit_score        INTEGER,
                audit_grade        TEXT,
                audit_date         TEXT,
                report_path        TEXT,
                cash_c             INTEGER,
                cash_a             INTEGER,
                cash_s             INTEGER,
                cash_h             INTEGER,
                icp_score          INTEGER,
                brand_score        INTEGER,
                seo_score          INTEGER,
                geo_score          INTEGER,
                created_at         TEXT
            )
        """)
        # Migrate existing tables that predate these columns
        existing = {row[1] for row in conn.execute("PRAGMA table_info(clients)")}
        for col, defn in [
            ("phone_number",      "TEXT"),
            ("marketing_consent", "INTEGER"),
            ("overall_score",     "REAL"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE clients ADD COLUMN {col} {defn}")
        conn.commit()


def save_intake_record(
    client_name:        str,
    email:              str,
    phone_number:       str = "",
    marketing_consent:  bool = False,
    business_type:      str = "",
    website:            str = "",
) -> int:
    """
    Insert a client record immediately after intake — before the audit runs.
    Returns the new row id.

    Fields not yet known (scores, report path) are left NULL and can be
    updated later via save_audit_result().
    """
    ensure_schema()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO clients
                (client_name, email, phone_number, marketing_consent,
                 business_type, website, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_name,
                email,
                phone_number,
                1 if marketing_consent else 0,
                business_type,
                website,
                now,
            ),
        )
        conn.commit()
        return cur.lastrowid


def save_audit_result(
    client_name:   str,
    email:         str,
    business_type: str,
    website:       str,
    audit_data:    Dict[str, Any],
    ai_insights:   Dict[str, Any],
    report_path:   str = "",
    audit_date:    Optional[str] = None,
) -> int:
    """
    Insert one audit record and return the new row id.

    audit_data  — the full dict from run_audit() (keys: seo, brand, icp, geo, etc.)
    ai_insights — the dict from AIAnalyzer.analyze() (keys: overall_score, cash_c_score, etc.)
    """
    ensure_schema()

    today = audit_date or date.today().isoformat()
    now   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    score = ai_insights.get("overall_score") or 0
    grade = ai_insights.get("overall_grade") or ""

    row = {
        "client_name":   client_name,
        "email":         email,
        "business_type": business_type,
        "website":       website,
        "audit_score":   int(score),
        "audit_grade":   grade,
        "audit_date":    today,
        "report_path":   report_path,
        "cash_c":        int(ai_insights.get("cash_c_score", 0)),
        "cash_a":        int(ai_insights.get("cash_a_score", 0)),
        "cash_s":        int(ai_insights.get("cash_s_score", 0)),
        "cash_h":        int(ai_insights.get("cash_h_score", 0)),
        "icp_score":     int(audit_data.get("icp",   {}).get("score", 0)),
        "brand_score":   int(audit_data.get("brand", {}).get("score", 0)),
        "seo_score":     int(audit_data.get("seo",   {}).get("score", 0)),
        "geo_score":     int(audit_data.get("geo",   {}).get("score", 0)),
        "created_at":    now,
    }

    with _connect() as conn:
        cur = conn.execute("""
            INSERT INTO clients (
                client_name, email, business_type, website,
                audit_score, audit_grade, audit_date, report_path,
                cash_c, cash_a, cash_s, cash_h,
                icp_score, brand_score, seo_score, geo_score,
                created_at
            ) VALUES (
                :client_name, :email, :business_type, :website,
                :audit_score, :audit_grade, :audit_date, :report_path,
                :cash_c, :cash_a, :cash_s, :cash_h,
                :icp_score, :brand_score, :seo_score, :geo_score,
                :created_at
            )
        """, row)
        conn.commit()
        return cur.lastrowid


def list_clients(limit: int = 50) -> list:
    """Return the most recent audit records."""
    ensure_schema()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM clients ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_client_by_id(row_id: int) -> Optional[dict]:
    ensure_schema()
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM clients WHERE id = ?", (row_id,)
        ).fetchone()
    return dict(row) if row else None
