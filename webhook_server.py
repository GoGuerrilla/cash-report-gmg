#!/usr/bin/env python3
from __future__ import annotations
"""
C.A.S.H. Report — Typeform Webhook Listener
============================================
Receives Typeform form_response events, maps fields to ClientConfig,
runs the full CASH audit in a background thread, and emails the PDF report
to the client automatically.

Usage
-----
  python3 webhook_server.py

Endpoints
---------
  POST /webhook        — Typeform webhook target
  GET  /health         — health / status check
  GET  /export-emails  — CSV export of opted-in emails (key-protected)

Typeform Setup
--------------
1. In Typeform → Build, set these Field References (Field Settings → Reference)
   on each question so the server can map them reliably:

   Ref                  Question / Field type
   ─────────────────────────────────────────────────────────────────
   business_name        Business / brand name              (Short text)
   website_url          Website URL or Linktree URL        (Website / Short text)
   target_market        Who is your target market / ICP?  (Long text)
   monthly_ad_budget    Monthly advertising budget ($)     (Number)
   email_list_size      Email list / subscriber count      (Number)
   email_frequency      How often do you email your list?  (Short text / Dropdown)
   competitor_urls      Competitor websites (comma-sep)    (Long text)
   biggest_challenge    Biggest marketing challenge        (Long text)
   contact_email        Your email address                 (Email)
   phone                Phone number                       (Phone number)
   marketing_consent    I agree to receive communications  (Yes/No)

2. In Typeform → Connect → Webhooks:
     URL    : http://<your-host>:5000/webhook
     Secret : <value of TYPEFORM_WEBHOOK_SECRET in .env>  (optional but recommended)

3. Add to .env:
     TYPEFORM_WEBHOOK_SECRET=<your typeform webhook secret>
     WEBHOOK_LOG_FILE=webhook_audit.log   (optional — default: stdout only)
     EXPORT_SECRET_KEY=<random secret>    (protects GET /export-emails)
     DATABASE_URL=<postgres dsn>          (Railway Postgres — omit for local SQLite)

Security
--------
  If TYPEFORM_WEBHOOK_SECRET is set, every request is verified via
  HMAC-SHA256 (X-Typeform-Signature header).  Requests that fail
  verification are rejected with HTTP 401.

Rate limiting
-------------
  The RateLimiter from intake/rate_limiter.py is enforced before each
  audit.  Blocked submissions receive a friendly rejection email.
"""

import concurrent.futures
import hashlib
import hmac
import json
import logging
import os
import re
import sys
import threading
import time
import traceback
from datetime import datetime

# ── Bootstrap path & env ──────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

# ── Playwright Chromium bootstrap ─────────────────────────────────
# Railway's build phase doesn't persist /root/.cache into the final
# image, so we install Chromium once at process startup and cache it
# under /app/ms-playwright (inside the app dir, always writable).
def _ensure_chromium():
    import subprocess
    pw_path = "/app/ms-playwright"
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = pw_path
    # Fast-path: check for any executable named *chrome* under pw_path
    found = False
    if os.path.isdir(pw_path):
        for root, _dirs, files in os.walk(pw_path):
            for fname in files:
                if "chrome" in fname and os.access(os.path.join(root, fname), os.X_OK):
                    found = True
                    break
            if found:
                break
    if found:
        print(f"[startup] Playwright Chromium already installed at {pw_path}", flush=True)
        return
    print(f"[startup] Installing Playwright Chromium to {pw_path} …", flush=True)
    env = {**os.environ, "PLAYWRIGHT_BROWSERS_PATH": pw_path}
    result = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        env=env, capture_output=True, text=True, timeout=300,
    )
    if result.returncode == 0:
        print("[startup] Playwright Chromium installed OK", flush=True)
    else:
        print(f"[startup] Playwright Chromium install FAILED:\n{result.stdout}\n{result.stderr}", flush=True)

_ensure_chromium()

# ── Logging ───────────────────────────────────────────────────────
_log_file = os.environ.get("WEBHOOK_LOG_FILE", "")
_handlers = [logging.StreamHandler()]
if _log_file:
    _handlers.append(logging.FileHandler(_log_file))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=_handlers,
)
log = logging.getLogger("webhook")

# ── Flask ─────────────────────────────────────────────────────────
from flask import Flask, request, jsonify, session, redirect

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "cash-admin-change-me-in-prod")

# ── CASH imports ──────────────────────────────────────────────────
from config import ClientConfig
from auditors.linktree_scraper import LinktreeScraper
from auditors import linkedin_scraper as _li_scraper
from auditors.website_auditor import WebsiteAuditor
from auditors.seo_auditor import SEOAuditor
from auditors.geo_auditor import GEOAuditor
from auditors.gbp_auditor import GBPAuditor
from auditors.social_auditor import SocialMediaAuditor
from auditors.content_auditor import ContentAuditor
from auditors.brand_auditor import BrandAuditor
from auditors.funnel_auditor import FunnelAuditor
from auditors.icp_auditor import ICPAuditor
from auditors.freshness_auditor import FreshnessAuditor
from auditors.competitor_auditor import CompetitorAuditor
from auditors.analytics_auditor import AnalyticsAuditor
from auditors.meta_auditor import MetaAuditor
from analyzers.ai_analyzer import AIAnalyzer
from reports.pdf_generator import PDFReportGenerator
from reports.docx_generator import DocxReportGenerator
from reports.email_sender import send_report
from intake.client_db import save_audit_result, get_opted_in_emails, list_clients
from intake.rate_limiter import RateLimiter, get_public_ip
from run_goguerrilla import _build_base_channel_data, _merge_website_data


# ══════════════════════════════════════════════════════════════════
#  TYPEFORM PAYLOAD PARSER
# ══════════════════════════════════════════════════════════════════

# Primary: match by field ref (set these in Typeform → Field Settings → Reference)
# Secondary: match by keywords found in the field title (lowercase)
_REF_MAP = {
    "business_name":    "business_name",
    "website_url":      "website_url",
    "linktree_url":     "website_url",   # treat same field
    "target_market":    "target_market",
    "monthly_ad_budget":"monthly_ad_budget",
    "email_list_size":  "email_list_size",
    "email_frequency":  "email_frequency",
    "competitor_urls":  "competitor_urls",
    "competitors":      "competitor_urls",
    "biggest_challenge":"biggest_challenge",
    "challenge":        "biggest_challenge",
    "contact_email":    "contact_email",
    "email":            "contact_email",
    "phone":            "phone",
    "phone_number":     "phone",
    "marketing_consent":"marketing_consent",
    "consent":          "marketing_consent",
}

# Keyword groups for title-based fallback
_TITLE_KEYWORDS = [
    ("business_name",     ["business name", "brand name", "company name"]),
    ("website_url",       ["website", "linktree", "web address", "url"]),
    ("target_market",     ["target market", "ideal client", "icp", "who do you serve",
                           "target audience", "niche"]),
    ("monthly_ad_budget", ["ad budget", "advertising budget", "monthly budget",
                           "marketing budget"]),
    ("email_list_size",   ["email list", "subscriber", "list size"]),
    ("email_frequency",   ["email frequency", "how often", "how frequently",
                           "email your list", "newsletter frequency"]),
    ("competitor_urls",   ["competitor", "competition"]),
    ("biggest_challenge", ["challenge", "biggest problem", "struggle",
                           "marketing challenge"]),
    ("contact_email",     ["email address", "your email", "contact email"]),
    ("phone",             ["phone", "mobile", "cell"]),
    ("marketing_consent", ["consent", "agree", "marketing communication",
                           "receive communication"]),
]


def _extract_answer_value(answer: dict) -> str:
    """Pull the human-readable value out of a Typeform answer object."""
    atype = answer.get("type", "")
    if atype == "text":
        return str(answer.get("text", ""))
    if atype == "email":
        return str(answer.get("email", ""))
    if atype == "phone_number":
        return str(answer.get("phone_number", ""))
    if atype == "number":
        return str(answer.get("number", ""))
    if atype == "boolean":
        return "yes" if answer.get("boolean") else "no"
    if atype == "choice":
        return answer.get("choice", {}).get("label", "")
    if atype == "choices":
        return ", ".join(answer.get("choices", {}).get("labels", []))
    if atype == "url":
        return str(answer.get("url", ""))
    if atype == "long_text":
        return str(answer.get("text", ""))
    if atype == "short_text":
        return str(answer.get("text", ""))
    if atype == "date":
        return str(answer.get("date", ""))
    # Fallback: try common keys
    for key in ("text", "email", "url", "number", "phone_number"):
        if key in answer:
            return str(answer[key])
    return ""


def _title_to_key(title: str) -> str:
    """Map a Typeform field title to a canonical key via keyword matching."""
    tl = title.lower()
    for key, keywords in _TITLE_KEYWORDS:
        if any(kw in tl for kw in keywords):
            return key
    return ""


def parse_typeform_payload(payload: dict) -> dict:
    """
    Parse a Typeform form_response webhook payload into a flat dict of
    canonical field keys → string values.

    Returns dict with keys from _REF_MAP values.
    """
    form_resp = payload.get("form_response", {})
    answers   = form_resp.get("answers", [])
    fields    = {f["id"]: f for f in form_resp.get("definition", {}).get("fields", [])}

    parsed: dict = {}

    for answer in answers:
        field_info = answer.get("field", {})
        field_id   = field_info.get("id", "")
        ref        = field_info.get("ref", "").strip().lower()
        title      = fields.get(field_id, {}).get("title", ref)
        value      = _extract_answer_value(answer).strip()

        if not value:
            continue

        # Primary: ref-based match
        canonical = _REF_MAP.get(ref, "")
        # Fallback: title keyword match
        if not canonical:
            canonical = _title_to_key(title)
        if not canonical:
            log.debug("  Unmatched field: ref=%r  title=%r  value=%r", ref, title, value)
            continue

        # Don't overwrite email/phone with a duplicate field
        if canonical not in parsed:
            parsed[canonical] = value

    log.info("  Parsed fields: %s", list(parsed.keys()))
    return parsed


# ══════════════════════════════════════════════════════════════════
#  BUILD ClientConfig FROM PARSED FIELDS
# ══════════════════════════════════════════════════════════════════

def _normalise_url(url: str) -> str:
    """Normalise a URL: strip whitespace, enforce https://, strip trailing slash."""
    url = url.strip()
    if not url:
        return url
    if url.lower().startswith("http://"):
        url = "https://" + url[7:]
    elif not url.lower().startswith("https://"):
        url = "https://" + url
    return url.rstrip("/")


def _parse_competitor_urls(raw: str) -> list:
    """Split comma/newline/semicolon-separated URLs into a list (max 3)."""
    parts = re.split(r"[,;\n]+", raw)
    urls  = []
    for p in parts:
        p = _normalise_url(p)
        if not p:
            continue
        raw_domain = re.sub(r"https?://", "", p).split("/")[0]
        if " " in raw_domain or "." not in raw_domain:
            log.warning("Competitor URL rejected (not a valid domain): %r", p)
            continue
        urls.append(p)
    return urls[:3]


def _parse_budget(raw: str) -> float:
    """Extract a float from strings like '$2,500/month' or '2500'."""
    digits = re.sub(r"[^\d.]", "", raw)
    try:
        return float(digits)
    except ValueError:
        return 0.0


def _parse_email_freq(raw: str):
    """Return (email_send_frequency, has_active_newsletter, has_email_marketing)."""
    r = raw.lower().strip()
    if any(x in r for x in ("never", "no", "none", "0", "don't", "dont")):
        return "never", False, False
    if any(x in r for x in ("daily",)):
        return "daily", True, True
    if any(x in r for x in ("biweekly", "bi-weekly", "twice a month",
                              "every 2 week", "2x a month")):
        return "biweekly", True, True
    if any(x in r for x in ("week",)):
        return "weekly", True, True
    if any(x in r for x in ("month",)):
        return "monthly", True, True
    if any(x in r for x in ("quarter",)):
        return "quarterly", True, False
    return raw.strip(), bool(raw.strip()), bool(raw.strip())


def _infer_industry(target_market: str) -> tuple:
    """
    Return (client_industry, industry_category) from target-market text.
    Falls back to 'General' / 'Other'.
    """
    t = target_market.lower()
    if any(k in t for k in ("financial advisor", "cpa", "accountant",
                              "attorney", "law firm", "lawyer", "ria",
                              "wealth manag", "fractional cfo", "fractional cmo")):
        return "Professional Services", "Professional B2B Services"
    if any(k in t for k in ("saas", "software", "tech", "developer",
                              "startup", "app")):
        return "SaaS & Tech", "SaaS & Tech"
    if any(k in t for k in ("agency", "marketing", "consulting", "consultant",
                              "b2b service", "coach", "speaker")):
        return "Agency & Consulting", "Agency & Consulting"
    if any(k in t for k in ("restaurant", "food", "cafe", "bar", "catering")):
        return "Restaurant & Food Service", "Restaurant & Food Service"
    if any(k in t for k in ("retail", "ecommerce", "e-commerce", "shop",
                              "store", "product")):
        return "Retail & E-commerce", "Retail & E-commerce"
    if any(k in t for k in ("real estate", "realtor", "property",
                              "mortgage", "broker")):
        return "Real Estate", "Real Estate"
    if any(k in t for k in ("healthcare", "medical", "dental", "clinic",
                              "doctor", "therapist", "health")):
        return "Healthcare & Medical", "Healthcare & Medical"
    if any(k in t for k in ("nonprofit", "non-profit", "charity",
                              "cause", "foundation")):
        return "Non-profit & Cause", "Non-profit & Cause"
    return "General Business", "Other"


def build_config_from_parsed(parsed: dict) -> ClientConfig:
    """Convert the flat parsed-field dict into a ClientConfig."""

    client_name     = parsed.get("business_name", "Unknown Client")
    contact_email   = parsed.get("contact_email", "")
    phone           = parsed.get("phone", "")
    consent_raw     = parsed.get("marketing_consent", "no").lower()
    marketing_consent = consent_raw in ("yes", "true", "1", "on")

    # URL routing: detect Linktree vs website
    raw_url         = parsed.get("website_url", "")
    norm_url        = _normalise_url(raw_url) if raw_url else ""
    if "linktr.ee" in norm_url.lower():
        website_url  = ""
        linktree_url = norm_url
    else:
        website_url  = norm_url
        linktree_url = ""

    target_market   = parsed.get("target_market", "")
    client_industry, industry_category = _infer_industry(target_market)

    budget          = _parse_budget(parsed.get("monthly_ad_budget", "0"))
    list_size       = int(_parse_budget(parsed.get("email_list_size", "0")))
    email_freq_raw  = parsed.get("email_frequency", "")
    email_freq, has_newsletter, has_email_mktg = _parse_email_freq(email_freq_raw)

    competitor_urls = _parse_competitor_urls(parsed.get("competitor_urls", ""))
    biggest_challenge = parsed.get("biggest_challenge", "")

    return ClientConfig(
        client_name               = client_name,
        contact_email             = contact_email,
        phone_number              = phone,
        marketing_consent         = marketing_consent,
        client_industry           = client_industry,
        industry_category         = industry_category,
        website_url               = website_url,
        linktree_url              = linktree_url,
        stated_target_market      = target_market,
        target_audience           = target_market,
        stated_icp_industry       = client_industry,
        primary_goal              = f"Generate leads from: {target_market[:80]}" if target_market else "Business growth",
        monthly_ad_budget         = budget,
        email_list_size           = list_size,
        email_send_frequency      = email_freq,
        has_active_newsletter     = has_newsletter,
        has_email_marketing       = has_email_mktg,
        competitor_urls           = competitor_urls,
        biggest_marketing_challenge = biggest_challenge,
        intake_completed          = True,
        agency_name               = "C.A.S.H. Report by GMG",
    )


# ══════════════════════════════════════════════════════════════════
#  SAFE AUDITOR WRAPPER
# ══════════════════════════════════════════════════════════════════

def _archive_report(pdf_path: str, client_name: str) -> str | None:
    """
    Copy a generated PDF to the permanent local archive:
      ~/Desktop/CASH GMG Audit/Client Reports/YYYY/MonthName/
    Returns the archive path on success, None on failure.
    """
    # Files are now saved directly to the monthly folder by _run_client_audit().
    if not pdf_path or not os.path.isfile(pdf_path):
        return None
    log.info("Report saved → %s", pdf_path)
    return pdf_path


def _safe_audit(label: str, fn, default: dict) -> dict:
    """
    Run an auditor callable, returning `default` (score=50 neutral) on
    any exception so a single auditor failure never kills the audit thread.
    """
    try:
        return fn()
    except Exception as exc:
        log.warning("Auditor [%s] failed — using neutral default. Error: %s", label, exc)
        return default


_NEUTRAL = {"score": 50, "grade": "C", "issues": [], "strengths": []}


def _safe_audit_timed(label: str, fn, default: dict) -> tuple:
    """Run an auditor, return (result, elapsed_seconds). Falls back to default on error."""
    t0 = time.time()
    try:
        result = fn()
    except Exception as exc:
        log.warning("Auditor [%s] failed — using neutral default. Error: %s", label, exc)
        result = default
    elapsed = round(time.time() - t0, 2)
    log.info("TIMING  %-20s  %.2fs", label, elapsed)
    return result, elapsed


def _send_hold_warning_email(config: ClientConfig, contact_email: str, reason: str):
    """Send admin warning when report is held due to insufficient data."""
    sg_key     = os.environ.get("SENDGRID_API_KEY", "").strip()
    from_addr  = (os.environ.get("SENDGRID_FROM_EMAIL")
                  or os.environ.get("REPORT_EMAIL_FROM", "")).strip()
    admin_addr = os.environ.get("ADMIN_NOTIFY_EMAIL", "gmg@goguerrilla.xyz").strip()
    if not sg_key or not from_addr or not admin_addr:
        log.warning("Hold warning email skipped — missing SendGrid config")
        return
    body = (
        f"⚠️ REPORT HELD — INSUFFICIENT DATA\n"
        f"{'─' * 40}\n"
        f"Client  : {config.client_name}\n"
        f"Website : {config.website_url or '—'}\n"
        f"Email   : {contact_email or '—'}\n\n"
        f"Reason: {reason}\n\n"
        f"Action required: manually verify the client's website and social data, "
        f"then re-trigger the audit from the admin panel."
    )
    try:
        import urllib.request as _ur, json as _json
        payload = _json.dumps({
            "personalizations": [{"to": [{"email": admin_addr}]}],
            "from":    {"email": from_addr},
            "subject": f"⚠️ Report Held — Insufficient Data: {config.client_name}",
            "content": [{"type": "text/plain", "value": body}],
        }).encode()
        req = _ur.Request(
            "https://api.sendgrid.com/v3/mail/send", data=payload,
            headers={"Authorization": f"Bearer {sg_key}",
                     "Content-Type": "application/json"}, method="POST")
        with _ur.urlopen(req, timeout=10) as r:
            log.info("Hold warning email sent → %s (status %s)", admin_addr, r.status)
    except Exception as e:
        log.warning("Hold warning email failed: %s", e)


def _data_confidence_check(
    config: ClientConfig,
    audit_data: dict,
    channel_data: dict,
) -> dict:
    """
    Post-Phase-3 data quality gate. Runs before AI synthesis and PDF generation.

    1. Cross-checks contradictions between data sources and corrects them in-place.
    2. Computes confidence scores: website, social, seo.
    3. Returns hold_report=True if minimum data threshold not met.
    4. Logs confidence summary.
    """
    li_data = channel_data.get("linkedin", {})
    yt_data = channel_data.get("youtube", {})
    fb_data = channel_data.get("facebook", {})
    ig_data = channel_data.get("instagram", {})

    seo_cs  = audit_data.get("seo", {}).get("crawl_signals", {})
    seo_vs  = seo_cs.get("validation_states", {})
    web_hp  = audit_data.get("website", {}).get("homepage", {})
    web_vs  = web_hp.get("validation_states", {}) if web_hp else {}

    corrections = []

    # ── 1a. Title/H1/meta/schema: GEO may still have stale false flags ───────
    _signal_issue_patterns = {
        "title":  ["missing title tag", "title could not be validated",
                   "no title tag found"],
        "meta":   ["missing meta description", "no meta description"],
        "h1":     ["no h1 heading found", "no h1 tag"],
        "schema": ["no structured data found", "no structured data"],
    }
    for sig, patterns in _signal_issue_patterns.items():
        if seo_vs.get(sig) == "found_rendered":
            geo_issues = audit_data.get("geo", {}).get("issues", [])
            cleaned = [i for i in geo_issues
                       if not any(p in i.lower() for p in patterns)]
            if len(cleaned) < len(geo_issues):
                audit_data["geo"]["issues"] = cleaned
                msg = f"GEO: removed false '{sig}' issue (JS renderer confirmed found_rendered)"
                corrections.append(msg)
                log.info("Data confidence correction: %s", msg)

    # ── 1b. YouTube: API confirmed videos but freshness shows Inactive ────────
    yt_recent = yt_data.get("videos_last_30_days", 0) or 0
    yt_ppw    = yt_data.get("posts_per_week")
    yt_days   = yt_data.get("days_since_last_post")
    if yt_recent > 0:
        fresh_channels = audit_data.get("freshness", {}).get("channels", {})
        for yt_key in ("YouTube", "youtube"):
            if yt_key in fresh_channels:
                fresh_yt = fresh_channels[yt_key]
                old_status = fresh_yt.get("status", "")
                if old_status in ("dead", "unknown", "unknown_inactive", "api_blocked"):
                    fresh_yt["status"] = "fresh" if yt_recent >= 4 else "recent"
                    if yt_ppw is not None:
                        fresh_yt["posts_per_week"] = yt_ppw
                    if yt_days is not None:
                        fresh_yt["days_since_last_post"] = yt_days
                    msg = (f"YouTube freshness corrected: {yt_recent} videos/30d → "
                           f"was={old_status!r} now={fresh_yt['status']!r}")
                    corrections.append(msg)
                    log.info("Data confidence correction: %s", msg)

    # ── 1c. LinkedIn >500 followers but section reports no social presence ─────
    li_followers = li_data.get("followers", 0) or 0
    if li_followers > 500:
        absence_patterns = [
            "no social presence", "no linkedin", "not active on linkedin",
            "no social media", "no social channels",
        ]
        for section in ("funnel", "icp", "brand"):
            issues  = audit_data.get(section, {}).get("issues", [])
            cleaned = [i for i in issues
                       if not any(p in i.lower() for p in absence_patterns)]
            if len(cleaned) < len(issues):
                audit_data[section]["issues"] = cleaned
                msg = (f"{section}: removed false 'no social' issue "
                       f"(LinkedIn has {li_followers:,} followers)")
                corrections.append(msg)
                log.info("Data confidence correction: %s", msg)

    # ── 2. Confidence scores ──────────────────────────────────────────────────
    # Website: weighted % of homepage signals verified by JS renderer vs static
    _signals = ("title", "meta", "h1", "schema", "og", "canonical", "viewport")
    found_rendered = sum(1 for s in _signals if web_vs.get(s) == "found_rendered")
    found_static   = sum(1 for s in _signals if web_vs.get(s) == "found")
    website_conf   = round((found_rendered * 1.0 + found_static * 0.7)
                           / len(_signals) * 100)

    # Social: % of active platforms with live API/scrape data (not neutral default)
    live = 0
    total_platforms = max(len(config.active_social_channels), 1)
    if "LinkedIn"  in config.active_social_channels and li_data.get("followers") is not None:
        live += 1
    if "YouTube"   in config.active_social_channels and yt_data.get("data_source") == "youtube_api_v3":
        live += 1
    if "Facebook"  in config.active_social_channels and fb_data.get("data_source") == "meta_graph_api":
        live += 1
    if "Instagram" in config.active_social_channels and ig_data.get("data_source") == "meta_graph_api":
        live += 1
    social_conf = round(live / total_platforms * 100)

    # SEO: PageSpeed API connected + Playwright render quality
    seo_method = audit_data.get("seo", {}).get("method", "")
    if seo_method == "pagespeed+crawl":
        seo_conf = min(100, 80 + found_rendered * 4)
    elif seo_cs.get("title"):
        seo_conf = 60
    else:
        seo_conf = 20

    log.info("Data confidence: website=%d%%  social=%d%%  seo=%d%%",
             website_conf, social_conf, seo_conf)
    if corrections:
        log.info("Data confidence corrections applied (%d): %s",
                 len(corrections), " | ".join(corrections))

    # ── 3. Minimum data threshold ─────────────────────────────────────────────
    web_failed  = audit_data.get("website", {}).get("pages_crawled", 0) == 0
    li_missing  = li_data.get("followers") is None
    yt_missing  = yt_data.get("data_source") != "youtube_api_v3"
    hold_report = web_failed and li_missing and yt_missing
    hold_reason = None
    if hold_report:
        hold_reason = (
            "Website scrape returned 0 pages, LinkedIn followers unavailable, "
            "and YouTube API unavailable. Cannot generate a reliable report."
        )
        log.error(
            "HOLD REPORT — %s (%s): %s",
            config.client_name, config.website_url, hold_reason
        )

    # ── 4 & 5. Confidence metadata + low-confidence notes ────────────────────
    notes = {}
    if website_conf < 50:
        notes["website"] = (
            "Note: Some data points could not be fully verified for this section"
        )
    if social_conf < 50:
        notes["social"] = (
            "Note: Some data points could not be fully verified for this section"
        )
    if seo_conf < 50:
        notes["seo"] = (
            "Note: Some data points could not be fully verified for this section"
        )

    return {
        "website":     website_conf,
        "social":      social_conf,
        "seo":         seo_conf,
        "corrections": corrections,
        "hold_report": hold_report,
        "hold_reason": hold_reason,
        "notes":       notes,
    }



# ══════════════════════════════════════════════════════════════════
#  CORE AUDIT RUNNER  (generalized — works for any client)
# ══════════════════════════════════════════════════════════════════

def _run_client_audit(config: ClientConfig, rl: RateLimiter,
                      contact_email: str, website_url: str,
                      ip_address: str | None):
    """
    Full CASH audit for a client config built from Typeform intake.
    Runs in a background thread — never called synchronously from Flask.

    Pipeline (with timing):
      Phase 1 : Social data collection — Linktree, LinkedIn, YouTube, Meta (sequential)
      Phase 2 : Website + SEO + GBP + Analytics in PARALLEL
      Phase 3 : GEO (needs SEO output), then Social/Brand/Funnel/ICP/Freshness/
                Content/Competitor all in PARALLEL
      Phase 4 : AI synthesis → PDF → DOCX → Report email (single email, PDF attached)
    """
    audit_wall_start = time.time()
    name = config.client_name
    log.info("=== AUDIT START: %s  [%s] ===", name, datetime.utcnow().isoformat())

    # ── Channel data skeleton ─────────────────────────────────────
    channel_data = _build_base_channel_data(config.website_url or config.linktree_url or "")
    config.preloaded_channel_data = channel_data

    audit_data: dict = {}

    # ══ Phase 1: Social data collection (sequential — each step ══
    #             feeds into channel_data used by later auditors)   ══
    phase1_start = time.time()

    # ── 1a. Linktree / website social scrape ─────────────────────
    linktree_data = {}
    if config.linktree_url:
        log.info("Scraping Linktree: %s", config.linktree_url)
        _t = time.time()
        linktree_data = LinktreeScraper(config.linktree_url).scrape()
        log.info("TIMING  linktree_scrape         %.2fs", time.time() - _t)
        log.info("Linktree scrape_status=%s  platforms=%s",
                 linktree_data.get("scrape_status"),
                 linktree_data.get("platforms_found"))
        if linktree_data.get("data_verified") and \
           linktree_data.get("classified_links"):
            from intake.questionnaire import _classified_to_platforms
            plat_data = _classified_to_platforms(
                linktree_data["classified_links"])
            if plat_data.get("linkedin_url"):
                config.linkedin_url = plat_data["linkedin_url"]
            if plat_data.get("instagram_handle"):
                config.instagram_handle = plat_data["instagram_handle"]
            if plat_data.get("youtube_channel_url"):
                config.youtube_channel_url = plat_data["youtube_channel_url"]
            if plat_data.get("facebook_page_url"):
                config.facebook_page_url = plat_data["facebook_page_url"]
            if plat_data.get("tiktok_handle"):
                config.tiktok_handle = plat_data["tiktok_handle"]
            if plat_data.get("discord_url"):
                config.discord_url = plat_data["discord_url"]
            if not config.website_url:
                config.website_url = plat_data.get(
                    "_website_from_linktree", "") or config.website_url
    elif config.website_url:
        log.info("Scraping website socials: %s", config.website_url)
        _t = time.time()
        from intake.questionnaire import _scrape_website_socials, _classified_to_platforms
        classified = _scrape_website_socials(config.website_url)
        log.info("TIMING  website_socials_scrape  %.2fs", time.time() - _t)
        if classified:
            platforms   = list(classified.keys())
            plat_data   = _classified_to_platforms(classified)
            if plat_data.get("linkedin_url"):
                config.linkedin_url = plat_data["linkedin_url"]
            if plat_data.get("instagram_handle"):
                config.instagram_handle = plat_data["instagram_handle"]
            if plat_data.get("youtube_channel_url"):
                config.youtube_channel_url = plat_data["youtube_channel_url"]
            if plat_data.get("facebook_page_url"):
                config.facebook_page_url = plat_data["facebook_page_url"]
            if plat_data.get("tiktok_handle"):
                config.tiktok_handle = plat_data["tiktok_handle"]
            if plat_data.get("discord_url"):
                config.discord_url = plat_data["discord_url"]
            linktree_data = {
                "source_url":      config.website_url,
                "profile_name":    name,
                "bio":             "",
                "classified_links": classified,
                "platforms_found": platforms,
                "data_verified":   True,
                "scrape_status":   "ok_website_fallback",
            }
            log.info("Website socials found: %s", platforms)

    if not linktree_data:
        linktree_data = {
            "source_url":      "",
            "profile_name":    name,
            "bio":             "",
            "classified_links": {},
            "platforms_found": [],
            "data_verified":   False,
            "scrape_status":   "no_url",
        }
    audit_data["linktree"] = linktree_data

    # ── 1b. LinkedIn scrape ───────────────────────────────────────
    if config.linkedin_url:
        log.info("Scraping LinkedIn: %s", config.linkedin_url)
        _t = time.time()
        li_data = _li_scraper.scrape(config.linkedin_url)
        log.info("TIMING  linkedin_scrape         %.2fs", time.time() - _t)
        src = li_data.get("data_source", "unknown")
        if src == "linkedin_html":
            for key in ("followers", "posts_per_week", "days_since_last_post",
                        "content_topics", "post_themes", "services_listed"):
                if li_data.get(key) is not None:
                    channel_data["linkedin"][key] = li_data[key]
            channel_data["linkedin"]["is_active"] = True
            log.info("LinkedIn: followers=%s ppw=%s", li_data.get("followers"), li_data.get("posts_per_week"))
        else:
            log.info("LinkedIn scrape returned source=%s", src)

    # ── 1c. YouTube ───────────────────────────────────────────────
    if config.youtube_channel_url:
        yt_key = os.environ.get("YOUTUBE_API_KEY", "")
        if yt_key:
            log.info("Fetching YouTube data...")
            log.info("YouTube channel URL → %s  (key_len=%s)", config.youtube_channel_url, len(yt_key))
            _t = time.time()
            from auditors.youtube_api import YouTubeAuditor
            yt = YouTubeAuditor(config.youtube_channel_url, yt_key).fetch()
            log.info("TIMING  youtube_api             %.2fs", time.time() - _t)
            if yt.get("data_source") == "youtube_api_v3":
                for key in ("posts_per_week", "days_since_last_post", "is_active",
                            "subscriber_count", "total_video_count", "videos_last_30_days",
                            "avg_views_per_video", "total_view_count", "data_source"):
                    if yt.get(key) is not None:
                        channel_data["youtube"][key] = yt[key]
                # Alias for FunnelAuditor compatibility
                channel_data["youtube"]["recent_video_count"] = yt.get("videos_last_30_days", 0) or 0
                log.info("YouTube: subscribers=%s  total_videos=%s  videos_last_30=%s  ppw=%s",
                         yt.get("subscriber_count"), yt.get("total_video_count"),
                         yt.get("videos_last_30_days"), yt.get("posts_per_week"))
            else:
                log.warning("YouTube API failed: raw_url=%s  error=%r  source=%s",
                            config.youtube_channel_url, yt.get("error"), yt.get("data_source"))
        else:
            log.warning("YouTube: YOUTUBE_API_KEY not set — channel scored at 50 neutral")

    # ── 1d. Meta Graph API ────────────────────────────────────────
    meta_app_id = os.environ.get("META_APP_ID", "")
    meta_secret = os.environ.get("META_APP_SECRET", "")
    meta_pg_tok = os.environ.get("META_PAGE_ACCESS_TOKEN", "").strip()
    if meta_app_id and meta_secret:
        log.info("Fetching Meta API data...")
        _t = time.time()
        fb_page = ""
        if config.facebook_page_url:
            m = re.search(r"facebook\.com/([^/?#]+)", config.facebook_page_url, re.I)
            if m:
                fb_page = m.group(1)
        meta_result = MetaAuditor(
            app_id=meta_app_id, app_secret=meta_secret,
            facebook_page_id=fb_page,
            instagram_handle=config.instagram_handle,
            page_access_token=meta_pg_tok,
        ).fetch()
        log.info("TIMING  meta_api                %.2fs", time.time() - _t)
        log.info("Meta: facebook_page_id=%r  instagram=%r  page_token=%s",
                 fb_page, config.instagram_handle, "set" if meta_pg_tok else "NOT SET")
        fb = meta_result.get("facebook", {})
        ig = meta_result.get("instagram", {})
        if fb.get("data_source") == "meta_graph_api":
            for key in ("followers", "posts_per_week", "days_since_last_post",
                        "engagement_rate", "reach_28d", "engagements_28d"):
                if fb.get(key) is not None:
                    channel_data["facebook"][key] = fb[key]
            channel_data["facebook"]["is_active"] = True
            log.info("Facebook: followers=%s  ppw=%s  engagement_rate=%s",
                     fb.get("followers"), fb.get("posts_per_week"), fb.get("engagement_rate"))
        else:
            log.warning("Facebook API failed: source=%s  error=%r  page_id=%r",
                        fb.get("data_source"), fb.get("error"), fb_page)
        if ig.get("data_source") == "meta_graph_api":
            for key in ("followers", "posts_per_week", "days_since_last_post",
                        "engagement_rate", "media_count"):
                if ig.get(key) is not None:
                    channel_data["instagram"][key] = ig[key]
            channel_data["instagram"]["is_active"] = True
            log.info("Instagram: followers=%s  ppw=%s",
                     ig.get("followers"), ig.get("posts_per_week"))
        else:
            log.warning("Instagram API failed: source=%s  error=%r  handle=%r",
                        ig.get("data_source"), ig.get("error"), config.instagram_handle)
        audit_data["meta"] = meta_result

    log.info("TIMING  PHASE1_social_collection  %.2fs", time.time() - phase1_start)

    # ══ Phase 2: Website + SEO + GBP + Analytics in PARALLEL ══════
    #   These all need only the URL / config — no cross-dependencies.
    #   SEO is the bottleneck (PageSpeed mobile+desktop, now parallel).
    phase2_start = time.time()

    target_url = config.website_url
    if not target_url and linktree_data.get("website_url"):
        target_url = linktree_data["website_url"]
    if not target_url:
        target_url = ""

    pagespeed_key = os.environ.get("PAGESPEED_API_KEY", "")
    places_key    = os.environ.get("GOOGLE_PLACES_API_KEY", pagespeed_key)
    ga_prop       = os.environ.get("GOOGLE_ANALYTICS_PROPERTY_ID", "")
    ga_sa         = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "")
    if ga_sa and not os.path.isfile(ga_sa):
        log.warning("GA service account file not found at %r — skipping GA (score=50)", ga_sa)
        ga_sa = ""

    _ga_neutral = {
        "score": 50, "grade": "C", "data_source": "not_available",
        "note": "Google Analytics unavailable — score set to neutral.",
        "monthly_visitors": None, "traffic_trend_pct": None,
        "traffic_trend_label": "—", "bounce_rate_pct": None,
        "avg_session_duration": "—", "top_traffic_sources": [],
        "top_landing_pages": [], "issues": [], "strengths": [],
    }

    if target_url:
        log.info("Phase 2: launching Website + SEO + GBP + Analytics in parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            fut_web = ex.submit(_safe_audit_timed, "website",
                                lambda: WebsiteAuditor(target_url, max_pages=5).run(), _NEUTRAL)
            fut_seo = ex.submit(_safe_audit_timed, "seo",
                                lambda: SEOAuditor(target_url, api_key=pagespeed_key).run(), _NEUTRAL)
            fut_gbp = ex.submit(_safe_audit_timed, "gbp",
                                lambda: GBPAuditor(business_name=name, website_url=target_url,
                                                   api_key=places_key).run(), _NEUTRAL)
            fut_ga  = ex.submit(_safe_audit_timed, "analytics",
                                lambda: AnalyticsAuditor(property_id=ga_prop,
                                                         service_account_json_path=ga_sa).run(),
                                _ga_neutral)
            audit_data["website"],    _ = fut_web.result()
            audit_data["seo"],        _ = fut_seo.result()
            audit_data["gbp"],        _ = fut_gbp.result()
            audit_data["analytics"],  _ = fut_ga.result()

        # ── JS render override: SEO Playwright results → Website static results ─
        # When SEO's Playwright renderer confirms a signal as "found_rendered",
        # suppress the corresponding false-positive issues from the static scrape
        # and update validation_states so GEO + report generators see correct values.
        seo_cs   = audit_data["seo"].get("crawl_signals", {})
        seo_vs   = seo_cs.get("validation_states", {})
        upgraded = [k for k, v in seo_vs.items() if v == "found_rendered"]

        if upgraded:
            web_hp = audit_data["website"].get("homepage", {})
            web_vs = web_hp.get("validation_states", {})

            # Issue strings to remove when JS render confirms the signal is present
            _suppress = {
                "title":     ["missing title tag", "title could not be validated"],
                "meta":      ["Missing meta description", "Meta description could not be validated"],
                "h1":        ["No H1 heading found", "H1 heading could not be validated"],
                "schema":    ["No structured data found", "Structured data could not be validated"],
                "og":        [],
                "canonical": [],
            }
            for sig in upgraded:
                if sig in _suppress:
                    web_vs[sig] = "found_rendered"
                    patterns = _suppress[sig]
                    if patterns:
                        audit_data["website"]["issues"] = [
                            i for i in audit_data["website"].get("issues", [])
                            if not any(p in i for p in patterns)
                        ]
                    log.info("JS render override: signal=%r → found_rendered, false flag suppressed", sig)

            # Copy rendered field values from SEO crawl_signals into website homepage
            _field_map = {
                "title":     "title",
                "meta":      "meta_description",
                "h1":        "h1s",
                "schema":    "schema_types",
                "og":        "has_og_tags",
                "canonical": "canonical_url",
            }
            for sig, field in _field_map.items():
                if sig in upgraded and field in seo_cs:
                    web_hp[field] = seo_cs[field]

            # Viewport: not tracked by SEO render — suppress false flag on JS-platform sites
            # when renderer successfully found other signals (proving JS execution worked)
            platform = (audit_data["website"].get("platform") or
                        audit_data["seo"].get("platform") or "").lower()
            if web_vs.get("viewport") == "missing" and platform in ("wix", "squarespace", "webflow", "spa"):
                web_vs["viewport"] = "found_rendered"
                audit_data["website"]["issues"] = [
                    i for i in audit_data["website"].get("issues", [])
                    if "viewport" not in i.lower()
                ]
                log.info("JS render override: viewport suppressed for platform=%r", platform)

            log.info("JS render override complete: upgraded=%s  platform=%r", upgraded, platform)

        _merge_website_data(channel_data, audit_data["website"])

        # GEO depends on SEO output — runs after Phase 2 completes
        _t = time.time()
        audit_data["geo"], _ = _safe_audit_timed(
            "geo", lambda: GEOAuditor(config, audit_data.get("seo", {})).run(), _NEUTRAL)
        log.info("TIMING  geo_after_seo           %.2fs", time.time() - _t)
    else:
        log.warning("No website URL — skipping website/SEO/GEO/GBP/analytics auditors")
        for key in ("website", "seo", "geo", "gbp", "analytics"):
            audit_data[key] = {"note": "No website URL provided", "score": 50}

    log.info("TIMING  PHASE2_web_seo_parallel   %.2fs", time.time() - phase2_start)

    # ══ Phase 3: Independent auditors in PARALLEL ═════════════════
    #   All depend only on config + linktree_data (already complete).
    #   Competitor also needs audit_data but only reads website/seo.
    phase3_start = time.time()
    log.info("Phase 3: launching Social + Brand + Funnel + ICP + Freshness + "
             "Content + Competitor in parallel...")

    _comp_default = {"skipped": True, "note": "Competitor audit failed.",
                     "competitors": [], "comparison": {}}

    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as ex:
        fut_social   = ex.submit(_safe_audit_timed, "social",
                                 lambda: SocialMediaAuditor(config).run(), _NEUTRAL)
        fut_brand    = ex.submit(_safe_audit_timed, "brand",
                                 lambda: BrandAuditor(config, linktree_data).run(), _NEUTRAL)
        fut_funnel   = ex.submit(_safe_audit_timed, "funnel",
                                 lambda: FunnelAuditor(config, linktree_data).run(), _NEUTRAL)
        fut_icp      = ex.submit(_safe_audit_timed, "icp",
                                 lambda: ICPAuditor(config, linktree_data).run(), _NEUTRAL)
        fut_fresh    = ex.submit(_safe_audit_timed, "freshness",
                                 lambda: FreshnessAuditor(config, linktree_data).run(), _NEUTRAL)
        fut_content  = ex.submit(_safe_audit_timed, "content",
                                 lambda: ContentAuditor(config, audit_data).run(), _NEUTRAL)
        if config.competitor_urls:
            fut_comp = ex.submit(_safe_audit_timed, "competitor",
                                 lambda: CompetitorAuditor(config, audit_data,
                                                           pagespeed_api_key=pagespeed_key).run(),
                                 _comp_default)
        else:
            fut_comp = None

        audit_data["social"],    _ = fut_social.result()
        audit_data["brand"],     _ = fut_brand.result()
        audit_data["funnel"],    _ = fut_funnel.result()
        audit_data["icp"],       _ = fut_icp.result()
        audit_data["freshness"], _ = fut_fresh.result()
        audit_data["content"],   _ = fut_content.result()
        if fut_comp:
            audit_data["competitor"], _ = fut_comp.result()
        else:
            audit_data["competitor"] = {
                "skipped": True, "note": "No competitor URLs provided.",
                "competitors": [], "comparison": {},
            }

    log.info("TIMING  PHASE3_parallel_auditors  %.2fs", time.time() - phase3_start)

    # ══ Data confidence check (post-Phase-3, pre-AI) ══════════════
    audit_data["confidence"] = _data_confidence_check(config, audit_data, channel_data)
    if audit_data["confidence"].get("hold_report"):
        _send_hold_warning_email(
            config, contact_email,
            audit_data["confidence"]["hold_reason"],
        )
        return

    # ══ Phase 4: AI synthesis → PDF → DOCX → Email ═══════════════
    phase4_start = time.time()

    log.info("Running AI synthesis...")
    _t = time.time()
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    audit_data["ai_insights"] = AIAnalyzer(anthropic_api_key=anthropic_key).analyze(
        config, audit_data
    )
    log.info("TIMING  ai_synthesis            %.2fs", time.time() - _t)

    ai            = audit_data.get("ai_insights", {})
    overall_score = ai.get("overall_score")
    overall_grade = ai.get("overall_grade", "")
    log.info("CASH score: C=%s A=%s S=%s H=%s  Overall=%s (%s)",
             ai.get("cash_c_score","—"), ai.get("cash_a_score","—"),
             ai.get("cash_s_score","—"), ai.get("cash_h_score","—"),
             overall_score, overall_grade)

    # ── Determine output format ───────────────────────────────────
    beta_docx_only = os.environ.get("BETA_DOCX_ONLY", "").strip().lower() == "true"
    if beta_docx_only:
        log.info("BETA_DOCX_ONLY=true — skipping PDF generation, will email DOCX")

    _reports_base = os.environ.get("REPORTS_DIR", "reports").rstrip("/")
    _today        = datetime.utcnow()
    _month_folder = f"Client Reports {_today.strftime('%Y %B')}"
    _report_dir   = os.path.join(_reports_base, _month_folder)
    os.makedirs(_report_dir, exist_ok=True)

    slug      = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    _basename = f"{slug}_{_today.strftime('%Y-%m-%d_%H%M%S')}"

    # ── Generate PDF (skipped when BETA_DOCX_ONLY=true) ──────────
    pdf_path = None
    if not beta_docx_only:
        pdf_path = os.path.abspath(os.path.join(_report_dir, f"{_basename}.pdf"))
        _t = time.time()
        try:
            PDFReportGenerator(config, audit_data).generate(pdf_path)
            if os.path.isfile(pdf_path):
                log.info("TIMING  pdf_generation          %.2fs  (%d bytes)",
                         time.time() - _t, os.path.getsize(pdf_path))
            else:
                log.error("PDF generation ran but file not found at: %s", pdf_path)
                pdf_path = None
        except Exception as pdf_err:
            log.error("PDF generation FAILED (%.2fs): %s\n%s",
                      time.time() - _t, pdf_err, traceback.format_exc())
            pdf_path = None

    # ── Generate DOCX ─────────────────────────────────────────────
    # Primary output in BETA_DOCX_ONLY mode; backup otherwise.
    docx_path = os.path.abspath(os.path.join(_report_dir, f"{_basename}.docx"))
    _t = time.time()
    try:
        DocxReportGenerator(config, audit_data).generate(docx_path)
        if os.path.isfile(docx_path):
            log.info("TIMING  docx_generation         %.2fs  (%d bytes)",
                     time.time() - _t, os.path.getsize(docx_path))
        else:
            log.warning("DOCX generation ran but file not found at: %s", docx_path)
            docx_path = None
    except Exception as e:
        log.warning("DOCX generation failed (%.2fs): %s", time.time() - _t, e)
        docx_path = None

    # ── Resolve which file gets emailed ───────────────────────────
    report_attachment  = docx_path if beta_docx_only else pdf_path
    attachment_label   = "DOCX"   if beta_docx_only else "PDF"

    # ── Email report ──────────────────────────────────────────────
    email_trigger_ts = datetime.utcnow().isoformat()
    log.info("=== EMAIL DELIVERY TRIGGERED at %s ===", email_trigger_ts)
    log.info("SENDGRID_API_KEY   : %s",
             f"set ({len(os.environ.get('SENDGRID_API_KEY',''))} chars)"
             if os.environ.get("SENDGRID_API_KEY") else "NOT SET")
    log.info("Report attachment  : %s  format=%s  exists=%s",
             report_attachment, attachment_label,
             os.path.isfile(report_attachment) if report_attachment else False)

    # 1. Send to the client
    client_email_ok = False
    if contact_email:
        _t = time.time()
        log.info("TIMING  sendgrid_trigger_start  → %s", contact_email)
        client_email_ok = send_report(
            report_path      = report_attachment,
            client_name      = name,
            overall_score    = overall_score,
            overall_grade    = overall_grade,
            to_addr          = contact_email,
            attachment_label = attachment_label,
        )
        log.info("TIMING  sendgrid_client_send    %.2fs  result=%s",
                 time.time() - _t, "SUCCESS" if client_email_ok else "FAILED")
        if not client_email_ok:
            log.error(
                "CLIENT EMAIL FAILED — client %r (%s) did NOT receive their report. "
                "%s at: %s. Fix SENDGRID_API_KEY or REPORT_EMAIL_PASSWORD and resend manually.",
                name, contact_email, attachment_label, report_attachment,
            )

    # 2. Always send a copy to the GMG team inbox
    gmg_inbox = os.environ.get("REPORT_EMAIL_TO", "")
    if gmg_inbox and gmg_inbox != contact_email:
        _t = time.time()
        ok2 = send_report(
            report_path      = report_attachment,
            client_name      = name,
            overall_score    = overall_score,
            overall_grade    = overall_grade,
            to_addr          = gmg_inbox,
            attachment_label = attachment_label,
        )
        log.info("TIMING  sendgrid_gmg_send       %.2fs  result=%s",
                 time.time() - _t, "SUCCESS" if ok2 else "FAILED")
        if not ok2:
            log.error(
                "GMG TEAM EMAIL FAILED — team copy for client %r not delivered to %s.",
                name, gmg_inbox,
            )

    log.info("TIMING  PHASE4_ai_pdf_email       %.2fs", time.time() - phase4_start)

    # ── Save to DB ────────────────────────────────────────────────
    _t = time.time()
    try:
        row_id = save_audit_result(
            client_name   = name,
            email         = contact_email,
            business_type = config.client_industry,
            website       = config.website_url,
            audit_data    = audit_data,
            ai_insights   = audit_data.get("ai_insights", {}),
            report_path   = report_attachment,
        )
        log.info("TIMING  db_save                 %.2fs  row=#%s", time.time() - _t, row_id)
    except Exception as e:
        log.warning("DB save failed (%.2fs): %s", time.time() - _t, e)

    # ── Log rate limit ────────────────────────────────────────────
    try:
        rl.log(email=contact_email, website_url=website_url, ip_address=ip_address)
    except Exception:
        pass

    # ── Admin notification email ──────────────────────────────────
    try:
        admin_notify = os.environ.get("ADMIN_NOTIFY_EMAIL", "gmg@goguerrilla.xyz").strip()
        from_addr    = (os.environ.get("SENDGRID_FROM_EMAIL")
                        or os.environ.get("REPORT_EMAIL_FROM", "")).strip()
        sg_key       = os.environ.get("SENDGRID_API_KEY", "").strip()
        if admin_notify and sg_key and from_addr:
            _elapsed  = round(time.time() - audit_wall_start, 1)
            pdf_info  = (f"{pdf_path} ({os.path.getsize(pdf_path):,} bytes)"
                         if pdf_path and os.path.isfile(pdf_path) else "not generated")
            docx_info = (f"{docx_path} ({os.path.getsize(docx_path):,} bytes)"
                         if docx_path and os.path.isfile(docx_path) else "not generated")
            body = (
                f"C.A.S.H. Audit Complete\n"
                f"{'─' * 40}\n"
                f"Business : {name}\n"
                f"Website  : {config.website_url or config.linktree_url or '—'}\n"
                f"Client   : {contact_email or '—'}\n"
                f"Source   : {getattr(config, 'audit_source', '—')}\n"
                f"\n"
                f"Score    : {overall_score}/100  Grade: {overall_grade}\n"
                f"Time     : {_elapsed}s\n"
                f"\n"
                f"PDF      : {pdf_info}\n"
                f"DOCX     : {docx_info}\n"
            )
            requests.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={
                    "Authorization": f"Bearer {sg_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "personalizations": [{"to": [{"email": admin_notify}]}],
                    "from":    {"email": from_addr},
                    "subject": f"New C.A.S.H. Report Generated — {name}",
                    "content": [{"type": "text/plain", "value": body}],
                },
                timeout=10,
            )
            log.info("Admin notification sent → %s", admin_notify)
    except Exception as e:
        log.warning("Admin notification failed (non-critical): %s", e)

    total_elapsed = round(time.time() - audit_wall_start, 1)
    log.info("=== AUDIT COMPLETE: %s  wall_time=%.1fs  [%s] ===",
             name, total_elapsed, datetime.utcnow().isoformat())


def _audit_thread(config: ClientConfig, rl: RateLimiter,
                  contact_email: str, website_url: str,
                  ip_address: str | None, submission_token: str):
    """Thread wrapper — catches all exceptions so the server never crashes."""
    try:
        _run_client_audit(config, rl, contact_email, website_url, ip_address)
    except Exception:
        log.error("Audit failed for token=%s:\n%s", submission_token,
                  traceback.format_exc())


# ══════════════════════════════════════════════════════════════════
#  SIGNATURE VERIFICATION
# ══════════════════════════════════════════════════════════════════

def _verify_typeform_signature(raw_body: bytes, header: str, secret: str) -> bool:
    """
    Verify X-Typeform-Signature: sha256=<hex> against the raw request body.
    Returns True if signature is valid or if no secret is configured.
    """
    if not secret:
        return True
    if not header:
        return False
    try:
        prefix, sig_hex = header.split("=", 1)
    except ValueError:
        return False
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_hex)


# ══════════════════════════════════════════════════════════════════
#  CORS
# ══════════════════════════════════════════════════════════════════

CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

def _add_cors(response):
    for k, v in CORS_HEADERS.items():
        response.headers[k] = v
    return response


# ══════════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ══════════════════════════════════════════════════════════════════

@app.route("/health", methods=["GET"])
def health():
    """Quick health check — confirms server is running."""
    from intake.rate_limiter import RateLimiter as _RL
    rl = _RL(bypass=True)
    status = rl.get_status()
    return jsonify({
        "status":        "ok",
        "server":        "C.A.S.H. Webhook Listener",
        "audits_today":  status["audits_today"],
        "daily_limit":   status["daily_limit"],
        "total_audits":  status["total_logged"],
        "timestamp":     datetime.utcnow().isoformat() + "Z",
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Typeform webhook endpoint.
    1. Verify HMAC signature (if TYPEFORM_WEBHOOK_SECRET is set).
    2. Parse payload and build ClientConfig.
    3. Rate-limit check.
    4. Return 200 immediately — audit runs in a background thread.
    """
    raw_body = request.get_data()
    secret   = os.environ.get("TYPEFORM_WEBHOOK_SECRET", "")

    # ── Signature check ───────────────────────────────────────────
    sig_header = request.headers.get("Typeform-Signature", "") or \
                 request.headers.get("X-Typeform-Signature", "")
    if not _verify_typeform_signature(raw_body, sig_header, secret):
        log.warning("Webhook rejected — invalid signature")
        return jsonify({"error": "invalid signature"}), 401

    # ── Parse JSON ────────────────────────────────────────────────
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        log.error("Webhook rejected — invalid JSON: %s", e)
        return jsonify({"error": "invalid json"}), 400

    event_type = payload.get("event_type", "")
    if event_type != "form_response":
        log.info("Webhook ignored — event_type=%r (not form_response)", event_type)
        return jsonify({"status": "ignored", "event_type": event_type}), 200

    form_resp = payload.get("form_response", {})
    token     = form_resp.get("token", "unknown")
    submitted = form_resp.get("submitted_at", "")
    log.info("Typeform submission received — token=%s  submitted_at=%s", token, submitted)

    # ── Parse fields ──────────────────────────────────────────────
    try:
        parsed = parse_typeform_payload(payload)
    except Exception as e:
        log.error("Field parsing failed: %s", e)
        return jsonify({"error": "field parsing failed"}), 500

    if not parsed.get("contact_email"):
        log.warning("Submission missing contact_email — cannot run audit")
        return jsonify({"error": "contact_email is required"}), 422

    # ── Build config ──────────────────────────────────────────────
    try:
        config = build_config_from_parsed(parsed)
    except Exception as e:
        log.error("Config build failed: %s", e)
        return jsonify({"error": "config build failed"}), 500

    config.audit_source = "full_intake"

    contact_email = config.contact_email
    website_url   = config.website_url or config.linktree_url
    ip_address    = request.remote_addr or None

    log.info("Client: %r  email: %s  url: %s", config.client_name, contact_email, website_url)

    # ── Rate limit check ──────────────────────────────────────────
    rl = RateLimiter()
    pub_ip = get_public_ip() if not rl.bypass else None
    allowed, reason = rl.check(
        email       = contact_email,
        website_url = website_url,
        ip_address  = pub_ip or ip_address,
    )
    if not allowed:
        log.info("Rate limit blocked: %s — %s", contact_email, reason.split("\n")[0])
        # Send a polite rejection email to the client
        _send_rejection_email(contact_email, config.client_name, reason)
        return jsonify({"status": "rate_limited", "message": reason}), 429

    # ── Launch audit in background thread ─────────────────────────
    t = threading.Thread(
        target   = _audit_thread,
        args     = (config, rl, contact_email, website_url,
                    pub_ip or ip_address, token),
        daemon   = True,
        name     = f"audit-{token[:8]}",
    )
    t.start()
    log.info("Audit thread launched — token=%s  client=%r", token, config.client_name)

    return jsonify({
        "status":  "accepted",
        "message": "Audit started — report will be emailed when complete.",
        "token":   token,
    }), 202


# ══════════════════════════════════════════════════════════════════
#  WIX FORM ENDPOINT
# ══════════════════════════════════════════════════════════════════

@app.route("/cash-report", methods=["OPTIONS"])
def cash_report_preflight():
    """Handle CORS preflight requests from browsers."""
    return _add_cors(app.response_class(status=204))


@app.route("/cash-report", methods=["POST"])
def cash_report():
    """
    Plain-JSON endpoint for the Wix CASH Report submission form.
    Accepts these fields:
      business_name, website_url, target_market, ad_budget,
      email_list_size, email_frequency, competitors,
      biggest_challenge, contact_email, phone, marketing_consent
    Returns 202 immediately; audit runs in a background thread.
    """
    try:
        data = request.get_json(force=True, silent=True)
    except Exception:
        data = None

    if not data:
        return _add_cors(jsonify({"success": False, "message": "Invalid or missing JSON body"})), 400

    # Map Wix field names → internal parsed-field dict
    parsed = {
        "business_name":      str(data.get("business_name", "")).strip(),
        "website_url":        str(data.get("website_url", "")).strip(),
        "target_market":      str(data.get("target_market", "")).strip(),
        "monthly_ad_budget":  str(data.get("ad_budget", "0")).strip(),
        "email_list_size":    str(data.get("email_list_size", "0")).strip(),
        "email_frequency":    str(data.get("email_frequency", "")).strip(),
        "competitor_urls":    str(data.get("competitors", "")).strip(),
        "biggest_challenge":  str(data.get("biggest_challenge", "")).strip(),
        "contact_email":      str(data.get("contact_email", "")).strip(),
        "phone":              str(data.get("phone", "")).strip(),
        "marketing_consent":  str(data.get("marketing_consent", "no")).strip(),
    }

    if not parsed["contact_email"]:
        return _add_cors(jsonify({"success": False, "message": "contact_email is required"})), 422

    log.info("Wix form submission — email=%s  business=%r",
             parsed["contact_email"], parsed["business_name"])

    try:
        config = build_config_from_parsed(parsed)
    except Exception as e:
        log.error("Config build failed for Wix submission: %s", e)
        return _add_cors(jsonify({"success": False, "message": "Submission failed"})), 500

    config.audit_source = "full_intake"

    contact_email = config.contact_email
    website_url   = config.website_url or config.linktree_url
    ip_address    = request.remote_addr or None

    # Rate limit check
    rl = RateLimiter()
    pub_ip = get_public_ip() if not rl.bypass else None
    allowed, reason = rl.check(
        email       = contact_email,
        website_url = website_url,
        ip_address  = pub_ip or ip_address,
    )
    if not allowed:
        log.info("Rate limit blocked Wix submission: %s", contact_email)
        _send_rejection_email(contact_email, config.client_name, reason)
        return _add_cors(jsonify({"success": False, "message": "Too many submissions. Please try again later."})), 429

    # Launch audit in background thread
    token = f"wix-{contact_email}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    t = threading.Thread(
        target = _audit_thread,
        args   = (config, rl, contact_email, website_url, pub_ip or ip_address, token),
        daemon = True,
        name   = f"wix-audit-{contact_email[:12]}",
    )
    t.start()
    log.info("Wix audit thread launched — client=%r  email=%s", config.client_name, contact_email)

    return _add_cors(jsonify({"success": True, "message": "Report request received"})), 202


# ══════════════════════════════════════════════════════════════════
#  EMAIL EXPORT ENDPOINT
# ══════════════════════════════════════════════════════════════════

@app.route("/export-emails", methods=["GET"])
def export_emails():
    """
    Export all opted-in client emails as a CSV download.

    Authentication
    --------------
    Requires the secret key to be passed via either:
      • Query param : GET /export-emails?key=<EXPORT_SECRET_KEY>
      • Header      : X-Export-Key: <EXPORT_SECRET_KEY>

    Set EXPORT_SECRET_KEY in .env (Railway → Variables) to enable this endpoint.
    If the env var is not set the endpoint returns 503 (misconfigured).

    CSV columns
    -----------
    email, client_name, business_type, website, audit_score, audit_date, created_at

    Example
    -------
      curl "https://your-app.railway.app/export-emails?key=mysecret" -o emails.csv
    """
    import csv
    import io

    secret = os.environ.get("EXPORT_SECRET_KEY", "").strip()
    if not secret:
        log.warning("/export-emails called but EXPORT_SECRET_KEY is not set")
        return jsonify({"error": "Export endpoint is not configured on this server"}), 503

    # Accept key from query param or header (constant-time compare)
    provided = (
        request.args.get("key", "")
        or request.headers.get("X-Export-Key", "")
    ).strip()

    if not hmac.compare_digest(provided.encode(), secret.encode()):
        log.warning("/export-emails rejected — invalid key from %s", request.remote_addr)
        return jsonify({"error": "Unauthorized"}), 401

    try:
        rows = get_opted_in_emails()
    except Exception as exc:
        log.error("/export-emails DB error: %s", exc)
        return jsonify({"error": "Database error", "detail": str(exc)}), 500

    # Build CSV in memory
    output = io.StringIO()
    fieldnames = ["email", "client_name", "business_type", "website",
                  "audit_score", "audit_date", "created_at"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore",
                            lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = output.getvalue().encode("utf-8")

    log.info("/export-emails served %d opted-in records to %s",
             len(rows), request.remote_addr)

    from flask import Response
    return Response(
        csv_bytes,
        status=200,
        mimetype="text/csv",
        headers={
            "Content-Disposition": (
                f'attachment; filename="cash_opted_in_emails_'
                f'{datetime.utcnow().strftime("%Y%m%d")}.csv"'
            ),
            "Content-Length": str(len(csv_bytes)),
        },
    )


# ══════════════════════════════════════════════════════════════════
#  ADMIN PORTAL
# ══════════════════════════════════════════════════════════════════

_LOGIN_PAGE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CASH Admin — Login</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#0f172a;color:#e2e8f0;min-height:100vh;
     display:flex;align-items:center;justify-content:center}
.card{background:#1e293b;border:1px solid #334155;border-radius:12px;
      padding:40px;width:100%;max-width:380px}
h1{font-size:1.3rem;font-weight:700;margin-bottom:6px;color:#f8fafc}
p{font-size:.85rem;color:#94a3b8;margin-bottom:28px}
label{display:block;font-size:.75rem;font-weight:600;color:#94a3b8;
      text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}
input[type=password]{width:100%;padding:10px 14px;background:#0f172a;
                     border:1px solid #475569;border-radius:8px;color:#f1f5f9;
                     font-size:1rem;outline:none}
input[type=password]:focus{border-color:#6366f1}
button{width:100%;margin-top:20px;padding:11px;background:#6366f1;
       border:none;border-radius:8px;color:#fff;font-size:1rem;
       font-weight:600;cursor:pointer}
button:hover{background:#4f46e5}
.err{color:#f87171;font-size:.85rem;margin-top:12px}
</style>
</head>
<body>
<div class="card">
  <h1>C.A.S.H. Admin</h1>
  <p>Enter the admin password to access the dashboard.</p>
  <form method="POST" action="/admin/login">
    <label for="pw">Password</label>
    <input type="password" id="pw" name="password" autofocus required>
    __ERROR__
    <button type="submit">Sign In</button>
  </form>
</div>
</body>
</html>"""


_DASHBOARD_PAGE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CASH Admin Portal</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#0f172a;color:#e2e8f0}
header{background:#1e293b;border-bottom:1px solid #334155;
       padding:16px 32px;display:flex;align-items:center;justify-content:space-between}
header h1{font-size:1.1rem;font-weight:700;color:#f8fafc}
header a{font-size:.8rem;color:#94a3b8;text-decoration:none}
header a:hover{color:#f1f5f9}
main{padding:32px;max-width:1500px;margin:0 auto}
.section-title{font-size:.75rem;font-weight:600;color:#94a3b8;
               text-transform:uppercase;letter-spacing:.06em;margin-bottom:14px}
.card{background:#1e293b;border:1px solid #334155;border-radius:12px;
      padding:24px;margin-bottom:28px}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:28px}
.stat{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:20px}
.stat .val{font-size:2rem;font-weight:700;color:#f8fafc}
.stat .lbl{font-size:.8rem;color:#94a3b8;margin-top:4px}
.trigger-form{display:grid;grid-template-columns:1fr 1fr 1fr auto;
              gap:12px;align-items:end}
label{display:block;font-size:.75rem;font-weight:600;color:#94a3b8;
      text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}
input[type=text],input[type=email],input[type=url]{
  width:100%;padding:9px 12px;background:#0f172a;
  border:1px solid #475569;border-radius:7px;color:#f1f5f9;font-size:.9rem}
input:focus{outline:none;border-color:#6366f1}
.btn{padding:10px 20px;background:#6366f1;border:none;border-radius:7px;
     color:#fff;font-size:.9rem;font-weight:600;cursor:pointer;
     white-space:nowrap;height:38px}
.btn:hover{background:#4f46e5}
table{width:100%;border-collapse:collapse;font-size:.875rem}
th{text-align:left;padding:10px 12px;font-size:.72rem;font-weight:600;
   color:#94a3b8;text-transform:uppercase;letter-spacing:.05em;
   border-bottom:1px solid #334155}
td{padding:11px 12px;border-bottom:1px solid #1e293b33;
   color:#cbd5e1;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#ffffff08}
.ok{color:#4ade80;font-weight:600}
.pending{color:#fbbf24;font-weight:600}
.sent{color:#4ade80}
.nosent{color:#475569}
.flash{padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:.875rem}
.flash-ok{background:#14532d;color:#4ade80;border:1px solid #16a34a}
.flash-err{background:#450a0a;color:#f87171;border:1px solid #b91c1c}
a.ul{color:#818cf8;text-decoration:none;font-size:.82rem}
a.ul:hover{text-decoration:underline}
</style>
</head>
<body>
<header>
  <h1>C.A.S.H. Admin Portal</h1>
  <a href="/admin/logout">Sign out</a>
</header>
<main>
  __FLASH__
  <div class="stats">
    <div class="stat"><div class="val">__TOTAL__</div><div class="lbl">Total Audits</div></div>
    <div class="stat"><div class="val">__COMPLETE__</div><div class="lbl">Completed</div></div>
    <div class="stat"><div class="val">__EMAILS__</div><div class="lbl">Emails Sent</div></div>
  </div>
  <div class="card">
    <div class="section-title">Trigger New Audit</div>
    <form method="POST" action="/admin/trigger" class="trigger-form">
      <div>
        <label>Business Name</label>
        <input type="text" name="business_name" placeholder="Acme Corp" required>
      </div>
      <div>
        <label>Website URL</label>
        <input type="url" name="website_url" placeholder="https://example.com" required>
      </div>
      <div>
        <label>Contact Email</label>
        <input type="email" name="contact_email" placeholder="client@example.com" required>
      </div>
      <div>
        <button type="submit" class="btn">&#9654; Run Audit</button>
      </div>
    </form>
  </div>
  <div class="card">
    <div class="section-title">All Audits (__TOTAL__)</div>
    <table>
      <thead>
        <tr>
          <th>#</th><th>Client</th><th>Email</th><th>Website</th>
          <th>Score</th><th>Grade</th><th>Date</th>
          <th>Status</th><th>Email Sent</th>
        </tr>
      </thead>
      <tbody>__ROWS__</tbody>
    </table>
  </div>
</main>
</body>
</html>"""


def _build_admin_html(rows: list, flash: str = "", flash_type: str = "ok") -> str:
    total    = len(rows)
    complete = sum(1 for r in rows if r.get("audit_grade"))
    sent     = sum(1 for r in rows if r.get("email") and r.get("audit_grade"))

    flash_html = ""
    if flash:
        cls = "flash-ok" if flash_type == "ok" else "flash-err"
        flash_html = f'<div class="flash {cls}">{flash}</div>'

    table_html = ""
    for r in rows:
        status    = "Complete" if r.get("audit_grade") else "Pending"
        stat_cls  = "ok" if status == "Complete" else "pending"
        is_sent   = bool(r.get("email") and r.get("audit_grade"))
        sent_html = "&#10003; Sent" if is_sent else "&#8212;"
        sent_cls  = "sent" if is_sent else "nosent"
        score     = r.get("audit_score") or "&#8212;"
        grade     = r.get("audit_grade") or "&#8212;"
        website   = r.get("website") or ""
        site_disp = (website[:38] + "…") if len(website) > 38 else website
        site_html = (f'<a class="ul" href="{website}" target="_blank">{site_disp}</a>'
                     if website else "&#8212;")
        date_val  = (r.get("audit_date") or r.get("created_at") or "")[:10] or "&#8212;"
        table_html += (
            f"<tr>"
            f"<td>{r.get('id', '&#8212;')}</td>"
            f"<td>{r.get('client_name', '&#8212;')}</td>"
            f"<td>{r.get('email') or '&#8212;'}</td>"
            f"<td>{site_html}</td>"
            f"<td style='text-align:center'>{score}</td>"
            f"<td style='text-align:center;font-weight:700'>{grade}</td>"
            f"<td>{date_val}</td>"
            f"<td class='{stat_cls}'>{status}</td>"
            f"<td class='{sent_cls}'>{sent_html}</td>"
            f"</tr>"
        )
    if not table_html:
        table_html = (
            '<tr><td colspan="9" style="text-align:center;color:#64748b;'
            'padding:32px">No audits yet.</td></tr>'
        )

    return (
        _DASHBOARD_PAGE
        .replace("__FLASH__",    flash_html)
        .replace("__TOTAL__",    str(total))
        .replace("__COMPLETE__", str(complete))
        .replace("__EMAILS__",   str(sent))
        .replace("__ROWS__",     table_html)
    )


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    pwd = os.environ.get("ADMIN_PASSWORD", "").strip()
    if not pwd:
        return jsonify({"error": "Admin portal not configured — set ADMIN_PASSWORD env var"}), 503
    error_html = ""
    if request.method == "POST":
        submitted = request.form.get("password", "")
        if hmac.compare_digest(submitted.encode(), pwd.encode()):
            session["admin_logged_in"] = True
            return redirect("/admin")
        error_html = '<p class="err">Incorrect password.</p>'
    return _LOGIN_PAGE.replace("__ERROR__", error_html), 200


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_logged_in", None)
    return redirect("/admin/login")


@app.route("/admin")
def admin_portal():
    if not session.get("admin_logged_in"):
        return redirect("/admin/login")
    try:
        rows = list_clients(limit=200)
    except Exception as exc:
        log.error("Admin portal DB error: %s", exc)
        rows = []
    flash      = request.args.get("flash", "")
    flash_type = request.args.get("ft", "ok")
    return _build_admin_html(rows, flash, flash_type), 200


@app.route("/admin/trigger", methods=["POST"])
def admin_trigger():
    if not session.get("admin_logged_in"):
        return redirect("/admin/login")

    from urllib.parse import urlencode

    business_name = request.form.get("business_name", "").strip()
    website_url   = _normalise_url(request.form.get("website_url", ""))
    contact_email = request.form.get("contact_email", "").strip()

    if not website_url or not contact_email:
        msg = "Website URL and contact email are required."
        return redirect("/admin?" + urlencode({"flash": msg, "ft": "err"}))

    if not business_name and website_url:
        _u = website_url.lower()
        for _pfx in ("https://www.", "http://www.", "https://", "http://"):
            if _u.startswith(_pfx):
                _u = _u[len(_pfx):]
                break
        business_name = _u.rstrip("/").capitalize()

    parsed = {
        "business_name":     business_name or "Manual Audit",
        "website_url":       website_url,
        "contact_email":     contact_email,
        "target_market":     "",
        "monthly_ad_budget": "0",
        "email_list_size":   "0",
        "email_frequency":   "",
        "competitor_urls":   "",
        "biggest_challenge": "",
        "phone":             "",
        "marketing_consent": "no",
    }

    try:
        config = build_config_from_parsed(parsed)
    except Exception as exc:
        log.error("Admin trigger config build failed: %s", exc)
        msg = f"Config build failed: {str(exc)[:80]}"
        return redirect("/admin?" + urlencode({"flash": msg, "ft": "err"}))

    config.audit_source = "admin_url_only"

    rl    = RateLimiter(bypass=True)
    token = f"admin-{contact_email}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    t = threading.Thread(
        target = _audit_thread,
        args   = (config, rl, contact_email, website_url, None, token),
        daemon = True,
        name   = f"admin-audit-{contact_email[:12]}",
    )
    t.start()
    log.info("Admin-triggered audit launched — client=%r  email=%s  url=%s",
             config.client_name, contact_email, website_url)

    label = business_name or website_url
    msg   = f"Audit started for {label} — report will be emailed when complete."
    return redirect("/admin?" + urlencode({"flash": msg, "ft": "ok"}))


@app.route("/admin/clear-rate-limit", methods=["POST"])
def admin_clear_rate_limit():
    # Auth
    if request.headers.get("X-Admin-Password", "") != os.environ.get("ADMIN_PASSWORD", ""):
        return jsonify({"error": "unauthorized"}), 403

    body        = request.get_json(force=True, silent=True) or {}
    email       = (body.get("email") or "").strip().lower()
    website_url = (body.get("website_url") or "").strip().lower()

    if not email and not website_url:
        return jsonify({"error": "provide at least one of: email, website_url"}), 400

    try:
        from intake.rate_limiter import _connect
        deleted = 0
        with _connect() as conn:
            if email:
                cur = conn.execute(
                    "DELETE FROM rate_limit_log WHERE LOWER(email) = ?", (email,)
                )
                deleted += cur.rowcount
            if website_url:
                cur = conn.execute(
                    "DELETE FROM rate_limit_log WHERE LOWER(website_url) LIKE ?",
                    (f"%{website_url}%",)
                )
                deleted += cur.rowcount
        return jsonify({
            "success":     True,
            "cleared":     deleted,
            "email":       email or None,
            "website_url": website_url or None,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════════
#  REJECTION EMAIL HELPER
# ══════════════════════════════════════════════════════════════════

def _send_rejection_email(to_addr: str, client_name: str, reason: str):
    """Send a friendly rate-limit rejection notice to the client."""
    import urllib.request, urllib.error, base64
    sg_key    = os.environ.get("SENDGRID_API_KEY", "").strip()
    from_addr = os.environ.get("REPORT_EMAIL_FROM", "").strip()
    if not sg_key or not from_addr or not to_addr:
        return
    subject = "Your C.A.S.H. Report Request — Action Required"
    body    = (
        f"Hi {client_name},\n\n"
        f"Thank you for submitting a C.A.S.H. Report request.\n\n"
        f"Unfortunately your request could not be processed right now:\n\n"
        f"{reason}\n\n"
        f"If you have questions, reach us at: gmg@goguerrilla.xyz\n\n"
        f"— C.A.S.H. Report by GMG · goguerrilla.xyz"
    )
    payload = json.dumps({
        "personalizations": [{"to": [{"email": to_addr}]}],
        "from":    {"email": from_addr},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
    }).encode()
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={"Authorization": f"Bearer {sg_key}",
                 "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            log.info("Rejection email sent to %s (status %s)", to_addr, r.status)
    except Exception as e:
        log.warning("Rejection email failed: %s", e)


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    log.info("Starting C.A.S.H. Webhook Listener on http://0.0.0.0:%d", port)
    log.info("  POST /webhook       — Typeform webhook target")
    log.info("  GET  /health        — health check")
    log.info("  GET  /export-emails — opted-in email CSV (key-protected)")
    log.info("  GET  /admin         — admin portal (password-protected)")
    wh_secret  = os.environ.get("TYPEFORM_WEBHOOK_SECRET", "")
    exp_secret  = os.environ.get("EXPORT_SECRET_KEY", "")
    admin_pwd   = os.environ.get("ADMIN_PASSWORD", "")
    db_url     = os.environ.get("DATABASE_URL", "")
    log.info("  Signature verification : %s", "ON" if wh_secret else "OFF (set TYPEFORM_WEBHOOK_SECRET)")
    log.info("  Email export key       : %s", "SET" if exp_secret else "NOT SET — /export-emails will return 503")
    log.info("  Admin password         : %s", "SET" if admin_pwd else "NOT SET — /admin will return 503")
    beta_mode  = os.environ.get("BETA_DOCX_ONLY", "").strip().lower() == "true"
    log.info("  Output format          : %s", "DOCX only (BETA_DOCX_ONLY=true)" if beta_mode else "PDF (default)")
    log.info("  Database backend       : %s", f"Postgres ({db_url[:30]}...)" if db_url else "SQLite (cash_clients.db)")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
