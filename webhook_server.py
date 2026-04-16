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
from flask import Flask, request, jsonify

app = Flask(__name__)

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
from intake.client_db import save_audit_result, get_opted_in_emails
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
    """Ensure URL has https:// prefix."""
    url = url.strip()
    if url and not re.match(r"https?://", url, re.I):
        url = "https://" + url
    return url


def _parse_competitor_urls(raw: str) -> list:
    """Split comma/newline/semicolon-separated URLs into a list (max 3)."""
    parts = re.split(r"[,;\n]+", raw)
    urls  = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if not re.match(r"https?://", p, re.I):
            p = "https://" + p
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
    import shutil
    if not pdf_path or not os.path.isfile(pdf_path):
        return None
    try:
        today      = datetime.utcnow()
        year       = today.strftime("%Y")
        month      = today.strftime("%B")          # e.g. "April"
        safe_name  = re.sub(r"[^a-zA-Z0-9]+", "_", client_name).strip("_")
        filename   = f"{safe_name}_CASH_Report_{today.strftime('%Y-%m-%d')}.pdf"
        archive_dir = os.path.expanduser(
            f"~/Desktop/CASH GMG Audit/Client Reports/{year}/{month}"
        )
        os.makedirs(archive_dir, exist_ok=True)
        dest = os.path.join(archive_dir, filename)
        shutil.copy2(pdf_path, dest)
        log.info("Report archived → %s", dest)
        return dest
    except Exception as exc:
        log.warning("Archive copy failed (non-fatal): %s", exc)
        return None


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


def _send_acknowledgment_email(to_addr: str, client_name: str) -> None:
    """
    Fire an immediate acknowledgment email as soon as the audit thread starts.
    Uses SendGrid directly (no PDF attachment needed). Never raises.
    Gives the client sub-5-second confirmation that their submission was received.
    """
    import urllib.request, urllib.error
    sg_key    = os.environ.get("SENDGRID_API_KEY", "").strip()
    from_addr = (os.environ.get("SENDGRID_FROM_EMAIL")
                 or os.environ.get("REPORT_EMAIL_FROM", "")).strip()
    if not sg_key or not from_addr or not to_addr:
        log.warning("ACK email skipped — missing SendGrid key, from, or to address")
        return
    subject = f"We received your C.A.S.H. Report request for {client_name}"
    body = (
        f"Hi,\n\n"
        f"We received your C.A.S.H. Report request for {client_name}.\n\n"
        f"Your full report is being generated now. This typically takes 3–5 minutes "
        f"while we audit your website, SEO, social channels, and competitive landscape.\n\n"
        f"We'll email you the complete PDF report as soon as it's ready.\n\n"
        f"In the meantime, if you have questions reach us at: gmg@goguerrilla.xyz\n\n"
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
        headers={"Authorization": f"Bearer {sg_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=10) as r:
            elapsed = round(time.time() - t0, 3)
            log.info("TIMING  ack_email_sendgrid      %.2fs  status=%d → %s",
                     elapsed, r.status, to_addr)
    except urllib.error.HTTPError as e:
        body_err = e.read().decode("utf-8", errors="replace")[:200]
        log.error("ACK email SendGrid error HTTP %d: %s", e.code, body_err)
    except Exception as exc:
        log.error("ACK email failed: %s", exc)


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
      Phase 0 : Acknowledgment email (< 3s — fires immediately)
      Phase 1 : Social data collection — Linktree, LinkedIn, YouTube, Meta (sequential)
      Phase 2 : Website + SEO + GBP + Analytics in PARALLEL
      Phase 3 : GEO (needs SEO output), then Social/Brand/Funnel/ICP/Freshness/
                Content/Competitor all in PARALLEL
      Phase 4 : AI synthesis → PDF → DOCX → Report email
    """
    audit_wall_start = time.time()
    name = config.client_name
    log.info("=== AUDIT START: %s  [%s] ===", name, datetime.utcnow().isoformat())

    # ── Phase 0: Acknowledgment email (fires before any scraping) ─
    _t = time.time()
    if contact_email:
        _send_acknowledgment_email(contact_email, name)
    log.info("TIMING  phase0_ack_email        %.2fs", time.time() - _t)

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
            _t = time.time()
            from auditors.youtube_api import YouTubeAuditor
            yt = YouTubeAuditor(config.youtube_channel_url, yt_key).fetch()
            log.info("TIMING  youtube_api             %.2fs", time.time() - _t)
            if yt.get("data_source") == "youtube_data_api":
                for key in ("posts_per_week", "days_since_last_post", "is_active"):
                    if yt.get(key) is not None:
                        channel_data["youtube"][key] = yt[key]
                log.info("YouTube: subs=%s ppw=%s", yt.get("subscriber_count"), yt.get("posts_per_week"))

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
        fb = meta_result.get("facebook", {})
        ig = meta_result.get("instagram", {})
        if fb.get("data_source") == "meta_graph_api":
            for key in ("followers", "posts_per_week", "days_since_last_post",
                        "engagement_rate", "reach_28d", "engagements_28d"):
                if fb.get(key) is not None:
                    channel_data["facebook"][key] = fb[key]
            channel_data["facebook"]["is_active"] = True
        if ig.get("data_source") == "meta_graph_api":
            for key in ("followers", "posts_per_week", "days_since_last_post",
                        "engagement_rate", "media_count"):
                if ig.get(key) is not None:
                    channel_data["instagram"][key] = ig[key]
            channel_data["instagram"]["is_active"] = True
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

    # ── Generate PDF ──────────────────────────────────────────────
    os.makedirs("reports", exist_ok=True)
    slug     = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    pdf_path = os.path.abspath(f"reports/{slug}_cash_report.pdf")

    _t = time.time()
    try:
        PDFReportGenerator(config, audit_data).generate(pdf_path)
        if os.path.isfile(pdf_path):
            log.info("TIMING  pdf_generation          %.2fs  (%d bytes)",
                     time.time() - _t, os.path.getsize(pdf_path))
            _archive_report(pdf_path, name)
        else:
            log.error("PDF generation ran but file not found at: %s", pdf_path)
    except Exception as pdf_err:
        log.error("PDF generation FAILED (%.2fs): %s\n%s",
                  time.time() - _t, pdf_err, traceback.format_exc())
        pdf_path = None

    # ── Generate DOCX backup ──────────────────────────────────────
    docx_path = os.path.abspath(f"reports/{slug}_cash_report.docx")
    _t = time.time()
    try:
        DocxReportGenerator(config, audit_data).generate(docx_path)
        log.info("TIMING  docx_generation         %.2fs", time.time() - _t)
    except Exception as e:
        log.warning("DOCX backup failed (%.2fs): %s", time.time() - _t, e)

    # ── Email report ──────────────────────────────────────────────
    email_trigger_ts = datetime.utcnow().isoformat()
    log.info("=== EMAIL DELIVERY TRIGGERED at %s ===", email_trigger_ts)
    log.info("SENDGRID_API_KEY   : %s",
             f"set ({len(os.environ.get('SENDGRID_API_KEY',''))} chars)"
             if os.environ.get("SENDGRID_API_KEY") else "NOT SET")
    log.info("PDF for attachment : %s  exists=%s",
             pdf_path, os.path.isfile(pdf_path) if pdf_path else False)

    # 1. Send to the client
    client_email_ok = False
    if contact_email:
        _t = time.time()
        log.info("TIMING  sendgrid_trigger_start  → %s", contact_email)
        client_email_ok = send_report(
            report_path   = pdf_path,
            client_name   = name,
            overall_score = overall_score,
            overall_grade = overall_grade,
            to_addr       = contact_email,
        )
        log.info("TIMING  sendgrid_client_send    %.2fs  result=%s",
                 time.time() - _t, "SUCCESS" if client_email_ok else "FAILED")
        if not client_email_ok:
            log.error(
                "CLIENT EMAIL FAILED — client %r (%s) did NOT receive their report. "
                "PDF at: %s. Fix SENDGRID_API_KEY or REPORT_EMAIL_PASSWORD and resend manually.",
                name, contact_email, pdf_path,
            )

    # 2. Always send a copy to the GMG team inbox
    gmg_inbox = os.environ.get("REPORT_EMAIL_TO", "")
    if gmg_inbox and gmg_inbox != contact_email:
        _t = time.time()
        ok2 = send_report(
            report_path   = pdf_path,
            client_name   = name,
            overall_score = overall_score,
            overall_grade = overall_grade,
            to_addr       = gmg_inbox,
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
            report_path   = pdf_path,
        )
        log.info("TIMING  db_save                 %.2fs  row=#%s", time.time() - _t, row_id)
    except Exception as e:
        log.warning("DB save failed (%.2fs): %s", time.time() - _t, e)

    # ── Log rate limit ────────────────────────────────────────────
    try:
        rl.log(email=contact_email, website_url=website_url, ip_address=ip_address)
    except Exception:
        pass

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
    wh_secret  = os.environ.get("TYPEFORM_WEBHOOK_SECRET", "")
    exp_secret = os.environ.get("EXPORT_SECRET_KEY", "")
    db_url     = os.environ.get("DATABASE_URL", "")
    log.info("  Signature verification : %s", "ON" if wh_secret else "OFF (set TYPEFORM_WEBHOOK_SECRET)")
    log.info("  Email export key       : %s", "SET" if exp_secret else "NOT SET — /export-emails will return 503")
    log.info("  Database backend       : %s", f"Postgres ({db_url[:30]}...)" if db_url else "SQLite (cash_clients.db)")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
