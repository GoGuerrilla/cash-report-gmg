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
  POST /webhook    — Typeform webhook target
  GET  /health     — health / status check

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

import hashlib
import hmac
import json
import logging
import os
import re
import sys
import threading
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
from intake.client_db import save_audit_result
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
#  CORE AUDIT RUNNER  (generalized — works for any client)
# ══════════════════════════════════════════════════════════════════

def _run_client_audit(config: ClientConfig, rl: RateLimiter,
                      contact_email: str, website_url: str,
                      ip_address: str | None):
    """
    Full CASH audit for a client config built from Typeform intake.
    Mirrors run_goguerrilla.run_audit() but is config-driven, not GMG-hardcoded.
    Runs in a background thread — never called synchronously from Flask.
    """
    name = config.client_name
    log.info("=== Starting CASH audit for: %s ===", name)

    # ── Channel data skeleton ─────────────────────────────────────
    channel_data = _build_base_channel_data(config.website_url or config.linktree_url or "")
    config.preloaded_channel_data = channel_data

    audit_data: dict = {}

    # ── 1a. Linktree / website social scrape ─────────────────────
    linktree_data = {}
    if config.linktree_url:
        log.info("Scraping Linktree: %s", config.linktree_url)
        linktree_data = LinktreeScraper(config.linktree_url).scrape()
    elif config.website_url:
        log.info("Scraping website socials: %s", config.website_url)
        # Use the website scraper from questionnaire as fallback
        from intake.questionnaire import _scrape_website_socials, _classified_to_platforms
        classified = _scrape_website_socials(config.website_url)
        if classified:
            platforms   = list(classified.keys())
            plat_data   = _classified_to_platforms(classified)
            # Populate config social handles from scraped data
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
        li_data = _li_scraper.scrape(config.linkedin_url)
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
        log.info("Fetching YouTube data...")
        yt_key = os.environ.get("YOUTUBE_API_KEY", "")
        if yt_key:
            from auditors.youtube_api import YouTubeAuditor
            yt = YouTubeAuditor(config.youtube_channel_url, yt_key).fetch()
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

    # ── 2. Website & SEO ──────────────────────────────────────────
    target_url = config.website_url
    if not target_url and linktree_data.get("website_url"):
        target_url = linktree_data["website_url"]
    if not target_url:
        target_url = ""

    if target_url:
        log.info("Auditing website: %s", target_url)
        website_auditor = WebsiteAuditor(target_url, max_pages=5)
        audit_data["website"] = website_auditor.run()
        _merge_website_data(channel_data, audit_data["website"])

        log.info("Auditing SEO...")
        pagespeed_key = os.environ.get("PAGESPEED_API_KEY", "")
        audit_data["seo"] = SEOAuditor(target_url, api_key=pagespeed_key).run()

        log.info("Auditing GEO...")
        audit_data["geo"] = GEOAuditor(config, audit_data.get("seo", {})).run()

        log.info("Auditing GBP...")
        places_key = os.environ.get("GOOGLE_PLACES_API_KEY",
                                     os.environ.get("PAGESPEED_API_KEY", ""))
        audit_data["gbp"] = GBPAuditor(
            business_name=name,
            website_url=target_url,
            api_key=places_key,
        ).run()

        log.info("Fetching Analytics...")
        ga_prop = os.environ.get("GOOGLE_ANALYTICS_PROPERTY_ID", "")
        ga_sa   = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "")
        audit_data["analytics"] = AnalyticsAuditor(
            property_id=ga_prop, service_account_json_path=ga_sa
        ).run()
    else:
        log.warning("No website URL — skipping website/SEO/GEO/GBP auditors")
        for key in ("website", "seo", "geo", "gbp", "analytics"):
            audit_data[key] = {"note": "No website URL provided", "score": 50}

    # ── 3–10. All remaining auditors ─────────────────────────────
    log.info("Auditing social channels...")
    audit_data["social"]    = SocialMediaAuditor(config).run()

    log.info("Auditing content efficiency...")
    audit_data["content"]   = ContentAuditor(config, audit_data).run()

    log.info("Auditing brand consistency...")
    audit_data["brand"]     = BrandAuditor(config, linktree_data).run()

    log.info("Auditing lead funnel...")
    audit_data["funnel"]    = FunnelAuditor(config, linktree_data).run()

    log.info("Auditing ICP alignment...")
    audit_data["icp"]       = ICPAuditor(config, linktree_data).run()

    log.info("Auditing content freshness...")
    audit_data["freshness"] = FreshnessAuditor(config, linktree_data).run()

    if config.competitor_urls:
        log.info("Auditing competitors: %s", config.competitor_urls)
        pagespeed_key = os.environ.get("PAGESPEED_API_KEY", "")
        audit_data["competitor"] = CompetitorAuditor(
            config, audit_data, pagespeed_api_key=pagespeed_key
        ).run()
    else:
        audit_data["competitor"] = {
            "skipped": True, "note": "No competitor URLs provided.",
            "competitors": [], "comparison": {},
        }

    log.info("Running AI synthesis...")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    audit_data["ai_insights"] = AIAnalyzer(anthropic_api_key=anthropic_key).analyze(
        config, audit_data
    )

    # ── Score summary ─────────────────────────────────────────────
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

    log.info("Generating PDF: %s", pdf_path)
    PDFReportGenerator(config, audit_data).generate(pdf_path)
    log.info("PDF saved: %s", pdf_path)

    # ── Generate DOCX backup ──────────────────────────────────────
    docx_path = os.path.abspath(f"reports/{slug}_cash_report.docx")
    try:
        DocxReportGenerator(config, audit_data).generate(docx_path)
        log.info("DOCX backup saved: %s", docx_path)
    except Exception as e:
        log.warning("DOCX backup failed: %s", e)

    # ── Email report ──────────────────────────────────────────────
    # 1. Send to the client
    if contact_email:
        log.info("Emailing report to client: %s", contact_email)
        send_report(
            report_path   = pdf_path,
            client_name   = name,
            overall_score = overall_score,
            overall_grade = overall_grade,
            to_addr       = contact_email,
        )

    # 2. Always send a copy to the GMG team inbox
    gmg_inbox = os.environ.get("REPORT_EMAIL_TO", "")
    if gmg_inbox and gmg_inbox != contact_email:
        log.info("Emailing copy to GMG team: %s", gmg_inbox)
        send_report(
            report_path   = pdf_path,
            client_name   = name,
            overall_score = overall_score,
            overall_grade = overall_grade,
            to_addr       = gmg_inbox,
        )

    # ── Save to DB ────────────────────────────────────────────────
    log.info("Saving to cash_clients.db...")
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
        log.info("DB saved (row #%s)", row_id)
    except Exception as e:
        log.warning("DB save failed: %s", e)

    # ── Log rate limit ────────────────────────────────────────────
    try:
        rl.log(email=contact_email, website_url=website_url, ip_address=ip_address)
    except Exception:
        pass

    log.info("=== Audit complete for: %s ===", name)


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
    log.info("  POST /webhook  — Typeform webhook target")
    log.info("  GET  /health   — health check")
    wh_secret = os.environ.get("TYPEFORM_WEBHOOK_SECRET", "")
    log.info("  Signature verification: %s", "ON" if wh_secret else "OFF (set TYPEFORM_WEBHOOK_SECRET)")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
