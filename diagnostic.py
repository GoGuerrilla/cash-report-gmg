#!/usr/bin/env python3
"""
C.A.S.H. Report — Full System Diagnostic
Checks every subsystem: APIs, scrapers, auditors, PDF generators, email, DB.
Run from project root:  python3 diagnostic.py
"""
import importlib
import json
import os
import sys
import sqlite3
import traceback
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

PASS  = "✅ PASS"
FAIL  = "❌ FAIL"
WARN  = "⚠️  WARN"
SKIP  = "⏭  SKIP"

results = []

def chk(label, status, note=""):
    tag = {"pass": PASS, "fail": FAIL, "warn": WARN, "skip": SKIP}.get(status, WARN)
    results.append((label, tag, note))
    pad = 46 - len(label)
    print(f"  {label}{' ' * max(1, pad)}{tag}  {note}")

def section(title):
    print(f"\n{'─' * 62}")
    print(f"  {title}")
    print(f"{'─' * 62}")


# ══════════════════════════════════════════════════════════════
#  1. PYTHON PACKAGES
# ══════════════════════════════════════════════════════════════
section("1 · PYTHON PACKAGES")
required = [
    ("reportlab",     "reportlab"),
    ("requests",      "requests"),
    ("bs4",           "beautifulsoup4"),
    ("dotenv",        "python-dotenv"),
    ("docx",          "python-docx"),
    ("google.oauth2", "google-auth"),
    ("google.auth.transport.requests", "google-auth"),
]
for mod, pkg in required:
    try:
        importlib.import_module(mod)
        chk(f"pkg: {pkg}", "pass")
    except ImportError:
        chk(f"pkg: {pkg}", "fail", f"pip install {pkg}")


# ══════════════════════════════════════════════════════════════
#  2. ENV / API KEYS
# ══════════════════════════════════════════════════════════════
section("2 · ENVIRONMENT / API KEYS")

def env(key, required=True):
    v = os.environ.get(key, "").strip()
    if v:
        chk(f"env: {key}", "pass", f"{v[:14]}…" if len(v) > 14 else v)
    elif required:
        chk(f"env: {key}", "fail", "not set in .env")
    else:
        chk(f"env: {key}", "warn", "optional — not set")
    return v

pagespeed_key = env("PAGESPEED_API_KEY")
yt_key        = env("YOUTUBE_API_KEY")
ga_prop       = env("GOOGLE_ANALYTICS_PROPERTY_ID")
sa_path       = env("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
gsc_site      = env("GSC_SITE_URL")
email_to      = env("REPORT_EMAIL_TO")
email_from    = env("REPORT_EMAIL_FROM")
sg_key        = env("SENDGRID_API_KEY")
env("REPORT_EMAIL_PASSWORD", required=False)
meta_app_id   = env("META_APP_ID",           required=False)
meta_secret   = env("META_APP_SECRET",        required=False)
meta_pg_tok   = env("META_PAGE_ACCESS_TOKEN", required=False)


# ══════════════════════════════════════════════════════════════
#  3. SERVICE ACCOUNT JSON
# ══════════════════════════════════════════════════════════════
section("3 · SERVICE ACCOUNT JSON")
sa_email = None
if sa_path and os.path.isfile(sa_path):
    try:
        with open(sa_path) as f:
            sa_data = json.load(f)
        sa_email = sa_data.get("client_email", "")
        chk("SA JSON: file readable",   "pass", sa_path.split("/")[-1])
        chk("SA JSON: client_email",    "pass", sa_email[:40] if sa_email else "missing")
        chk("SA JSON: private_key",     "pass" if sa_data.get("private_key") else "fail")
        chk("SA JSON: project_id",      "pass", sa_data.get("project_id","?"))
    except Exception as e:
        chk("SA JSON: parse", "fail", str(e)[:60])
else:
    chk("SA JSON: file exists", "fail", sa_path or "path not set")


# ══════════════════════════════════════════════════════════════
#  4. LIVE API CALLS
# ══════════════════════════════════════════════════════════════
section("4 · LIVE API CALLS")

# PageSpeed
try:
    url = (f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
           f"?url=https://goguerrilla.xyz&strategy=mobile&category=seo"
           + (f"&key={pagespeed_key}" if pagespeed_key else ""))
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.loads(r.read())
    score = round(d["lighthouseResult"]["categories"]["seo"]["score"] * 100)
    chk("PageSpeed API: goguerrilla.xyz", "pass", f"SEO score {score}/100")
except Exception as e:
    chk("PageSpeed API", "fail", str(e)[:60])

# YouTube Data API
try:
    yt_url = (f"https://www.googleapis.com/youtube/v3/channels"
              f"?part=statistics&forHandle=@gogmg&key={yt_key}")
    with urllib.request.urlopen(yt_url, timeout=20) as r:
        d = json.loads(r.read())
    subs = d["items"][0]["statistics"].get("subscriberCount","?")
    chk("YouTube Data API", "pass", f"{subs} subscribers")
except Exception as e:
    chk("YouTube Data API", "fail", str(e)[:60])

# Google Analytics (service account)
try:
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GReq
    if sa_path and os.path.isfile(sa_path) and ga_prop:
        creds = service_account.Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        creds.refresh(GReq())
        import urllib.parse
        req_url = (f"https://analyticsdata.googleapis.com/v1beta/properties/"
                   f"{ga_prop}:runReport")
        payload = json.dumps({
            "dateRanges": [{"startDate": "30daysAgo", "endDate": "today"}],
            "metrics":    [{"name": "sessions"}],
        }).encode()
        req = urllib.request.Request(req_url, data=payload, method="POST",
            headers={"Authorization": f"Bearer {creds.token}",
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        sessions = d["rows"][0]["metricValues"][0]["value"]
        chk("Google Analytics 4 API", "pass", f"{sessions} sessions last 30d")
    else:
        chk("Google Analytics 4 API", "skip", "SA path or property ID missing")
except Exception as e:
    chk("Google Analytics 4 API", "fail", str(e)[:60])

# Google Search Console API
try:
    if sa_path and os.path.isfile(sa_path) and gsc_site:
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request as GReq2
        import datetime, urllib.parse
        creds2 = service_account.Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        creds2.refresh(GReq2())
        end   = datetime.date.today() - datetime.timedelta(days=3)
        start = end - datetime.timedelta(days=28)
        site_enc = urllib.parse.quote(gsc_site.rstrip("/") + "/", safe="")
        gsc_url = (f"https://searchconsole.googleapis.com/v1/sites/"
                   f"{site_enc}/searchAnalytics/query")
        payload = json.dumps({
            "startDate": start.isoformat(),
            "endDate":   end.isoformat(),
            "dimensions": ["query"],
            "rowLimit":  5,
        }).encode()
        req = urllib.request.Request(gsc_url, data=payload, method="POST",
            headers={"Authorization": f"Bearer {creds2.token}",
                     "Content-Type":  "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        n_rows = len(d.get("rows", []))
        if n_rows:
            top = d["rows"][0]["keys"][0]
            chk("Google Search Console API", "pass",
                f"{n_rows} keywords returned, top: '{top}'")
        else:
            chk("Google Search Console API", "warn",
                "Connected but 0 rows — grant service account access in GSC → Users & permissions")
    else:
        chk("Google Search Console API", "skip",
            "SA path or GSC_SITE_URL not set")
except urllib.error.HTTPError as e:
    body = e.read().decode("utf-8", errors="replace")[:120]
    if e.code == 403:
        chk("Google Search Console API", "warn",
            f"403 Forbidden — service account not added as GSC user yet. "
            f"Add {sa_email or 'SA email'} in GSC → Settings → Users & permissions")
    elif e.code == 404:
        chk("Google Search Console API", "warn",
            f"404 — GSC_SITE_URL not a verified property: {gsc_site}")
    else:
        chk("Google Search Console API", "fail", f"HTTP {e.code}: {body}")
except Exception as e:
    chk("Google Search Console API", "fail", str(e)[:80])

# SendGrid
try:
    if sg_key:
        req = urllib.request.Request(
            "https://api.sendgrid.com/v3/user/profile",
            headers={"Authorization": f"Bearer {sg_key}"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
        username = d.get("username", d.get("email", "—"))
        chk("SendGrid API", "pass", f"account: {username}")
    else:
        chk("SendGrid API", "skip", "SENDGRID_API_KEY not set")
except urllib.error.HTTPError as e:
    body = e.read().decode()[:120]
    chk("SendGrid API", "fail", f"HTTP {e.code}: {body[:80]}")
except Exception as e:
    chk("SendGrid API", "fail", str(e)[:60])

# Meta Graph API
try:
    if meta_app_id and meta_secret:
        app_token = f"{meta_app_id}|{meta_secret}"
        # Token debug endpoint — works with App Token, just confirms token validity
        debug_url = (f"https://graph.facebook.com/v19.0/debug_token"
                     f"?input_token={app_token}&access_token={app_token}")
        with urllib.request.urlopen(debug_url, timeout=15) as r:
            d = json.loads(r.read())
        data = d.get("data", {})
        is_valid = data.get("is_valid", False)
        app_id_ret = data.get("app_id", "?")
        if is_valid:
            chk("Meta Graph API: app token", "pass",
                f"valid  app_id={app_id_ret}")
        else:
            err = data.get("error", {}).get("message", "invalid token")
            chk("Meta Graph API: app token", "warn", f"not valid: {err[:60]}")
        # Page Access Token — just check it's set
        if meta_pg_tok:
            chk("Meta Graph API: page token", "pass", "set (page-level data available)")
        else:
            chk("Meta Graph API: page token", "warn",
                "META_PAGE_ACCESS_TOKEN empty — FB/IG insights unavailable")
    else:
        chk("Meta Graph API: app token",  "skip", "META_APP_ID / META_APP_SECRET not set")
        chk("Meta Graph API: page token", "skip", "META_APP_ID / META_APP_SECRET not set")
except Exception as e:
    chk("Meta Graph API", "fail", str(e)[:80])

# SendGrid sender verification
try:
    if sg_key and email_from:
        req = urllib.request.Request(
            "https://api.sendgrid.com/v3/verified_senders",
            headers={"Authorization": f"Bearer {sg_key}"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
        verified = [s["from_email"] for s in d.get("results", []) if s.get("verified")]
        if email_from in verified:
            chk("SendGrid sender verified", "pass", email_from)
        else:
            chk("SendGrid sender verified", "warn",
                f"{email_from} NOT verified — go to SendGrid → Sender Auth → Verify Single Sender")
    else:
        chk("SendGrid sender verified", "skip", "key or from address missing")
except Exception as e:
    chk("SendGrid sender verified", "warn", str(e)[:60])


# ══════════════════════════════════════════════════════════════
#  5. SCRAPERS
# ══════════════════════════════════════════════════════════════
section("5 · SCRAPERS")

# Linktree
try:
    from auditors.linktree_scraper import LinktreeScraper
    result = LinktreeScraper("https://linktr.ee/goguerrilla").scrape()
    n = len(result.get("platforms_found", []))
    chk("Linktree scraper", "pass" if n > 0 else "warn", f"{n} platforms found: {', '.join(result.get('platforms_found', []))}")
except Exception as e:
    chk("Linktree scraper", "fail", str(e)[:60])

# LinkedIn scraper
try:
    from auditors import linkedin_scraper as li
    data = li.scrape("https://www.linkedin.com/company/guerrilla-marketing-gurus-llc")
    src  = data.get("data_source", "unknown")
    flw  = data.get("followers")
    ppw  = data.get("posts_per_week")
    dslp = data.get("days_since_last_post")
    if src in ("linkedin_html", "linkedin_reachable"):
        chk("LinkedIn scraper", "pass",
            f"followers={flw}  posts/wk={ppw}  last={dslp}d ago  src={src}")
    else:
        chk("LinkedIn scraper", "warn", f"data_source={src}")
except Exception as e:
    chk("LinkedIn scraper", "fail", str(e)[:60])

# Homepage scrape (on-page SEO)
try:
    import requests as _req
    from bs4 import BeautifulSoup as BS
    r = _req.get("https://goguerrilla.xyz", timeout=15,
                 headers={"User-Agent": "Mozilla/5.0"})
    soup = BS(r.text, "html.parser")
    t_tag = soup.find("title")
    title = t_tag.get_text().strip() if t_tag else ""
    h1s   = soup.find_all("h1")
    chk("Homepage scrape", "pass" if r.status_code < 400 else "fail",
        f"HTTP {r.status_code} | title: '{title[:40]}' | {len(h1s)} H1(s)")
except Exception as e:
    chk("Homepage scrape", "fail", str(e)[:60])


# ══════════════════════════════════════════════════════════════
#  6. AUDITOR IMPORTS
# ══════════════════════════════════════════════════════════════
section("6 · AUDITOR IMPORTS")

auditors = [
    ("WebsiteAuditor",    "auditors.website_auditor",  "WebsiteAuditor"),
    ("SEOAuditor",        "auditors.seo_auditor",      "SEOAuditor"),
    ("GEOAuditor",        "auditors.geo_auditor",      "GEOAuditor"),
    ("GBPAuditor",        "auditors.gbp_auditor",      "GBPAuditor"),
    ("BrandAuditor",      "auditors.brand_auditor",    "BrandAuditor"),
    ("ICPAuditor",        "auditors.icp_auditor",      "ICPAuditor"),
    ("FunnelAuditor",     "auditors.funnel_auditor",   "FunnelAuditor"),
    ("ContentAuditor",    "auditors.content_auditor",  "ContentAuditor"),
    ("FreshnessAuditor",  "auditors.freshness_auditor","FreshnessAuditor"),
    ("SocialMediaAuditor","auditors.social_auditor",   "SocialMediaAuditor"),
    ("CompetitorAuditor", "auditors.competitor_auditor","CompetitorAuditor"),
    ("AnalyticsAuditor",  "auditors.analytics_auditor","AnalyticsAuditor"),
    ("YouTubeAuditor",    "auditors.youtube_api",      "YouTubeAuditor"),
    ("MetaAuditor",       "auditors.meta_auditor",     "MetaAuditor"),
    ("AIAnalyzer",        "analyzers.ai_analyzer",     "AIAnalyzer"),
]
for label, mod, cls in auditors:
    try:
        m = importlib.import_module(mod)
        getattr(m, cls)
        chk(f"import: {label}", "pass")
    except Exception as e:
        chk(f"import: {label}", "fail", str(e)[:60])


# ══════════════════════════════════════════════════════════════
#  7. GEO AUDITOR LIVE TEST (on-page + schema scrape only)
# ══════════════════════════════════════════════════════════════
section("7 · GEO AUDITOR — ON-PAGE + SCHEMA LIVE TEST")
try:
    from config import ClientConfig
    from auditors.geo_auditor import GEOAuditor
    cfg = ClientConfig(
        client_name="Guerrilla Marketing Group",
        website_url="https://goguerrilla.xyz",
        stated_target_market="financial advisors CPAs attorneys fractional CMO",
        linkedin_url="https://www.linkedin.com/company/guerrilla-marketing-gurus-llc",
        youtube_channel_url="https://www.youtube.com/@gogmg",
        facebook_page_url="https://www.facebook.com/gogmg",
    )
    geo = GEOAuditor(cfg, {})
    onpage = geo._scrape_homepage()
    if onpage:
        chk("GEO: homepage scrape",    "pass", f"{onpage.get('word_count',0)} words")
        chk("GEO: title tag",
            "pass" if onpage.get("title") else "warn",
            f"'{onpage.get('title','MISSING')[:50]}'")
        chk("GEO: meta description",
            "pass" if onpage.get("meta_description") else "warn",
            f"{len(onpage.get('meta_description',''))} chars")
        chk("GEO: H1 count",
            "pass" if onpage.get("h1s") else "warn",
            f"{len(onpage.get('h1s',[]))} found")
        chk("GEO: H2 count",
            "pass" if len(onpage.get("h2s",[])) >= 2 else "warn",
            f"{len(onpage.get('h2s',[]))} found")
        chk("GEO: schema types",
            "pass" if onpage.get("schema_types") else "warn",
            ", ".join(onpage.get("schema_types",[])[:4]) or "none detected")
        chk("GEO: FAQPage schema",
            "pass" if onpage.get("has_faq_schema") else "warn",
            "present" if onpage.get("has_faq_schema") else "not found")
    else:
        chk("GEO: homepage scrape", "fail", "returned empty dict")

    # GSC connectivity
    gsc_rows = geo._fetch_gsc_rows()
    if gsc_rows:
        top = gsc_rows[0]["query"]
        chk("GEO: Search Console rows", "pass",
            f"{len(gsc_rows)} keywords, top: '{top}'")
    else:
        chk("GEO: Search Console rows", "warn",
            "0 rows — service account not yet added as GSC user (score 50 neutral)")
except Exception as e:
    chk("GEO auditor live test", "fail", traceback.format_exc().strip().splitlines()[-1][:80])


# ══════════════════════════════════════════════════════════════
#  8. COMPETITOR AUDITOR
# ══════════════════════════════════════════════════════════════
section("8 · COMPETITOR AUDITOR")
try:
    from config import ClientConfig
    from auditors.competitor_auditor import CompetitorAuditor
    cfg2 = ClientConfig(
        client_name="Test Client",
        website_url="https://goguerrilla.xyz",
        competitor_urls=["https://contentmarketinginstitute.com"],
    )
    ca = CompetitorAuditor(cfg2, {}, pagespeed_api_key=pagespeed_key)
    chk("CompetitorAuditor: init", "pass", f"{len(ca.competitor_urls)} URL(s) loaded")
    comp = ca._audit_competitor("https://contentmarketinginstitute.com")
    chk("CompetitorAuditor: scrape",
        "pass" if comp.get("reachable") else "warn",
        f"SEO={comp.get('seo_score')}  Perf={comp.get('performance_score')}  "
        f"Social={comp.get('social_channel_count')} channels")
except Exception as e:
    chk("CompetitorAuditor", "fail", traceback.format_exc().strip().splitlines()[-1][:80])


# ══════════════════════════════════════════════════════════════
#  9. REPORT GENERATORS
# ══════════════════════════════════════════════════════════════
section("9 · REPORT GENERATORS")

# PDF generator import + instantiation
try:
    from config import ClientConfig
    from reports.pdf_generator import PDFReportGenerator
    cfg3 = ClientConfig(client_name="Diag Test", website_url="https://example.com")
    gen  = PDFReportGenerator(cfg3, {})
    chk("PDFReportGenerator: import", "pass")
except Exception as e:
    chk("PDFReportGenerator: import", "fail", str(e)[:60])

# Teaser PDF generator
try:
    from reports.teaser_pdf import generate_teaser
    chk("teaser_pdf: import", "pass")
except Exception as e:
    chk("teaser_pdf: import", "fail", str(e)[:60])

# DOCX generator
try:
    from reports.docx_generator import DocxReportGenerator
    chk("DocxReportGenerator: import", "pass")
except Exception as e:
    chk("DocxReportGenerator: import", "fail", str(e)[:60])

# Email sender
try:
    from reports.email_sender import send_report
    chk("email_sender: import", "pass")
except Exception as e:
    chk("email_sender: import", "fail", str(e)[:60])


# ══════════════════════════════════════════════════════════════
#  10. DATABASE
# ══════════════════════════════════════════════════════════════
section("10 · DATABASE")
try:
    conn = sqlite3.connect("cash_clients.db")
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM clients")
    n = cur.fetchone()[0]
    cur.execute("PRAGMA table_info(clients)")
    cols = [row[1] for row in cur.fetchall()]
    conn.close()
    chk("SQLite DB: accessible", "pass", f"{n} audit record(s)")
    chk("SQLite DB: overall_score col",
        "pass" if "overall_score" in cols else "fail",
        "present" if "overall_score" in cols else "MISSING — run migration")
    for required_col in ["cash_c", "cash_a", "cash_s", "cash_h", "geo_score"]:
        chk(f"SQLite DB: {required_col} col",
            "pass" if required_col in cols else "fail",
            "present" if required_col in cols else "MISSING")
except Exception as e:
    chk("SQLite DB", "fail", str(e)[:60])


# ══════════════════════════════════════════════════════════════
#  11. CONFIG / RUN SCRIPT
# ══════════════════════════════════════════════════════════════
section("11 · CONFIG & RUN SCRIPT")
try:
    from config import ClientConfig
    cfg4 = ClientConfig(
        client_name="Diag",
        website_url="https://example.com",
        competitor_urls=["https://example.com"],
    )
    chk("ClientConfig: instantiate", "pass")
    chk("ClientConfig: competitor_urls field",
        "pass" if hasattr(cfg4, "competitor_urls") else "fail")
    chk("ClientConfig: stated_target_market",
        "pass" if hasattr(cfg4, "stated_target_market") else "fail")
except Exception as e:
    chk("ClientConfig", "fail", str(e)[:60])

try:
    import ast
    with open("run_goguerrilla.py") as f:
        ast.parse(f.read())
    chk("run_goguerrilla.py: syntax", "pass")
except SyntaxError as e:
    chk("run_goguerrilla.py: syntax", "fail", str(e)[:60])
except Exception as e:
    chk("run_goguerrilla.py: syntax", "fail", str(e)[:60])


# ══════════════════════════════════════════════════════════════
#  SUMMARY
# ══════════════════════════════════════════════════════════════
print(f"\n{'═' * 62}")
print("  DIAGNOSTIC SUMMARY")
print(f"{'═' * 62}")
passes = sum(1 for _, tag, _ in results if "PASS" in tag)
warns  = sum(1 for _, tag, _ in results if "WARN" in tag)
fails  = sum(1 for _, tag, _ in results if "FAIL" in tag)
skips  = sum(1 for _, tag, _ in results if "SKIP" in tag)
total  = len(results)
print(f"  Total checks : {total}")
print(f"  {PASS}     : {passes}")
print(f"  {WARN}     : {warns}")
print(f"  {FAIL}     : {fails}")
print(f"  {SKIP}     : {skips}")

if fails:
    print(f"\n  ❌ FAILURES ({fails}):")
    for label, tag, note in results:
        if "FAIL" in tag:
            print(f"     • {label}: {note}")

if warns:
    print(f"\n  ⚠️  WARNINGS ({warns}):")
    for label, tag, note in results:
        if "WARN" in tag:
            print(f"     • {label}: {note}")

print(f"\n{'═' * 62}\n")
sys.exit(1 if fails else 0)
