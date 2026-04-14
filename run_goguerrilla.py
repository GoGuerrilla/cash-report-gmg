#!/usr/bin/env python3
"""
Full marketing audit for Guerrilla Marketing Group (goguerrilla.xyz)
Linktree: https://linktr.ee/goguerrilla
Stated ICP: Financial advisors, CPAs, attorneys, law firms, fractional CFOs,
            and corporations seeking fractional CMO services

All data is scraped live on each run. Social platforms that block public
scraping (Instagram, Facebook, YouTube) are scored at 50 neutral
and flagged as api_blocked — never invented or hardcoded.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

# Load .env from project root before any other imports
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(_env_path)
except ImportError:
    pass

from config import ClientConfig
from auditors.youtube_api import YouTubeAuditor
from auditors.competitor_auditor import CompetitorAuditor
from auditors.analytics_auditor import AnalyticsAuditor
from auditors.gbp_auditor import GBPAuditor
from auditors.linktree_scraper import LinktreeScraper
from auditors import linkedin_scraper as _li_scraper
from auditors.brand_auditor import BrandAuditor
from auditors.funnel_auditor import FunnelAuditor
from auditors.icp_auditor import ICPAuditor
from auditors.freshness_auditor import FreshnessAuditor
from auditors.social_auditor import SocialMediaAuditor
from auditors.content_auditor import ContentAuditor
from auditors.website_auditor import WebsiteAuditor
from auditors.seo_auditor import SEOAuditor
from auditors.geo_auditor import GEOAuditor
from analyzers.ai_analyzer import AIAnalyzer
from reports.docx_generator import DocxReportGenerator
from reports.pdf_generator import PDFReportGenerator
from reports.email_sender import send_report
from intake.client_db import save_audit_result
from intake.rate_limiter import RateLimiter, get_public_ip
from auditors.meta_auditor import MetaAuditor


def _check_env_keys():
    """Print API key status at startup — no secrets logged, just pass/fail."""
    ps_key    = os.environ.get("PAGESPEED_API_KEY", "")
    ai_key    = os.environ.get("ANTHROPIC_API_KEY", "")
    yt_key    = os.environ.get("YOUTUBE_API_KEY", "")
    ga_prop   = os.environ.get("GOOGLE_ANALYTICS_PROPERTY_ID", "")
    ga_sa     = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "")
    email_to  = os.environ.get("REPORT_EMAIL_TO", "")
    email_from = os.environ.get("REPORT_EMAIL_FROM", "")
    email_pw  = os.environ.get("REPORT_EMAIL_PASSWORD", "")
    email_ok  = bool(email_to and email_from and email_pw)
    print("  API keys:")
    print(f"    PageSpeed  : {'✅ loaded' if ps_key else '⚠️  not set — free-tier rate limits apply'}")
    print(f"    YouTube    : {'✅ loaded' if yt_key else '⚠️  not set — YouTube scored at 50 neutral'}")
    print(f"    Analytics  : {'✅ property ' + ga_prop if ga_prop and ga_sa else '⚠️  not configured — website traffic scored at 50 neutral'}")
    meta_id  = os.environ.get("META_APP_ID", "")
    meta_tok = os.environ.get("META_PAGE_ACCESS_TOKEN", "").strip()
    print(f"    Anthropic  : {'✅ loaded' if ai_key else '⚠️  not set — narrative sections will use rule-based fallback'}")
    print(f"    Meta API   : {'✅ App ID loaded' + (' + Page Token' if meta_tok else ' (Page Token missing — IG metrics limited)') if meta_id else '⚠️  not set — Facebook/Instagram scored at 50 neutral'}")
    print(f"    Email      : {'✅ → ' + email_to if email_ok else '⚠️  not configured — report saved locally only'}")


def _build_base_channel_data(website_url: str) -> dict:
    """
    Minimal preloaded_channel_data skeleton — no invented metrics.
    Social channel booleans / counts are populated by live scraping where
    possible; blocked platforms use None (scored 50 neutral by auditors).
    The 'website' sub-dict is overwritten after WebsiteAuditor runs.
    """
    return {
        # Social channels that block all public scraping — neutral 50, no data
        "linkedin":  {"followers": None, "posts_per_week": None,
                      "days_since_last_post": None, "is_active": None,
                      "content_topics": [], "post_themes": [],
                      "services_listed": [], "engagement_level": None},
        "instagram": {"handle": None, "is_active": None,
                      "posts_per_week": None, "days_since_last_post": None},
        "youtube":   {"channel": None, "is_active": None,
                      "recent_video_count": None,
                      "posts_per_week": None, "days_since_last_post": None},
        "facebook":  {"page": None, "is_active": None,
                      "posts_per_week": None, "days_since_last_post": None},
        "discord":   {"members": None, "is_active": None,
                      "posts_per_week": None, "days_since_last_post": None},
        # Website placeholder — overwritten by WebsiteAuditor results
        "website": {
            "platform": None,
            "target_audience_mentioned": None,
            "icp_mentions": [],
            "has_lead_magnet": None,
            "has_email_optin": None,
            "has_contact_form": None,
            "has_newsletter": None,
            "has_blog": None,
            "has_podcast": None,
            "has_pricing": None,
            "has_case_studies": None,
            "has_proposal_cta": None,
            "has_free_trial": None,
            "has_testimonials": None,
            "has_certifications": None,
            "has_media_mentions": None,
            "has_client_logos": None,
            "booking_url": None,
            "pages": [],
            "web3_content": None,
        },
    }


def _merge_website_data(channel_data: dict, website_audit: dict):
    """
    Extract what we can from the live WebsiteAuditor result and write it into
    preloaded_channel_data["website"] so ICP/funnel auditors get fresh data.

    WebsiteAuditor returns: url, status, homepage (page analysis dict),
    pages (list), scores (dict), issues, strengths.
    We derive boolean flags from the live homepage text analysis.
    """
    if website_audit.get("status") not in ("ok",):
        return  # Crawl failed — keep None placeholders

    homepage = website_audit.get("homepage", {})
    pages    = website_audit.get("pages", [])
    site     = channel_data["website"]

    # Combine all page text for keyword detection
    all_text = " ".join(
        " ".join([
            p.get("title", ""),
            p.get("meta_description", ""),
            " ".join(p.get("h1_text", [])),
        ])
        for p in pages
    ).lower()

    def _has(*keywords) -> bool:
        return any(kw in all_text for kw in keywords)

    # Derive boolean flags from crawled content
    site["has_lead_magnet"]   = _has("free guide", "free download", "lead magnet",
                                      "free ebook", "free resource", "free checklist")
    site["has_email_optin"]   = _has("subscribe", "sign up", "opt in", "opt-in",
                                      "join our list", "get updates") or \
                                 homepage.get("has_email_visible", False)
    site["has_contact_form"]  = _has("contact us", "get in touch", "send a message",
                                      "contact form", "reach out")
    site["has_newsletter"]    = _has("newsletter", "weekly email", "biweekly",
                                      "subscribe to our")
    site["has_blog"]          = _has("blog", "article", "post", "read more",
                                      "latest news")
    site["has_podcast"]       = _has("podcast", "listen", "episode", "spotify")
    site["has_pricing"]       = _has("pricing", "price", "per month", "/month",
                                      "starting at", "packages")
    site["has_case_studies"]  = _has("case study", "case studies", "success story",
                                      "client results", "how we helped")
    site["has_proposal_cta"]  = _has("get a proposal", "free audit", "free consultation",
                                      "request a quote", "book a call")
    site["has_free_trial"]    = _has("free trial", "try for free", "start free")
    site["has_testimonials"]  = _has("testimonial", "review", "what our clients",
                                      "five star", "★", "⭐")
    site["has_certifications"] = _has("certified", "certification", "google partner",
                                       "hubspot", "credential")
    site["has_media_mentions"] = _has("featured in", "as seen in", "press", "media")
    site["has_client_logos"]   = _has("our clients", "clients include", "trusted by",
                                       "worked with")

    # ICP signal detection
    icp_keywords = [
        "financial advisor", "financial advisors", "cpa", "cpas",
        "attorney", "attorneys", "law firm", "law firms",
        "fractional cfo", "fractional cmo", "professional services",
        "business professional", "business professionals",
        "payment", "fintech", "finance company",
        "small business", "medium business", "smb",
        "corporation", "corporations",
    ]
    found_icp = [kw for kw in icp_keywords if kw in all_text]
    site["icp_mentions"] = found_icp
    site["target_audience_mentioned"] = ", ".join(found_icp[:4]) if found_icp else ""

    # Page list
    site["pages"] = [p.get("url", "") for p in pages if p.get("url")]

    # Web3 content
    site["web3_content"] = _has("web3", "nft", "blockchain", "crypto", "defi", "dao")

    # Platform detection (from generator/X-Powered-By headers if available)
    # WebsiteAuditor doesn't capture response headers — leave as None


def run_audit():
    print("\n" + "="*60)
    print("  C.A.S.H. REPORT BY GMG — GUERRILLA MARKETING GROUP")
    print("  Content · Audience · Sales · Hold (Retention)")
    print("  Target ICP: Financial Advisors, CPAs, Attorneys, Law Firms, Fractional CFOs & Corporations")
    print("="*60 + "\n")

    _check_env_keys()
    print()

    # ── Rate limit check ──────────────────────────────────────
    # BYPASS_RATE_LIMIT=1 skips all checks (set in .env for internal GMG use).
    rl         = RateLimiter()   # reads BYPASS_RATE_LIMIT from env
    _audit_email   = "gmg@goguerrilla.xyz"
    _audit_website = "https://www.goguerrilla.xyz/"
    _audit_ip      = get_public_ip() if not rl.bypass else None

    allowed, reason = rl.check(
        email       = _audit_email,
        website_url = _audit_website,
        ip_address  = _audit_ip,
    )
    if not allowed:
        print("\n" + "─"*60)
        print("  ⛔  AUDIT BLOCKED BY RATE LIMITER")
        print("─"*60)
        for line in reason.split("\n"):
            print(f"  {line}")
        print("─"*60 + "\n")
        return

    # ── Channel data skeleton (all None — filled by live scrapers) ──
    channel_data = _build_base_channel_data("https://www.goguerrilla.xyz/")

    # ── Config ────────────────────────────────────────────────
    config = ClientConfig(
        client_name="Guerrilla Marketing Group",
        client_industry="B2B Marketing Agency",
        industry_category="B2B Services",
        website_url="https://www.goguerrilla.xyz/",
        linktree_url="https://linktr.ee/goguerrilla",
        facebook_page_url="https://www.facebook.com/GuerrillaMarketingGroup",
        instagram_handle="go.guerrilla",
        linkedin_url="https://www.linkedin.com/company/guerrilla-marketing-gurus-llc/",
        youtube_channel_url="https://www.youtube.com/@goguerrilla",
        discord_url="https://discord.gg/cA2nfpY2xG",
        monthly_ad_budget=0,
        team_size=2,
        primary_goal="B2B client acquisition for professional services firms",
        target_audience=(
            "Business professionals, financial advisors, CPAs, attorneys, "
            "payment and finance companies, small and medium businesses, "
            "and corporations seeking fractional CMO services"
        ),
        stated_target_market=(
            "Business professionals, financial advisors, CPAs, attorneys, "
            "payment and finance companies, small and medium businesses, "
            "and corporations seeking fractional CMO services"
        ),
        stated_icp_industry=(
            "Professional services / Financial services / Legal / "
            "Payments & Fintech / SMB / Fractional C-Suite"
        ),
        stated_value_prop=(
            "Bold content marketing that helps financial advisors, CPAs, attorneys, "
            "payment and finance companies, small and medium businesses, and "
            "corporations build authority, generate referrals, and grow their "
            "client base through fractional CMO-level strategy."
        ),
        current_client_count=0,
        current_client_types="",
        email_list_size=0,
        has_email_marketing=False,
        has_active_newsletter=False,
        has_referral_system=False,
        referral_system_description="",
        has_lead_magnet=False,
        booking_tool="Google Calendar",
        platform_posting_frequency={},
        intake_completed=True,
        preloaded_channel_data=channel_data,
        top_competitors=[
            "FMG Suite", "Twenty Over Ten", "Snappy Kraken",
            "Broadridge Advisor Solutions", "Vestorly",
            "localmarketingagency.com", "contentmarketinginstitute.com",
            "guerrillamarketing.com",
        ],
        competitor_urls=[
            "localmarketingagency.com",
            "contentmarketinginstitute.com",
            "guerrillamarketing.com",
        ],
        agency_name="C.A.S.H. Report by GMG",
    )

    audit_data = {}

    # ── 1. Linktree — live scrape every time ──────────────────
    print("→ Scraping Linktree (live)...")
    linktree_data = LinktreeScraper(config.linktree_url).scrape()
    status = linktree_data.get("scrape_status", "unknown")
    verified = linktree_data.get("data_verified", False)
    platforms = linktree_data.get("platforms_found", [])
    if verified:
        print(f"  ✅ Linktree scraped — {len(platforms)} platforms found: {', '.join(platforms)}")
        print(f"     Bio: {linktree_data.get('bio','')[:80]}")
    else:
        print(f"  ⚠️  Linktree scrape failed ({status}) — Linktree data unverified, scored neutral")
    audit_data["linktree"] = linktree_data

    # ── 1b. LinkedIn company page scrape ─────────────────────
    if config.linkedin_url:
        print("→ Scraping LinkedIn company page (live)...")
        li_data = _li_scraper.scrape(config.linkedin_url)
        src = li_data.get("data_source", "unknown")
        if src == "linkedin_html":
            ppw  = li_data.get("posts_per_week")
            days = li_data.get("days_since_last_post")
            fol  = li_data.get("followers")
            print(f"  ✅ LinkedIn: {fol or '?'} followers · "
                  f"{ppw or '?'}x/week · last post {days} day(s) ago")
        else:
            print(f"  ⚠️  LinkedIn scrape partial ({src}) — frequency unverified")
        # Merge into preloaded channel data so freshness auditor picks it up
        channel_data["linkedin"].update({
            k: v for k, v in li_data.items()
            if k in ("followers", "posts_per_week", "days_since_last_post", "is_active")
            and v is not None
        })

    # ── 1d. YouTube Data API v3 ───────────────────────────────
    yt_key = os.environ.get("YOUTUBE_API_KEY", "")
    if yt_key:
        print("→ Fetching YouTube channel data (live via YouTube Data API v3)...")
        yt_data = YouTubeAuditor("@goguerrilla", yt_key).fetch()
        if yt_data.get("data_source") == "youtube_api_v3":
            # Merge live data into channel skeleton so all downstream auditors see it
            channel_data["youtube"].update(yt_data)
            audit_data["youtube"] = yt_data
            subs  = yt_data.get("subscriber_count", 0)
            vids  = yt_data.get("total_video_count", 0)
            last  = yt_data.get("days_since_last_post", "?")
            v30   = yt_data.get("videos_last_30_days", 0)
            print(f"  ✅ YouTube: {subs:,} subscribers · {vids:,} total videos · "
                  f"{v30} uploads (last 30 days) · last upload {last} day(s) ago")
        else:
            err = yt_data.get("error", "unknown error")
            print(f"  ⚠️  YouTube API failed ({err}) — channel scored at 50 neutral")
            audit_data["youtube"] = yt_data
    else:
        print("  ⚠️  YOUTUBE_API_KEY not set — YouTube channel scored at 50 neutral")

    # ── 1e. Meta Graph API (Facebook + Instagram) ────────────
    meta_app_id  = os.environ.get("META_APP_ID", "")
    meta_secret  = os.environ.get("META_APP_SECRET", "")
    meta_pg_tok  = os.environ.get("META_PAGE_ACCESS_TOKEN", "").strip()
    if meta_app_id and meta_secret:
        print("→ Fetching Meta data (Facebook + Instagram via Graph API)...")
        meta_result = MetaAuditor(
            app_id             = meta_app_id,
            app_secret         = meta_secret,
            facebook_page_id   = "GuerrillaMarketingGroup",
            instagram_handle   = "go.guerrilla",
            page_access_token  = meta_pg_tok,
        ).fetch()

        fb = meta_result.get("facebook", {})
        ig = meta_result.get("instagram", {})

        # Merge Facebook live data into channel skeleton
        for key in ("followers", "fan_count", "posts_per_week",
                    "days_since_last_post", "is_active", "engagement_rate",
                    "reach_28d", "engagements_28d", "data_source"):
            if fb.get(key) is not None:
                channel_data["facebook"][key] = fb[key]

        # Merge Instagram live data into channel skeleton
        for key in ("followers", "posts_per_week", "days_since_last_post",
                    "is_active", "engagement_rate", "avg_likes_per_post",
                    "avg_comments_per_post", "data_source"):
            if ig.get(key) is not None:
                channel_data["instagram"][key] = ig[key]

        audit_data["meta"] = meta_result

        # Print summary
        fb_src = fb.get("data_source", "?")
        if fb_src == "meta_graph_api":
            fol   = fb.get("followers", 0)
            ppw   = fb.get("posts_per_week")
            days  = fb.get("days_since_last_post")
            er    = fb.get("engagement_rate")
            reach = fb.get("reach_28d")
            print(f"  ✅ Facebook: {fol:,} followers"
                  + (f" · {ppw}x/week" if ppw else "")
                  + (f" · last post {days}d ago" if days is not None else "")
                  + (f" · ER {er}%" if er else "")
                  + (f" · reach {reach:,}" if reach else ""))
        else:
            print(f"  ⚠️  Facebook API: {fb.get('error', fb_src)}")

        ig_src = ig.get("data_source", "?")
        if ig_src == "meta_graph_api":
            ig_fol  = ig.get("followers", 0)
            ig_ppw  = ig.get("posts_per_week")
            ig_days = ig.get("days_since_last_post")
            ig_er   = ig.get("engagement_rate")
            print(f"  ✅ Instagram: {ig_fol:,} followers"
                  + (f" · {ig_ppw}x/week" if ig_ppw else "")
                  + (f" · last post {ig_days}d ago" if ig_days is not None else "")
                  + (f" · ER {ig_er}%" if ig_er else ""))
        elif ig_src == "meta_no_page_token":
            print("  ⚠️  Instagram: add META_PAGE_ACCESS_TOKEN to .env for live IG metrics")
        elif ig_src == "meta_no_ig_linked":
            print("  ⚠️  Instagram: no IG Business Account linked to the Facebook Page")
        else:
            print(f"  ⚠️  Instagram API: {ig.get('error', ig_src)}")
    else:
        print("  ⚠️  META_APP_ID / META_APP_SECRET not set — Facebook/Instagram scored at 50 neutral")

    # ── 2. Website & SEO ──────────────────────────────────────
    print("→ Auditing website...")
    website_auditor = WebsiteAuditor(config.website_url, max_pages=5)
    audit_data["website"] = website_auditor.run()

    # Merge live website data into preloaded config for downstream auditors
    _merge_website_data(channel_data, audit_data["website"])

    print("→ Auditing SEO...")
    pagespeed_key = os.environ.get("PAGESPEED_API_KEY", "")
    audit_data["seo"] = SEOAuditor(config.website_url, api_key=pagespeed_key).run()
    seo_method = audit_data["seo"].get("method", "unknown")
    seo_score  = audit_data["seo"].get("score", "—")
    print(f"  SEO score: {seo_score}/100  (method: {seo_method})")

    print("→ Auditing GEO (AI visibility)...")
    try:
        audit_data["geo"] = GEOAuditor(config, audit_data["seo"]).run()
    except Exception as _geo_err:
        print(f"  ⚠️  GEO auditor failed ({_geo_err}) — scored at 50 neutral")
        audit_data["geo"] = {"score": 50, "grade": "C", "issues": [], "strengths": []}

    print("→ Auditing Google Business Profile...")
    places_key = os.environ.get("GOOGLE_PLACES_API_KEY", os.environ.get("PAGESPEED_API_KEY", ""))
    gbp_result = GBPAuditor(
        business_name="Guerrilla Marketing Group",
        website_url=config.website_url,
        api_key=places_key,
    ).run()
    audit_data["gbp"] = gbp_result
    if gbp_result.get("found"):
        print(f"  ✅ GBP found: {gbp_result.get('business_name')} · "
              f"{gbp_result.get('review_count')} reviews · {gbp_result.get('photo_count')} photos · "
              f"Score: {gbp_result.get('score')}/100")
    else:
        print(f"  ⚠️  GBP: {gbp_result.get('note', 'Not found')} — scored at 50 neutral")

    print("→ Fetching Google Analytics traffic data...")
    ga_prop = os.environ.get("GOOGLE_ANALYTICS_PROPERTY_ID", "")
    ga_sa   = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "")
    if ga_sa and not os.path.isfile(ga_sa):
        print(f"  ⚠️  GA service account file not found at {ga_sa!r} — skipping GA (score=50)")
        ga_sa = ""
    try:
        ga_result = AnalyticsAuditor(property_id=ga_prop, service_account_json_path=ga_sa).run()
    except Exception as _ga_err:
        print(f"  ⚠️  GA auditor failed ({_ga_err}) — scored at 50 neutral")
        ga_result = {
            "score": 50, "grade": "C", "data_source": "not_available",
            "note": "Google Analytics unavailable — score set to neutral.",
            "monthly_visitors": None, "traffic_trend_pct": None,
            "traffic_trend_label": "—", "bounce_rate_pct": None,
            "avg_session_duration": "—", "top_traffic_sources": [],
            "top_landing_pages": [], "issues": [], "strengths": [],
        }
    audit_data["analytics"] = ga_result
    if ga_result.get("data_source") == "google_analytics_data_api_v4":
        visitors = ga_result.get("monthly_visitors", 0)
        trend    = ga_result.get("traffic_trend_label", "—")
        print(f"  ✅ Analytics: {visitors:,} visitors/month · Trend: {trend}")
    else:
        print(f"  ⚠️  Analytics: {ga_result.get('note', 'No data')} — scored at 50 neutral")

    # ── 3. Social media ───────────────────────────────────────
    print("→ Auditing social channels...")
    audit_data["social"] = SocialMediaAuditor(config).run()

    # ── 4. Content efficiency ─────────────────────────────────
    print("→ Auditing content efficiency...")
    audit_data["content"] = ContentAuditor(config, audit_data).run()

    # ── 5. Brand consistency ──────────────────────────────────
    print("→ Auditing brand consistency...")
    audit_data["brand"] = BrandAuditor(config, linktree_data).run()

    # ── 6. Lead funnel ────────────────────────────────────────
    print("→ Auditing lead funnel...")
    audit_data["funnel"] = FunnelAuditor(config, linktree_data).run()

    # ── 7. ICP alignment ──────────────────────────────────────
    print("→ Auditing ICP alignment (stated vs actual)...")
    audit_data["icp"] = ICPAuditor(config, linktree_data).run()

    # ── 8. Content freshness ──────────────────────────────────
    print("→ Auditing content freshness...")
    audit_data["freshness"] = FreshnessAuditor(config, linktree_data).run()

    # ── 9. Competitor analysis ────────────────────────────────
    _ca = CompetitorAuditor(config, audit_data, pagespeed_api_key=pagespeed_key)
    if _ca.competitor_urls:
        from urllib.parse import urlparse as _up
        _domains = [_up(u).netloc or u for u in _ca.competitor_urls]
        print(f"→ Auditing {len(_ca.competitor_urls)} competitor(s): {', '.join(_domains)}")
        audit_data["competitor"] = _ca.run()
    else:
        print("→ Competitor analysis skipped — no URLs provided.")
        audit_data["competitor"] = {"skipped": True, "note": "No competitor URLs provided.",
                                    "competitors": [], "comparison": {}}

    # ── 10. AI/rule-based synthesis ───────────────────────────
    print("→ Running AI synthesis...")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    audit_data["ai_insights"] = AIAnalyzer(anthropic_api_key=anthropic_key).analyze(config, audit_data)

    # ── 11. Print C.A.S.H. score summary ─────────────────────
    ai = audit_data.get("ai_insights", {})
    cash_scores = ai.get("component_scores", {})
    print("\n" + "─"*60)
    print("  C.A.S.H. SCORE SUMMARY")
    print("─"*60)
    print(f"  {'C — Content':<28} {ai.get('cash_c_score', cash_scores.get('C', '—'))}/100")
    print(f"  {'A — Audience':<28} {ai.get('cash_a_score', cash_scores.get('A', '—'))}/100")
    print(f"  {'S — Sales':<28} {ai.get('cash_s_score', cash_scores.get('S', '—'))}/100")
    print(f"  {'H — Hold / Retention':<28} {ai.get('cash_h_score', cash_scores.get('H', '—'))}/100")
    print("─"*60)
    print(f"  {'OVERALL C.A.S.H. SCORE':<28} {ai.get('overall_score', '—')}/100  ({ai.get('overall_grade', '')})")
    print("─"*60)
    geo = audit_data.get("geo", {})
    if geo:
        print(f"  {'GEO (AI Visibility)':<28} {geo.get('score', '—')}/100  ({geo.get('grade', '')})")

    lt_note = "" if verified else "  ⚠️  Linktree data unverified — platform scores reflect neutral 50\n"
    if lt_note:
        print(lt_note)

    verdict = audit_data.get("icp", {}).get("icp_verdict", "")
    if verdict:
        print(f"\n  ICP VERDICT:\n  {verdict[:220]}...\n")

    # ── 11. Generate PDF report (primary) ────────────────────
    import shutil
    from datetime import datetime as _dt
    import re as _re

    os.makedirs("reports", exist_ok=True)
    slug = config.client_name.lower().replace(" ", "_").replace("/", "_")
    pdf_path  = f"reports/{slug}_cash_report.pdf"
    docx_path = f"reports/{slug}_cash_report.docx"

    print(f"\n→ Generating C.A.S.H. Report PDF: {pdf_path}")
    PDFReportGenerator(config, audit_data).generate(pdf_path)
    print(f"✅  PDF report saved: {pdf_path}")
    print(f"    Open with: open \"{pdf_path}\"\n")

    # ── Archive copy to ~/Desktop/CASH GMG Audit/Client Reports/
    try:
        today      = _dt.now()
        safe_name  = _re.sub(r"[^a-zA-Z0-9]+", "_", config.client_name).strip("_")
        filename   = f"{safe_name}_CASH_Report_{today.strftime('%Y-%m-%d')}.pdf"
        archive_dir = os.path.expanduser(
            f"~/Desktop/CASH GMG Audit/Client Reports/{today.strftime('%Y')}/{today.strftime('%B')}"
        )
        os.makedirs(archive_dir, exist_ok=True)
        dest = os.path.join(archive_dir, filename)
        shutil.copy2(os.path.abspath(pdf_path), dest)
        print(f"✅  Report archived → {dest}")
    except Exception as _arc_err:
        print(f"⚠️  Archive copy failed (non-fatal): {_arc_err}")

    # ── 11b. Generate Word document (backup) ──────────────────
    try:
        DocxReportGenerator(config, audit_data).generate(docx_path)
        print(f"✅  Word backup saved: {docx_path}\n")
    except Exception as e:
        print(f"⚠️  Word backup skipped: {e}\n")

    # ── 11c. Email full PDF report ────────────────────────────
    send_report(
        report_path   = os.path.abspath(pdf_path),
        client_name   = config.client_name,
        overall_score = ai.get("overall_score"),
        overall_grade = ai.get("overall_grade"),
    )

    # ── 12. Save to client database ───────────────────────────
    print("→ Saving audit result to cash_clients.db...")
    try:
        row_id = save_audit_result(
            client_name   = config.client_name,
            email         = getattr(config, "contact_email", ""),
            business_type = config.client_industry,
            website       = config.website_url,
            audit_data    = audit_data,
            ai_insights   = audit_data.get("ai_insights", {}),
            report_path   = os.path.abspath(pdf_path),
        )
        print(f"✅  Client record saved (row #{row_id})\n")
    except Exception as e:
        print(f"⚠️  DB save failed: {e}\n")

    # ── 13. Log audit for rate limiting ───────────────────────
    try:
        rl.log(
            email       = _audit_email,
            website_url = _audit_website,
            ip_address  = _audit_ip,
        )
    except Exception:
        pass

    return audit_data


if __name__ == "__main__":
    run_audit()
