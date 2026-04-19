from dataclasses import dataclass, field
from typing import List, Dict, Any


def grade(score: int) -> str:
    """Universal grade function: A=80+, B=65+, C=50+, D=35+, F=below 35."""
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    if score >= 35: return "D"
    return "F"


@dataclass
class ClientConfig:
    # ── Identity ──────────────────────────────────────────────
    client_name: str = "Client"
    contact_email: str = ""            # primary contact email for DB records
    phone_number: str = ""             # optional contact phone number
    marketing_consent: bool = False    # client consented to marketing communications
    client_industry: str = "General"   # free-text description
    industry_category: str = "Other"   # canonical subcategory: one of industry_benchmarks.INDUSTRIES
    client_category: str = ""          # parent group: one of industry_benchmarks.INDUSTRY_GROUPS
    website_url: str = ""
    linktree_url: str = ""
    agency_name: str = "C.A.S.H. Report by GMG"

    # ── Social channels ───────────────────────────────────────
    facebook_page_url: str = ""
    instagram_handle: str = ""
    linkedin_url: str = ""
    youtube_channel_url: str = ""
    tiktok_handle: str = ""
    discord_url: str = ""

    # ── API keys ──────────────────────────────────────────────
    meta_access_token: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    pagespeed_api_key: str = ""   # Google PageSpeed Insights API key
    ga_property_id: str = ""      # Google Analytics 4 property ID (numeric, e.g. "123456789")

    # ── Intake questionnaire answers ──────────────────────────
    monthly_ad_budget: float = 0.0
    team_size: int = 1
    primary_goal: str = "Brand awareness"
    target_audience: str = ""
    stated_target_market: str = ""       # e.g. "Financial advisors and CPAs"
    stated_icp_industry: str = ""        # e.g. "Financial services / Wealth management"
    stated_value_prop: str = ""          # what they claim to deliver to the ICP
    current_client_count: int = 0
    current_client_types: str = ""       # e.g. "RIAs, solo financial advisors"
    email_list_size: int = 0
    has_active_newsletter: bool = False
    has_referral_system: bool = False
    referral_system_description: str = ""
    intake_completed: bool = False

    # ── Intake: posting frequency & marketing tools ───────────
    # Keys match platform names: "LinkedIn", "Instagram", "YouTube", etc.
    platform_posting_frequency: Dict[str, float] = field(default_factory=dict)
    has_email_marketing: bool = False   # separate from newsletter (bulk campaigns)
    has_lead_magnet: bool = False        # free resource in exchange for email
    booking_tool: str = ""              # e.g. "Calendly", "Google Calendar", "None"
    team_hourly_rate: float = 0.0       # client-provided; 0 = not provided, skip cost estimates
    email_send_frequency: str = ""      # e.g. "weekly", "monthly", "biweekly", "never"

    # ── Competitive analysis ──────────────────────────────────
    competitor_urls: List[str] = field(default_factory=list)   # up to 3 competitor URLs
    biggest_marketing_challenge: str = ""

    # ── Pre-loaded channel data ────────────────────────────────
    # Used when live scraping returns JS/CSS blobs
    preloaded_channel_data: Dict[str, Any] = field(default_factory=dict)
    top_competitors: List[str] = field(default_factory=list)

    # ── Audit provenance ───────────────────────────────────────
    # "full_intake"    — came from Typeform webhook or Wix /cash-report form
    # "admin_url_only" — triggered from /admin portal (URL + email only; no intake data)
    audit_source: str = "full_intake"

    @property
    def active_social_channels(self):
        channels = []
        if self.facebook_page_url:   channels.append("Facebook")
        if self.instagram_handle:    channels.append("Instagram")
        if self.linkedin_url:        channels.append("LinkedIn")
        if self.youtube_channel_url: channels.append("YouTube")
        if self.tiktok_handle:       channels.append("TikTok")
        if self.discord_url:         channels.append("Discord")
        return channels

    @property
    def has_ai(self):
        return bool(self.anthropic_api_key or self.openai_api_key)
