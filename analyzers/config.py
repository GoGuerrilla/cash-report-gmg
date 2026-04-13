from dataclasses import dataclass, field
from typing import List

@dataclass
class ClientConfig:
    client_name: str = "Client"
    client_industry: str = "General"
    website_url: str = ""
    facebook_page_url: str = ""
    instagram_handle: str = ""
    linkedin_url: str = ""
    youtube_channel_url: str = ""
    tiktok_handle: str = ""
    pinterest_handle: str = ""
    google_analytics_property_id: str = ""
    google_analytics_credentials_path: str = ""
    meta_access_token: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    monthly_ad_budget: float = 0.0
    team_size: int = 1
    primary_goal: str = "Brand awareness"
    target_audience: str = ""
    top_competitors: List[str] = field(default_factory=list)
    agency_name: str = "Marketing Audit Report"
    agency_logo_path: str = ""

    @property
    def active_social_channels(self):
        channels = []
        if self.facebook_page_url: channels.append("Facebook")
        if self.instagram_handle: channels.append("Instagram")
        if self.linkedin_url: channels.append("LinkedIn")
        if self.youtube_channel_url: channels.append("YouTube")
        if self.tiktok_handle: channels.append("TikTok")
        return channels

    @property
    def has_meta_api(self):
        return bool(self.meta_access_token)

    @property
    def has_ai(self):
        return bool(self.anthropic_api_key or self.openai_api_key)
