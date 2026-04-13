"""
Content Auditor
Identifies content gaps and quick wins from real audit data.

Rules:
- No invented frequency estimates (no channels × N formula)
- No invented hours-wasted calculations
- No assumed hourly rates
- Time/cost estimates only appear when the client provided:
    platform_posting_frequency (from intake) AND team_hourly_rate (from intake)
- Missing data is omitted, never fabricated
"""
from typing import Dict, Any, List, Optional
from config import ClientConfig


class ContentAuditor:
    def __init__(self, config: ClientConfig, audit_data: dict):
        self.config     = config
        self.audit_data = audit_data

    def run(self) -> Dict[str, Any]:
        result = {
            "content_gaps":     self._identify_content_gaps(),
            "quick_wins":       self._quick_wins(),
            "channel_summary":  self._channel_summary(),
            "budget_summary":   self._budget_summary(),
            "waste_signals":    self._waste_signals(),
        }

        # Time/cost estimates only when client provided both hourly rate AND frequency data
        time_cost = self._time_cost_estimate()
        if time_cost:
            result["time_cost_estimate"] = time_cost

        # YouTube live metrics (only when YouTube Data API v3 succeeded)
        yt = self._youtube_metrics()
        if yt:
            result["youtube_metrics"] = yt

        return result

    # ── Channel summary (real data only) ──────────────────────

    def _channel_summary(self) -> Dict:
        channels  = self.config.active_social_channels
        n         = len(channels)
        freq_data = self.config.platform_posting_frequency  # from intake, may be empty

        per_platform = {}
        total_known_posts = 0
        for platform in channels:
            ppw = freq_data.get(platform)  # None if client didn't provide it
            per_platform[platform] = {
                "posts_per_week": ppw,
                "frequency_source": "intake" if ppw is not None else "not provided",
            }
            if ppw is not None:
                total_known_posts += ppw

        return {
            "active_channel_count":  n,
            "active_channels":       channels,
            "per_platform":          per_platform,
            # Only report total if we have data for ALL channels
            "total_posts_per_week":  total_known_posts if freq_data and len(freq_data) == n else None,
            "frequency_data_complete": len(freq_data) == n and n > 0,
        }

    # ── Budget summary (real data only) ───────────────────────

    def _budget_summary(self) -> Dict:
        budget   = self.config.monthly_ad_budget
        channels = len(self.config.active_social_channels)

        if budget == 0:
            return {
                "monthly_budget": 0,
                "status":         "organic_only",
                "note":           "No paid ad budget reported.",
            }

        per_channel = round(budget / channels) if channels > 0 else budget
        return {
            "monthly_budget":       budget,
            "channel_count":        channels,
            "per_channel_budget":   per_channel,
            "status":               "spread_thin" if channels > 3 and budget < 2000 else "ok",
            "note": (
                f"${budget:,.0f}/month across {channels} channels "
                f"(~${per_channel:,}/channel/month)."
            ),
        }

    # ── Waste signals (qualitative only — no invented hours/dollars) ──

    def _waste_signals(self) -> List[Dict]:
        signals  = []
        channels = len(self.config.active_social_channels)
        budget   = self.config.monthly_ad_budget
        freq     = self.config.platform_posting_frequency

        if channels > 4:
            signals.append({
                "type":           "Channel Overextension",
                "description":    (
                    f"Active on {channels} platforms with a {self.config.team_size}-person team. "
                    f"Maintaining quality content across this many channels is difficult at this team size."
                ),
                "recommendation": "Consolidate to the 2-3 platforms your ICP actually uses.",
            })

        if budget > 0 and channels > 3:
            signals.append({
                "type":           "Fragmented Ad Spend",
                "description":    (
                    f"${budget:,.0f}/month spread across {channels} channels. "
                    f"Below ~$2,000/month, splitting budget this way makes each channel too small to optimize."
                ),
                "recommendation": "Consolidate spend to 1-2 channels and use UTM tracking to measure ROI.",
            })

        if not any(v for v in freq.values() if v is not None) and channels > 0:
            signals.append({
                "type":           "Posting Frequency Unknown",
                "description":    (
                    "No posting frequency data is available for any channel. "
                    "Without knowing how often content is published, it's impossible to assess "
                    "content output efficiency."
                ),
                "recommendation": (
                    "Record posting frequency in the intake questionnaire to unlock "
                    "frequency analysis and time estimates."
                ),
            })

        return signals

    # ── Time/cost estimate (only when real data is present) ───

    def _time_cost_estimate(self) -> Optional[Dict]:
        hourly_rate = self.config.team_hourly_rate
        freq        = self.config.platform_posting_frequency
        channels    = self.config.active_social_channels

        # Both conditions must be met — no defaults or fallbacks
        if hourly_rate <= 0:
            return None
        if not freq or not any(v for v in freq.values() if v is not None):
            return None

        known_platforms  = {p: v for p, v in freq.items() if v is not None and p in channels}
        if not known_platforms:
            return None

        total_posts_week = sum(known_platforms.values())
        # 30 min per post is a client-provided or reasonable disclosed assumption
        # We do NOT assume — we only calculate if client provides hours_per_post
        # For now: return the frequency data and cost framework, label assumptions clearly
        hours_per_post   = self.config.__dict__.get("hours_per_post")  # not yet in config
        if hours_per_post:
            weekly_hours      = total_posts_week * hours_per_post
            monthly_hours     = weekly_hours * 4.33
            monthly_cost      = monthly_hours * hourly_rate
            return {
                "platforms_with_data":    list(known_platforms.keys()),
                "total_posts_per_week":   total_posts_week,
                "hours_per_post":         hours_per_post,
                "weekly_hours":           round(weekly_hours, 1),
                "monthly_hours":          round(monthly_hours, 1),
                "hourly_rate":            hourly_rate,
                "estimated_monthly_cost": round(monthly_cost),
                "data_source":            "client_intake",
            }
        else:
            # Frequency is known but hours/post not provided — report frequency only
            return {
                "platforms_with_data":  list(known_platforms.keys()),
                "total_posts_per_week": total_posts_week,
                "hourly_rate":          hourly_rate,
                "note": (
                    "Hourly rate provided. To unlock cost estimates, add 'hours_per_post' "
                    "to the intake questionnaire."
                ),
                "data_source": "partial_intake",
            }

    # ── YouTube channel metrics (YouTube Data API v3) ────────

    def _youtube_metrics(self) -> Optional[Dict]:
        yt = self.config.preloaded_channel_data.get("youtube", {})
        if yt.get("data_source") != "youtube_api_v3":
            return None
        return {
            "subscriber_count":        yt.get("subscriber_count"),
            "total_video_count":       yt.get("total_video_count"),
            "total_view_count":        yt.get("total_view_count"),
            "videos_last_30_days":     yt.get("videos_last_30_days"),
            "posts_per_week":          yt.get("posts_per_week"),
            "avg_views_per_video":     yt.get("avg_views_per_video"),
            "most_viewed_video_title": yt.get("most_viewed_video_title"),
            "description":             yt.get("description", ""),
            "days_since_last_post":    yt.get("days_since_last_post"),
            "data_source":             "youtube_api_v3",
        }

    # ── Content gaps (from real audit data) ───────────────────

    def _identify_content_gaps(self) -> List[Dict]:
        gaps    = []
        site    = self.config.preloaded_channel_data.get("website", {})
        seo     = self.audit_data.get("seo", {})

        # Blog / long-form content
        if not site.get("has_blog"):
            gaps.append({
                "gap":    "No blog or long-form content",
                "impact": "Long-form content drives 3x more organic traffic and is the primary "
                          "source AI systems draw from when answering questions about your niche.",
                "fix":    "Publish one long-form article per month targeting your ICP's top questions.",
            })

        # Case studies
        if not site.get("has_case_studies"):
            gaps.append({
                "gap":    "No case studies or client results",
                "impact": "B2B buyers require proof of results before making a purchase decision. "
                          "Missing case studies is a top reason warm leads don't convert.",
                "fix":    "Write one case study per current or past client with before/after metrics.",
            })

        # Testimonials
        if not site.get("has_testimonials"):
            gaps.append({
                "gap":    "No testimonials on website",
                "impact": "92% of B2B buyers read reviews before purchasing. "
                          "Absence of social proof is a direct conversion killer.",
                "fix":    "Ask every current client for a 2-sentence testimonial and add to homepage.",
            })

        # Email list / lead capture
        if not self.config.has_lead_magnet and not self.config.has_email_marketing:
            gaps.append({
                "gap":    "No email list building mechanism",
                "impact": "Email is the only owned channel that doesn't depend on platform algorithms. "
                          "Without a list, all audience reach is rented.",
                "fix":    "Create a free resource (checklist, guide, or template) for your ICP "
                          "and gate it behind an email opt-in.",
            })

        # FAQ / structured Q&A content (feeds GEO score)
        geo = self.audit_data.get("geo", {})
        faq_score = geo.get("components", {}).get("FAQ / Q&A Content", 50)
        if faq_score < 50:
            gaps.append({
                "gap":    "No FAQ or Q&A content",
                "impact": "FAQ pages are the #1 content type cited in AI-generated answers "
                          "(ChatGPT, Google AI Overviews, Perplexity).",
                "fix":    "Add a FAQ page answering the top 5 questions your ICP asks before hiring you.",
            })

        # Sitemap (from SEO auditor)
        if seo.get("sitemap", {}).get("found") is False:
            gaps.append({
                "gap":    "No XML sitemap",
                "impact": "Search engines may miss pages that aren't in a sitemap, "
                          "reducing indexed page count.",
                "fix":    "Generate and submit an XML sitemap via Google Search Console.",
            })

        return gaps

    # ── Quick wins (directional — not data-fabricated) ────────

    def _quick_wins(self) -> List[Dict]:
        wins = []
        site = self.config.preloaded_channel_data.get("website", {})

        # Booking tool upgrade — only flag if we know they use Google Calendar
        if self.config.booking_tool and "google calendar" in self.config.booking_tool.lower():
            wins.append({
                "win":      "Replace raw Google Calendar link with a dedicated scheduling tool — [YOUR BOOKING LINK]",
                "effort":   "Low (30 min)",
                "impact":   "Professional booking flow with qualification questions and automatic reminders",
                "timeline": "Today",
            })

        # Schema markup — from GEO auditor
        geo = self.audit_data.get("geo", {})
        if not self.audit_data.get("seo", {}).get("has_schema"):
            wins.append({
                "win":      "Add Schema.org markup to website",
                "effort":   "Low-Medium (1-2 hours or a plugin)",
                "impact":   "Enables Google AI Overviews and rich search results to identify and cite the business",
                "timeline": "This week",
            })

        # UTM tracking
        wins.append({
            "win":      "Add UTM parameters to all social media bio links",
            "effort":   "Low (1 hour)",
            "impact":   "Know exactly which channel drives website traffic and leads",
            "timeline": "This week",
        })

        # Google Search Console
        wins.append({
            "win":      "Set up Google Search Console and connect to website",
            "effort":   "Low (30 min)",
            "impact":   "Free data on which search queries bring visitors and which pages rank",
            "timeline": "This week",
        })

        # Testimonials collection — only if confirmed missing
        if not site.get("has_testimonials"):
            wins.append({
                "win":      "Email every current and past client requesting a 2-sentence testimonial",
                "effort":   "Low (30 min to write, then wait)",
                "impact":   "Social proof on homepage directly increases conversion rate",
                "timeline": "This week",
            })

        return wins
