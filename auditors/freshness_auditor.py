"""
Content Freshness Auditor
Evaluates recency of posts, update frequency, and posting consistency
across all linked social channels.

Scoring priority per platform:
  1. Preloaded data (scraping / pre-gathered JSON)
  2. Intake questionnaire answers (platform_posting_frequency)
  3. API-blocked default → 50 neutral (never 0)

Instagram, Facebook, and YouTube block all public scraping and
require official API access (Meta Graph API, YouTube Data API).
These platforms are scored at 50 (neutral) when no data is available,
and the report notes that API integration would unlock live metrics.
"""
from typing import Dict, Any, List
from config import ClientConfig
from datetime import date
from auditors.industry_benchmarks import (
    get_posting_benchmarks, get_platform_weight, POSTING_BENCHMARKS
)


# Platforms that reliably block scraping — score 50 neutral when no data
SCRAPE_BLOCKED = {"Instagram", "Facebook", "YouTube"}

# Generic fallback benchmarks (used when industry-specific ones are not available)
PLATFORM_BENCHMARKS = POSTING_BENCHMARKS.get("Other", {
    "LinkedIn":  {"min": 2, "ideal": 4,  "max": 7},
    "Instagram": {"min": 3, "ideal": 7,  "max": 14},
    "YouTube":   {"min": 1, "ideal": 2,  "max": 5},
    "Facebook":  {"min": 3, "ideal": 5,  "max": 14},
    "TikTok":    {"min": 5, "ideal": 14, "max": 28},
    "Discord":   {"min": 3, "ideal": 7,  "max": 14},
})

# Days since last post thresholds
FRESHNESS_THRESHOLDS = {
    "fresh":  7,
    "recent": 30,
    "stale":  90,
    "dead":   91,
}

# Preloaded data key names per platform
_PRELOAD_KEYS = {
    "LinkedIn":  "linkedin",
    "Instagram": "instagram",
    "YouTube":   "youtube",
    "Facebook":  "facebook",
    "TikTok":    "tiktok",
    "Discord":   "discord",
}


class FreshnessAuditor:
    def __init__(self, config: ClientConfig, linktree_data: Dict[str, Any]):
        self.config      = config
        self.linktree    = linktree_data
        self.preloaded   = config.preloaded_channel_data
        self.intake_freq = config.platform_posting_frequency  # {"Instagram": 3.0, ...}
        self.audit_date  = date.today()
        self.industry    = getattr(config, "industry_category", "Other") or "Other"

    def run(self) -> Dict[str, Any]:
        channel_results   = {}
        api_blocked_noted = []
        platforms = self.linktree.get("platforms_found", [])

        for platform in platforms:
            if platform in ("Email", "Website"):
                continue
            channel_data = self._audit_channel(platform)
            channel_results[platform] = channel_data
            if channel_data.get("data_source") == "api_blocked":
                api_blocked_noted.append(platform)

        overall_score   = self._overall_freshness_score(channel_results)
        posting_rhythm  = self._posting_rhythm_analysis(channel_results)
        recommendations = self._recommendations(channel_results)

        issues, strengths = [], []
        for ch, data in channel_results.items():
            issues.extend(data.get("issues", []))
            strengths.extend(data.get("strengths", []))

        # Add a single consolidated note when blocked platforms were scored neutral
        scraping_note = None
        if api_blocked_noted:
            scraping_note = (
                f"Note: {', '.join(api_blocked_noted)} block public data access and "
                f"require official API credentials (Meta Graph API / YouTube Data API) "
                f"for live metrics. These channels are scored at 50/100 (neutral) until "
                f"intake posting-frequency answers or API integration are available."
            )

        return {
            "score":          overall_score,
            "grade":          self._grade(overall_score),
            "audit_date":     str(self.audit_date),
            "channels":       channel_results,
            "posting_rhythm": posting_rhythm,
            "issues":         issues,
            "strengths":      strengths,
            "recommendations": recommendations,
            "scraping_note":  scraping_note,
            "api_blocked_platforms": api_blocked_noted,
        }

    # ── Per-channel logic ──────────────────────────────────────

    def _audit_channel(self, platform: str) -> Dict:
        preload_key = _PRELOAD_KEYS.get(platform, platform.lower())
        preloaded   = self.preloaded.get(preload_key, {})
        benchmarks  = get_posting_benchmarks(platform, self.industry)
        issues, strengths = [], []

        # Pull raw signals — preloaded data first
        posts_per_week       = preloaded.get("posts_per_week")
        days_since_last_post = preloaded.get("days_since_last_post")
        last_post_date       = preloaded.get("last_post_date")
        is_active            = preloaded.get("is_active")

        # Fill in from intake if preloaded is missing
        data_source = "preloaded"
        if posts_per_week is None and platform in self.intake_freq:
            posts_per_week = self.intake_freq[platform]
            data_source = "intake"

        # ── Determine freshness status ─────────────────────────
        if days_since_last_post is not None:
            if days_since_last_post <= FRESHNESS_THRESHOLDS["fresh"]:
                status = "fresh"
                strengths.append(
                    f"✅ {platform}: Posted within the last {days_since_last_post} day(s).")
            elif days_since_last_post <= FRESHNESS_THRESHOLDS["recent"]:
                status = "recent"
                strengths.append(
                    f"✅ {platform}: Last post was {days_since_last_post} days ago.")
            elif days_since_last_post <= FRESHNESS_THRESHOLDS["stale"]:
                status = "stale"
                issues.append(
                    f"🟡 {platform}: Last post was {days_since_last_post} days ago — "
                    f"going stale. Algorithms deprioritize infrequent posters.")
            else:
                status = "dead"
                issues.append(
                    f"🔴 {platform}: No post detected in {days_since_last_post}+ days — "
                    f"channel appears abandoned.")

        elif is_active is False:
            status = "unknown_inactive"
            issues.append(f"🟡 {platform}: Channel appears inactive or unverifiable.")

        elif platform in SCRAPE_BLOCKED:
            # Blocked by the platform — score neutral, don't penalise
            status = "api_blocked"
            data_source = "api_blocked"
            # No issue added here — consolidated note handled at the run() level

        else:
            status = "unknown"
            issues.append(
                f"🟡 {platform}: Posting frequency and recency could not be verified.")

        # ── Frequency scoring (works regardless of freshness status) ──
        if posts_per_week is not None:
            if posts_per_week >= benchmarks["ideal"]:
                strengths.append(
                    f"✅ {platform}: Posting {posts_per_week}x/week — at ideal frequency "
                    f"({benchmarks['ideal']}x/week).")
            elif posts_per_week >= benchmarks["min"]:
                strengths.append(
                    f"✅ {platform}: Posting {posts_per_week}x/week meets minimum "
                    f"({benchmarks['min']}+/week).")
            else:
                issues.append(
                    f"🟡 {platform}: Posting {posts_per_week}x/week is below recommended "
                    f"minimum of {benchmarks['min']}x/week.")

        return {
            "platform":            platform,
            "status":              status,
            "data_source":         data_source,
            "posts_per_week":      posts_per_week,
            "days_since_last_post": days_since_last_post,
            "last_post_date":      last_post_date,
            "benchmarks":          benchmarks,
            "issues":              issues,
            "strengths":           strengths,
        }

    # ── Scoring ───────────────────────────────────────────────

    def _overall_freshness_score(self, channels: Dict) -> int:
        if not channels:
            return 50
        weighted_scores = []
        total_weight    = 0.0

        for platform, data in channels.items():
            status = data.get("status", "unknown")
            ppw    = data.get("posts_per_week")
            bench  = data.get("benchmarks", {})
            weight = get_platform_weight(platform, self.industry)

            if status == "fresh":          base = 90
            elif status == "recent":       base = 70
            elif status == "stale":        base = 40
            elif status == "dead":         base = 25   # confirmed abandoned: D range floor, never 0
            elif status == "api_blocked":  base = 50   # always exactly 50 — no adjustment
            else:                          base = 50   # unknown → neutral

            # Frequency adjustment — only on platforms with real data
            if status != "api_blocked" and ppw is not None and bench.get("ideal"):
                freq_ratio = min(ppw / bench["ideal"], 1.5)
                base = min(100, int(base * freq_ratio))

            weighted_scores.append(base * weight)
            total_weight    += weight

        if total_weight == 0:
            return 50
        return round(sum(weighted_scores) / total_weight)

    # ── Rhythm analysis ───────────────────────────────────────

    def _posting_rhythm_analysis(self, channels: Dict) -> Dict:
        active   = [p for p, d in channels.items()
                    if d.get("status") in ("fresh", "recent")]
        stale    = [p for p, d in channels.items()
                    if d.get("status") == "stale"]
        inactive = [p for p, d in channels.items()
                    if d.get("status") in ("dead", "unknown_inactive")]
        blocked  = [p for p, d in channels.items()
                    if d.get("status") == "api_blocked"]
        unknown  = [p for p, d in channels.items()
                    if d.get("status") == "unknown"]
        return {
            "active_channels":      active,
            "stale_channels":       stale,
            "inactive_channels":    inactive,
            "api_blocked_channels": blocked,
            "unverified_channels":  unknown,
            "consistency_note": (
                "Consistent posting is a ranking signal on all major platforms. "
                "Missing 2+ weeks on any channel causes algorithmic suppression."
            ),
        }

    # ── Helpers ───────────────────────────────────────────────

    def _grade(self, score: int) -> str:
        if score >= 80: return "A"
        if score >= 65: return "B"
        if score >= 50: return "C"
        if score >= 35: return "D"
        return "F"

    def _recommendations(self, channels: Dict) -> List[Dict]:
        recs = []
        stale_or_dead = [
            p for p, d in channels.items()
            if d.get("status") in ("stale", "dead", "unknown_inactive")
        ]
        if stale_or_dead:
            recs.append({
                "priority": "HIGH",
                "action":   f"Restart or officially close: {', '.join(stale_or_dead)}",
                "reason":   "Abandoned channels signal a dying brand to anyone who finds them.",
                "fix":      "Post a redirect 'where to find us' and deactivate, or commit to "
                            "a minimum schedule.",
            })
        recs.append({
            "priority": "HIGH",
            "action":   "Build a 90-day content calendar before the next posting cycle",
            "reason":   "Reactive posting leads to inconsistent frequency and off-brand content.",
            "fix":      "Batch-create 30 LinkedIn posts in one session using a single pillar piece.",
        })
        recs.append({
            "priority": "MEDIUM",
            "action":   "Implement a content repurposing system",
            "reason":   "One long-form video → 10+ social posts reduces production time by 60%.",
            "fix":      "Record one 10-min 'tip for financial advisors, CPAs, and attorneys' video → transcript → "
                        "5 LinkedIn posts → 3 email bullets → 2 YouTube Shorts.",
        })
        recs.append({
            "priority": "LOW",
            "action":   "Use a scheduling tool (Buffer, Later, or LinkedIn native scheduler)",
            "reason":   "Posting manually every day is a willpower tax that fails over time.",
            "fix":      "Schedule a full week of posts every Monday morning.",
        })
        return recs
