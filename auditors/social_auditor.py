from typing import Dict, Any, List
from config import ClientConfig
from auditors.industry_benchmarks import (
    get_platform_weight, get_primary_platforms, get_recommended_platforms, INDUSTRIES
)

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False


class SocialMediaAuditor:
    def __init__(self, config: ClientConfig):
        self.config   = config
        self.industry = getattr(config, "industry_category", "Other") or "Other"

    def run(self) -> Dict[str, Any]:
        results = {
            "channels_configured": self.config.active_social_channels,
            "channel_count": len(self.config.active_social_channels),
            "platforms": {}, "overall_issues": [],
            "overall_strengths": [], "channel_scores": {},
            "recommendations": []
        }
        if self.config.facebook_page_url:
            results["platforms"]["Facebook"] = self._audit_facebook()
        if self.config.instagram_handle:
            results["platforms"]["Instagram"] = self._audit_instagram()
        if self.config.linkedin_url:
            results["platforms"]["LinkedIn"] = self._audit_linkedin()
        if self.config.youtube_channel_url:
            results["platforms"]["YouTube"] = self._audit_youtube()
        if self.config.tiktok_handle:
            results["platforms"]["TikTok"] = self._audit_tiktok()
        results["overall_issues"] = self._cross_channel_issues(results)
        results["overall_strengths"] = self._cross_channel_strengths(results)
        results["recommendations"] = self._prioritized_recommendations(results)
        for platform, data in results["platforms"].items():
            results["channel_scores"][platform] = data.get("score", 50)
        return results

    def _audit_facebook(self) -> Dict:
        data = {"platform": "Facebook", "url": self.config.facebook_page_url,
                "issues": [], "strengths": [], "score": 60, "metrics": {}}
        data["metrics"]["profile_complete"] = self._check_url_reachable(self.config.facebook_page_url)
        if data["metrics"]["profile_complete"]:
            data["strengths"].append("✅ Facebook page is active and reachable")
        else:
            data["issues"].append("🔴 Facebook page URL appears unreachable")
        data["best_practices"] = [
            "Post 3-5x per week for optimal organic reach",
            "Video posts get 135% more reach than photo posts",
            "Best times: Tuesday-Thursday 9am-3pm",
            "Respond to comments within 24 hours",
        ]
        return data

    def _audit_instagram(self) -> Dict:
        handle = self.config.instagram_handle.lstrip("@")
        data = {"platform": "Instagram", "handle": f"@{handle}",
                "url": f"https://www.instagram.com/{handle}/",
                "issues": [], "strengths": [], "score": 60, "metrics": {}}
        reachable = self._check_url_reachable(data["url"])
        if reachable:
            data["strengths"].append("✅ Instagram profile is active")
        else:
            data["issues"].append("🔴 Instagram profile appears unreachable")
        data["best_practices"] = [
            "Reels get 22% more engagement than standard video",
            "Post 4-7x per week on feed; Stories daily",
            "Use 5-10 highly relevant hashtags",
            "Best times: M/W/F 6am-9am and 12pm-3pm",
        ]
        return data

    def _audit_linkedin(self) -> Dict:
        data = {"platform": "LinkedIn", "url": self.config.linkedin_url,
                "issues": [], "strengths": [], "score": 60, "metrics": {}}
        reachable = self._check_url_reachable(self.config.linkedin_url)
        if reachable:
            data["strengths"].append("✅ LinkedIn company page is active")
        else:
            data["issues"].append("🔴 LinkedIn page appears unreachable")
        data["best_practices"] = [
            "Post 2-5x per week for B2B audiences",
            "Articles get 3x more reach than plain text posts",
            "Best times: Tuesday-Thursday 7am-9am and 12pm-2pm",
        ]
        return data

    def _audit_youtube(self) -> Dict:
        data = {"platform": "YouTube", "url": self.config.youtube_channel_url,
                "issues": [], "strengths": [], "score": 55, "metrics": {}}
        data["best_practices"] = [
            "Upload minimum 1 video/week",
            "Optimize titles with primary keyword in first 60 characters",
            "Thumbnails with faces get 38% more clicks",
            "Videos 7-15 minutes perform best for watch time",
        ]
        return data

    def _audit_tiktok(self) -> Dict:
        handle = self.config.tiktok_handle.lstrip("@")
        data = {"platform": "TikTok", "handle": f"@{handle}",
                "url": f"https://www.tiktok.com/@{handle}",
                "issues": [], "strengths": [], "score": 55, "metrics": {}}
        data["best_practices"] = [
            "Post 1-4x per day for rapid growth",
            "First 3 seconds must hook viewers",
            "Videos 21-34 seconds see highest completion rates",
            "Use trending sounds when relevant",
        ]
        return data

    def _check_url_reachable(self, url: str) -> bool:
        if not REQUESTS_OK or not url:
            return False
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
        try:
            r = requests.head(url, timeout=10, allow_redirects=True, headers=headers)
            return r.status_code < 400
        except requests.exceptions.SSLError:
            # SSL blocked by server TLS policy — treat as reachable (don't penalise)
            return True
        except Exception:
            return False

    def _cross_channel_issues(self, results: dict) -> List[str]:
        issues = []
        n        = results["channel_count"]
        industry = self.industry
        primary  = get_primary_platforms(industry)
        active   = results.get("channels_configured", [])

        if n == 0:
            issues.append("🔴 CRITICAL: No social media channels configured")
        elif n == 1:
            issues.append("🟡 Only one social platform in use — high dependency risk")
        elif n > 6:
            issues.append("🟡 Active on many platforms — risk of spreading team too thin")

        # Flag missing primary platforms as critical for this industry
        for p in primary:
            platform_configured = {
                "LinkedIn":  self.config.linkedin_url,
                "Instagram": self.config.instagram_handle,
                "Facebook":  self.config.facebook_page_url,
                "YouTube":   self.config.youtube_channel_url,
                "TikTok":    self.config.tiktok_handle,
                "Discord":   self.config.discord_url,
            }.get(p, "")
            if not platform_configured:
                issues.append(
                    f"🔴 CRITICAL: Not on {p} — the primary discovery channel for "
                    f"{industry} businesses. This is a significant gap."
                )

        # Warn about high-weight platforms not in use
        recommended = get_recommended_platforms(industry)
        for p in recommended:
            if p in primary:
                continue  # already flagged above
            if p == "Google Business Profile":
                continue  # handled by GBP auditor
            platform_configured = {
                "LinkedIn":  self.config.linkedin_url,
                "Instagram": self.config.instagram_handle,
                "Facebook":  self.config.facebook_page_url,
                "YouTube":   self.config.youtube_channel_url,
                "TikTok":    self.config.tiktok_handle,
                "Discord":   self.config.discord_url,
            }.get(p, "")
            if not platform_configured:
                issues.append(
                    f"🟡 Not on {p} — recommended for {industry} to reach and educate the target audience."
                )

        issues.append("🟡 Cross-platform content repurposing strategy unknown")
        return issues

    def _cross_channel_strengths(self, results: dict) -> List[str]:
        strengths = []
        n        = results["channel_count"]
        industry = self.industry
        primary  = get_primary_platforms(industry)

        if n >= 3:
            strengths.append(f"✅ Present on {n} social platforms")

        # Celebrate primary platforms that ARE active
        for p in primary:
            platform_configured = {
                "LinkedIn":  self.config.linkedin_url,
                "Instagram": self.config.instagram_handle,
                "Facebook":  self.config.facebook_page_url,
                "YouTube":   self.config.youtube_channel_url,
                "TikTok":    self.config.tiktok_handle,
                "Discord":   self.config.discord_url,
            }.get(p, "")
            if platform_configured:
                weight = get_platform_weight(p, industry)
                label  = "primary" if weight >= 1.4 else "recommended"
                strengths.append(f"✅ Active on {p} — {label} channel for {industry}")

        return strengths

    def _prioritized_recommendations(self, results: dict) -> List[Dict]:
        recs     = []
        n        = results["channel_count"]
        industry = self.industry
        primary  = get_primary_platforms(industry)

        if n > 5:
            recs.append({
                "priority": "HIGH",
                "action":   f"Consolidate to 2–3 platforms your {industry} ICP uses most",
                "reason":   "Spreading across too many channels reduces quality and wastes production time",
                "estimated_time_saved": "5–10 hours/week",
            })

        # Recommend adding missing primary platforms
        for p in primary:
            platform_configured = {
                "LinkedIn":  self.config.linkedin_url,
                "Instagram": self.config.instagram_handle,
                "Facebook":  self.config.facebook_page_url,
                "YouTube":   self.config.youtube_channel_url,
                "TikTok":    self.config.tiktok_handle,
            }.get(p, "")
            if not platform_configured:
                recs.append({
                    "priority": "CRITICAL",
                    "action":   f"Create and activate {p} profile immediately",
                    "reason":   f"{p} is the primary discovery channel for {industry} businesses",
                    "estimated_time_saved": "N/A — this is a revenue-critical gap",
                })

        recs.append({
            "priority": "HIGH",
            "action":   "Implement content repurposing system",
            "reason":   "Create once, publish everywhere — reduces effort by 40–60%",
            "estimated_time_saved": "3–8 hours/week",
        })
        recs.append({
            "priority": "MEDIUM",
            "action":   "Use a scheduling tool (Buffer, Later, Hootsuite)",
            "reason":   "Batch scheduling eliminates daily decision fatigue",
            "estimated_time_saved": "2–4 hours/week",
        })
        return recs


