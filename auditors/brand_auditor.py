"""
Brand Consistency Auditor
Evaluates coherence of brand name, voice, positioning, and messaging
across all linked social channels.
"""
from typing import Dict, Any, List
from config import ClientConfig


# Platform audience profiles — who actually uses each platform
PLATFORM_AUDIENCE = {
    "LinkedIn":   {"type": "B2B professionals", "b2b_fit": 95, "financial_svc_fit": 85},
    "YouTube":    {"type": "Broad / how-to seekers", "b2b_fit": 65, "financial_svc_fit": 65},
    "Facebook":   {"type": "Broad consumers / local businesses", "b2b_fit": 50, "financial_svc_fit": 40},
    "Instagram":  {"type": "Visual / lifestyle / B2C", "b2b_fit": 40, "financial_svc_fit": 25},
    "TikTok":     {"type": "Entertainment / Gen Z", "b2b_fit": 30, "financial_svc_fit": 15},
    "Discord":    {"type": "Gaming / crypto / developer communities", "b2b_fit": 25, "financial_svc_fit": 10},
    "Pinterest":  {"type": "Visual / DIY / lifestyle", "b2b_fit": 20, "financial_svc_fit": 10},
}


class BrandAuditor:
    def __init__(self, config: ClientConfig, linktree_data: Dict[str, Any]):
        self.config = config
        self.linktree = linktree_data
        self.preloaded = config.preloaded_channel_data

    def run(self) -> Dict[str, Any]:
        bio_analysis    = self._analyze_bio_consistency()
        name_analysis   = self._analyze_name_consistency()
        voice_analysis  = self._analyze_brand_voice()
        platform_fit    = self._analyze_platform_fit()
        messaging       = self._analyze_messaging_signals()
        visual_note     = self._visual_brand_note()

        issues    = []
        strengths = []

        issues.extend(bio_analysis["issues"])
        strengths.extend(bio_analysis["strengths"])

        issues.extend(name_analysis["issues"])
        strengths.extend(name_analysis["strengths"])

        issues.extend(voice_analysis["issues"])
        strengths.extend(voice_analysis["strengths"])

        issues.extend(platform_fit["issues"])
        strengths.extend(platform_fit["strengths"])

        issues.extend(messaging["issues"])
        strengths.extend(messaging["strengths"])

        # Visual brand note is informational — not penalised, not counted as strength
        issues.extend(visual_note["issues"])
        strengths.extend(visual_note["strengths"])

        name_consistent = name_analysis.get("name_consistent", False)
        score = self._compute_score(issues, strengths, name_consistent)

        return {
            "score":          score,
            "grade":          self._grade(score),
            "bio_analysis":   bio_analysis,
            "name_analysis":  name_analysis,
            "voice_analysis": voice_analysis,
            "platform_fit":   platform_fit,
            "messaging":      messaging,
            "visual_note":    visual_note,
            "issues":         issues,
            "strengths":      strengths,
            "recommendations": self._recommendations(platform_fit, bio_analysis),
        }

    def _analyze_bio_consistency(self) -> Dict:
        issues, strengths = [], []
        bio = self.linktree.get("bio", "")
        stated_market = self.config.stated_target_market.lower()

        # Check if bio mentions the stated target market
        icp_keywords = [w for w in stated_market.split() if len(w) > 3]
        bio_lower = bio.lower()
        icp_hits = [kw for kw in icp_keywords if kw in bio_lower]

        if icp_hits:
            strengths.append(f"✅ Linktree bio references target market language: {', '.join(icp_hits)}")
        else:
            issues.append(
                f"🔴 Linktree bio does NOT mention stated target market "
                f"('{self.config.stated_target_market}'). "
                f"Bio says: '{bio[:120]}...'"
            )

        # Check for conflicting audience signals
        broad_terms = ["entrepreneurs", "small businesses", "everyone", "any business"]
        found_broad = [t for t in broad_terms if t in bio_lower]
        if found_broad and stated_market and "financial" in stated_market:
            issues.append(
                f"🟡 Bio uses broad/general language ('{', '.join(found_broad)}') "
                f"which conflicts with a B2B financial services positioning."
            )

        # Web3/crypto signals — problematic for regulated financial services
        web3_terms = ["web3", "nft", "blockchain", "crypto", "defi"]
        found_web3 = [t for t in web3_terms if t in bio_lower]
        if found_web3 and "financial" in stated_market:
            issues.append(
                f"🔴 Bio mentions Web3/crypto terms ({', '.join(found_web3)}) — "
                f"this creates compliance risk and credibility concerns with "
                f"regulated financial advisors, CPAs, and attorneys."
            )

        if not bio:
            issues.append("🔴 Linktree bio is empty — first touchpoint has no positioning.")
        elif len(bio) < 50:
            issues.append("🟡 Linktree bio is very short — missed opportunity to communicate value prop.")
        else:
            strengths.append("✅ Linktree bio has descriptive content.")

        return {"issues": issues, "strengths": strengths, "bio_text": bio}

    def _analyze_name_consistency(self) -> Dict:
        issues, strengths = [], []
        names = {
            "Linktree":   self.linktree.get("profile_name", ""),
            "LinkedIn":   self.preloaded.get("linkedin", {}).get("name", ""),
            "Instagram":  self.preloaded.get("instagram", {}).get("handle", ""),
        }
        filled = {k: v for k, v in names.items() if v}

        name_consistent = False
        if len(filled) >= 2:
            base_names = list(filled.values())
            # Derive a root token from the client name for generic cross-client use
            client_root = self.config.client_name.lower().split()[0]  # e.g. "guerrilla"
            all_consistent = all(
                any(token in n.lower() for token in [client_root, "gmg"])
                for n in base_names
            )
            if all_consistent:
                name_consistent = True
                strengths.append(
                    f"✅ Brand name is consistent across {len(filled)} detected channels.")
            else:
                issues.append("🟡 Brand name varies across channels — can confuse search/discovery.")

        return {
            "issues":         issues,
            "strengths":      strengths,
            "names_checked":  filled,
            "name_consistent": name_consistent,
        }

    def _analyze_brand_voice(self) -> Dict:
        issues, strengths = [], []
        stated_market = self.config.stated_target_market.lower()

        linkedin_topics = self.preloaded.get("linkedin", {}).get("content_topics", [])
        bio = self.linktree.get("bio", "").lower()

        # "Bold" / "guerrilla" — great for SMB, bad for financial services
        edgy_words = ["bold", "guerrilla", "disruptive", "rebel", "rule-breaking"]
        found_edgy = [w for w in edgy_words if w in bio]
        if found_edgy and "financial" in stated_market:
            issues.append(
                f"🟡 Brand voice uses aggressive/edgy language "
                f"({', '.join(found_edgy)}) which may deter conservative "
                f"financial advisors, CPAs, and attorneys who value trust and compliance."
            )
        elif found_edgy:
            strengths.append(f"✅ Distinctive brand voice: {', '.join(found_edgy)}.")

        # Check if LinkedIn content aligns with the stated ICP
        financial_topics = ["financial advisor", "cpa", "ria", "wealth management",
                            "tax planning", "compliance", "fiduciary", "advisor marketing",
                            "attorney", "law firm", "legal marketing", "fractional cfo",
                            "fractional cmo", "professional services"]
        if linkedin_topics:
            icp_aligned = [t for t in linkedin_topics if
                           any(f in t.lower() for f in financial_topics)]
            if not icp_aligned:
                issues.append(
                    "🔴 LinkedIn content topics show NO financial services content. "
                    "Posting about general marketing (repurposing, systems, community) "
                    "to a financial advisors, CPAs, and attorneys audience misses the mark on relevance."
                )
            else:
                strengths.append(f"✅ LinkedIn touches professional services ICP topics: {', '.join(icp_aligned[:3])}")

        return {"issues": issues, "strengths": strengths}

    def _analyze_platform_fit(self) -> Dict:
        issues, strengths = [], []
        stated_market = self.config.stated_target_market.lower()
        is_b2b_financial = ("financial" in stated_market or "cpa" in stated_market
                            or "advisor" in stated_market or "attorney" in stated_market
                            or "law firm" in stated_market or "fractional" in stated_market)

        platforms = self.linktree.get("platforms_found", [])
        scored = {}
        for p in platforms:
            if p in PLATFORM_AUDIENCE:
                info = PLATFORM_AUDIENCE[p]
                fit  = info["financial_svc_fit"] if is_b2b_financial else info["b2b_fit"]
                scored[p] = fit

        high_fit   = [p for p, s in scored.items() if s >= 70]
        medium_fit = [p for p, s in scored.items() if 40 <= s < 70]
        low_fit    = [p for p, s in scored.items() if s < 40]

        if high_fit:
            strengths.append(f"✅ Strong-fit platforms for target market: {', '.join(high_fit)}")
        if medium_fit:
            issues.append(f"🟡 Medium-fit platforms (use sparingly): {', '.join(medium_fit)}")
        if low_fit:
            issues.append(
                f"🔴 Low-fit platforms for B2B financial services: {', '.join(low_fit)}. "
                f"Time spent here is mostly wasted when targeting financial advisors, CPAs, attorneys, and law firms."
            )

        # Discord specifically
        if "Discord" in platforms and is_b2b_financial:
            issues.append(
                "🔴 Discord (66 members) is a gaming/crypto community platform — "
                "virtually no financial advisors, CPAs, or attorneys use it for professional discovery. "
                "This investment of time/energy is near-zero ROI for this ICP."
            )

        return {
            "issues": issues,
            "strengths": strengths,
            "platform_scores": scored,
            "high_fit": high_fit,
            "medium_fit": medium_fit,
            "low_fit": low_fit,
        }

    def _analyze_messaging_signals(self) -> Dict:
        issues, strengths = [], []
        preloaded = self.preloaded

        # Check if website messaging matches Linktree
        website_audience = preloaded.get("website", {}).get("target_audience_mentioned", "")
        linktree_bio = self.linktree.get("bio", "")
        if website_audience and linktree_bio:
            if website_audience.lower() not in linktree_bio.lower():
                issues.append(
                    f"🟡 Website target audience ('{website_audience}') "
                    f"not reflected in Linktree bio — fragmented first impressions."
                )

        # Value prop check
        value_prop = self.config.stated_value_prop
        if not value_prop:
            issues.append("🟡 No clear stated value proposition found in public-facing copy.")
        else:
            strengths.append(f"✅ Value proposition defined: '{value_prop[:80]}'")

        # Social proof
        has_testimonials = preloaded.get("website", {}).get("has_testimonials", False)
        has_case_studies = preloaded.get("website", {}).get("has_case_studies", False)
        if not has_testimonials and not has_case_studies:
            issues.append(
                "🔴 No testimonials or case studies visible in public content. "
                "Financial advisors, CPAs, and attorneys require strong social proof before engaging a vendor."
            )

        return {"issues": issues, "strengths": strengths}

    def _visual_brand_note(self) -> Dict:
        """
        Visual brand elements (logo, colour palette, typography, imagery style)
        cannot be assessed through scraping. Return a neutral informational note
        — not counted as an issue or strength in scoring.
        """
        return {
            "issues": [
                "🟡 Visual brand audit requires manual review — logo consistency, "
                "colour palette, and imagery style across platforms cannot be assessed "
                "through automated scraping. Score for visual elements: 50 (neutral)."
            ],
            "strengths": [],
            "visual_score": 50,
            "note": "Visual brand audit requires manual review",
        }

    def _compute_score(self, issues: List[str], strengths: List[str],
                       name_consistent: bool = False) -> int:
        # Exclude the visual-note line from scoring — it's informational, not a real finding
        def _is_visual(i): return "Visual brand audit requires manual review" in i
        def _is_unknown(i): return "could not be verified" in i or "unknown" in i.lower()

        scored_issues = [i for i in issues if not _is_visual(i)]

        critical_c = sum(1 for i in scored_issues if "🔴" in i and not _is_unknown(i))
        critical_u = sum(1 for i in scored_issues if "🔴" in i and _is_unknown(i))
        warning_c  = sum(1 for i in scored_issues if "🟡" in i and not _is_unknown(i))
        warning_u  = sum(1 for i in scored_issues if "🟡" in i and _is_unknown(i))
        base = (50
                - (critical_c * 10)
                - (critical_u *  2)
                - (warning_c  *  4)
                - (warning_u  *  1)
                + (len(strengths) * 6))

        has_real_data = (critical_c + warning_c) > 0
        # Floor at 25: confirmed missing brand elements score D range, not below.
        # Floor at 50: no real data → neutral.
        score = max(25, min(100, base)) if has_real_data else 50

        # Additional floor: if the brand name is verifiably consistent across platforms,
        # ensure at least 50 — the brand has a foundational identity.
        if name_consistent:
            score = max(50, score)

        return score

    def _grade(self, score: int) -> str:
        if score >= 80: return "A"
        if score >= 65: return "B"
        if score >= 50: return "C"
        if score >= 35: return "D"
        return "F"

    def _recommendations(self, platform_fit: Dict, bio_analysis: Dict) -> List[Dict]:
        recs = []
        low_fit = platform_fit.get("low_fit", [])
        if low_fit:
            recs.append({
                "priority": "HIGH",
                "action": f"Pause or deprioritize: {', '.join(low_fit)}",
                "reason": "These platforms have near-zero overlap with financial advisors, CPAs, attorneys, and law firms.",
                "impact": "Recover 5-10 hours/week to invest in LinkedIn and email."
            })
        recs.append({
            "priority": "HIGH",
            "action": "Rewrite Linktree bio to speak directly to financial advisors, CPAs, attorneys, and law firms",
            "reason": "First touchpoint must immediately signal relevance to the ICP.",
            "impact": "Higher link-click conversion from the right audience."
        })
        recs.append({
            "priority": "HIGH",
            "action": "Remove or reframe Web3/NFT/Blockchain language from all public copy",
            "reason": "FINRA/SEC-regulated advisors are risk-averse; crypto-adjacent language triggers compliance red flags.",
            "impact": "Removes a major credibility barrier with the target ICP."
        })
        recs.append({
            "priority": "MEDIUM",
            "action": "Standardize social handles to one consistent format",
            "reason": "go.guerrilla vs GoGuerrillaX vs GuerrillaMarketingGroup creates confusion.",
            "impact": "Easier cross-platform discovery and tagging."
        })
        return recs
