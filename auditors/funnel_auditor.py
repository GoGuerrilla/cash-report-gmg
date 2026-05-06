"""
Lead Funnel Quality Auditor
Evaluates the quality and completeness of the client's lead conversion path:
awareness → interest → consideration → contact/conversion.
"""
from typing import Dict, Any, List
from config import ClientConfig



class FunnelAuditor:
    def __init__(self, config: ClientConfig, linktree_data: Dict[str, Any]):
        self.config = config
        self.linktree = linktree_data
        self.preloaded = config.preloaded_channel_data

    def run(self) -> Dict[str, Any]:
        awareness  = self._audit_awareness()
        capture    = self._audit_lead_capture()
        nurture    = self._audit_nurture()
        conversion = self._audit_conversion()
        trust      = self._audit_trust_signals()

        all_issues    = (awareness["issues"] + capture["issues"] +
                         nurture["issues"] + conversion["issues"] + trust["issues"])
        all_strengths = (awareness["strengths"] + capture["strengths"] +
                         nurture["strengths"] + conversion["strengths"] + trust["strengths"])

        score = self._compute_score(all_issues, all_strengths)

        return {
            "score": score,
            "grade": self._grade(score),
            "stages": {
                "awareness":  awareness,
                "capture":    capture,
                "nurture":    nurture,
                "conversion": conversion,
                "trust":      trust,
            },
            "issues":          all_issues,
            "strengths":       all_strengths,
            "funnel_gaps":     self._identify_gaps(awareness, capture, nurture, conversion),
            "recommendations": self._recommendations(capture, nurture, conversion),
        }

    def _audit_awareness(self) -> Dict:
        issues, strengths = [], []
        platforms = self.linktree.get("platforms_found", [])
        preloaded = self.preloaded

        linkedin_followers = preloaded.get("linkedin", {}).get("followers") or 0
        discord_members    = preloaded.get("discord", {}).get("members") or 0

        # Channel count
        if len(platforms) >= 5:
            issues.append(
                f"🟡 Active on {len(platforms)} channels — awareness spread too thin. "
                f"Depth on 2-3 channels typically beats breadth on 8 — focus where your ICP actually spends time."
            )
        if "LinkedIn" in platforms:
            strengths.append("✅ LinkedIn presence — strong B2B awareness signal.")

        # Follower sizes
        if linkedin_followers and linkedin_followers < 2000:
            issues.append(
                f"🟡 LinkedIn has {linkedin_followers:,} followers — below the threshold "
                f"(~5,000+) typically needed for meaningful organic reach to a B2B buyer audience."
            )
        elif linkedin_followers >= 5000:
            strengths.append(f"✅ LinkedIn has {linkedin_followers:,} followers — solid B2B reach.")

        if discord_members and discord_members < 500:
            issues.append(
                f"🔴 Discord has only {discord_members} members — too small to drive "
                f"meaningful leads, and a low-fit channel for most B2B service businesses."
            )

        return {"issues": issues, "strengths": strengths}

    def _audit_lead_capture(self) -> Dict:
        issues, strengths = [], []
        preloaded = self.preloaded

        has_lead_magnet     = preloaded.get("website", {}).get("has_lead_magnet", False)
        has_email_optin     = preloaded.get("website", {}).get("has_email_optin", False)
        has_contact_form    = preloaded.get("website", {}).get("has_contact_form", False)
        booking_url         = preloaded.get("website", {}).get("booking_url", "")
        email               = self.linktree.get("email", "")

        if has_lead_magnet:
            strengths.append("✅ Lead magnet present on website.")
        else:
            issues.append(
                "🔴 No lead magnet found. Cold visitors need a low-risk first step "
                "(free audit, checklist, guide, or short case study) to justify giving their contact info. "
                "Without one, top-of-funnel traffic walks away anonymous."
            )

        if has_email_optin:
            strengths.append("✅ Email opt-in found — top of funnel capture in place.")
        else:
            issues.append(
                "🔴 No visible email opt-in/capture form. Email marketing typically yields ~42:1 ROI "
                "and is the highest-ownership nurture channel for most service businesses."
            )

        if has_contact_form:
            strengths.append("✅ Contact form present.")
        else:
            issues.append("🟡 No contact form visible — friction for inbound leads.")

        if booking_url:
            if "calendar.google.com" in booking_url or "calendly" in booking_url.lower():
                issues.append(
                    "🟡 Booking goes to a raw Google Calendar link. "
                    "This reads as unprofessional for most service-business buyers. "
                    "Replace with a dedicated scheduling tool (e.g. [YOUR BOOKING LINK])."
                )
            else:
                strengths.append(f"✅ Booking/scheduling link present: {booking_url[:60]}")

        if email:
            strengths.append(f"✅ Direct email contact available: {email}")
        else:
            issues.append("🟡 No public email address visible — removes a contact option.")

        return {"issues": issues, "strengths": strengths}

    def _audit_nurture(self) -> Dict:
        issues, strengths = [], []
        preloaded = self.preloaded

        # Use intake answers when available, fall back to preloaded scrape data
        has_newsletter  = self.config.has_active_newsletter or preloaded.get("website", {}).get("has_newsletter", False)
        has_blog        = preloaded.get("website", {}).get("has_blog", False)
        linkedin_freq   = preloaded.get("linkedin", {}).get("posts_per_week") or 0
        youtube_videos  = preloaded.get("youtube", {}).get("recent_video_count") or 0
        email_list_size = self.config.email_list_size

        if has_newsletter:
            strengths.append("✅ Newsletter/email nurture sequence active.")
        else:
            issues.append(
                "🔴 No email nurture sequence found. "
                "B2B service sales cycles often run 60-180 days — "
                "without nurture, leads go cold before they convert."
            )

        if email_list_size > 500:
            strengths.append(f"✅ Email list of {email_list_size:,} contacts — solid nurture base.")
        elif email_list_size > 0:
            issues.append(
                f"🟡 Email list is small ({email_list_size} contacts). "
                "Grow it with a lead magnet tailored to your ICP (e.g. a free guide, audit, checklist, or short report)."
            )
        elif not has_newsletter:
            issues.append(
                "🔴 No email list reported. Email marketing typically yields ~42:1 ROI and is "
                "the highest-ownership nurture channel for most service businesses."
            )

        if has_blog:
            strengths.append("✅ Blog/content hub present for SEO and long-cycle nurture.")
        else:
            issues.append(
                "🟡 No blog or content hub visible. Long-form content builds trust "
                "over an extended B2B service sales cycle."
            )

        if linkedin_freq >= 3:
            strengths.append(f"✅ LinkedIn posting {linkedin_freq}x/week sustains top-of-mind awareness.")
        elif linkedin_freq > 0:
            issues.append(f"🟡 LinkedIn frequency ({linkedin_freq}x/week) below 3x/week minimum.")

        if youtube_videos == 0:
            issues.append(
                "🟡 No YouTube videos confirmed. Video is the highest-trust content format — "
                "a 'how we help [your ICP]' series builds authority faster than written content."
            )
        elif youtube_videos >= 5:
            strengths.append(f"✅ YouTube library has {youtube_videos} recent videos.")

        return {"issues": issues, "strengths": strengths}

    def _audit_conversion(self) -> Dict:
        issues, strengths = [], []
        preloaded = self.preloaded

        has_pricing      = preloaded.get("website", {}).get("has_pricing", False)
        has_case_studies = preloaded.get("website", {}).get("has_case_studies", False)
        has_proposal_cta = preloaded.get("website", {}).get("has_proposal_cta", False)
        has_free_trial   = preloaded.get("website", {}).get("has_free_trial", False)

        if has_pricing:
            strengths.append("✅ Pricing info available — removes friction for serious buyers.")
        else:
            issues.append(
                "🟡 No pricing visible. B2B buyers often "
                "disqualify vendors who hide pricing. Even 'starting from' or range pricing builds trust."
            )

        if has_case_studies:
            strengths.append("✅ Case studies present — critical social proof for B2B conversion.")
        else:
            issues.append(
                "🔴 No case studies found. Without proof of outcomes for clients in your ICP, "
                "conversion rates stay low. One strong, specific case study typically outperforms 100 generic testimonials."
            )

        if has_proposal_cta or has_free_trial:
            strengths.append("✅ Clear conversion CTA (proposal/trial) present.")
        else:
            issues.append(
                "🟡 No 'Get a Proposal' or 'Free Audit' CTA found. "
                "B2B buyers need a specific, low-friction next step."
            )

        # Check social-bio classified links for direct conversion path
        links = self.linktree.get("classified_links", {})
        booking_links = links.get("Website", [])
        if booking_links:
            strengths.append("✅ Public website link present — provides path to conversion.")

        return {"issues": issues, "strengths": strengths}

    def _audit_trust_signals(self) -> Dict:
        issues, strengths = [], []
        preloaded = self.preloaded

        has_testimonials   = preloaded.get("website", {}).get("has_testimonials", False)
        has_certifications = preloaded.get("website", {}).get("has_certifications", False)
        has_media_mentions = preloaded.get("website", {}).get("has_media_mentions", False)
        has_client_logos   = preloaded.get("website", {}).get("has_client_logos", False)
        linkedin_followers = preloaded.get("linkedin", {}).get("followers") or 0
        has_referral       = self.config.has_referral_system
        referral_desc      = self.config.referral_system_description
        client_count       = self.config.current_client_count
        client_types       = self.config.current_client_types

        if has_testimonials:
            strengths.append("✅ Testimonials present on website.")
        else:
            # Per Dave 2026-05-06: testimonials may exist but live in widgets,
            # images, or JS components our crawler can't parse. Don't label
            # this CRITICAL when we can't confirm absence — soften severity
            # and add the most-likely-cause framing so the operator knows
            # whether to add testimonials or just surface existing ones.
            issues.append(
                "🟡 Testimonials not detected in the public content we crawled. "
                "They may exist in a format our crawler couldn't parse — "
                "image-embedded quotes, third-party review widgets (Google "
                "Reviews iframe, Trustpilot), or dynamically-loaded JS "
                "components. If you have testimonials, surface 1-3 of them as "
                "plain text on the homepage with named attribution so AI "
                "engines and human visitors can see them."
            )

        if has_referral:
            ref_note = f" ({referral_desc})" if referral_desc else ""
            strengths.append(f"✅ Referral system in place{ref_note}.")
        else:
            issues.append(
                "🟡 Referral program not detected on website. May exist as a "
                "private signup flow or be communicated 1:1 with clients. If "
                "you have one, surface a public mention or signup CTA on the "
                "homepage to maximise referral velocity."
            )

        if client_count > 10:
            strengths.append(f"✅ {client_count} active clients — proof of market fit.")
            if client_types:
                strengths.append(f"✅ Client portfolio includes: {client_types}.")
        elif client_count > 0:
            issues.append(
                f"🟡 {client_count} current clients reported. "
                "Build case studies from these relationships immediately."
            )

        if has_certifications:
            strengths.append("✅ Marketing certifications/credentials displayed.")
        else:
            issues.append(
                "🟡 Certifications/credentials not detected in the public "
                "content we crawled. Often live as image badges or footer "
                "logos that the crawler can't read as text. If you hold "
                "industry credentials or partner badges, add an alt-text "
                "label or plain-text mention so they register as trust signals."
            )

        if has_client_logos:
            strengths.append("✅ Client logos visible — at-a-glance social proof.")
        else:
            issues.append("🟡 No client logos or 'as seen in' badges.")

        if has_media_mentions:
            strengths.append("✅ Media mentions or press coverage present.")

        if linkedin_followers and linkedin_followers >= 1000:
            strengths.append(f"✅ {linkedin_followers:,} LinkedIn followers adds credibility.")

        return {"issues": issues, "strengths": strengths}

    def _identify_gaps(self, *stages) -> List[str]:
        gaps = []
        for stage in stages:
            for issue in stage.get("issues", []):
                if "🔴" in issue:
                    gaps.append(issue.replace("🔴 ", "CRITICAL GAP: "))
        return gaps

    def _compute_score(self, issues: List[str], strengths: List[str]) -> int:
        def _is_unknown(i): return "could not be verified" in i or "unknown" in i.lower()
        critical_c = sum(1 for i in issues if "🔴" in i and not _is_unknown(i))
        critical_u = sum(1 for i in issues if "🔴" in i and _is_unknown(i))
        warning_c  = sum(1 for i in issues if "🟡" in i and not _is_unknown(i))
        warning_u  = sum(1 for i in issues if "🟡" in i and _is_unknown(i))
        # Cap deductions to prevent cascade collapse when many funnel gaps exist.
        # A business with zero funnel but some social presence = D (30-35), not F.
        crit_deduction = min(critical_c, 4) * 8   # max -32
        warn_deduction = min(warning_c,  5) * 3   # max -15
        base = (50
                - crit_deduction
                - (critical_u * 2)
                - warn_deduction
                - (warning_u  * 1)
                + (len(strengths) * 7))
        has_real_data = (critical_c + warning_c) > 0
        return max(30, min(100, base)) if has_real_data else 50

    def _grade(self, score: int) -> str:
        if score >= 80: return "A"
        if score >= 65: return "B"
        if score >= 50: return "C"
        if score >= 35: return "D"
        return "F"

    def _recommendations(self, capture: Dict, nurture: Dict, conversion: Dict) -> List[Dict]:
        return [
            {
                "priority": "HIGH",
                "action": "Build a lead magnet specific to each ICP segment",
                "example": "e.g. a free audit, a '[N] common mistakes' guide, "
                           "a category playbook, or a checklist tied to your ICP's actual pain points",
                "timeline": "2-3 weeks",
                "impact": "Creates email list of warm ICP leads"
            },
            {
                "priority": "HIGH",
                "action": "Replace Google Calendar booking link with a dedicated scheduling tool",
                "example": "[YOUR BOOKING LINK]",
                "timeline": "1 day",
                "impact": "Looks professional; adds buffer questions to qualify leads"
            },
            {
                "priority": "HIGH",
                "action": "Add an email opt-in + 5-email nurture sequence",
                "example": "Free guide → Case study → FAQ → Objection handling → Discovery call CTA",
                "timeline": "2-4 weeks",
                "impact": "Captures and warms leads over the 60-180 day financial B2B sales cycle"
            },
            {
                "priority": "MEDIUM",
                "action": "Publish 1-2 case studies per ICP segment on the website",
                "example": "Format: How we helped [client / segment] achieve [specific outcome] in [timeframe]. "
                           "Use real numbers when possible.",
                "timeline": "1-2 months",
                "impact": "Single biggest conversion lever for B2B and high-consideration buyers"
            },
            {
                "priority": "MEDIUM",
                "action": "Add visible pricing or 'starting from' range",
                "example": "e.g. 'Starting from $X/month' or '$X–$Y per project' — even a range builds trust.",
                "timeline": "1 week",
                "impact": "Pre-qualifies leads and reduces friction with detail-oriented buyers"
            },
        ]
