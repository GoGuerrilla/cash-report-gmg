"""
ICP (Ideal Customer Profile) Alignment Auditor
Compares the client's stated target market against their actual public content,
platform choices, messaging, and brand positioning.

Signal detection is driven entirely by the stated_target_market field from
the intake form — no hardcoded niche assumptions. The auditor extracts
keywords from whatever the client typed in Q4 and uses those to score
content alignment throughout.
"""
import re
from typing import Dict, Any, List
from config import ClientConfig
from auditors.industry_benchmarks import get_compliance_framing

# ── Generic SMB / broad-audience signals ──────────────────────
# These indicate the content is written for a general audience rather
# than a specific professional ICP. Flagged as misalignment when the
# client's stated ICP is a narrow professional segment.
GENERAL_SMB_SIGNALS = [
    "entrepreneur", "small business", "startup", "solopreneur", "freelancer",
    "local business", "e-commerce", "ecommerce", "side hustle",
    "web3", "nft", "blockchain", "crypto", "dao", "defi", "token",
    "community building", "gaming",
]

# ── Platform B2B fit (platform → (ICP fit note, is_b2b_positive)) ─
PLATFORM_FIT = {
    "LinkedIn":  ("Primary B2B discovery and authority channel",              True),
    "YouTube":   ("Strong for education-driven B2B buyers",                   True),
    "Facebook":  ("Moderate — useful for B2B retargeting but low organic reach", True),
    "Instagram": ("Low–moderate — lifestyle/B2C skew; works for visual niches", False),
    "TikTok":    ("Low for professional B2B buyers; high for B2C/consumer niches", False),
    "Discord":   ("Gaming/crypto communities — near-zero B2B professional services overlap", False),
}

# Map display name → preloaded channel-data key (case-mismatch between
# linktree.platforms_found, which uses display names, and the preloaded
# dict which uses lowercase keys). X reports under "twitter".
_PLATFORM_PRELOAD_KEY = {
    "LinkedIn":  "linkedin",
    "YouTube":   "youtube",
    "Facebook":  "facebook",
    "Instagram": "instagram",
    "X":         "twitter",
    "Twitter":   "twitter",
    "TikTok":    "tiktok",
    "Discord":   "discord",
}

# Stopwords stripped before building ICP keyword set
_STOPWORDS = {
    "and", "or", "the", "a", "an", "in", "at", "for", "of", "to", "with",
    "who", "that", "is", "are", "their", "our", "your", "my", "we", "i",
    "on", "by", "as", "from", "about", "be", "it", "this", "they",
}


def _derive_icp_keywords(stated_target_market: str) -> List[str]:
    """
    Tokenise the stated target market into searchable keyword phrases.
    Returns single words AND two-word phrases for richer matching.
    e.g. "Financial advisors and CPAs at RIA firms"
      → ["financial", "advisors", "cpas", "ria", "firms",
         "financial advisors", "ria firms"]
    """
    if not stated_target_market:
        return []

    text = stated_target_market.lower()
    # Strip punctuation
    text = re.sub(r"[^\w\s]", " ", text)
    words = [w for w in text.split() if w not in _STOPWORDS and len(w) > 2]

    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for w in words:
        if w not in seen:
            seen.add(w)
            uniq.append(w)

    # Also add two-word phrases
    phrases = [f"{uniq[i]} {uniq[i+1]}" for i in range(len(uniq) - 1)]

    return uniq + phrases


def _icp_hit_count(text: str, keywords: List[str]) -> List[str]:
    """Return keywords found in text (case-insensitive)."""
    tl = text.lower()
    return [kw for kw in keywords if kw in tl]


class ICPAuditor:
    def __init__(self, config: ClientConfig, linktree_data: Dict[str, Any]):
        self.config    = config
        self.linktree  = linktree_data
        self.preloaded = config.preloaded_channel_data
        # Per Dave 2026-05-07: when no stated_target_market is captured,
        # the previous "their ideal client" fallback was leaking into quoted
        # contexts like `in the 'their ideal client' segment` — looks like
        # a placeholder bug to the reader. Switch to "your ideal client"
        # (still a fallback but reads as direct guidance, not a stub) and
        # flag the missing-ICP case so quoted findings can be skipped or
        # rephrased without the awkward quote.
        self.has_stated_icp = bool(config.stated_target_market)
        self.icp       = config.stated_target_market or "your ideal client"
        self.keywords  = _derive_icp_keywords(config.stated_target_market)

    def run(self) -> Dict[str, Any]:
        stated    = self._assess_stated_icp()
        content   = self._assess_content_alignment()
        platforms = self._assess_platform_alignment()
        language  = self._assess_language_alignment()
        gaps      = self._identify_alignment_gaps(stated, content, platforms, language)

        all_issues    = (stated["issues"] + content["issues"] +
                         platforms["issues"] + language["issues"])
        all_strengths = (stated["strengths"] + content["strengths"] +
                         platforms["strengths"] + language["strengths"])

        score = self._compute_alignment_score(all_issues, all_strengths)

        return {
            "score":              score,
            "grade":              self._grade(score),
            "stated_icp":         stated,
            "content_alignment":  content,
            "platform_alignment": platforms,
            "language_alignment": language,
            "alignment_gaps":     gaps,
            "issues":             all_issues,
            "strengths":          all_strengths,
            "icp_verdict":        self._icp_verdict(score, content),
            "recommendations":    self._recommendations(content, platforms),
        }

    # ── Sub-assessments ────────────────────────────────────────

    def _assess_stated_icp(self) -> Dict:
        issues, strengths = [], []

        if not self.config.stated_target_market:
            # Per Dave 2026-05-07: previous message hard-coded "Complete Q4
            # in the intake form" — wrong for admin-triggered audits where
            # the admin form has a different field structure. Reword to be
            # path-agnostic.
            issues.append(
                "🟡 No target market provided — ICP alignment can't be "
                "scored without one. If you have an ideal client in mind, "
                "supply industry + role + revenue range (e.g. 'B2B SaaS "
                "founders, $1-10M ARR') and rerun the audit."
            )
            return {"issues": issues, "strengths": strengths, "stated": ""}

        # Website ICP mentions
        website_mentions = self.preloaded.get("website", {}).get("icp_mentions", [])
        if website_mentions:
            strengths.append(
                f"✅ Website explicitly mentions target audience: {', '.join(website_mentions[:3])}"
            )
        else:
            issues.append(
                f"🔴 Website does not call out '{self.icp}' as the target client. "
                f"Ideal buyers scanning the homepage won't self-identify as the right fit."
            )

        # Social-bio ICP check — only fire when we actually have a bio to
        # check. Per Dave 2026-05-11: GMG audit said both "No social
        # profile bio could be read" (NOT DETECTED) AND "Social bio does
        # not mention [target market]" (VERIFIED GAP) — logically
        # incoherent. We can't claim the bio doesn't mention something
        # when we never read the bio.
        bio = self.linktree.get("bio", "").lower()
        if not bio:
            # Brand auditor already emits one honest "bio not detected"
            # finding (brand_auditor.py:_analyze_bio_consistency). No
            # need to duplicate it here, and we can't make a meaningful
            # alignment claim against an unread bio.
            pass
        else:
            bio_hits = _icp_hit_count(bio, self.keywords)
            if bio_hits:
                strengths.append(
                    f"✅ Social bio mentions ICP-relevant terms: {', '.join(bio_hits[:4])}"
                )
            elif self.has_stated_icp:
                # Only assert misalignment when we have BOTH a real bio
                # and a real target market to compare against. Without
                # either, we're inventing a finding.
                issues.append(
                    f"🔴 Social bio does not mention '{self.icp}'. "
                    f"The first brand touchpoint fails to attract the stated ICP."
                )

        return {
            "issues":    issues,
            "strengths": strengths,
            "stated":    self.config.stated_target_market,
            "industry":  self.config.stated_icp_industry,
        }

    def _assess_content_alignment(self) -> Dict:
        issues, strengths = [], []

        # Gather all content text — include LinkedIn headlines from live scraper
        li              = self.preloaded.get("linkedin", {})
        linkedin_topics = li.get("content_topics", [])
        post_themes     = li.get("post_themes", [])
        recent_headlines = li.get("recent_headlines", [])
        all_content     = " ".join(linkedin_topics + post_themes + recent_headlines).lower()
        content_scraped = bool(all_content.strip())

        icp_hits     = _icp_hit_count(all_content, self.keywords) if content_scraped else []
        general_hits = [s for s in GENERAL_SMB_SIGNALS if s in all_content] if content_scraped else []

        if content_scraped:
            if icp_hits:
                strengths.append(
                    f"✅ Content contains {len(icp_hits)} ICP-relevant signal(s): "
                    f"{', '.join(icp_hits[:5])}"
                )
            else:
                issues.append(
                    f"🔴 CRITICAL: Zero content signals matching '{self.icp}' found across "
                    f"all scraped channels. Content appears written for a general audience, "
                    f"not specifically for {self.icp}."
                )

            if general_hits:
                issues.append(
                    f"🟡 Content contains {len(general_hits)} broad/SMB audience signal(s) "
                    f"that may dilute messaging for '{self.icp}': "
                    f"{', '.join(general_hits[:5])}"
                )

            # ICP alignment ratio — only meaningful when content was scraped
            total = len(icp_hits) + len(general_hits) + 1
            pct   = round(len(icp_hits) / total * 100)
            note  = f"📊 ICP content alignment: {pct}% ICP signals vs {100 - pct}% general/SMB signals."
            if pct < 50:
                issues.append(note)
            else:
                strengths.append(note.replace("📊", "✅"))
        else:
            # No content scraped — flag as unverifiable but don't set pct to misleading 0
            pct = None
            issues.append(
                "🟡 Content could not be scraped from social channels — "
                "ICP alignment of content is unverifiable."
            )

        return {
            "issues":                issues,
            "strengths":             strengths,
            "icp_signals_found":     icp_hits,
            "general_signals_found": general_hits,
            "alignment_percentage":  pct,      # None = unverifiable, int = real measurement
            "content_scraped":       content_scraped,
        }

    def _is_platform_dormant(self, platform: str) -> bool:
        """True if a platform is detected but has no recent activity.

        Uses preloaded channel data (which carries activity signals from
        the social-scraping phase). A dormant account is detected via:
          - days_since_last_post > 60 days, OR
          - posts_per_week explicitly == 0, OR
          - is_active explicitly False

        Returns False (treat as active) when there's no preloaded data
        for the platform — we'd rather miss a dormant flag than falsely
        suppress a real strength.
        """
        key = _PLATFORM_PRELOAD_KEY.get(platform)
        if not key:
            return False
        data = self.preloaded.get(key, {}) or {}
        if data.get("is_active") is False:
            return True
        days_since = data.get("days_since_last_post")
        if isinstance(days_since, (int, float)) and days_since > 60:
            return True
        ppw = data.get("posts_per_week")
        if isinstance(ppw, (int, float)) and ppw == 0:
            return True
        return False

    def _assess_platform_alignment(self) -> Dict:
        issues, strengths = [], []
        platforms = self.linktree.get("platforms_found", [])

        # Detect B2B context from ICP keywords
        b2b_terms  = {"advisor", "advisors", "cpa", "cpas", "attorney", "attorneys",
                      "lawyer", "lawyers", "b2b", "firm", "firms", "corporate",
                      "executive", "director", "manager", "cfo", "cmo", "agency",
                      "enterprise", "professional", "services", "consultant", "consultants"}
        icp_lower  = self.config.stated_target_market.lower()
        is_b2b     = any(t in icp_lower for t in b2b_terms)

        for platform in platforms:
            if platform in ("Email", "Website"):
                continue
            # Dormancy check — Horizon Advisers 2026-05-15: the audit
            # listed "✓ VERIFIED — On X: Platform detected" as a strength
            # for an account with 469 days of silence and 6 followers.
            # A dormant account is not a strength; calling it one
            # misleads the operator and anyone checking the report.
            dormant = self._is_platform_dormant(platform)
            fit_note, positive = PLATFORM_FIT.get(platform, ("Platform detected", True))
            if dormant:
                issues.append(
                    f"🟡 On {platform}: account detected but inactive — "
                    f"the most recent activity is stale. Either reactivate "
                    f"with a posting cadence the platform's algorithm "
                    f"rewards, or deprioritise and archive so the dead "
                    f"profile doesn't signal abandonment to anyone who "
                    f"checks."
                )
                continue
            if positive:
                strengths.append(f"✅ On {platform}: {fit_note}.")
            else:
                if is_b2b:
                    issues.append(
                        f"🔴 On {platform}: {fit_note}. "
                        f"'{self.icp}' typically does not discover vendors here."
                    )
                else:
                    strengths.append(f"✅ On {platform}: {fit_note}.")

        # LinkedIn check for B2B ICPs. Per Dave 2026-05-06: don't surface a
        # "critical gap" when we may simply have failed to find an existing
        # profile — soften the framing and offer a single most-likely reason
        # so the operator can self-diagnose without reading 5 duplicate
        # LinkedIn callouts across pillars.
        if is_b2b and "LinkedIn" not in platforms:
            issues.append(
                f"🟡 LinkedIn presence not detected for '{self.icp}'. "
                f"Most likely cause: no profile is linked from your website "
                f"footer/header (where the audit looks). If a profile exists, "
                f"add a LinkedIn link to your homepage so AI engines and B2B "
                f"buyers can associate it with this business."
            )

        # Email newsletter for any professional ICP.
        # Trust intake-form fields (config.has_active_newsletter,
        # config.email_list_size) over website-crawl heuristics — the
        # client knows whether they run a newsletter; we can only see
        # an embedded signup widget. Horizon Advisers 2026-05-15: the
        # report contradicted itself by emitting "No email newsletter
        # detected" here while the Retention section showed "Newsletter:
        # Active, 3,000 contacts" (from intake).
        newsletter_intake = bool(getattr(self.config, "has_active_newsletter", False))
        newsletter_size   = int(getattr(self.config, "email_list_size", 0) or 0)
        newsletter_site   = bool(self.preloaded.get("website", {}).get("has_newsletter", False))
        if not (newsletter_intake or newsletter_size > 0 or newsletter_site):
            issues.append(
                f"🟡 No email newsletter detected. Email is the highest-ownership channel "
                f"for nurturing '{self.icp}' prospects."
            )

        return {"issues": issues, "strengths": strengths, "platforms_reviewed": platforms}

    def _assess_language_alignment(self) -> Dict:
        issues, strengths = [], []

        li        = self.preloaded.get("linkedin", {})
        bio       = self.linktree.get("bio", "").lower()
        topics    = " ".join(li.get("content_topics", [])).lower()
        headlines = " ".join(li.get("recent_headlines", [])).lower()
        # Include scraped website pages text so the ICP-keyword check
        # sees homepage copy too — Horizon Advisers 2026-05-18: HA's
        # homepage said "individuals, families and businesses get their
        # finances in order" but the prior version of this method only
        # scanned LinkedIn bio + topics + headlines and reported "no
        # ICP language found." run_goguerrilla._merge_website_data now
        # exposes the crawled pages text as preloaded.website.pages_text
        # (capped at 60KB) so this scan can use it with the
        # stated_target_market-derived keywords.
        website_text = (
            self.preloaded.get("website", {}).get("pages_text", "") or ""
        ).lower()
        # Homepage H2 service-block text — strong audience signal when
        # title/H1 don't include ICP terms. Horizon Advisers 2026-05-18:
        # HA's homepage title "Pursue Financial Confidence" and H1
        # "Welcome to Horizon Advisers" didn't contain audience terms,
        # but H2 service blocks did ("Individual Financial Planning",
        # "Employee Benefit Services", "Elder Care and Asset Protection").
        #
        # First pass (c7ba724) used stated_target_market-derived
        # keywords for H2 matching, but those don't match the actual
        # H2 word forms — HA's stated ICP says "individuals" (plural,
        # the firm's word) while the H2 says "Individual" (singular,
        # the service framing); says "businesses" while H2 says
        # "Employee Benefit"; says "families" while H2 says "Elder
        # Care". Real-world service-block H2s rarely match the firm's
        # plural-noun ICP framing literally. Use a hardcoded vocabulary
        # of common audience terms so the detector catches the actual
        # H2 patterns regardless of the firm's chosen ICP framing.
        _AUDIENCE_H2_TERMS = (
            "individual", "person", "personal",
            "famil",  # covers family / families
            "couple",
            "business", "company", "companies",
            "employer", "employee", "corporate", "executive",
            "retiree", "retirement", "elder", "senior",
            "professional",
            "physician", "doctor", "medical practitioner",
            "attorney", "lawyer", "legal practitioner",
            "founder", "entrepreneur",
            "homeowner", "home owner",
            "small business owner", "business owner",
        )
        h2_text_list = (
            self.preloaded.get("website", {}).get("homepage_h2_text", []) or []
        )
        matched_h2s = []
        for h2 in h2_text_list:
            h2_lc = (h2 or "").lower()
            if any(term in h2_lc for term in _AUDIENCE_H2_TERMS):
                matched_h2s.append(h2)
        # Track for the gap-suppression check below
        h2_audience_hits = bool(matched_h2s)
        if matched_h2s:
            strengths.append(
                f"✅ Audience communicated via H2 service blocks: "
                f"{', '.join(matched_h2s[:3])}"
            )

        all_text = f"{bio} {topics} {headlines} {website_text}"

        # Check for ICP-specific language
        icp_hits = _icp_hit_count(all_text, self.keywords)
        if icp_hits:
            strengths.append(
                f"✅ ICP-relevant language in bio/content: {', '.join(icp_hits[:5])}"
            )
        elif self.has_stated_icp and not h2_audience_hits:
            # Only fire this gap when we actually have a stated ICP to
            # compare against AND no H2 service block names the audience.
            # Per Dave 2026-05-18: Horizon Advisers homepage communicated
            # audience via H2 service blocks ("Individual Financial
            # Planning", "Employee Benefit Services") — surfacing the
            # H2 strength above is sufficient; no separate gap finding.
            # Per Dave 2026-05-11: GMG audit showed both
            # "No language specifically targeting 'your ideal client'"
            # AND "No target market provided" — logically incoherent
            # because we can't claim the language doesn't match a target
            # we don't know. Suppress the alignment-vs-content claim when
            # the target market itself is the unknown.
            #
            # Per Dave 2026-05-18: when website pages were crawled but
            # ZERO ICP keywords appeared anywhere, soften the framing
            # because a JS-heavy site may have the ICP language in
            # hydrated content the static fetch missed. Use NOT DETECTED
            # 🟡 (renders as "NOT DETECTED" amber badge) instead of
            # VERIFIED GAP 🔴 so the operator knows this is a maybe-
            # missed signal, not a confirmed absence.
            _platform = (
                self.preloaded.get("website", {}).get("platform") or ""
            ).lower()
            _js_blind = _platform in ("", "unknown", "wix", "squarespace",
                                       "webflow", "shopify")
            if _js_blind and website_text:
                issues.append(
                    f"🟡 No language specifically targeting '{self.icp}' "
                    f"detected in the public copy we crawled. JS-hydrated "
                    f"or widget-rendered text on this platform may not "
                    f"appear in our scan — verify the homepage, bio, and "
                    f"profile copy includes the exact words '{self.icp}' "
                    f"or close variants."
                )
            else:
                issues.append(
                    f"🔴 No language specifically targeting '{self.icp}' detected in bio or content. "
                    f"Use the exact words your ideal client uses to describe their own problems."
                )

        # Generic/misaligned language check
        general_hits = [s for s in GENERAL_SMB_SIGNALS if s in all_text]
        if general_hits:
            issues.append(
                f"🟡 Broad/general audience language detected: {', '.join(general_hits[:5])}. "
                f"This creates messaging dissonance when the stated ICP is '{self.icp}'."
            )

        return {"issues": issues, "strengths": strengths}

    # ── Scoring ────────────────────────────────────────────────

    def _identify_alignment_gaps(self, *sections) -> List[str]:
        gaps = []
        for section in sections:
            for issue in section.get("issues", []):
                if "🔴" in issue:
                    gaps.append(issue.replace("🔴 ", ""))
        return gaps

    def _compute_alignment_score(self, issues: List[str], strengths: List[str]) -> int:
        def _is_unverifiable(i):
            return "unverifiable" in i or "could not be scraped" in i or "unknown" in i.lower()

        critical_real = sum(1 for i in issues if "🔴" in i and not _is_unverifiable(i))
        critical_unkn = sum(1 for i in issues if "🔴" in i and _is_unverifiable(i))
        warning_real  = sum(1 for i in issues if "🟡" in i and not _is_unverifiable(i))
        warning_unkn  = sum(1 for i in issues if "🟡" in i and _is_unverifiable(i))

        base = (50
                - (critical_real * 10)
                - (critical_unkn * 2)
                - (warning_real  * 4)
                - (warning_unkn  * 1)
                + (len(strengths) * 6))

        has_real_data = (critical_real + warning_real) > 0
        return max(25, min(100, base)) if has_real_data else 50

    def _grade(self, score: int) -> str:
        if score >= 80: return "A"
        if score >= 65: return "B"
        if score >= 50: return "C"
        if score >= 35: return "D"
        return "F"

    def _icp_verdict(self, score: int, content: Dict) -> str:
        pct             = content.get("alignment_percentage")   # None = unverifiable
        content_scraped = content.get("content_scraped", False)
        icp_hits        = content.get("icp_signals_found", [])
        icp             = self.icp

        # ── Score-based tier ───────────────────────────────────
        if score >= 70:   score_tier = 2   # high
        elif score >= 50: score_tier = 1   # medium
        else:             score_tier = 0   # low

        # ── Content-based tier (only when actually scraped) ────
        # When content was scraped, take the more conservative of the two tiers
        # so a high platform score can never produce a "well-aligned" verdict
        # alongside zero content signals.
        if content_scraped and pct is not None:
            if pct >= 60:   content_tier = 2
            elif pct >= 25: content_tier = 1
            else:           content_tier = 0   # scraped but low/zero → pull verdict down
            effective_tier = min(score_tier, content_tier)
        else:
            effective_tier = score_tier   # unverifiable content — don't penalise

        # ── Content signal line ─────────────────────────────────
        if not content_scraped:
            content_line = (
                "Platform selection and website messaging are the primary alignment signals — "
                "social content could not be verified from public scraping."
            )
        elif pct == 0:
            content_line = (
                f"No ICP-specific content signals were detected in scraped social content. "
                f"Posts appear written for a general audience rather than specifically for {icp}."
            )
        elif pct < 50:
            content_line = (
                f"Only {pct}% of detected content signals target this ICP — "
                f"messaging is diluted by general or broad-audience content."
            )
        else:
            kw_sample = ", ".join(icp_hits[:4])
            content_line = (
                f"{pct}% of detected content signals match this ICP"
                + (f" (keywords: {kw_sample})" if kw_sample else "")
                + "."
            )

        # ── Verdict copy ────────────────────────────────────────
        if effective_tier == 2:
            return (
                f"Strong ICP alignment detected for: {icp}. "
                f"Platform choices, website messaging, and content positioning are on-target. "
                f"{content_line}"
            )
        elif effective_tier == 1:
            return (
                f"Partial alignment with the stated ICP: {icp}. "
                f"Platform presence and channel selection are appropriate, "
                f"but content messaging gaps are limiting reach to the right buyers. "
                f"{content_line}"
            )
        else:
            return (
                f"CRITICAL MISALIGNMENT: Public content, language, and platform choices "
                f"do not clearly target '{icp}'. "
                f"{content_line} "
                f"An ideal client encountering this brand today would likely not recognise "
                f"it as relevant and would disengage."
            )

    def _recommendations(self, content: Dict, platforms: Dict) -> List[Dict]:
        icp = self.icp
        pct = content.get("alignment_percentage")  # None = unverifiable
        recs = []

        if pct is None or pct < 50:
            recs.append({
                "priority": "CRITICAL",
                "action":   f"Rewrite homepage, bio, and social profiles to speak directly to: {icp}",
                "detail":   (
                    f"Use the exact language {icp} use to describe their pain points and goals. "
                    f"Every headline should make ideal buyers think 'this is for me.'"
                ),
                "timeline": "1–2 weeks",
            })
            recs.append({
                "priority": "CRITICAL",
                "action":   f"Create a dedicated content series for {icp}",
                "detail":   (
                    f"Publish at least 4 pieces of content per month addressing the specific "
                    f"challenges, goals, and objections of {icp}. "
                    f"Interview 2–3 current clients in this segment for exact language."
                ),
                "timeline": "Start immediately",
            })

        if "LinkedIn" in [p for p in platforms.get("platforms_reviewed", [])
                          if "not on" not in p.lower()]:
            pass  # LinkedIn present — no LinkedIn rec needed
        else:
            recs.append({
                "priority": "HIGH",
                "action":   "Activate LinkedIn as the primary B2B channel",
                "detail":   (
                    f"LinkedIn is the highest-ROI channel for reaching {icp}. "
                    f"Post 3–4x/week with ICP-specific content. Optimise the company page "
                    f"headline to include the ICP and core value prop."
                ),
                "timeline": "This week",
            })

        # Per Dave 2026-05-07: when no ICP was captured at intake, drop the
        # awkward `'<icp>' segment` quoted form — reads like a placeholder
        # bug. Use a clean phrasing that doesn't quote a fallback string.
        _icp_seg = (
            f"in the '{icp}' segment" if self.has_stated_icp
            else "across the audiences you currently serve"
        )
        recs.append({
            "priority": "HIGH",
            "action":   f"Conduct ICP interviews with 3 current or past clients {_icp_seg}",
            "detail":   (
                "Record their exact words around pain points, goals, and buying triggers. "
                "Use that verbatim language across all marketing channels."
            ),
            "timeline": "2–3 weeks",
        })

        # Industry-aware case-study rec — Horizon Advisers 2026-05-18:
        # this hardcoded recommendation shipped to a Financial Advisory
        # firm with "before/after metrics" framing despite the synthesis
        # compliance directive, because the row is built here rule-based
        # and bypasses the AI. For industries with advertising/endorsement
        # rules (Financial Advisory, Legal, Healthcare & Medical,
        # Accounting & CPA), substitute a compliant alternative that
        # avoids client-specific outcome claims.
        _ind        = self.config.industry_category or self.config.client_industry or ""
        _compliance = get_compliance_framing(_ind)
        if _compliance:
            recs.append({
                "priority": "MEDIUM",
                "action":   f"Publish anonymized case-style narratives relevant to {icp}",
                "detail":   (
                    f"Industry compliance ({_compliance['regulator']}) "
                    f"restricts client-specific outcome claims. Use "
                    f"anonymized narratives with no client identification "
                    f"and no performance numbers, reviewed by a compliance "
                    f"officer before publishing."
                ),
                "timeline": "3–4 weeks",
            })
        else:
            recs.append({
                "priority": "MEDIUM",
                "action":   f"Build a case study page showing results for {icp}",
                "detail":   (
                    "B2B buyers require proof before hiring. One detailed case study with "
                    "before/after metrics is worth 100 generic testimonials."
                ),
                "timeline": "3–4 weeks",
            })

        return recs
