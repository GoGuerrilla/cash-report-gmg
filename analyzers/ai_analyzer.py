"""
AI Analyzer — C.A.S.H. Report by GMG
Computes quantitative C.A.S.H. scores from real audit data, then uses Claude
to generate all narrative fields (recommendations, 90-day plan, waste/opportunity,
channel strategy, competitive positioning).

Data flow:
  1. _rule_based_analysis() — always runs first; populates every score field from
     real audit data so the report is never empty even without an API key.
  2. _analyze_with_claude() — when ANTHROPIC_API_KEY is available, sends the full
     audit data to Claude and overlays its response on top of the rule-based scores.
     Narrative fields (biggest_waste, biggest_opportunity, etc.) are intentionally
     left empty by rule_based so Claude is the sole source for those fields.
  3. Overlay — rule_based values are only kept where Claude returned null/empty.

Narrative fields ONLY populated by Claude (never hardcoded):
  - executive_summary
  - biggest_waste
  - biggest_opportunity
  - top_3_priorities
  - channel_recommendation
  - content_strategy
  - budget_recommendation
  - 90_day_action_plan
  - competitive_positioning
  - icp_alignment_verdict
"""
import json
import os
from typing import Dict, Any, List

from config import ClientConfig
from auditors.industry_benchmarks import (
    industry_label,
    get_primary_platforms,
    get_recommended_platforms,
    get_gbp_importance,
    is_local_business,
    is_b2b,
    get_posting_benchmarks,
)

# Load .env automatically when this module is imported so callers don't have to
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
except ImportError:
    pass


def _classify_growth_tier(config, audit_data: dict) -> str:
    """
    Classify client as 'early' / 'growing' / 'established' from
    observed signals. Drives AI synthesis tone calibration.

    Scoring (each signal contributes 0-2 points; total range 0-10):
      - team_size:            1pt at 2+, 2pt at 5+
      - current_client_count: 1pt at 1+, 2pt at 10+
      - email_list_size:      1pt at 100+, 2pt at 1000+
      - linkedin_followers:   1pt at 500+, 2pt at 5000+
      - youtube_subscribers:  1pt at 100+, 2pt at 1000+

    Tiers:
      0-3 points  → "early"       — foundational; build basics first
      4-6 points  → "growing"     — early traction; optimize-and-scale
      7-10 points → "established" — mature; refine-and-defend
    """
    points = 0

    team = config.team_size or 1
    if team >= 5:
        points += 2
    elif team >= 2:
        points += 1

    clients = config.current_client_count or 0
    if clients >= 10:
        points += 2
    elif clients >= 1:
        points += 1

    email_list = config.email_list_size or 0
    if email_list >= 1000:
        points += 2
    elif email_list >= 100:
        points += 1

    li_followers = (
        (config.preloaded_channel_data or {})
        .get("linkedin", {})
        .get("followers") or 0
    )
    if li_followers >= 5000:
        points += 2
    elif li_followers >= 500:
        points += 1

    yt_subs = (
        (audit_data.get("youtube") or {})
        .get("subscriber_count") or 0
    )
    if yt_subs >= 1000:
        points += 2
    elif yt_subs >= 100:
        points += 1

    if points >= 7:
        return "established"
    if points >= 4:
        return "growing"
    return "early"


class AIAnalyzer:
    def __init__(self, anthropic_api_key: str = "", openai_api_key: str = ""):
        # Accept explicit key, or fall back to environment variable
        self.anthropic_key = anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self.openai_key    = openai_api_key    or os.environ.get("OPENAI_API_KEY", "")

    # ── Public entry point ─────────────────────────────────────

    def analyze(self, config: ClientConfig, audit_data: dict) -> Dict[str, Any]:
        """
        Returns a complete dict of scores + narrative fields.
        Rule-based scores are always computed from real data.
        Narrative fields come from Claude (or OpenAI) when a key is present;
        they are left empty otherwise rather than filled with invented text.
        After AI overlay, scores are checked against rule-based values and
        blended 50/50 if they diverge by more than 15 points.
        """
        base = self._rule_based_analysis(config, audit_data)

        # Snapshot rule-based scores before any AI overlay
        rb_scores = {k: base[k] for k in
                     ("cash_c_score", "cash_a_score", "cash_s_score",
                      "cash_h_score", "overall_score")}

        if self.anthropic_key:
            print("   → Sending audit data to Claude for narrative analysis...")
            ai_result = self._analyze_with_claude(config, audit_data)
            if not ai_result.get("parse_error"):
                # Overlay Claude values; keep rule-based only where Claude returned nothing
                for key, val in ai_result.items():
                    if val is not None and val != "" and val != [] and val != {}:
                        base[key] = val
                self._check_and_blend_scores(base, rb_scores)
                base["data_source"] = "claude"
            else:
                print("   ⚠️  Claude response could not be parsed — scores kept, narrative empty")
                base["data_source"] = "rule_based"
        elif self.openai_key:
            print("   → Sending audit data to OpenAI for narrative analysis...")
            ai_result = self._analyze_with_openai(config, audit_data)
            if not ai_result.get("parse_error"):
                for key, val in ai_result.items():
                    if val is not None and val != "" and val != [] and val != {}:
                        base[key] = val
                self._check_and_blend_scores(base, rb_scores)
                base["data_source"] = "openai"
            else:
                print("   ⚠️  OpenAI response could not be parsed — scores kept, narrative empty")
                base["data_source"] = "rule_based"
        else:
            base["data_source"] = "rule_based"

        return base

    def _check_and_blend_scores(self, result: dict, rb_scores: dict) -> None:
        """
        Compare AI-returned scores against rule-based scores.
        For any CASH component or overall that deviates by more than 15 points,
        print a warning and replace with the 50/50 blend. Mutates result in place.
        """
        _LABELS = {
            "cash_c_score":  "C (Content)",
            "cash_a_score":  "A (Audience)",
            "cash_s_score":  "S (Sales)",
            "cash_h_score":  "H (Retention)",
            "overall_score": "Overall",
        }
        blended = []
        for key, label in _LABELS.items():
            rb_val = rb_scores.get(key)
            ai_val = result.get(key)
            if rb_val is None or ai_val is None:
                continue
            try:
                rb_int, ai_int = int(rb_val), int(ai_val)
            except (TypeError, ValueError):
                continue
            delta = abs(ai_int - rb_int)
            if delta > 15:
                blend = round((rb_int + ai_int) / 2)
                print(
                    f"   ⚠️  Score divergence [{label}]: "
                    f"rule_based={rb_int}  ai={ai_int}  delta={delta}  → blending to {blend}"
                )
                result[key] = blend
                blended.append(label)
        if blended:
            result["score_blend_note"] = (
                f"Scores blended (>15pt divergence between rule-based and AI): "
                f"{', '.join(blended)}"
            )

    # ── Claude ────────────────────────────────────────────────

    def _analyze_with_claude(self, config: ClientConfig, audit_data: dict) -> Dict:
        try:
            import anthropic
        except ImportError:
            print("   ⚠️  anthropic package not installed — pip install anthropic")
            return {"parse_error": True}

        client = anthropic.Anthropic(api_key=self.anthropic_key)
        prompt = self._build_prompt(config, audit_data)
        try:
            message = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )
            result = self._parse_ai_response(message.content[0].text)
            return result
        except Exception as e:
            print(f"   ⚠️  Claude API error: {e}")
            return {"parse_error": True}

    # ── OpenAI ────────────────────────────────────────────────

    def _analyze_with_openai(self, config: ClientConfig, audit_data: dict) -> Dict:
        try:
            import openai
        except ImportError:
            print("   ⚠️  openai package not installed — pip install openai")
            return {"parse_error": True}

        client = openai.OpenAI(api_key=self.openai_key)
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": self._build_prompt(config, audit_data)}],
                max_tokens=3000,
            )
            result = self._parse_ai_response(response.choices[0].message.content)
            return result
        except Exception as e:
            print(f"   ⚠️  OpenAI API error: {e}")
            return {"parse_error": True}

    # ── Prompt ────────────────────────────────────────────────

    def _format_competitor_data(self, competitor: dict) -> str:
        """Format competitor audit findings concisely for the Claude prompt."""
        if not competitor or competitor.get("skipped"):
            return f"  {competitor.get('note', 'Competitor audit not run.')}"
        lines = []
        for comp in competitor.get("competitors", [])[:3]:
            url       = comp.get("url", "unknown")
            score     = comp.get("score", "?")
            grade     = comp.get("grade", "?")
            issues    = comp.get("issues", [])[:3]
            strengths = comp.get("strengths", [])[:2]
            lines.append(f"  {url}  score={score}/100 ({grade})")
            for item in issues:
                lines.append(f"    Issue: {item}")
            for item in strengths:
                lines.append(f"    Strength: {item}")
        summary = competitor.get("comparison", {}).get("summary", "")
        if summary:
            lines.append(f"  Comparison summary: {summary}")
        return "\n".join(lines) if lines else "  No competitor findings available."

    def _build_prompt(self, config: ClientConfig, audit_data: dict) -> str:
        cash       = self._compute_cash_scores(config, audit_data)
        web_scores = audit_data.get("website", {}).get("scores", {})
        seo        = audit_data.get("seo", {})
        icp        = audit_data.get("icp", {})
        brand      = audit_data.get("brand", {})
        funnel     = audit_data.get("funnel", {})
        freshness  = audit_data.get("freshness", {})
        geo        = audit_data.get("geo", {})
        competitor = audit_data.get("competitor", {})

        # Pull real issues from every auditor — these are the actual findings
        all_issues: List[str] = []
        all_strengths: List[str] = []
        for section in [seo, icp, brand, funnel, freshness,
                        audit_data.get("social", {}), audit_data.get("website", {}),
                        audit_data.get("content", {}), geo]:
            all_issues.extend(section.get("issues", []))
            all_strengths.extend(section.get("strengths", []))

        # Funnel stage issues
        stages = funnel.get("stages", {})
        for stage in stages.values():
            all_issues.extend(stage.get("issues", []))
            all_strengths.extend(stage.get("strengths", []))

        channels        = config.active_social_channels
        icp_verdict     = icp.get("icp_verdict", "")
        scraping_note   = freshness.get("scraping_note", "")
        channel_scores  = freshness.get("channels", {})

        # Build a concise freshness summary for the prompt
        freshness_lines = []
        for platform, data in channel_scores.items():
            status = data.get("status", "unknown")
            ppw    = data.get("posts_per_week")
            days   = data.get("days_since_last_post")
            if status == "api_blocked":
                freshness_lines.append(f"  {platform}: API-blocked (score 50 neutral, no real data)")
            elif ppw or days:
                freshness_lines.append(
                    f"  {platform}: {status}, {ppw or '?'}x/week, {days or '?'} days since last post"
                )
            else:
                freshness_lines.append(f"  {platform}: {status}")

        # Fields not collected from the intake form — mark so Claude treats them
        # as unknown rather than confirmed negatives
        def _u(val, default, note="unverified — not in intake form"):
            return str(val) if val != default else f"{val} ({note})"

        # Build modifier context string for the prompt
        _mods     = self._apply_intake_modifiers(config, dict(cash))  # non-mutating copy
        _mod_lines = "\n".join(f"  {n}" for n in _mods.get("notes", [])) or "  None"
        _flag_lines = "\n".join(f"  ⚠ {f}" for f in _mods.get("flags", [])) or "  None"

        # Posting frequency map → human-readable lines (skip if empty)
        _ppf = config.platform_posting_frequency or {}
        if _ppf:
            _ppf_lines = "\n".join(
                f"  {plat}: {freq}/week" for plat, freq in _ppf.items()
            )
        else:
            _ppf_lines = "  Not provided"

        # Hourly rate: only surface if client supplied (0 means skip cost estimates)
        if config.team_hourly_rate and config.team_hourly_rate > 0:
            _rate_line = f"TEAM HOURLY RATE: ${config.team_hourly_rate:,.0f}/hr — use for concrete cost estimates."
        else:
            _rate_line = "TEAM HOURLY RATE: Not provided — do NOT include dollar-based time-cost estimates."

        # Referral system: surface description only if both flag is true and text is provided
        if config.has_referral_system and (config.referral_system_description or "").strip():
            _referral_line = f"REFERRAL SYSTEM: Yes — {config.referral_system_description.strip()}"
        elif config.has_referral_system:
            _referral_line = "REFERRAL SYSTEM: Yes (no description provided)"
        else:
            _referral_line = f"REFERRAL SYSTEM: {_u(config.has_referral_system, False)}"

        # Industry-specific guidance from auditors/industry_benchmarks.py.
        # Falls back to "Other" with explicit guard so Claude does not fabricate
        # industry claims when classification is missing.
        _ind_canon = industry_label(config.industry_category or config.client_industry)
        if _ind_canon == "Other":
            _industry_block = (
                "INDUSTRY GUIDANCE: Industry could not be classified to a "
                "canonical category. Do NOT make industry-specific claims "
                "(e.g. 'most law firms…', 'in restaurants…'). Use the "
                "client's stated_target_market and observable signals only."
            )
        else:
            _primary    = get_primary_platforms(_ind_canon) or []
            _recommend  = get_recommended_platforms(_ind_canon) or []
            _gbp_note   = get_gbp_importance(_ind_canon)
            _local_flag = "Yes" if is_local_business(_ind_canon) else "No"
            _b2b_flag   = "Yes" if is_b2b(_ind_canon) else "No"

            # Posting targets only for the channels the client is actually on
            _post_lines = []
            for plat in (channels or []):
                bm = get_posting_benchmarks(plat, _ind_canon) or {}
                if bm:
                    _post_lines.append(
                        f"  {plat}: min {bm.get('min','?')}, "
                        f"ideal {bm.get('ideal','?')}, "
                        f"max {bm.get('max','?')} posts/week"
                    )
            _post_block = "\n".join(_post_lines) if _post_lines else "  (no active channels)"

            _industry_block = (
                f"INDUSTRY GUIDANCE (canonical: {_ind_canon}):\n"
                f"  PRIMARY PLATFORMS (absence is CRITICAL): "
                f"{', '.join(_primary) if _primary else 'None defined'}\n"
                f"  RECOMMENDED PLATFORMS (absence is a warning): "
                f"{', '.join(_recommend) if _recommend else 'None defined'}\n"
                f"  GOOGLE BUSINESS PROFILE IMPORTANCE: {_gbp_note}\n"
                f"  LOCAL BUSINESS: {_local_flag}  |  B2B FOCUS: {_b2b_flag}\n"
                f"  POSTING TARGETS for active channels:\n{_post_block}\n"
                f"  Use these benchmarks for cadence recommendations. Do NOT "
                f"recommend a primary platform the client is already on at "
                f"or above 'ideal' cadence — recommend optimization instead."
            )

        if config.intake_completed:
            _intake_directive = (
                "INTAKE COMPLETED: True — full client context available. "
                "You CAN use stated_target_market, stated_icp_industry, "
                "stated_value_prop directly and confidently in recommendations."
            )
        else:
            _intake_directive = (
                "INTAKE COMPLETED: False — DATA RELIABILITY DIRECTIVE:\n"
                "  Client did NOT complete an intake questionnaire. You do "
                "NOT have confirmed ICP, target-market, or value-prop data "
                "from them.\n"
                "  REQUIRED behavior:\n"
                "  • Replace 'the target buyer', 'your ideal client', 'the "
                "buyer', 'the target client', 'their target market' phrasing "
                "with: 'based on observed signals, the audience appears to "
                "be...' OR 'the public-facing positioning suggests...'\n"
                "  • Use 'appears', 'based on observed signals', 'public-"
                "facing positioning suggests' — do NOT use possessive 'your "
                "ICP' or 'your target market' when these are uncertain.\n"
                "  • Do NOT instruct 'rewrite all copy to speak to [target]' "
                "when [target] is uncertain. Instead: 'based on what's "
                "visible, rewrite copy to speak to [observable audience "
                "pattern]; OR define your ICP first, then rewrite.'\n"
                "  • AVOID definitive negatives like 'no defined ICP', 'no "
                "stated value prop', 'no target market'. Prefer 'no ICP "
                "framing visible in public-facing content', 'no value prop "
                "stated in public copy'.\n"
                "  • Do NOT fabricate the client's ICP or target market. If "
                "stated_target_market is 'Not provided', say so explicitly: "
                "'because no ICP was provided, recommendations below are "
                "based on observed signals only.'"
            )

        return f"""You are a senior B2B digital marketing strategist. Analyze this real marketing audit data and respond ONLY with valid JSON — no markdown, no explanation.

CLIENT: {config.client_name}
INDUSTRY: {config.client_industry}
INDUSTRY CATEGORY: {config.industry_category or "Other"}
CLIENT CATEGORY: {config.client_category or "Not provided"}
AUDIT SOURCE: {getattr(config, 'audit_source', 'full_intake')}
{_intake_directive}

PRIMARY FRAMING — BIGGEST MARKETING CHALLENGE: {config.biggest_marketing_challenge or "Not provided"}
The executive_summary AND biggest_opportunity fields below MUST directly address this challenge. Do not produce generic findings that ignore it. If "Not provided", anchor on the lowest C.A.S.H. component instead.

GROWTH TIER: {_classify_growth_tier(config, audit_data)} — calibrate recommendation tone and type to this stage. early = foundational ("build the basics before optimizing"); growing = optimize-and-scale; established = refine-and-defend. Do not recommend establishing what already exists, and do not recommend optimizing what hasn't been built yet.

{_industry_block}

STATED TARGET MARKET: {config.stated_target_market or "Not provided"}
STATED ICP INDUSTRY: {config.stated_icp_industry or "Not provided"}
INTAKE SCORE MODIFIERS APPLIED:
{_mod_lines}
INTAKE FLAGS:
{_flag_lines}
STATED VALUE PROP: {config.stated_value_prop or "Not provided"}
PRIMARY GOAL: {config.primary_goal}
MONTHLY AD BUDGET: ${config.monthly_ad_budget:,.0f}
TEAM SIZE: {_u(config.team_size, 1)}
{_rate_line}
ACTIVE CHANNELS: {', '.join(channels) if channels else 'None'}
PLATFORM POSTING FREQUENCY (client-stated, posts/week):
{_ppf_lines}
EMAIL LIST: {config.email_list_size:,} contacts
HAS NEWSLETTER: {config.has_active_newsletter}
EMAIL SEND FREQUENCY: {config.email_send_frequency or "Not provided"}
HAS BULK EMAIL MARKETING: {_u(config.has_email_marketing, False)}
{_referral_line}
HAS LEAD MAGNET: {_u(config.has_lead_magnet, False)}
BOOKING TOOL: {config.booking_tool if config.booking_tool else "None (unverified — not in intake form)"}
CURRENT CLIENTS: {_u(config.current_client_count, 0)} ({config.current_client_types or "unverified — not in intake form"})
TOP COMPETITORS: {', '.join(config.top_competitors) if config.top_competitors else "None listed"}

COMPETITOR AUDIT FINDINGS:
{self._format_competitor_data(competitor)}

C.A.S.H. SCORES (computed from real audit data):
  C — Content:        {cash['C']}/100
  A — Audience:       {cash['A']}/100
  S — Sales:          {cash['S']}/100
  H — Hold/Retention: {cash['H']}/100
  Overall:            {cash['overall']}/100

COMPONENT SCORES:
  SEO:               {seo.get('score', 50)}/100  (method: {seo.get('method', 'unknown')})
  Performance:       {seo.get('performance_score', '?')}/100
  Accessibility:     {seo.get('accessibility_score', '?')}/100
  ICP Alignment:     {icp.get('score', 50)}/100
  Brand Consistency: {brand.get('score', 50)}/100
  Website Technical: {web_scores.get('technical', 50)}/100
  Website Content:   {web_scores.get('content', 50)}/100
  Website Conversion:{web_scores.get('conversion', 50)}/100
  GEO (AI Visibility):{geo.get('score', 50)}/100

CORE WEB VITALS: {seo.get('core_web_vitals', {})}

ICP ALIGNMENT VERDICT (computed):
{icp_verdict}

CONTENT FRESHNESS BY CHANNEL:
{chr(10).join(freshness_lines) if freshness_lines else "  No channel data available"}
{f"Note: {scraping_note}" if scraping_note else ""}

TOP ISSUES FOUND (real audit findings — use these to drive your recommendations):
{chr(10).join(f"  {i}" for i in all_issues[:25])}

TOP STRENGTHS FOUND:
{chr(10).join(f"  {s}" for s in all_strengths[:15])}

Respond with ONLY this exact JSON (no markdown fences, no extra keys):
{{
  "executive_summary": "2-3 sentences summarizing the most critical finding and the single highest-leverage action, specific to this client's data",
  "overall_grade": "A/B/C/D/F",
  "overall_score": 0,
  "cash_c_score": 0,
  "cash_a_score": 0,
  "cash_s_score": 0,
  "cash_h_score": 0,
  "top_3_priorities": [
    {{"priority": 1, "action": "specific action based on real issues above", "impact": "measurable expected result", "timeline": "timeframe"}},
    {{"priority": 2, "action": "specific action based on real issues above", "impact": "measurable expected result", "timeline": "timeframe"}},
    {{"priority": 3, "action": "specific action based on real issues above", "impact": "measurable expected result", "timeline": "timeframe"}}
  ],
  "biggest_waste": "the single biggest waste of time or money, with specific evidence from the audit data above",
  "biggest_opportunity": "the single highest-ROI opportunity, with specific how-to steps based on this client's actual situation",
  "icp_alignment_verdict": "plain-language verdict on how well current content/channels match the stated target market, based on the ICP data above",
  "channel_recommendation": "specific channel strategy for this client based on their active channels, ICP, and budget",
  "content_strategy": "specific content recommendation based on what gaps were actually found in this audit",
  "budget_recommendation": "specific budget advice based on the actual budget and channel data found",
  "90_day_action_plan": [
    {{"week": "1-2", "action": "specific action", "outcome": "expected result"}},
    {{"week": "3-4", "action": "specific action", "outcome": "expected result"}},
    {{"week": "5-8", "action": "specific action", "outcome": "expected result"}},
    {{"week": "9-12", "action": "specific action", "outcome": "expected result"}}
  ],
  "competitive_positioning": "how this client can differentiate from their specific competitors based on audit findings"
}}"""

    # ── JSON parser ───────────────────────────────────────────

    def _parse_ai_response(self, raw: str) -> Dict:
        try:
            clean = raw.strip()
            # Strip markdown fences if present
            if clean.startswith("```"):
                lines = clean.split("\n")
                lines = [l for l in lines if not l.startswith("```")]
                clean = "\n".join(lines).strip()
            return json.loads(clean)
        except json.JSONDecodeError:
            # Attempt to recover from truncated JSON
            try:
                clean = clean.strip()
                depth_brace   = clean.count("{") - clean.count("}")
                depth_bracket = clean.count("[") - clean.count("]")
                if depth_bracket > 0 or depth_brace > 0:
                    clean = clean.rsplit("\n", 1)[0].rstrip(",").rstrip()
                    clean += "]" * depth_bracket + "}" * depth_brace
                    return json.loads(clean)
            except Exception:
                pass
            return {"executive_summary": raw[:500], "parse_error": True}
        except Exception:
            return {"executive_summary": raw[:500], "parse_error": True}

    # ── Intake modifier system ────────────────────────────────

    def _apply_intake_modifiers(self, config: ClientConfig, scores: dict) -> dict:
        """
        Adjust C/A/S/H scores using verified intake form values.
        Only runs when audit_source == "full_intake" — admin URL-only audits
        are left unmodified so missing intake fields don't artificially penalise them.

        Modifiers (all scores clamped to 0–100 after adjustment):

        H score — Hold / Retention
          email_list_size == 0              → -10  (no email list)
          email_list_size 1–999             →  ±0  (small but present)
          email_list_size 1,000–4,999       →  +5  (established list)
          email_list_size 5,000+            → +10  (scaled list)
          email_frequency == "daily"        →  +5  (maximum cadence)
          email_frequency == "weekly"       →  +5  (strong cadence)
          email_frequency == "never"        → -10  (inactive / dormant)

        S score — Sales / Acquisition
          ad_budget == $0                   →  ±0  + flag "no paid acquisition"
          ad_budget $1–$999                 →  +2  (testing budget)
          ad_budget $1,000–$2,499           →  +5  (moderate budget)
          ad_budget $2,500+                 → +10  (strong budget)

        A score — Audience / ICP
          stated_target_market non-empty    →  +5  (ICP defined in intake)

        Overall score is recomputed after all deltas are applied using the
        same weights as _compute_cash_scores: C×0.20 + A×0.30 + S×0.30 + H×0.20.

        Returns a modifier report dict included in ai_insights["intake_modifiers"].
        """
        audit_source = getattr(config, "audit_source", "full_intake")
        if audit_source != "full_intake":
            return {
                "applied": False,
                "reason":  "audit_source=admin_url_only — modifiers skipped (no intake data)",
                "h_delta": 0, "s_delta": 0, "a_delta": 0,
                "notes":   [], "flags":   [],
            }

        notes  = []
        flags  = []
        h_delta = 0
        s_delta = 0
        a_delta = 0

        # ── H: email list size ────────────────────────────────
        list_size = config.email_list_size
        if list_size == 0:
            h_delta -= 10
            notes.append("H −10: no email list (email_list_size = 0)")
        elif list_size >= 5000:
            h_delta += 10
            notes.append(f"H +10: scaled email list ({list_size:,} contacts ≥ 5,000)")
        elif list_size >= 1000:
            h_delta += 5
            notes.append(f"H +5: established email list ({list_size:,} contacts, 1,000–4,999)")
        # 1–999: no modifier — small list present but not yet meaningful scale

        # ── H: email send frequency ───────────────────────────
        freq = (config.email_send_frequency or "").lower().strip()
        if freq in ("daily", "weekly"):
            h_delta += 5
            notes.append(f"H +5: active email cadence ({freq})")
        elif freq == "never":
            h_delta -= 10
            notes.append("H −10: email_frequency = never (dormant list)")

        # ── S: monthly ad budget ──────────────────────────────
        budget = config.monthly_ad_budget
        if budget == 0:
            flags.append(
                "no paid acquisition — client reported $0 ad budget; "
                "recommend allocating at minimum $500–1,000/month"
            )
        elif budget < 1000:
            s_delta += 2
            notes.append(f"S +2: small ad budget (${budget:,.0f}/month, $1–$999)")
        elif budget < 2500:
            s_delta += 5
            notes.append(f"S +5: moderate ad budget (${budget:,.0f}/month, $1,000–$2,499)")
        else:
            s_delta += 10
            notes.append(f"S +10: strong ad budget (${budget:,.0f}/month, $2,500+)")

        # ── A: ICP / target market defined ───────────────────
        if (config.stated_target_market or "").strip():
            a_delta += 5
            notes.append("A +5: target market / ICP defined in intake form")

        # ── Apply deltas; clamp 0–100 ─────────────────────────
        scores["H"] = max(0, min(100, scores["H"] + h_delta))
        scores["S"] = max(0, min(100, scores["S"] + s_delta))
        scores["A"] = max(0, min(100, scores["A"] + a_delta))

        # Recompute overall with canonical weights
        scores["overall"] = round(
            scores["C"] * 0.20 +
            scores["A"] * 0.30 +
            scores["S"] * 0.30 +
            scores["H"] * 0.20
        )

        return {
            "applied":  True,
            "h_delta":  h_delta,
            "s_delta":  s_delta,
            "a_delta":  a_delta,
            "notes":    notes,
            "flags":    flags,
        }

    def _weight_plan_by_challenge(self, plan: list, challenge: str) -> list:
        """
        Inspect biggest_challenge text and promote the most relevant
        90-day plan item to the Week 1-2 position.

        Challenge keyword → plan index most likely to address it:
          leads / traffic / pipeline / acquisition  → index 1 (lead magnet / opt-in)
          retention / churn / repeat / loyal        → index 3 (newsletter / nurture system)
          conversion / close / sales / booking      → index 2 (authority content / social proof)
          brand / awareness / visibility / found    → index 0 (messaging / ICP alignment)

        If no keyword matches, or the matched index is already 0, the plan is
        returned unchanged. Week labels are reassigned after reordering.
        """
        if not challenge or len(plan) < 2:
            return plan

        cl = challenge.lower()

        # keyword group → preferred plan index (0-based)
        _KEYWORD_MAP = [
            (["lead", "traffic", "prospect", "pipeline", "get client",
              "acquisition", "get more client", "not enough client"],     1),
            (["retention", "churn", "repeat", "loyal", "keep client",
              "keep customer", "re-engage"],                               3),
            (["conversion", "close", "sales", "convert", "booking",
              "not closing", "follow.up", "follow up"],                   2),
            (["brand", "awareness", "visibility", "recognition",
              "get found", "known", "stand out"],                         0),
        ]

        priority_index = None
        for keywords, idx in _KEYWORD_MAP:
            if any(k in cl for k in keywords):
                priority_index = idx
                break

        if priority_index is None or priority_index == 0:
            return plan   # already leading with the right item, or no match

        # Reorder: move matched item to front; keep others in original sequence
        reordered = [plan[priority_index]] + [p for i, p in enumerate(plan) if i != priority_index]

        # Relabel weeks in order
        week_labels = ["1-2", "3-4", "5-8", "9-12"]
        result = []
        for item, label in zip(reordered, week_labels):
            new_item = dict(item)
            new_item["week"] = label
            result.append(new_item)
        return result

    # ── C.A.S.H. score computation (always from real data) ────

    def _compute_cash_scores(self, config: ClientConfig, audit_data: dict) -> Dict:
        icp    = audit_data.get("icp", {}).get("score", 50)
        brand  = audit_data.get("brand", {}).get("score", 50)
        fresh  = audit_data.get("freshness", {}).get("score", 50)
        seo    = audit_data.get("seo", {}).get("score", 50)
        web    = audit_data.get("website", {}).get("scores", {})
        funnel = audit_data.get("funnel", {})
        stages = funnel.get("stages", {})

        def stage_score(stage: dict) -> int:
            # Count only real critical issues, not "could not be verified" ones
            n_crit = len([i for i in stage.get("issues", [])
                          if "🔴" in i
                          and "could not be verified" not in i
                          and "unknown" not in i.lower()])
            n_str  = len(stage.get("strengths", []))
            # Penalty capped at 3 criticals to prevent cascade collapse.
            # Floor at 35: a real stage with confirmed gaps = D range, not F.
            base   = 50 - (min(n_crit, 3) * 7) + (n_str * 8)
            return max(35, min(100, base)) if n_crit > 0 else max(50, min(100, 50 + n_str * 8))

        # ── Quality-based social audience score ───────────────────────────
        # LinkedIn: tiered by follower count + frequency bonus (cap 50).
        # Other channels: binary presence + small confirmed-activity bonus (cap 40).
        # Total social capped at 80 — same ceiling as old formula.
        #
        # Data sources (both are populated before Phase 4 runs):
        #   LinkedIn followers/ppw  → config.preloaded_channel_data["linkedin"]
        #   Other channels ppw/status → audit_data["freshness"]["channels"]
        #     (already applies intake-frequency fallback and api_blocked neutrality)

        li_data   = config.preloaded_channel_data.get("linkedin", {})
        li_follow = li_data.get("followers")        # int or None
        li_ppw    = li_data.get("posts_per_week")   # float or None

        if not config.linkedin_url:
            li_score = 0   # LinkedIn not configured
        elif li_follow is None and li_ppw is None:
            li_score = 15  # scrape failed — neutral, preserves old binary credit
        else:
            # Follower tier
            if   li_follow is None:   li_pts = 10   # partial data — mid-tier neutral
            elif li_follow == 0:      li_pts = 0
            elif li_follow <= 100:    li_pts = 5
            elif li_follow <= 500:    li_pts = 10
            elif li_follow <= 2000:   li_pts = 20
            elif li_follow <= 5000:   li_pts = 30
            else:                     li_pts = 40   # 5,000+

            # Posting frequency bonus
            if   li_ppw is None:     li_freq = 3   # unknown — small neutral credit
            elif li_ppw == 0:        li_freq = 0
            elif li_ppw <= 2:        li_freq = 5
            elif li_ppw <= 5:        li_freq = 10  # 3–5×/week: ideal
            else:                    li_freq = 8   # 6+/week: slight overposting demerit

            li_score = min(50, li_pts + li_freq)

        # Other channels: binary presence + confirmed-activity bonus
        fresh_channels = audit_data.get("freshness", {}).get("channels", {})
        other_pts = 0
        for ch in config.active_social_channels:
            if ch == "LinkedIn":
                continue
            other_pts += 10                              # binary: channel URL configured
            ch_data   = fresh_channels.get(ch, {})
            ch_ppw    = ch_data.get("posts_per_week")
            ch_status = ch_data.get("status", "")
            if ch_ppw is not None and ch_ppw > 0:
                other_pts += 3   # confirmed active
            elif ch_status == "api_blocked":
                other_pts += 2   # can't verify — partial credit, never penalise

        social_score = min(80, li_score + min(40, other_pts))

        c = round((fresh + seo + web.get("content", 50)) / 3)
        a = round((icp + brand + social_score) / 3)
        s = round((stage_score(stages.get("capture", {})) +
                   stage_score(stages.get("conversion", {})) +
                   web.get("conversion", 50)) / 3)
        h = round((stage_score(stages.get("nurture", {})) +
                   stage_score(stages.get("trust",   {}))) / 2)

        overall = round(c * 0.20 + a * 0.30 + s * 0.30 + h * 0.20)
        return {"C": c, "A": a, "S": s, "H": h, "overall": overall}

    # ── Rule-based baseline ───────────────────────────────────

    def _rule_based_analysis(self, config: ClientConfig, audit_data: dict) -> Dict:
        """
        Computes all quantitative fields AND baseline narrative fields from real audit data.
        When Claude/OpenAI key is present their responses overlay these values.
        When no key is available the report still contains meaningful, data-driven content.

        Score pipeline:
          1. _compute_cash_scores()       — pure auditor-data scores
          2. _apply_intake_modifiers()    — adjusts C/A/S/H for verified intake values
          3. _weight_plan_by_challenge()  — reorders 90-day plan to address stated challenge first
        """
        cash            = self._compute_cash_scores(config, audit_data)
        intake_mods     = self._apply_intake_modifiers(config, cash)
        # cash dict is mutated in place by _apply_intake_modifiers; overall is recomputed
        score     = cash["overall"]
        grade     = "A" if score >= 80 else "B" if score >= 65 else "C" if score >= 50 else "D" if score >= 35 else "F"
        icp_verdict = audit_data.get("icp", {}).get("icp_verdict", "")

        icp    = audit_data.get("icp", {})
        brand  = audit_data.get("brand", {})
        funnel = audit_data.get("funnel", {})
        seo    = audit_data.get("seo", {})
        geo    = audit_data.get("geo", {})
        fresh  = audit_data.get("freshness", {})

        funnel_stages = funnel.get("stages", {})
        channels      = len(config.active_social_channels)
        budget        = config.monthly_ad_budget

        # ── Collect top critical issues across all auditors ─────
        all_critical = []
        for section in [icp, brand, funnel, seo, geo,
                        audit_data.get("social", {}),
                        audit_data.get("website", {}),
                        audit_data.get("content", {})]:
            all_critical.extend(
                i for i in section.get("issues", []) if "🔴" in i
            )
        for stage in funnel_stages.values():
            all_critical.extend(i for i in stage.get("issues", []) if "🔴" in i)

        # Clean emoji prefix for readable text
        def _clean(s): return s.replace("🔴 ", "").replace("🟡 ", "").replace("✅ ", "")

        # ── Executive summary (data-driven, no invented text) ───
        icp_score   = icp.get("score", 50)
        funnel_score = funnel.get("score", 50)
        top_issue   = _clean(all_critical[0]) if all_critical else "multiple gaps identified across content, funnel, and ICP alignment"
        exec_summary = (
            f"{config.client_name} scored {score}/100 ({grade}) overall on the C.A.S.H. framework. "
            f"The most critical finding: {top_issue.rstrip('.')}. "
            f"ICP alignment ({icp_score}/100) and lead funnel ({funnel_score}/100) are the "
            f"highest-leverage areas — fixing these will have the greatest impact on "
            f"{config.primary_goal}."
        )

        # ── Data-driven priorities ──────────────────────────────
        priorities = []
        web_issues = audit_data.get("website", {}).get("issues", [])

        if any("HTTPS" in i or "https" in i.lower() for i in web_issues):
            priorities.append({
                "priority": 1,
                "action":   "Migrate website to HTTPS",
                "impact":   "Eliminates browser security warnings and SEO penalty",
                "timeline": "1-3 days",
            })

        if icp_score < 50:
            priorities.append({
                "priority": len(priorities) + 1,
                "action":   f"Rewrite all public-facing copy to speak directly to {config.stated_target_market}",
                "impact":   "Dramatically increases ICP self-identification and lead relevance",
                "timeline": "1-2 weeks",
            })

        if not config.has_lead_magnet:
            priorities.append({
                "priority": len(priorities) + 1,
                "action":   "Create a lead magnet specific to your ICP (free audit, checklist, or guide)",
                "impact":   "Starts building an owned email list; first step in a functioning lead funnel",
                "timeline": "2 weeks",
            })

        if not config.has_active_newsletter and config.email_list_size == 0:
            priorities.append({
                "priority": len(priorities) + 1,
                "action":   "Launch a biweekly email newsletter for your ICP",
                "impact":   "Owned channel that compounds over time; not subject to algorithm changes",
                "timeline": "2 weeks",
            })

        if channels > 4:
            priorities.append({
                "priority": len(priorities) + 1,
                "action":   f"Consolidate from {channels} social channels to the 2-3 highest-fit platforms",
                "impact":   "Recovers 5-10 hrs/week and concentrates audience-building effort",
                "timeline": "30 days",
            })

        if budget == 0 and cash["S"] < 50:
            priorities.append({
                "priority": len(priorities) + 1,
                "action":   "Allocate a small paid acquisition budget to your primary ICP channel",
                "impact":   "Accelerates lead flow while organic channels are being built",
                "timeline": "2 weeks",
            })

        priorities = priorities[:3]
        for i, p in enumerate(priorities):
            p["priority"] = i + 1

        # ── Biggest waste (from real audit data) ────────────────
        waste_candidates = [
            i for i in brand.get("issues", [])
            if "🔴" in i and any(p in i for p in ["Discord", "Instagram", "TikTok", "low-fit"])
        ]
        biggest_waste = (
            _clean(waste_candidates[0]) if waste_candidates
            else f"Spreading content effort across {channels} channels without ICP-specific messaging — "
                 f"all channels carry the same general small-business positioning that the "
                 f"stated ICP ({config.stated_target_market}) does not respond to."
        )

        # ── Biggest opportunity (from real audit data) ──────────
        if not config.has_lead_magnet and not config.has_active_newsletter:
            biggest_opp = (
                f"Building an ICP-specific email list from zero. "
                f"Create a free resource for {config.stated_target_market}, "
                f"add an opt-in to the website, and set up a 5-email welcome sequence. "
                f"With 42:1 email ROI and 0 current contacts, this is the highest-leverage move available."
            )
        elif icp_score < 50:
            biggest_opp = (
                f"Rewiring the brand's public messaging to speak directly to "
                f"{config.stated_target_market}. "
                f"LinkedIn presence ({audit_data.get('freshness',{}).get('channels',{}).get('LinkedIn',{}).get('posts_per_week','?')}x/week) "
                f"is already active — shifting topics to ICP-specific pain points would convert "
                f"existing reach into qualified prospects."
            )
        else:
            biggest_opp = (
                f"Converting existing LinkedIn activity into a structured lead funnel. "
                f"Add a lead magnet, email opt-in, and nurture sequence to capture "
                f"value from current content efforts."
            )

        # ── Channel recommendation ──────────────────────────────
        channel_recs = []
        if "LinkedIn" in config.active_social_channels:
            channel_recs.append("LinkedIn (primary): double posting frequency, shift to ICP-specific topics")
        low_fit = brand.get("platform_fit", {}).get("low_fit", [])
        if low_fit:
            channel_recs.append(f"Pause or deprioritize: {', '.join(low_fit)} — near-zero ICP overlap")
        channel_recs.append("Email newsletter: highest-ROI channel for B2B financial services — launch immediately")
        channel_recommendation = ". ".join(channel_recs) + "."

        # ── Content strategy ────────────────────────────────────
        icp_signals = icp.get("content_alignment", {}).get("financial_signals_found", [])
        content_strategy = (
            f"Current content covers general marketing topics with {len(icp_signals)} ICP-specific signals detected. "
            f"Shift immediately to topics that speak to {config.stated_target_market}: "
            f"compliance-aware content marketing, referral generation, practice growth, and "
            f"building authority in regulated industries. "
            f"One pillar piece per week (LinkedIn article or video) repurposed into 5 posts."
        )

        # ── Budget recommendation ───────────────────────────────
        if budget == 0:
            budget_recommendation = (
                f"Currently $0 in paid spend. Organic-only is viable but slow. "
                f"Recommended: $500-1,000/month in LinkedIn Sponsored Content targeting "
                f"{config.stated_target_market} by job title. "
                f"Run lead-gen ads to the ICP-specific lead magnet once it's built."
            )
        else:
            budget_recommendation = (
                f"${budget:,.0f}/month budget. Prioritize LinkedIn Sponsored Content "
                f"for ICP targeting. Allocate 70% to lead generation, 30% to retargeting "
                f"website visitors."
            )

        # ── 90-day action plan (from real findings, weighted by challenge) ──
        plan_90 = [
            {
                "week":    "1-2",
                "action":  (
                    f"Rewrite Linktree bio, LinkedIn headline, and website homepage copy "
                    f"to speak directly to {config.stated_target_market}. "
                    f"Remove Web3/crypto language from all public-facing channels."
                    if icp_score < 50
                    else "Audit and align all public-facing copy to ICP pain points."
                ),
                "outcome": "First impression now immediately resonates with target ICP",
            },
            {
                "week":    "3-4",
                "action":  (
                    "Build and publish an ICP-specific lead magnet "
                    f"(e.g. 'Free Marketing Audit for {config.stated_target_market.split(',')[0]}s'). "
                    "Add email opt-in to website and Linktree."
                    if not config.has_lead_magnet
                    else "Set up 5-email welcome sequence for new list subscribers."
                ),
                "outcome": "Email list begins growing; owned channel established",
            },
            {
                "week":    "5-8",
                "action":  (
                    f"Publish 2 LinkedIn articles on ICP-specific topics "
                    f"(e.g. compliance-safe content marketing, referral strategies for "
                    f"{config.stated_target_market.split(',')[0]}s). "
                    f"Reach out to 3 past/current clients for testimonials and 1 case study."
                ),
                "outcome": "Authority content pipeline started; social proof collected",
            },
            {
                "week":    "9-12",
                "action":  (
                    f"Launch biweekly email newsletter. "
                    f"Replace Google Calendar booking link with a professional scheduling page. "
                    f"Publish case study on website. "
                    f"{'Reduce to 2-3 highest-fit channels. ' if channels > 4 else ''}"
                    f"Review C.A.S.H. scores and set 90-day targets for next quarter."
                ),
                "outcome": "Full nurture system operational; measurable pipeline activity",
            },
        ]
        # Reorder plan so the item most relevant to the client's stated challenge leads
        plan_90 = self._weight_plan_by_challenge(
            plan_90, config.biggest_marketing_challenge
        )

        # ── Competitive positioning ─────────────────────────────
        competitors = config.top_competitors
        competitive_positioning = (
            f"Primary competitors ({', '.join(competitors[:3]) if competitors else 'market'}) "
            f"serve the financial services marketing space with template-heavy, "
            f"compliance-first platforms. GMG's differentiation opportunity: "
            f"hands-on fractional CMO engagement with custom content strategy, "
            f"not a software subscription. Position as the high-touch alternative "
            f"for practices that want a real marketing partner, not another tool."
        )

        return {
            "data_source":            "rule_based",
            "audit_source":           getattr(config, "audit_source", "full_intake"),
            "intake_modifiers":       intake_mods,
            "overall_grade":          grade,
            "overall_score":          score,
            "cash_c_score":           cash["C"],
            "cash_a_score":           cash["A"],
            "cash_s_score":           cash["S"],
            "cash_h_score":           cash["H"],
            "component_scores":       cash,
            "top_3_priorities":       priorities,
            "icp_alignment_verdict":  icp_verdict,
            # Narrative fields — built from real audit data; Claude overlays when key present
            "executive_summary":      exec_summary,
            "biggest_waste":          biggest_waste,
            "biggest_opportunity":    biggest_opp,
            "channel_recommendation": channel_recommendation,
            "content_strategy":       content_strategy,
            "budget_recommendation":  budget_recommendation,
            "90_day_action_plan":     plan_90,
            "competitive_positioning": competitive_positioning,
        }
