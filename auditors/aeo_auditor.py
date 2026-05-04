"""
auditors/aeo_auditor.py — Answer Engine Optimization (AEO) pillar.

Third pillar of the Visibility Score alongside SEO and GEO. Measures how well
a website answers real customer questions across Google AI Overviews, ChatGPT,
Perplexity, voice search, and FAQ-style discovery.

  SEO  helps people find you
  GEO  helps AI systems understand and summarize you
  AEO  helps answer engines choose your content as the answer

Score composition:
  AEO_SCORE =
    questionCoverage     * 0.20 +
    directAnswerQuality  * 0.20 +
    structuredData       * 0.20 +
    entityClarity        * 0.15 +
    conversationalSearch * 0.15 +
    trustSignals         * 0.10

Status: scaffold (Part 1). All six category scores stubbed at 50 neutral.
Real detection lands incrementally — see project_phase2_aeo_pillar.md for the
full spec. The auditor returns a complete result dict so wiring + report
rendering can be developed against the full shape from day one.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from config import ClientConfig
from auditors.industry_benchmarks import industry_label, is_local_business

log = logging.getLogger(__name__)

# ── Scoring weights per the pillar spec ──────────────────────────────────────

CATEGORY_WEIGHTS: Dict[str, float] = {
    "Question Coverage":     0.20,
    "Direct Answer Quality": 0.20,
    "Structured Data":       0.20,
    "Entity Clarity":        0.15,
    "Conversational Search": 0.15,
    "Trust Signals":         0.10,
}

# ── Score band → status label ────────────────────────────────────────────────

def _band(score: int) -> str:
    if score >= 90: return "excellent"
    if score >= 75: return "strong"
    if score >= 60: return "moderate"
    if score >= 40: return "weak"
    return "poor"


def _grade(score: int) -> str:
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    if score >= 35: return "D"
    return "F"


# ─────────────────────────────────────────────────────────────────────────────
# AEOAuditor — public API mirrors the pattern of GEOAuditor / SEOAuditor:
# constructor takes (config, audit_data); run() returns a normalized dict with
# score, grade, components, issues, strengths, and recommendations.
# ─────────────────────────────────────────────────────────────────────────────

class AEOAuditor:
    def __init__(self, config: ClientConfig, audit_data: Dict[str, Any]):
        self.config     = config
        self.audit_data = audit_data
        self.site       = (audit_data or {}).get("website", {}) or {}
        self.industry   = industry_label(
            getattr(config, "industry_category", None)
            or getattr(config, "client_industry", None)
        )

    def run(self) -> Dict[str, Any]:
        components: Dict[str, Dict[str, Any]] = {
            "Question Coverage":     self._score_question_coverage(),
            "Direct Answer Quality": self._score_direct_answer_quality(),
            "Structured Data":       self._score_structured_data(),
            "Entity Clarity":        self._score_entity_clarity(),
            "Conversational Search": self._score_conversational_search(),
            "Trust Signals":         self._score_trust_signals(),
        }

        weighted = sum(
            comp["score"] * CATEGORY_WEIGHTS[name]
            for name, comp in components.items()
        )
        score = max(0, min(100, round(weighted)))

        all_issues:    List[str] = []
        all_strengths: List[str] = []
        for comp in components.values():
            all_issues.extend(comp.get("issues", []))
            all_strengths.extend(comp.get("strengths", []))

        return {
            "score":        score,
            "grade":        _grade(score),
            "band":         _band(score),
            "status":       "ok",
            "components":   components,
            "issues":       all_issues,
            "strengths":    all_strengths,
            "weights":      CATEGORY_WEIGHTS,
            "industry":     self.industry,
            "data_source":  "aeo_auditor_v1",
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Category scorers — Part 1 stubs. Each returns a {score, issues,
    # strengths, detail} dict. Real detection lands in subsequent commits.
    # ─────────────────────────────────────────────────────────────────────────

    def _score_question_coverage(self) -> Dict[str, Any]:
        """
        FAQ presence + question-format headings + common-question patterns.
        Pulls signals from preloaded_channel_data["website"] (set by
        _merge_website_data) and Apify content (apify_has_faqpage flag from
        Push 4) — no re-scraping.
        """
        site = (self.config.preloaded_channel_data or {}).get("website", {})
        has_faqpage_schema = bool(site.get("apify_has_faqpage"))
        # has_blog often correlates with Q&A content
        has_blog = bool(site.get("has_blog"))
        # Page text heuristic — count '?' question patterns in homepage h1/title text
        pages = self.site.get("pages", []) or []
        homepage = pages[0] if pages else {}
        text_blob = " ".join([
            homepage.get("title", "") or "",
            homepage.get("meta_description", "") or "",
            " ".join(homepage.get("h1_text", []) or []),
        ])
        h2_count = homepage.get("h2_count", 0) or 0
        has_question_heading = "?" in text_blob

        score = 35
        issues, strengths = [], []

        if has_faqpage_schema:
            score += 30
            strengths.append("✅ FAQPage schema detected — top format for AI citation")
        else:
            issues.append("🟡 No FAQPage schema — biggest single AEO lever")

        if has_blog:
            score += 15
            strengths.append("✅ Blog/content hub present — supports Q&A long-form")
        else:
            issues.append("🟡 No blog or content hub visible — limits Q&A depth")

        if has_question_heading:
            score += 10
            strengths.append("✅ Question-format heading detected on homepage")
        elif h2_count >= 4:
            score += 5
            strengths.append("✅ Structured H2 hierarchy supports answer extraction")
        else:
            issues.append("🟡 No question-format headings — convert key H2s to questions")

        return {
            "score":     max(0, min(100, score)),
            "issues":    issues,
            "strengths": strengths,
            "detail":    f"FAQ schema={has_faqpage_schema} blog={has_blog} ?heading={has_question_heading}",
        }

    def _score_direct_answer_quality(self) -> Dict[str, Any]:
        """
        LLM-evaluated: 1-3 sentence clear answers immediately following each
        question. Defers to Phase B (AEO LLM-eval categories).
        """
        return {
            "score":     50,
            "issues":    [],
            "strengths": [],
            "detail":    "LLM-eval category — deferred to Phase B.",
        }

    def _score_structured_data(self) -> Dict[str, Any]:
        """
        FAQPage / HowTo / QAPage / Article schema detection. Reads schema_types
        from the homepage Apify-rendered DOM (already extracted by website_auditor)
        plus the apify_has_faqpage flag set in _merge_website_data.
        """
        site = (self.config.preloaded_channel_data or {}).get("website", {})
        pages = self.site.get("pages", []) or []
        homepage = pages[0] if pages else {}
        schema_types_lc = [
            (s or "").lower() for s in (homepage.get("schema_types", []) or [])
        ]

        has_faqpage   = "faqpage" in schema_types_lc or bool(site.get("apify_has_faqpage"))
        has_howto     = "howto" in schema_types_lc
        has_qapage    = "qapage" in schema_types_lc
        has_article   = any(t in schema_types_lc for t in ("article", "blogposting", "newsarticle"))
        has_org       = any(t in schema_types_lc for t in ("organization", "localbusiness", "service"))

        score = 30
        issues, strengths = [], []

        if has_faqpage:
            score += 30
            strengths.append("✅ FAQPage schema present — top AEO ranking signal")
        else:
            issues.append("🔴 No FAQPage schema — single highest-leverage AEO action")

        if has_howto:
            score += 15
            strengths.append("✅ HowTo schema present — supports tutorial-style queries")
        if has_qapage:
            score += 10
            strengths.append("✅ QAPage schema present — direct Q&A signal")
        if has_article:
            score += 10
            strengths.append("✅ Article/BlogPosting schema present")
        if has_org:
            score += 10
            strengths.append("✅ Organization/LocalBusiness/Service schema — entity baseline")
        else:
            issues.append("🟡 No Organization/LocalBusiness/Service schema — entity foundation missing")

        return {
            "score":     max(0, min(100, score)),
            "issues":    issues,
            "strengths": strengths,
            "detail":    f"schemas={schema_types_lc[:8]}",
        }

    def _score_entity_clarity(self) -> Dict[str, Any]:
        """
        Business name, location, industry, target audience, services, founder,
        contact, social links, consistent brand language. Part 1 stub.
        """
        return {
            "score":     50,
            "issues":    [],
            "strengths": [],
            "detail":    "Part 1 stub — detection deferred to Part 2/B.",
        }

    def _score_conversational_search(self) -> Dict[str, Any]:
        """
        Long-tail natural-language phrasing, 'near me' patterns, conversational
        tone (LLM eval), snippetable 40-60 word answers. Phase B LLM category.
        """
        return {
            "score":     50,
            "issues":    [],
            "strengths": [],
            "detail":    "LLM-eval category — deferred to Phase B.",
        }

    def _score_trust_signals(self) -> Dict[str, Any]:
        """
        Re-scores existing trust signals from _merge_website_data under the AEO
        weight. Same has_testimonials, has_case_studies, has_certifications,
        has_media_mentions, has_client_logos booleans the Hold pillar uses —
        no re-detection here, just a different weight on the existing facts
        (per project_phase2_aeo_pillar.md: 'one source per signal').
        """
        site = (self.config.preloaded_channel_data or {}).get("website", {})
        signals = {
            "testimonials":   bool(site.get("has_testimonials")),
            "case_studies":   bool(site.get("has_case_studies")),
            "certifications": bool(site.get("has_certifications")),
            "media_mentions": bool(site.get("has_media_mentions")),
            "client_logos":   bool(site.get("has_client_logos")),
        }
        present_count = sum(1 for v in signals.values() if v)

        # 0 → 30 (poor floor), 1 → 50, 2 → 65, 3 → 80, 4 → 90, 5 → 95
        tiers = {0: 30, 1: 50, 2: 65, 3: 80, 4: 90, 5: 95}
        score = tiers.get(present_count, 95)

        issues, strengths = [], []
        if signals["testimonials"]:
            strengths.append("✅ Testimonials present — direct trust signal for AI citation")
        else:
            issues.append("🔴 No testimonials — AI systems discount unverified expertise claims")
        if signals["case_studies"]:
            strengths.append("✅ Case studies present — outcome-focused content boosts AI trust")
        else:
            issues.append("🟡 No case studies — add 1-2 outcome stories for AEO depth")
        if signals["certifications"]:
            strengths.append("✅ Certifications visible — credentials reinforce authority")
        if signals["media_mentions"]:
            strengths.append("✅ Media mentions present — third-party authority signal")
        if signals["client_logos"]:
            strengths.append("✅ Client logos present — social proof at a glance")

        return {
            "score":     score,
            "issues":    issues,
            "strengths": strengths,
            "detail":    f"present={present_count}/5  signals={signals}",
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level convenience for callers that want a one-line entry point.
# ─────────────────────────────────────────────────────────────────────────────

def run_aeo(config: ClientConfig, audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """One-shot entry — equivalent to AEOAuditor(config, audit_data).run()."""
    return AEOAuditor(config, audit_data).run()
