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
        Detects FAQ presence + question-format headings + common-question patterns
        on page text. Part 1 stub — real detection in Part 2.
        """
        return {
            "score":     50,
            "issues":    [],
            "strengths": [],
            "detail":    "Part 1 stub — detection deferred to Part 2.",
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
        FAQPage / HowTo / QAPage / Article schema detection. Re-uses
        _merge_website_data signals where possible. Part 1 stub.
        """
        return {
            "score":     50,
            "issues":    [],
            "strengths": [],
            "detail":    "Part 1 stub — detection deferred to Part 2.",
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
        Re-scores existing has_testimonials, has_case_studies, has_certifications,
        has_media_mentions, has_client_logos signals from _merge_website_data
        under AEO weight. One source per signal — no re-detection.
        Part 1 stub — real re-scoring in Part 2.
        """
        return {
            "score":     50,
            "issues":    [],
            "strengths": [],
            "detail":    "Part 1 stub — re-scoring deferred to Part 2.",
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level convenience for callers that want a one-line entry point.
# ─────────────────────────────────────────────────────────────────────────────

def run_aeo(config: ClientConfig, audit_data: Dict[str, Any]) -> Dict[str, Any]:
    """One-shot entry — equivalent to AEOAuditor(config, audit_data).run()."""
    return AEOAuditor(config, audit_data).run()
