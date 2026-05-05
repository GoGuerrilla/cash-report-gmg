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
import os
import re
from typing import Any, Dict, List, Optional

from config import ClientConfig
from auditors.industry_benchmarks import industry_label, is_local_business

log = logging.getLogger(__name__)

# Phase B — LLM-eval model. Haiku is sufficient for short numeric grading
# tasks and keeps per-audit cost in the $0.01-0.02 range. Configurable via
# AEO_EVAL_MODEL env var if a future tier needs a stronger model.
_EVAL_MODEL = os.environ.get("AEO_EVAL_MODEL", "claude-haiku-4-5-20251001")
_EVAL_MAX_TOKENS = 200
_EVAL_TIMEOUT   = 30  # seconds — each call should complete in 2-5s

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
        # Cached page text — collected once, fed to LLM-eval scorers
        self._page_text = self._extract_page_text()
        self._anthropic_key = (
            getattr(config, "anthropic_api_key", "")
            or os.environ.get("ANTHROPIC_API_KEY", "")
        ).strip()

    def _extract_page_text(self) -> str:
        """Concatenate homepage + first inner page text for LLM-eval input."""
        pages = self.site.get("pages", []) or []
        chunks: List[str] = []
        for p in pages[:3]:
            if not isinstance(p, dict):
                continue
            for key in ("text", "content"):
                t = p.get(key)
                if t and isinstance(t, str):
                    chunks.append(t)
                    break
            # Also pull headings — useful signal for question-format detection
            for h in p.get("headings", []) or []:
                if isinstance(h, dict):
                    txt = h.get("text") or ""
                    if txt:
                        chunks.append(txt)
        text = " ".join(chunks).strip()
        # Cap at 8000 chars — Haiku handles much more but we want fast/cheap
        return text[:8000]

    def _llm_score(self, label: str, instruction: str) -> Optional[Dict[str, Any]]:
        """
        Run a single Haiku call to score one AEO category.
        Returns {"score": int, "explain": str} or None on any failure
        (caller falls back to 50 stub).
        """
        if not self._anthropic_key:
            log.info(
                "aeo llm-eval [%s] skipped: no anthropic_api_key "
                "(config_attr=%r env=%r)",
                label,
                bool(getattr(self.config, "anthropic_api_key", None)),
                bool(os.environ.get("ANTHROPIC_API_KEY")),
            )
            return None
        if not self._page_text:
            pages = self.site.get("pages", []) or []
            log.info(
                "aeo llm-eval [%s] skipped: no page_text (pages_count=%d)",
                label, len(pages),
            )
            return None
        try:
            import anthropic
            client = anthropic.Anthropic(
                api_key=self._anthropic_key, timeout=_EVAL_TIMEOUT,
            )
            prompt = (
                f"{instruction}\n\n"
                f"WEBSITE CONTENT:\n{self._page_text}\n\n"
                f"Respond ONLY with valid JSON of the shape: "
                f'{{"score": <integer 0-100>, "explain": "<one-sentence justification>"}}'
            )
            message = client.messages.create(
                model=_EVAL_MODEL,
                max_tokens=_EVAL_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = (message.content[0].text or "").strip()
            # Strip markdown fences if present
            if raw.startswith("```"):
                raw = "\n".join(raw.split("\n")[1:-1]).strip()
            import json
            data = json.loads(raw)
            score = int(data.get("score", 50))
            score = max(0, min(100, score))
            explain = (data.get("explain") or "").strip()[:200]
            log.info("aeo llm-eval [%s] score=%d explain=%r", label, score, explain[:80])
            return {"score": score, "explain": explain}
        except Exception as exc:
            log.warning("aeo llm-eval [%s] failed: %s — falling back to stub", label, exc)
            return None

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
        LLM-eval: scan the website content and grade how often question-format
        headings or queries are followed by a clear, short (1-3 sentence) answer
        in plain language. Higher score = AI systems can extract a snippet directly.
        Falls back to 50 stub when no Anthropic key or no page text.
        """
        result = self._llm_score(
            "Direct Answer Quality",
            "You are evaluating Direct Answer Quality for AEO scoring. Look at "
            "the website content below. On a 0-100 scale, grade how well the "
            "content provides DIRECT, CLEAR answers to common customer "
            "questions in 1-3 sentences. Reward content that pairs a question "
            "(or implicit question topic) with a short, plain-language answer "
            "an AI system could lift verbatim as a snippet. Penalize long, "
            "marketing-speak paragraphs that bury the answer or never give one. "
            "0 = no answers extractable, 50 = some content but answers are "
            "long/buried, 80+ = several clear question→answer pairs.",
        )
        if result is None:
            return {"score": 50, "issues": [], "strengths": [],
                    "detail": "LLM-eval skipped (no key or no page text)."}
        score = result["score"]
        issues, strengths = [], []
        if score < 50:
            issues.append("🔴 Few or no direct, snippet-extractable answers detected — "
                          "AI systems are unlikely to cite this content as the answer")
        elif score < 70:
            issues.append("🟡 Some answers exist but are buried in marketing prose — "
                          "tighten to 1-3 sentence direct responses for AEO citation")
        else:
            strengths.append(f"✅ Direct answer quality strong ({score}/100) — "
                             f"AI systems can extract clear snippets")
        return {"score": score, "issues": issues, "strengths": strengths,
                "detail": result["explain"]}

    # (Conversational Search and Entity Clarity follow the same pattern below)

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
        LLM-eval: how clearly does the website establish business identity for
        an AI system reading the page cold? Checklist: business name visible,
        location/service area, industry/category, target audience, services
        offered, founder/team mention, contact info, consistent brand language.
        """
        result = self._llm_score(
            "Entity Clarity",
            "You are evaluating Entity Clarity for AEO scoring. Look at the "
            "website content below. On a 0-100 scale, grade how clearly an AI "
            "system can identify these eight signals: (1) business name, "
            "(2) location or service area, (3) industry/category, (4) target "
            "audience, (5) services offered, (6) founder or team, "
            "(7) contact info (email/phone/form), (8) consistent brand language. "
            "Each signal present = ~12 points. Penalize ambiguous identity, "
            "missing fields, or contradictory descriptions across sections.",
        )
        if result is None:
            return {"score": 50, "issues": [], "strengths": [],
                    "detail": "LLM-eval skipped (no key or no page text)."}
        score = result["score"]
        issues, strengths = [], []
        if score < 50:
            issues.append("🔴 Entity identity is unclear to AI systems — "
                          "name/location/services/contact need to be unambiguous")
        elif score < 70:
            issues.append("🟡 Some entity signals missing — "
                          "tighten the homepage to surface all 8 identity fields")
        else:
            strengths.append(f"✅ Entity clarity strong ({score}/100) — "
                             f"AI systems can confidently identify the business")
        return {"score": score, "issues": issues, "strengths": strengths,
                "detail": result["explain"]}

    def _score_conversational_search(self) -> Dict[str, Any]:
        """
        LLM-eval: does the content match how real people TYPE/SPEAK queries to
        ChatGPT, Google AI Overviews, voice assistants? Checks: long-tail
        natural-language phrasing, 'near me' / location patterns (when relevant),
        conversational tone, snippetable 40-60 word answer chunks.
        """
        # Local-business hint: 'near me' patterns matter more for local clients
        local_hint = " (this is a LOCAL business — 'near me' patterns + location specificity should weigh heavier)" if is_local_business(self.industry) else ""
        result = self._llm_score(
            "Conversational Search",
            "You are evaluating Conversational Search readiness for AEO scoring."
            f"{local_hint} Look at the website content below. On a 0-100 scale, "
            "grade how well the content matches conversational query patterns "
            "AI search engines extract. Reward: long-tail natural phrasing, "
            "snippetable 40-60 word answer chunks, plain-language tone, "
            "question-style language. Penalize: corporate jargon, walls of text, "
            "list/bullet-only content with no narrative answers, content that "
            "reads like a sales brochure rather than a conversation.",
        )
        if result is None:
            return {"score": 50, "issues": [], "strengths": [],
                    "detail": "LLM-eval skipped (no key or no page text)."}
        score = result["score"]
        issues, strengths = [], []
        if score < 50:
            issues.append("🔴 Content reads as marketing copy, not conversational — "
                          "rewrite key sections in the language buyers actually search with")
        elif score < 70:
            issues.append("🟡 Tone mostly formal — add natural-language Q&A snippets "
                          "for voice search and AI Overview extraction")
        else:
            strengths.append(f"✅ Conversational tone strong ({score}/100) — "
                             f"matches how real users query AI search engines")
        return {"score": score, "issues": issues, "strengths": strengths,
                "detail": result["explain"]}

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
