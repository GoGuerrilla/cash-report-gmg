"""
GEO Auditor — Generative Engine Optimization  (enhanced)
Measures how likely a brand is to appear in AI-generated answers:
  ChatGPT, Google AI Overviews, Perplexity, Claude, Gemini, etc.

Scoring components (weights)
─────────────────────────────
  SERP Visibility      20%  — GSC keyword rankings, clicks, avg position
  On-page SEO          15%  — title, meta desc, H1/H2 keyword optimisation
  Schema Markup        15%  — structured data + FAQPage schema
  FAQ / Q&A Content    15%  — Q&A content AI systems prefer to cite
  E-E-A-T Signals      15%  — author credentials, trust proof, expertise
  Brand Authority      15%  — social breadth, LinkedIn, GBP
  AI Citation Score     5%  — composite citation-likelihood estimate

Data sources
─────────────
  • Google Search Console API (service account) → SERP keywords + positions
  • Live homepage scrape                         → on-page SEO signals
  • SEO auditor data (passed in)                 → schema, Lighthouse scores
  • Config / preloaded channel data              → social, LinkedIn

Graceful degradation: every component floors at 50 neutral when data is
unavailable — never 0.  The caller never needs to guard for None.
"""
import json
import os
import re
import urllib.parse
import urllib.request
import urllib.error
from datetime import date, timedelta
from typing import Dict, Any, List, Optional, Tuple

from config import ClientConfig

# ── Optional third-party imports ──────────────────────────────
try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

try:
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GoogleRequest
    GOOGLE_AUTH_OK = True
except ImportError:
    GOOGLE_AUTH_OK = False

# ── Constants ──────────────────────────────────────────────────
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

_STOPWORDS = {
    "and", "or", "the", "a", "an", "in", "at", "for", "of", "to", "with",
    "who", "that", "is", "are", "their", "our", "your", "my", "we", "i",
    "on", "by", "as", "from", "about", "be", "it", "this", "they", "have",
    "has", "was", "were", "will", "can", "do", "not", "what", "how",
}

GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]


def _grade(score: int) -> str:
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    if score >= 35: return "D"
    return "F"


def _icp_keywords(text: str) -> set:
    """Extract meaningful keywords from stated target market."""
    tokens = re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
    return {t for t in tokens if t not in _STOPWORDS}


class GEOAuditor:
    def __init__(self, config: ClientConfig, seo_data: Dict[str, Any]):
        self.config   = config
        self.seo_data = seo_data
        self.site     = config.preloaded_channel_data.get("website", {})
        self.linkedin = config.preloaded_channel_data.get("linkedin", {})
        self.icp_kws  = _icp_keywords(config.stated_target_market or "")
        self._onpage: Optional[Dict] = None   # cached homepage scrape

    # ═══════════════════════════════════════════════════════════
    #  Public entry point
    # ═══════════════════════════════════════════════════════════

    def run(self) -> Dict[str, Any]:
        try:
            return self._run_inner()
        except Exception as exc:
            import traceback as _tb
            import logging as _log
            _log.getLogger("webhook").warning(
                "GEO auditor failed — returning score=50 neutral. Error: %s\n%s",
                exc, _tb.format_exc())
            return {
                "score": 50, "grade": "C",
                "note": f"GEO audit unavailable: {exc}",
                "components": {}, "component_detail": {},
                "issues": [], "strengths": [], "recommendations": [],
                "platform_notes": [], "serp_keywords": [],
                "serp_summary": {
                    "total_clicks": None, "total_impressions": None,
                    "avg_position": None, "top_3_count": 0, "top_10_count": 0,
                    "branded_keywords": 0, "non_branded_keywords": 0,
                },
                "onpage_detail": {
                    "title": "", "meta_description": "", "h1s": [], "h2s": [],
                    "has_faq_schema": False, "schema_types": [], "word_count": 0,
                },
            }

    def _run_inner(self) -> Dict[str, Any]:
        # One scrape shared by on-page and schema checks
        self._onpage = self._scrape_homepage()

        # All seven components
        serp      = self._score_serp_visibility()
        onpage    = self._score_onpage_seo()
        schema    = self._score_schema()
        faq       = self._score_faq()
        eeat      = self._score_eeat()
        authority = self._score_authority()
        citation  = self._score_citation(schema, faq, eeat, authority)

        components = {
            "SERP Visibility":    serp,
            "On-page SEO":        onpage,
            "Schema Markup":      schema,
            "FAQ / Q&A Content":  faq,
            "E-E-A-T Signals":    eeat,
            "Brand Authority":    authority,
            "AI Citation Score":  citation,
        }

        weights = {
            "SERP Visibility":   0.20,
            "On-page SEO":       0.15,
            "Schema Markup":     0.15,
            "FAQ / Q&A Content": 0.15,
            "E-E-A-T Signals":   0.15,
            "Brand Authority":   0.15,
            "AI Citation Score": 0.05,
        }

        overall = round(sum(components[k]["score"] * weights[k] for k in components))
        issues, strengths = self._collect_issues_strengths(components)
        recommendations   = self._build_recommendations(components)

        crawl_available = bool(self._onpage)
        gsc_available   = serp.get("total_clicks") is not None

        return {
            "score":            overall,
            "grade":            _grade(overall),
            "components":       {k: v["score"] for k, v in components.items()},
            "component_detail": components,
            "issues":           issues,
            "strengths":        strengths,
            "recommendations":  recommendations,
            "platform_notes":   self._platform_notes(components),
            # Data source status — surfaced clearly in the report
            "data_sources": {
                "public_crawl": (
                    "available" if crawl_available
                    else "unavailable — site could not be reached"
                ),
                "gsc": (
                    "connected — SERP data included" if gsc_available
                    else "unavailable — audit completed using public-site signals only"
                ),
            },
            # Expose rich sub-data for the PDF section
            "serp_keywords":    serp.get("top_keywords", []),
            "serp_summary":     {
                "total_clicks":       serp.get("total_clicks"),
                "total_impressions":  serp.get("total_impressions"),
                "avg_position":       serp.get("avg_position"),
                "top_3_count":        serp.get("top_3_count", 0),
                "top_10_count":       serp.get("top_10_count", 0),
                "branded_keywords":   serp.get("branded_keywords", 0),
                "non_branded_keywords": serp.get("non_branded_keywords", 0),
            },
            "onpage_detail": {
                "title":            onpage.get("title", ""),
                "meta_description": onpage.get("meta_description", ""),
                "h1s":              onpage.get("h1s", []),
                "h2s":              onpage.get("h2s", []),
                "has_faq_schema":   onpage.get("has_faq_schema", False),
                "schema_types":     onpage.get("schema_types", []),
                "word_count":       onpage.get("word_count", 0),
                # Extended crawl signals
                "canonical_url":    onpage.get("canonical_url", ""),
                "is_noindex":       onpage.get("is_noindex", False),
                "og_tags":          onpage.get("og_tags", []),
                "has_og_image":     onpage.get("has_og_image", False),
                "has_twitter_card": onpage.get("has_twitter_card", False),
            },
        }

    # ═══════════════════════════════════════════════════════════
    #  Homepage scraper (shared)
    # ═══════════════════════════════════════════════════════════

    def _scrape_homepage(self) -> Dict:
        url = self.config.website_url
        if not url or not REQUESTS_OK:
            return {}
        try:
            r = requests.get(url, headers=_HEADERS, timeout=15, allow_redirects=True)
            if r.status_code >= 400:
                return {}
            soup = BeautifulSoup(r.text, "html.parser")

            # Title tag
            title_tag = soup.find("title")
            title = title_tag.get_text().strip() if title_tag else ""

            # Meta description
            meta_tag = soup.find("meta", attrs={"name": re.compile("^description$", re.I)})
            meta_desc = (meta_tag.get("content") or "").strip() if meta_tag else ""

            # H1 / H2 headings
            h1s = [h.get_text().strip() for h in soup.find_all("h1") if h.get_text().strip()]
            h2s = [h.get_text().strip() for h in soup.find_all("h2") if h.get_text().strip()][:12]

            # JSON-LD structured data
            scripts = soup.find_all("script", type="application/ld+json")
            schema_types: List[str] = []
            has_faq_schema = False
            for tag in scripts:
                try:
                    data = json.loads(tag.string or "")
                except (json.JSONDecodeError, TypeError):
                    continue
                items = data if isinstance(data, list) else [data]
                for item in items:
                    t = item.get("@type", "")
                    if t:
                        schema_types.append(t)
                    if t == "FAQPage":
                        has_faq_schema = True
                    # Also catch @graph arrays
                    for sub in item.get("@graph", []):
                        st = sub.get("@type", "")
                        if st:
                            schema_types.append(st)
                        if st == "FAQPage":
                            has_faq_schema = True

            # Word count
            word_count = len(re.findall(r"\b\w+\b", soup.get_text(separator=" ")))

            # Canonical tag
            canon_tag = soup.find("link", rel="canonical")
            canonical_url = canon_tag.get("href", "").strip() if canon_tag else ""

            # Indexability — meta robots noindex check
            robots_meta = soup.find("meta", attrs={"name": re.compile("^robots$", re.I)})
            robots_content = (robots_meta.get("content") or "").lower() if robots_meta else ""
            is_noindex = "noindex" in robots_content

            # Open Graph tags
            og_tags = soup.find_all(
                "meta", attrs={"property": lambda x: x and x.startswith("og:")}
            )
            og_list = [t.get("property", "") for t in og_tags]

            # Twitter Card tags
            tw_tags = soup.find_all(
                "meta", attrs={"name": lambda x: x and x.lower().startswith("twitter:")}
            )
            tw_list = [t.get("name", "") for t in tw_tags]

            return {
                "title":              title,
                "meta_description":   meta_desc,
                "h1s":                h1s,
                "h2s":                h2s,
                "schema_types":       schema_types,
                "has_faq_schema":     has_faq_schema,
                "word_count":         word_count,
                "canonical_url":      canonical_url,
                "has_canonical":      bool(canonical_url),
                "is_noindex":         is_noindex,
                "og_tags":            og_list,
                "has_og_tags":        len(og_list) > 0,
                "has_og_image":       any("og:image" in t for t in og_list),
                "has_og_title":       any("og:title" in t for t in og_list),
                "twitter_tags":       tw_list,
                "has_twitter_card":   len(tw_list) > 0,
            }
        except Exception:
            return {}

    # ═══════════════════════════════════════════════════════════
    #  1. SERP Visibility — Google Search Console API
    # ═══════════════════════════════════════════════════════════

    def _fetch_gsc_rows(self) -> List[Dict]:
        """Call GSC Search Analytics API via service-account credentials."""
        sa_path  = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "")
        gsc_site = os.environ.get("GSC_SITE_URL", "").rstrip("/") + "/"
        if not sa_path or not os.path.isfile(sa_path) or gsc_site == "/" or not GOOGLE_AUTH_OK:
            return []
        try:
            creds = service_account.Credentials.from_service_account_file(
                sa_path, scopes=GSC_SCOPES
            )
            creds.refresh(GoogleRequest())
            token = creds.token

            end_date   = date.today() - timedelta(days=3)   # GSC ~3-day lag
            start_date = end_date - timedelta(days=90)
            encoded    = urllib.parse.quote(gsc_site, safe="")
            api_url    = (
                f"https://searchconsole.googleapis.com/v1/sites/"
                f"{encoded}/searchAnalytics/query"
            )
            payload = json.dumps({
                "startDate":  start_date.isoformat(),
                "endDate":    end_date.isoformat(),
                "dimensions": ["query"],
                "rowLimit":   25,
                "orderBy":    [{"fieldName": "impressions", "sortOrder": "DESCENDING"}],
            }).encode()
            req = urllib.request.Request(
                api_url, data=payload, method="POST",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type":  "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            rows = []
            for r in body.get("rows", []):
                rows.append({
                    "query":       r["keys"][0],
                    "clicks":      int(r.get("clicks", 0)),
                    "impressions": int(r.get("impressions", 0)),
                    "ctr":         round(r.get("ctr", 0) * 100, 1),
                    "position":    round(r.get("position", 99), 1),
                })
            return rows
        except Exception:
            return []

    def _score_serp_visibility(self) -> Dict:
        try:
            rows = self._fetch_gsc_rows()
        except Exception:
            rows = []

        if not rows:
            return {
                "score":   50,
                "status":  "unknown",
                "detail":  "Search Console data unavailable — audit completed using public-site signals only.",
                "issues":  [
                    "🟡 Connect Google Search Console to unlock query, click, impression, and ranking insights"
                ],
                "strengths":          [],
                "top_keywords":       [],
                "avg_position":       None,
                "total_clicks":       None,
                "total_impressions":  None,
                "top_3_count":        0,
                "top_10_count":       0,
                "branded_keywords":   0,
                "non_branded_keywords": 0,
            }

        total_clicks       = sum(r["clicks"]      for r in rows)
        total_impressions  = sum(r["impressions"]  for r in rows)
        positions          = [r["position"] for r in rows if r["position"] < 99]
        avg_position       = round(sum(positions) / len(positions), 1) if positions else None
        top_3              = [r for r in rows if r["position"] <= 3]
        top_10             = [r for r in rows if r["position"] <= 10]

        # Branded vs non-branded split
        brand_terms = set((self.config.client_name or "").lower().split())
        branded     = [r for r in rows if any(t in r["query"].lower() for t in brand_terms)]
        non_branded = [r for r in rows if not any(t in r["query"].lower() for t in brand_terms)]

        issues, strengths = [], []
        score = 40   # base — real data present is a good start

        if top_3:
            score += 20
            strengths.append(f"✅ {len(top_3)} keyword(s) ranking in top 3 positions")
        if top_10:
            score += 15
            if not top_3:
                strengths.append(f"✅ {len(top_10)} keyword(s) ranking on page 1 (positions 4–10)")
        elif not top_10:
            issues.append("🔴 No keywords on page 1 (top 10) — prioritize content optimization for existing rankings")

        if total_clicks > 1000:
            score += 15
            strengths.append(f"✅ {total_clicks:,} organic clicks in 90 days — strong search demand")
        elif total_clicks > 200:
            score += 8
            strengths.append(f"✅ {total_clicks:,} organic clicks in 90 days")
        elif total_clicks == 0:
            score = max(score - 10, 20)
            issues.append("🔴 Zero organic clicks in 90 days — site is not converting search impressions")

        if avg_position:
            if avg_position <= 10:
                score += 10
                strengths.append(f"✅ Average position: {avg_position} (page 1 average)")
            elif avg_position <= 20:
                issues.append(f"🟡 Average position {avg_position} — many rankings near page 2, optimize to reach page 1")
            else:
                score = max(score - 10, 20)
                issues.append(f"🔴 Average position {avg_position} — most keywords buried on page 3+")

        if non_branded:
            strengths.append(f"✅ {len(non_branded)} non-branded keyword(s) — organic discovery beyond brand searches")
        if not non_branded and branded:
            issues.append("🟡 All traffic is branded — build non-branded topic authority to attract cold audiences")

        if total_impressions > 0:
            avg_ctr = round(total_clicks / total_impressions * 100, 1)
            if avg_ctr < 2:
                issues.append(f"🟡 Low CTR ({avg_ctr}%) — test stronger title tags and meta descriptions to improve click-through")

        return {
            "score":              max(20, min(95, score)),
            "status":             "pass" if score >= 65 else ("neutral" if score >= 50 else "fail"),
            "detail":             (
                f"{total_clicks:,} clicks | {total_impressions:,} impressions | "
                f"avg pos {avg_position} | {len(top_10)} page-1 keywords"
            ),
            "issues":             issues,
            "strengths":          strengths,
            "top_keywords":       rows[:15],
            "avg_position":       avg_position,
            "total_clicks":       total_clicks,
            "total_impressions":  total_impressions,
            "top_3_count":        len(top_3),
            "top_10_count":       len(top_10),
            "branded_keywords":   len(branded),
            "non_branded_keywords": len(non_branded),
        }

    # ═══════════════════════════════════════════════════════════
    #  2. On-page SEO
    # ═══════════════════════════════════════════════════════════

    def _score_onpage_seo(self) -> Dict:
        op = self._onpage or {}
        if not op:
            return {
                "score": 50, "status": "unknown",
                "detail": "Homepage could not be scraped.",
                "issues":    ["🟡 On-page SEO check unavailable — homepage not reachable"],
                "strengths": [],
                "title": "", "meta_description": "", "h1s": [], "h2s": [],
                "has_faq_schema": False, "schema_types": [], "word_count": 0,
            }

        title     = op.get("title", "")
        meta      = op.get("meta_description", "")
        h1s       = op.get("h1s", [])
        h2s       = op.get("h2s", [])
        h1_text   = " ".join(h1s).lower()
        h2_text   = " ".join(h2s).lower()
        title_l   = title.lower()
        meta_l    = meta.lower()

        issues, strengths = [], []
        score = 100

        # ── Title tag ──────────────────────────────────────────
        if not title:
            score -= 25
            issues.append("🔴 No title tag found — critical for SEO and AI indexing")
        else:
            tlen = len(title)
            if tlen < 30:
                score -= 12
                issues.append(f"🟡 Title too short ({tlen} chars) — aim for 50–60 characters")
            elif tlen > 70:
                score -= 6
                issues.append(f"🟡 Title too long ({tlen} chars) — Google truncates above ~60")
            else:
                strengths.append(f"✅ Title length optimal ({tlen} chars)")

            if self.icp_kws:
                hits = [w for w in self.icp_kws if w in title_l]
                if hits:
                    strengths.append(f"✅ Title contains ICP keyword(s): {', '.join(sorted(hits)[:4])}")
                else:
                    score -= 10
                    issues.append("🟡 Title tag lacks target-market keywords — include ICP-specific terms")

        # ── Meta description ───────────────────────────────────
        if not meta:
            score -= 18
            issues.append("🔴 No meta description — missed CTR and AI summary opportunity")
        else:
            mlen = len(meta)
            if mlen < 100:
                score -= 8
                issues.append(f"🟡 Meta description short ({mlen} chars) — aim for 140–160 characters")
            elif mlen > 165:
                score -= 5
                issues.append(f"🟡 Meta description long ({mlen} chars) — Google truncates above ~160")
            else:
                strengths.append(f"✅ Meta description length optimal ({mlen} chars)")

            if self.icp_kws and any(w in meta_l for w in self.icp_kws):
                strengths.append("✅ Meta description references ICP keywords")
            elif meta:
                issues.append("🟡 Meta description doesn't reference target market — include ICP language for higher CTR")

        # ── H1 ─────────────────────────────────────────────────
        if not h1s:
            score -= 20
            issues.append("🔴 No H1 tag — every page needs exactly one keyword-rich H1")
        elif len(h1s) > 1:
            score -= 5
            issues.append(f"🟡 Multiple H1 tags ({len(h1s)}) — consolidate to a single primary H1")
        else:
            strengths.append("✅ Single H1 tag (correct structure)")
            if self.icp_kws and any(w in h1_text for w in self.icp_kws):
                strengths.append("✅ H1 references ICP-relevant keyword")
            else:
                score -= 8
                issues.append("🟡 H1 does not reference target market — include ICP keywords in the H1")

        # ── H2 structure ───────────────────────────────────────
        if len(h2s) < 2:
            score -= 10
            issues.append("🟡 Fewer than 2 H2 tags — use H2s to structure content around ICP questions")
        else:
            strengths.append(f"✅ {len(h2s)} H2 subheadings — good content structure")
            if self.icp_kws and any(w in h2_text for w in self.icp_kws):
                strengths.append("✅ H2 tags include ICP-relevant phrasing")

        # ── FAQPage schema ─────────────────────────────────────
        if op.get("has_faq_schema"):
            score = min(score + 8, 100)
            strengths.append("✅ FAQPage schema detected — directly boosts Google AI Overview eligibility")
        else:
            issues.append("🟡 No FAQPage schema — add FAQ schema to boost AI Overview inclusion probability")

        # ── Word count ─────────────────────────────────────────
        wc = op.get("word_count", 0)
        if wc < 300:
            score -= 8
            issues.append(f"🟡 Low homepage word count ({wc} words) — thin content limits AI citation potential")
        elif wc >= 800:
            strengths.append(f"✅ Substantial homepage content ({wc:,} words)")

        # ── Canonical tag ──────────────────────────────────────
        if op.get("has_canonical"):
            strengths.append("✅ Canonical tag present — prevents duplicate content signals")
        else:
            score -= 5
            issues.append("🟡 No canonical tag — add <link rel='canonical'> to prevent duplicate content")

        # ── Indexability ───────────────────────────────────────
        if op.get("is_noindex"):
            score -= 30
            issues.append("🔴 Page has a noindex directive — search engines cannot index this page")
        else:
            strengths.append("✅ Page is indexable — no noindex directive detected")

        # ── Social tags ────────────────────────────────────────
        if op.get("has_og_tags"):
            strengths.append("✅ Open Graph tags present — social sharing optimized")
            if not op.get("has_og_image"):
                score -= 3
                issues.append("🟡 Missing og:image — add an image for richer social sharing previews")
        else:
            score -= 5
            issues.append("🟡 No Open Graph tags — add og:title, og:description, og:image")
        if op.get("has_twitter_card"):
            strengths.append("✅ Twitter Card tags present")
        else:
            issues.append("🟡 No Twitter Card tags — add twitter:card and twitter:image for X/Twitter previews")

        return {
            "score":           max(20, min(100, score)),
            "status":          "pass" if score >= 65 else ("neutral" if score >= 50 else "fail"),
            "detail":          (
                f"Title {len(title)}ch | Meta {len(meta)}ch | "
                f"H1×{len(h1s)} H2×{len(h2s)} | {wc:,} words | "
                f"Canonical: {'✅' if op.get('has_canonical') else '❌'} | "
                f"OG: {'✅' if op.get('has_og_tags') else '❌'}"
            ),
            "issues":          issues,
            "strengths":       strengths,
            "title":           title,
            "meta_description": meta,
            "h1s":             h1s,
            "h2s":             h2s,
            "has_faq_schema":  op.get("has_faq_schema", False),
            "schema_types":    op.get("schema_types", []),
            "word_count":      wc,
            "canonical_url":   op.get("canonical_url", ""),
            "is_noindex":      op.get("is_noindex", False),
            "og_tags":         op.get("og_tags", []),
            "has_og_image":    op.get("has_og_image", False),
            "has_twitter_card": op.get("has_twitter_card", False),
        }

    # ═══════════════════════════════════════════════════════════
    #  3. Schema Markup
    # ═══════════════════════════════════════════════════════════

    def _is_wix(self) -> bool:
        return "wix" in (self.site.get("platform") or "").lower()

    def _score_schema(self) -> Dict:
        has_schema   = self.seo_data.get("has_schema")
        schema_items = self.seo_data.get("schema_items", [])
        is_wix       = self._is_wix()
        # Supplement with what we found in the live scrape
        scraped_types = (self._onpage or {}).get("schema_types", [])
        has_faq_schema = (self._onpage or {}).get("has_faq_schema", False)

        # Treat scraped schema as evidence if Lighthouse wasn't conclusive
        if scraped_types and has_schema is not True:
            has_schema   = True
            schema_items = schema_items or scraped_types

        if has_schema is True:
            base  = min(95, 75 + len(schema_items) * 5)
            bonus = 10 if has_faq_schema else 0
            score = min(95, base + bonus)
            strengths = [f"✅ {len(schema_items)} schema type(s): {', '.join(schema_items[:4])}"]
            if has_faq_schema:
                strengths.append("✅ FAQPage schema — highest-value schema type for AI Overviews")
            return {
                "score": score, "status": "pass",
                "detail": f"{len(schema_items)} schema type(s) detected.",
                "issues": [], "strengths": strengths,
            }
        elif has_schema is False:
            score    = 45 if is_wix else 20
            wix_note = (" Wix may inject basic schema Lighthouse cannot verify." if is_wix else "")
            return {
                "score": score, "status": "fail" if not is_wix else "partial",
                "detail": f"No verified structured data found.{wix_note}",
                "issues": [
                    "🟡 No Schema.org markup — add Organization + LocalBusiness + FAQPage schema"
                    + (" (Wix: use a Schema app or embed JSON-LD in page header)" if is_wix else "")
                ],
                "strengths": [],
            }
        return {
            "score": 50, "status": "unknown",
            "detail": "Schema status unverified.",
            "issues": ["🟡 Schema markup unverified — add Organization + LocalBusiness + FAQPage schema"],
            "strengths": [],
        }

    # ═══════════════════════════════════════════════════════════
    #  4. FAQ / Q&A Content
    # ═══════════════════════════════════════════════════════════

    def _score_faq(self) -> Dict:
        has_blog     = self.site.get("has_blog", False)
        pages        = self.site.get("pages", [])
        has_faq_page = any("faq" in str(p).lower() or "question" in str(p).lower() for p in pages)
        has_faq_schema = (self._onpage or {}).get("has_faq_schema", False)

        issues, strengths = [], []
        score = 50

        if has_faq_schema:
            score = 85
            strengths.append("✅ FAQPage schema confirms structured Q&A — top format for AI citation")
        elif has_faq_page:
            score = 72
            strengths.append("✅ FAQ page detected — preferred content format for AI systems")
            issues.append("🟡 Add FAQPage schema markup to your FAQ page to maximise AI Overview inclusion")
        elif has_blog:
            score = 58
            strengths.append("✅ Blog present — publish Q&A-format articles for AI citation")
            issues.append("🟡 No dedicated FAQ page — AI systems strongly prefer explicit Q&A format")
        else:
            score = 30
            issues.append("🟡 No blog and no FAQ page — AI systems have little Q&A content to cite")
            issues.append(
                "🟡 Create a FAQ page answering: 'What does a fractional CMO do for financial advisors "
                "and CPAs?', 'How do law firms find more clients?', 'What is marketing ROI for professional services?'"
            )

        return {
            "score":  score,
            "status": "pass" if score >= 65 else ("neutral" if score >= 50 else "fail"),
            "detail": f"Blog: {'Yes' if has_blog else 'No'} | FAQ page: {'Yes' if has_faq_page else 'No'} | FAQ schema: {'Yes' if has_faq_schema else 'No'}",
            "issues": issues, "strengths": strengths,
        }

    # ═══════════════════════════════════════════════════════════
    #  5. E-E-A-T Signals
    # ═══════════════════════════════════════════════════════════

    def _score_eeat(self) -> Dict:
        signals = {
            "testimonials":   self.site.get("has_testimonials", False),
            "case_studies":   self.site.get("has_case_studies", False),
            "certifications": self.site.get("has_certifications", False),
            "media_mentions": self.site.get("has_media_mentions", False),
            "client_logos":   self.site.get("has_client_logos", False),
        }
        known  = sum(1 for v in signals.values() if v is True)
        absent = sum(1 for v in signals.values() if v is False)
        total  = known + absent

        issues, strengths = [], []

        if total == 0:
            return {
                "score": 50, "status": "unknown",
                "detail": "E-E-A-T signals unverified.",
                "issues": ["🟡 E-E-A-T signals not verified — add testimonials, case studies, and credentials to site"],
                "strengths": [], "signals": signals,
            }

        base  = round(known / total * 100)
        score = max(25, min(90, base))

        if signals.get("testimonials"):
            strengths.append("✅ Client testimonials present — trust signal for AI systems")
        else:
            issues.append("🔴 No testimonials — AI systems discount unverified expertise claims")

        if signals.get("case_studies"):
            strengths.append("✅ Case studies present — AI systems prefer demonstrated results")
        else:
            issues.append("🟡 No case studies — add 1–2 outcome-focused client stories")

        if signals.get("certifications"):
            strengths.append("✅ Credentials / certifications visible")
        else:
            issues.append("🟡 No certifications displayed — add relevant credentials and memberships")

        if signals.get("media_mentions"):
            strengths.append("✅ Media mentions present — strong authority signal")
        else:
            issues.append("🟡 No media mentions — pursue guest articles on financial advisor, CPA, and legal publications")

        if signals.get("client_logos"):
            strengths.append("✅ Client logos displayed — social proof for AI trust scoring")

        return {
            "score":  score,
            "status": "pass" if score >= 65 else ("neutral" if score >= 50 else "fail"),
            "detail": f"{known}/{total} E-E-A-T signals present.",
            "signals": signals, "issues": issues, "strengths": strengths,
        }

    # ═══════════════════════════════════════════════════════════
    #  6. Brand Authority
    # ═══════════════════════════════════════════════════════════

    def _score_authority(self) -> Dict:
        n_channels   = len(self.config.active_social_channels)
        li_followers = self.linkedin.get("followers") or 0
        has_linkedin = bool(self.config.linkedin_url)

        issues, strengths = [], []
        channel_score = min(30, n_channels * 6)

        if li_followers >= 5000:
            li_score = 40
            strengths.append(f"✅ LinkedIn: {li_followers:,} followers — strong authority signal")
        elif li_followers >= 1000:
            li_score = 25
            strengths.append(f"✅ LinkedIn: {li_followers:,} followers")
            issues.append("🟡 LinkedIn followers <5,000 — AI systems weight high-follower pages more")
        elif li_followers > 0:
            li_score = 12
            issues.append(f"🟡 LinkedIn: {li_followers:,} followers — grow to 5,000+ for authority signals")
        elif has_linkedin:
            li_score = 10
            issues.append("🟡 LinkedIn follower count unknown")
        else:
            li_score = 0
            issues.append("🔴 No LinkedIn presence — primary B2B discovery channel")

        gbp_score = 15   # neutral — not verifiable without Places API here
        score = max(20, min(90, channel_score + li_score + gbp_score))

        if n_channels >= 4:
            strengths.append(f"✅ Present on {n_channels} social platforms — broad brand footprint")
        elif n_channels >= 2:
            strengths.append(f"✅ Present on {n_channels} social platforms")
        else:
            issues.append("🔴 Minimal social presence — AI uses social signals to gauge brand authority")

        return {
            "score":  score,
            "status": "pass" if score >= 65 else ("neutral" if score >= 50 else "fail"),
            "detail": f"{n_channels} social channels | LinkedIn {li_followers:,} followers",
            "issues": issues, "strengths": strengths,
        }

    # ═══════════════════════════════════════════════════════════
    #  7. AI Citation Score (composite)
    # ═══════════════════════════════════════════════════════════

    def _score_citation(self, schema, faq, eeat, authority) -> Dict:
        composite = round(
            schema["score"]    * 0.30 +
            faq["score"]       * 0.25 +
            eeat["score"]      * 0.25 +
            authority["score"] * 0.20
        )
        score = max(25, min(90, composite))

        if score >= 65:
            return {
                "score": score, "status": "pass",
                "detail": "Moderate-to-good AI citation likelihood.",
                "strengths": ["✅ Brand signals sufficient for occasional AI mention"],
                "issues":    ["🟡 Add Q&A content and schema to increase citation frequency"],
            }
        if score >= 45:
            return {
                "score": score, "status": "neutral",
                "detail": "Low AI citation likelihood — brand lacks required signals.",
                "strengths": [],
                "issues": [
                    "🔴 Unlikely to appear in ChatGPT / Perplexity without schema + FAQ content",
                    "🟡 AI Overviews prioritize E-E-A-T — add testimonials and case studies",
                ],
            }
        return {
            "score": score, "status": "fail",
            "detail": "Very low AI citation likelihood.",
            "strengths": [],
            "issues": [
                "🔴 No structured data, no FAQ content, minimal trust signals",
                "🔴 Immediate action: add Schema.org markup + FAQ page + 1 case study",
            ],
        }

    # ═══════════════════════════════════════════════════════════
    #  Aggregation helpers
    # ═══════════════════════════════════════════════════════════

    def _collect_issues_strengths(self, components: Dict) -> Tuple[List, List]:
        seen_i, seen_s = set(), set()
        issues, strengths = [], []
        for comp in components.values():
            for i in comp.get("issues", []):
                if i not in seen_i:
                    seen_i.add(i); issues.append(i)
            for s in comp.get("strengths", []):
                if s not in seen_s:
                    seen_s.add(s); strengths.append(s)
        return issues, strengths

    def _platform_notes(self, components: Dict) -> Dict[str, str]:
        schema_ok  = components["Schema Markup"]["status"] == "pass"
        eeat_ok    = components["E-E-A-T Signals"]["status"] == "pass"
        faq_ok     = components["FAQ / Q&A Content"]["status"] == "pass"
        citation_s = components["AI Citation Score"]["score"]
        serp_ok    = components["SERP Visibility"]["status"] == "pass"
        onpage_ok  = components["On-page SEO"]["status"] == "pass"

        chatgpt = (
            "Likely to appear when users ask about marketing for financial advisors and CPAs — "
            "brand is indexed and has some structured signals. Add FAQPage schema + case studies "
            "to increase citation frequency."
            if citation_s >= 55 else
            "Unlikely to appear — ChatGPT pulls from indexed, structured content. "
            "Add schema, FAQ page, and Q&A blog posts targeting your ICP's exact questions."
        )
        google_aio = (
            "Eligible for Google AI Overviews — schema and E-E-A-T signals are present. "
            "Optimize GBP and add FAQPage schema to increase frequency."
            if schema_ok and eeat_ok else
            "Not yet eligible for Google AI Overviews. "
            "Priority: add Organization + FAQPage schema and at least 2 E-E-A-T trust signals."
        )
        perplexity = (
            "Moderate Perplexity citation potential — E-E-A-T signals detected. "
            "Publish 2–3 long-form pieces on fractional CMO for financial advisors to build topic authority."
            if eeat_ok else
            "Low Perplexity citation likelihood — insufficient topical authority. "
            "Publish case studies and guest posts on financial advisor, CPA, and legal publications."
        )
        serp_note = (
            "Ranking keywords verified in Search Console — focus on moving page-2 keywords to page 1."
            if serp_ok else
            "Search Console data unavailable — audit completed using public-site signals only. "
            "Connect Google Search Console to unlock query, click, impression, and ranking insights."
        )
        return {
            "ChatGPT":            chatgpt,
            "Google AI Overview":  google_aio,
            "Perplexity":          perplexity,
            "Search Console":      serp_note,
        }

    # ═══════════════════════════════════════════════════════════
    #  Recommendations
    # ═══════════════════════════════════════════════════════════

    def _build_recommendations(self, components: Dict) -> List[Dict]:
        recs = []
        serp_score   = components["SERP Visibility"]["score"]
        onpage_score = components["On-page SEO"]["score"]
        schema_score = components["Schema Markup"]["score"]
        faq_score    = components["FAQ / Q&A Content"]["score"]
        eeat_score   = components["E-E-A-T Signals"]["score"]
        auth_score   = components["Brand Authority"]["score"]

        onpage_det   = components["On-page SEO"]
        serp_det     = components["SERP Visibility"]

        # On-page quick wins
        if not onpage_det.get("title"):
            recs.append({
                "priority": "CRITICAL", "timeline": "Today",
                "action":   "Add a keyword-rich title tag to the homepage",
                "detail":   "Format: 'Fractional CMO for Financial Advisors & CPAs | Guerrilla Marketing Group' — include ICP language.",
                "impact":   "Title tag is the #1 on-page ranking factor for Google and AI indexers",
            })
        elif onpage_score < 65:
            recs.append({
                "priority": "HIGH", "timeline": "This week",
                "action":   "Rewrite title tag and meta description to include ICP keywords",
                "detail":   f"Current title: '{onpage_det.get('title','')[:60]}'. Update to include: financial advisors, CPAs, attorneys, fractional CMO.",
                "impact":   "Keyword-aligned title/meta boosts CTR and AI summary accuracy",
            })

        if not onpage_det.get("meta_description"):
            recs.append({
                "priority": "HIGH", "timeline": "Today",
                "action":   "Add a meta description to the homepage",
                "detail":   "Write 140–160 chars that include your ICP, value prop, and a CTA. Example: 'GMG provides fractional CMO services for financial advisors, CPAs, and law firms. Get a 90-day growth plan.'",
                "impact":   "Meta description directly affects click-through rate from Google SERPs",
            })

        if onpage_det.get("has_faq_schema") is False:
            recs.append({
                "priority": "HIGH", "timeline": "1–2 days",
                "action":   "Add FAQPage JSON-LD schema to homepage or FAQ page",
                "detail":   "Include 5–8 questions your ICP actually asks: 'What does a fractional CMO do?', 'How do financial advisors get more referrals?', 'What is the ROI of marketing for CPAs?'",
                "impact":   "FAQPage schema is the single highest-leverage action for Google AI Overview inclusion",
            })

        if schema_score < 65:
            recs.append({
                "priority": "HIGH", "timeline": "1–2 days",
                "action":   "Implement Organization + LocalBusiness + Service schema",
                "detail":   "Add JSON-LD in the <head> covering: business name, URL, logo, address, services, founder. Use Google's Rich Results Test to verify.",
                "impact":   "Structured data is how AI systems identify, classify, and cite businesses",
            })

        if faq_score < 65:
            recs.append({
                "priority": "HIGH", "timeline": "1 week",
                "action":   "Create a dedicated FAQ page targeting ICP search queries",
                "detail":   "Answer: 'What does GMG do for financial advisors?', 'How does a fractional CMO help CPAs?', 'What is the cost of fractional CMO services?' Use FAQPage schema.",
                "impact":   "Q&A is the #1 content format AI systems extract for answer generation",
            })

        if eeat_score < 65:
            recs.append({
                "priority": "HIGH", "timeline": "2–3 weeks",
                "action":   "Add testimonials, case studies, and credentials to site",
                "detail":   "Collect 3 client quotes (2 sentences each). Write 1 case study: 'How GMG grew [client] LinkedIn to X followers.' Display any certifications, awards, or media features.",
                "impact":   "E-E-A-T is Google AI Overview's primary source-quality filter",
            })

        avg_pos = serp_det.get("avg_position")  # None when GSC unavailable
        if serp_score < 50 or (avg_pos is not None and avg_pos > 20):
            recs.append({
                "priority": "MEDIUM", "timeline": "30–60 days",
                "action":   "Build topical content around ICP search queries",
                "detail":   "Publish 4–6 blog posts targeting: 'marketing for financial advisors', 'how CPAs get referrals', 'fractional CMO cost', 'law firm marketing strategy'. 800–1,200 words each.",
                "impact":   "Topical authority is the primary driver of non-branded keyword rankings and AI citation",
            })

        if auth_score < 65:
            recs.append({
                "priority": "MEDIUM", "timeline": "1–2 months",
                "action":   "Publish guest content on financial advisor, CPA, and legal platforms",
                "detail":   "Targets: FA Magazine, Investment Advisor, XY Planning Network, NAPFA, Journal of Accountancy, Above the Law. One article = one authoritative backlink + AI citation source.",
                "impact":   "Third-party mentions are the strongest signal for Perplexity and ChatGPT citations",
            })

        recs.append({
            "priority": "MEDIUM", "timeline": "1 week",
            "action":   "Claim, complete, and optimize Google Business Profile",
            "detail":   "Fill all fields: services, description (750 chars), photos (10+), hours. Post 2×/week. Request reviews after every engagement.",
            "impact":   "GBP is a primary data source for Google AI Overviews local answers",
        })

        return recs
