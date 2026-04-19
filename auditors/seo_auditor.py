"""
SEO Auditor
Primary data source: direct public site crawl (always runs first).
Optional enhancement: Google PageSpeed Insights API (Lighthouse scores + CWV).

Design principles
─────────────────
  1. Platform detection first — Wix / WordPress / Shopify / Squarespace / Webflow
     are all SSR and scrape normally.  Next.js is also SSR.  React / Vue SPAs
     render client-side; raw HTML may be empty.

  2. URL normalization — tracking parameters are stripped and HTTP redirects are
     followed once (via fetch_url_ex) before any checks run.

  3. Raw HTML vs rendered DOM — after the raw crawl, if any critical signal is
     missing and the platform is a JS-SPA (or all-fail detected), Playwright
     renders the page and re-checks.  Signals found only after rendering are
     labelled 'found_rendered' — never scored as failures.

  4. Three-state checks — each signal is one of:
       'found'          — present in raw HTML
       'found_rendered' — absent in raw HTML but present after Playwright render
       'missing'        — fetch succeeded, element genuinely absent
       'unable'         — fetch failed OR JS-SPA with sparse HTML

  5. All-fail auto-retry — if title + meta + H1 + schema all absent, the raw
     crawl is retried once before attempting Playwright.

  6. Data quality — every audit run returns a data_quality dict recording
     normalization, platform detection, schema depth, render usage, and an
     overall reliability score (0–100).
"""
import time
from urllib.parse import urlparse
from typing import Dict, Any, List, Optional, Tuple

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

from auditors.scrape_utils import (
    fetch_url, fetch_url_ex, parse_html, normalize_url, detect_platform,
    extract_schema, render_page, PLAYWRIGHT_OK,
    get_title, get_meta_description, get_canonical,
    get_og_tags, get_twitter_card, get_robots_meta,
    get_headings, get_word_count,
)

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

PSI_AUDIT_MAP = {
    "robots-txt":         "robots.txt",
    "document-title":     "Page title tag",
    "meta-description":   "Meta description",
    "viewport":           "Mobile viewport",
    "canonical":          "Canonical URL",
    "structured-data":    "Structured data / Schema",
    "image-alt":          "Image alt text",
    "link-text":          "Descriptive link text",
    "crawlable-anchors":  "Crawlable anchors",
    "hreflang":           "hreflang tags",
}

_JS_SPA_PLATFORMS = {"react", "vue"}
# Signals where render comparison is meaningful
_RENDER_CHECK_KEYS = ("title", "meta", "h1", "schema", "og")


def _grade(score: int) -> str:
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    if score >= 35: return "D"
    return "F"


def _vstate(value: Any, crawl_quality: str, platform: str,
            word_count: int = 0) -> str:
    """
    Tri-state validation: 'found' | 'missing' | 'unable'
    'found_rendered' is set later by render comparison, not here.
    """
    if crawl_quality == "failed":
        return "unable"
    if not value and platform in _JS_SPA_PLATFORMS and word_count < 150:
        return "unable"
    return "found" if value else "missing"


def _compute_reliability(dq: Dict) -> int:
    """
    Score 0–100 reflecting audit data completeness and verification depth.
    Components:
      Raw crawl succeeded           30 pts
      Platform identified           15 pts
      Schema detected (yes/partial) 20 / 10 pts
      Rendered DOM checked           15 pts
      All key signals found          10 pts
      URL normalized                  5 pts
      Redirects resolved              5 pts
    """
    s = 0
    if dq.get("raw_crawl_ok"):                                    s += 30
    if dq.get("platform_detected", "unknown") != "unknown":       s += 15
    sq = dq.get("schema_quality", "no")
    if sq == "yes":                                                s += 20
    elif sq == "partial":                                          s += 10
    if dq.get("render_used"):                                      s += 15
    if dq.get("all_signals_found"):                                s += 10
    if dq.get("url_normalized"):                                   s +=  5
    if dq.get("redirects_resolved"):                               s +=  5
    return min(s, 100)


class SEOAuditor:
    def __init__(self, url: str, api_key: str = ""):
        self.base_url         = url.rstrip("/")
        self._original_url    = url.rstrip("/")
        self.domain           = urlparse(url).netloc
        self.api_key          = api_key
        self._homepage_soup:  Optional[Any] = None
        self._homepage_html:  Optional[str] = None
        self._homepage_ok:    Optional[bool] = None
        self._platform:       str  = "unknown"
        self._retry_done:     bool = False
        self._render_used:    bool = False
        self._render_status:  str  = "not attempted"

    # ─────────────────────────────────────────────────────────────
    #  Public entry point
    # ─────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        # ── Step 0: URL normalisation ──────────────────────────
        # fetch_url_ex follows redirects, strips tracking params, returns
        # (html, status, final_url) so the normalisation and homepage fetch
        # happen in ONE network request.
        html, status, final_url = fetch_url_ex(self.base_url)
        redirects_resolved = (final_url != self.base_url)
        if final_url:
            self.base_url = final_url
            self.domain   = urlparse(final_url).netloc
        self._homepage_html = html
        self._homepage_ok   = bool(html and status not in (0, 404))

        # ── Step 1: Public site crawl ──────────────────────────
        crawl    = self._run_public_crawl()
        crawl_ok = crawl.get("reachable", False)

        # ── Attach data quality metadata ───────────────────────
        signals  = crawl.get("crawl_signals", {})
        vs       = signals.get("validation_states", {})
        sq       = self._schema_quality(signals)
        all_found = all(
            v in ("found", "found_rendered")
            for k, v in vs.items()
            if k in _RENDER_CHECK_KEYS
        )
        dq = {
            "url_normalized":     True,
            "original_url":       self._original_url,
            "final_url":          self.base_url,
            "redirects_resolved": redirects_resolved,
            "platform_detected":  self._platform,
            "schema_quality":     sq,
            "render_used":        self._render_used,
            "render_status":      self._render_status,
            "raw_crawl_ok":       crawl_ok,
            "all_signals_found":  all_found,
        }
        dq["reliability_score"] = _compute_reliability(dq)
        crawl["data_quality"]   = dq

        # ── Step 2: PageSpeed API (optional, mobile + desktop in parallel) ──
        import concurrent.futures as _cf
        with _cf.ThreadPoolExecutor(max_workers=2) as _ex:
            _fut_m = _ex.submit(self._fetch_pagespeed, "mobile")
            _fut_d = _ex.submit(self._fetch_pagespeed, "desktop")
            psi_mobile  = _fut_m.result()
            psi_desktop = _fut_d.result()
        psi = psi_mobile or psi_desktop

        if not psi:
            crawl["data_sources"] = {
                "public_crawl":  "available" if crawl_ok else "unavailable — site could not be reached",
                "pagespeed_api": "unavailable — audit uses direct site crawl only",
            }
            return crawl

        merged = self._merge_psi(crawl, psi, crawl_ok)
        merged["data_quality"] = dq   # preserve through PSI merge
        return merged

    # ─────────────────────────────────────────────────────────────
    #  Public site crawl
    # ─────────────────────────────────────────────────────────────

    def _run_public_crawl(self) -> Dict[str, Any]:
        signals = self._fetch_and_parse_homepage()

        # ── All-fail auto-retry (raw) ─────────────────────────
        if (
            signals and self._homepage_ok
            and not self._retry_done
            and self._all_empty(signals)
        ):
            time.sleep(2)
            self._retry_done    = True
            self._homepage_html = None
            self._homepage_soup = None
            self._homepage_ok   = None
            signals = self._fetch_and_parse_homepage()

        # ── Rendered DOM comparison ───────────────────────────
        # Trigger when: (a) JS-SPA platform, OR (b) critical signals still missing
        if signals and self._homepage_ok:
            needs_render = (
                self._platform in _JS_SPA_PLATFORMS
                or self._has_missing_criticals(signals)
            )
            if needs_render:
                signals = self._run_render_comparison(signals)

        robots  = self._check_robots_txt()
        sitemap = self._check_sitemap()

        reachable = self._homepage_ok is True
        issues, strengths = self._evaluate_public_crawl(signals, robots, sitemap)

        if not reachable:
            score = 50
        else:
            score = max(25, min(85, round(len(strengths) / 10 * 100)))

        canon_dict = {
            "present": bool(signals.get("canonical_url")) if signals else None,
            "value":   signals.get("canonical_url", "") if signals else None,
        }
        og_dict = {
            "present":      signals.get("has_og_tags") if signals else None,
            "tags":         signals.get("og_tags", []) if signals else [],
            "has_og_image": signals.get("has_og_image", False) if signals else False,
            "has_og_title": signals.get("has_og_title", False) if signals else False,
        }

        return {
            "method":        "public_crawl",
            "score":         score,
            "grade":         _grade(score),
            "reachable":     reachable,
            "platform":      self._platform,
            "crawl_signals": signals or {},
            "robots_txt":    robots,
            "sitemap":       sitemap,
            "canonical":     canon_dict,
            "open_graph":    og_dict,
            "has_schema":    signals.get("has_schema", False) if signals else False,
            "schema_items":  signals.get("schema_types", []) if signals else [],
            "issues":        issues,
            "strengths":     strengths,
        }

    # ─────────────────────────────────────────────────────────────
    #  Homepage fetch and signal extraction
    # ─────────────────────────────────────────────────────────────

    def _extract_signals_from_soup(
        self, soup: Any, html: str, crawl_quality: str
    ) -> Dict:
        """
        Run all signal extractors on a parsed BeautifulSoup object and return
        a normalised signals dict with validation_states.  Used for both raw
        HTML and rendered DOM to enable side-by-side comparison.
        """
        self._platform = detect_platform(soup, html)

        title       = get_title(soup)
        meta_desc   = get_meta_description(soup)
        headings    = get_headings(soup, self._platform)
        canonical   = get_canonical(soup)
        robots_meta = get_robots_meta(soup)
        og          = get_og_tags(soup)
        twitter     = get_twitter_card(soup)
        schema_types, has_faq = extract_schema(soup)
        wc          = get_word_count(soup)

        vs = {
            "title":        _vstate(title,              crawl_quality, self._platform, wc),
            "meta":         _vstate(meta_desc,          crawl_quality, self._platform, wc),
            "h1":           _vstate(headings["h1s"],    crawl_quality, self._platform, wc),
            "canonical":    _vstate(canonical,          crawl_quality, self._platform, wc),
            "schema":       _vstate(schema_types,       crawl_quality, self._platform, wc),
            "og":           _vstate(og.get("present"),  crawl_quality, self._platform, wc),
            "twitter":      _vstate(twitter.get("present"), crawl_quality, self._platform, wc),
            "indexability": "found" if crawl_quality != "failed" else "unable",
        }

        return {
            "title":            title,
            "meta_description": meta_desc,
            "h1s":              headings["h1s"],
            "h2s":              headings["h2s"],
            "canonical_url":    canonical,
            "has_canonical":    bool(canonical),
            "is_indexable":     robots_meta["is_indexable"],
            "og_tags":          og["tags"],
            "has_og_tags":      og["present"] or False,
            "has_og_image":     og["has_og_image"],
            "has_og_title":     og["has_og_title"],
            "twitter_tags":     twitter["tags"],
            "has_twitter_card": twitter["present"] or False,
            "schema_types":     schema_types,
            "has_schema":       len(schema_types) > 0,
            "has_faq_schema":   has_faq,
            "word_count":       wc,
            "platform":         self._platform,
            "crawl_quality":    crawl_quality,
            "validation_states": vs,
        }

    def _fetch_and_parse_homepage(self) -> Optional[Dict]:
        """
        Fetch or use cached homepage HTML and extract all signals.
        Returns None if site is unreachable.
        """
        if self._homepage_ok is False and self._homepage_html is None:
            return None

        html   = self._homepage_html
        if html is None:
            html, status = fetch_url(self.base_url)
            if not html or status == 0:
                self._homepage_ok   = False
                return None
            self._homepage_ok   = True
            self._homepage_html = html

        soup = parse_html(html)
        if not soup:
            return self._empty_signals("failed")

        self._homepage_soup = soup

        if self._platform in _JS_SPA_PLATFORMS and get_word_count(soup) < 150:
            crawl_quality = "partial"
        else:
            crawl_quality = "ok"

        return self._extract_signals_from_soup(soup, html, crawl_quality)

    def _run_render_comparison(self, raw_signals: Dict) -> Dict:
        """
        Render the page with Playwright and compare against raw signals.

        For any signal that was 'missing' or 'unable' in raw HTML but is
        'found' in the rendered DOM, upgrade the state to 'found_rendered'.
        The underlying value is also updated so downstream checks are accurate.

        Returns the (possibly upgraded) signals dict.
        """
        rendered_html, status_msg = render_page(self.base_url)
        self._render_status = status_msg

        if not rendered_html:
            return raw_signals  # render failed — keep raw signals unchanged

        self._render_used = True
        soup = parse_html(rendered_html)
        if not soup:
            return raw_signals

        rendered = self._extract_signals_from_soup(soup, rendered_html, "ok")

        # Upgrade states: if missing/unable in raw but found in rendered
        vs_raw      = raw_signals.get("validation_states", {})
        vs_rendered = rendered.get("validation_states", {})
        upgraded    = dict(vs_raw)

        _signal_map = {
            "title":    ("title",        "title"),
            "meta":     ("meta_description", "meta_description"),
            "h1":       ("h1s",          "h1s"),
            "canonical":("canonical_url","canonical_url"),
            "schema":   ("schema_types", "schema_types"),
            "og":       ("has_og_tags",  "has_og_tags"),
            "twitter":  ("has_twitter_card", "has_twitter_card"),
        }

        merged = dict(raw_signals)
        for vs_key, (raw_field, _) in _signal_map.items():
            raw_state  = vs_raw.get(vs_key, "missing")
            rend_state = vs_rendered.get(vs_key, "missing")
            if raw_state in ("missing", "unable") and rend_state == "found":
                upgraded[vs_key] = "found_rendered"
                # Copy the rendered value into merged signals
                merged[raw_field] = rendered[raw_field]
                # Sync boolean helpers
                if raw_field == "h1s":
                    merged["h2s"] = rendered["h2s"]
                elif raw_field == "schema_types":
                    merged["has_schema"]    = rendered["has_schema"]
                    merged["has_faq_schema"]= rendered["has_faq_schema"]
                elif raw_field == "has_og_tags":
                    merged["og_tags"]       = rendered["og_tags"]
                    merged["has_og_image"]  = rendered["has_og_image"]
                    merged["has_og_title"]  = rendered["has_og_title"]

        merged["validation_states"] = upgraded
        merged["render_comparison"] = {
            "used":             True,
            "upgraded_signals": [k for k, v in upgraded.items() if v == "found_rendered"],
        }
        return merged

    # ─────────────────────────────────────────────────────────────
    #  Evaluation  (four-state aware)
    # ─────────────────────────────────────────────────────────────

    def _evaluate_public_crawl(
        self,
        signals: Optional[Dict],
        robots: Dict,
        sitemap: Dict,
    ) -> Tuple[List[str], List[str]]:
        issues, strengths = [], []

        if not self._homepage_ok:
            issues.append(
                "🟡 Site could not be reached — verify the URL is correct and publicly accessible"
            )
            return issues, strengths

        if not signals:
            return issues, strengths

        vs            = signals.get("validation_states", {})
        platform      = signals.get("platform", "unknown")
        crawl_quality = signals.get("crawl_quality", "ok")
        rc            = signals.get("render_comparison", {})

        if crawl_quality == "partial" and not rc.get("used"):
            issues.append(
                f"🔵 Platform detected as client-side {platform.upper()} — some signals "
                "could not be validated from raw HTML"
            )
        if rc.get("used") and rc.get("upgraded_signals"):
            upg = ", ".join(rc["upgraded_signals"])
            strengths.append(
                f"✅ Rendered DOM verified — {upg} found after JavaScript execution"
            )

        # ── Indexability ──────────────────────────────────────
        if vs.get("indexability") == "unable":
            pass
        elif not signals.get("is_indexable"):
            issues.append("🔴 Page has a noindex directive — search engines cannot index this page")
        else:
            strengths.append("✅ Page is indexable — no noindex directive detected")

        # ── Title tag ─────────────────────────────────────────
        ts    = vs.get("title", "missing")
        title = signals.get("title", "")
        if ts == "unable":
            issues.append("🔵 Title tag could not be validated (client-side rendering likely)")
        elif ts == "found_rendered":
            tlen = len(title)
            strengths.append(f"✅ Title tag found after render ({tlen} chars) — consider server-side rendering for better SEO")
        elif title:
            tlen = len(title)
            if 30 <= tlen <= 70:
                strengths.append(f"✅ Title tag present ({tlen} chars)")
            elif tlen < 30:
                issues.append(f"🟡 Title tag too short ({tlen} chars) — aim for 50–60 characters")
            else:
                issues.append(f"🟡 Title tag long ({tlen} chars) — Google truncates above ~60")
        else:
            issues.append("🔴 No title tag — critical for SEO and AI indexing")

        # ── Meta description ──────────────────────────────────
        ms   = vs.get("meta", "missing")
        meta = signals.get("meta_description", "")
        if ms == "unable":
            issues.append("🔵 Meta description could not be validated")
        elif ms == "found_rendered":
            strengths.append("✅ Meta description found after render — consider server-side rendering for better SEO")
        elif meta:
            mlen = len(meta)
            if 100 <= mlen <= 165:
                strengths.append(f"✅ Meta description present ({mlen} chars)")
            else:
                issues.append(f"🟡 Meta description length ({mlen} chars) — aim for 140–160 characters")
        else:
            issues.append("🔴 No meta description — missed CTR and AI summary opportunity")

        # ── H1 / H2 headings ─────────────────────────────────
        h1s = signals.get("h1s", [])
        h2s = signals.get("h2s", [])
        hs  = vs.get("h1", "missing")
        if hs == "unable":
            issues.append("🔵 H1 heading could not be validated (client-side rendering likely)")
        elif hs == "found_rendered":
            strengths.append("✅ H1 heading found after render — consider server-side rendering for SEO")
        elif not h1s:
            issues.append("🔴 No H1 tag — every page needs a keyword-rich H1 heading")
        elif len(h1s) == 1:
            strengths.append("✅ Single H1 tag (correct heading structure)")
        else:
            issues.append(f"🟡 Multiple H1 tags ({len(h1s)}) — consolidate to a single primary H1")
        if hs not in ("unable",):
            if len(h2s) >= 2:
                strengths.append(f"✅ {len(h2s)} H2 subheadings — well-structured content")
            else:
                issues.append("🟡 Fewer than 2 H2 tags — use H2s to structure content around key topics")

        # ── Canonical tag ─────────────────────────────────────
        cs = vs.get("canonical", "missing")
        if cs == "unable":
            pass
        elif cs == "found_rendered":
            strengths.append("✅ Canonical tag found after render — add it to server-side HTML for full SEO benefit")
        elif signals.get("has_canonical"):
            strengths.append("✅ Canonical tag present — prevents duplicate content signals")
        else:
            issues.append("🟡 No canonical tag — add a canonical link element to the homepage")

        # ── Schema markup ─────────────────────────────────────
        ss = vs.get("schema", "missing")
        if ss == "unable":
            pass
        elif ss in ("found", "found_rendered"):
            types  = signals.get("schema_types", [])
            note   = " (JS-rendered — move to static HTML for reliability)" if ss == "found_rendered" else ""
            strengths.append(f"✅ Structured data present ({', '.join(types[:3])}){note}")
        else:
            issues.append("🟡 No structured data / schema markup found")

        # ── Robots.txt ────────────────────────────────────────
        if robots.get("exists") is True:
            strengths.append("✅ robots.txt file present")
            if robots.get("sitemap_referenced"):
                strengths.append("✅ Sitemap referenced in robots.txt")
            else:
                issues.append("🟡 Sitemap not referenced in robots.txt")
        elif robots.get("exists") is False:
            issues.append("🟡 No robots.txt file found")

        # ── XML Sitemap ───────────────────────────────────────
        if sitemap.get("found"):
            strengths.append("✅ XML sitemap found")
        elif sitemap.get("found") is False:
            issues.append("🔴 No XML sitemap found — search engines may miss pages")

        # ── Open Graph ────────────────────────────────────────
        os_ = vs.get("og", "missing")
        if os_ == "unable":
            pass
        elif os_ in ("found", "found_rendered"):
            strengths.append("✅ Open Graph tags present — optimized for social sharing")
            if signals.get("has_og_image"):
                strengths.append("✅ og:image present")
            else:
                issues.append("🟡 Missing og:image — add an image for richer social sharing previews")
        else:
            issues.append("🟡 No Open Graph tags — add og:title, og:description, og:image")

        # ── Twitter Card ──────────────────────────────────────
        tw = vs.get("twitter", "missing")
        if tw == "unable":
            pass
        elif tw in ("found", "found_rendered"):
            strengths.append("✅ Twitter Card tags present")
        else:
            issues.append("🟡 No Twitter Card tags — add twitter:card and twitter:image for X/Twitter previews")

        return issues, strengths

    # ─────────────────────────────────────────────────────────────
    #  PageSpeed API
    # ─────────────────────────────────────────────────────────────

    def _fetch_pagespeed(self, strategy: str) -> Dict:
        if not REQUESTS_OK:
            return {}
        params = {
            "url":      self.base_url,
            "strategy": strategy,
            "category": ["seo", "performance", "accessibility", "best-practices"],
        }
        if self.api_key:
            params["key"] = self.api_key
        try:
            r = requests.get(PAGESPEED_ENDPOINT, params=params, timeout=60)
            return r.json() if r.status_code == 200 else {}
        except Exception:
            return {}

    def _merge_psi(self, crawl: Dict, psi: Dict, crawl_ok: bool) -> Dict[str, Any]:
        lh     = psi.get("lighthouseResult", {})
        cats   = lh.get("categories", {})
        audits = lh.get("audits", {})

        seo_raw  = cats.get("seo",           {}).get("score")
        perf_raw = cats.get("performance",    {}).get("score")
        acc_raw  = cats.get("accessibility",  {}).get("score")

        lh_seo_score  = round(seo_raw  * 100) if seo_raw  is not None else crawl["score"]
        lh_perf_score = round(perf_raw * 100) if perf_raw is not None else 50
        lh_acc_score  = round(acc_raw  * 100) if acc_raw  is not None else 50

        audit_results: Dict[str, Dict] = {}
        for audit_id, label in PSI_AUDIT_MAP.items():
            a = audits.get(audit_id, {})
            audit_score = a.get("score")
            if audit_score is None:
                audit_results[audit_id] = {"label": label, "passed": None, "display": "—"}
            else:
                passed = audit_score >= 0.9
                audit_results[audit_id] = {
                    "label":   label,
                    "passed":  passed,
                    "display": a.get("displayValue", "Pass" if passed else "Fail"),
                }

        fcp  = audits.get("first-contentful-paint",   {}).get("displayValue", "—")
        lcp  = audits.get("largest-contentful-paint",  {}).get("displayValue", "—")
        cls_ = audits.get("cumulative-layout-shift",   {}).get("displayValue", "—")
        tbt  = audits.get("total-blocking-time",       {}).get("displayValue", "—")

        sd_audit   = audits.get("structured-data", {})
        psi_schema = [
            item.get("description", str(item))
            for item in sd_audit.get("details", {}).get("items", [])
            if isinstance(item, dict)
        ]
        has_schema   = (
            (audit_results.get("structured-data", {}).get("passed") or False)
            or crawl.get("has_schema", False)
        )
        schema_items = psi_schema or crawl.get("schema_items", [])

        psi_issues, psi_strengths = self._evaluate_pagespeed(
            audit_results, crawl["sitemap"], crawl["open_graph"],
            lh_seo_score, lh_perf_score,
        )
        _crawl_kws = (
            "canonical", "noindex", "indexable", "h1", "h2",
            "twitter", "heading", "subheading", "client-side", "render",
        )
        crawl_extra_issues    = [i for i in crawl["issues"]    if any(k in i.lower() for k in _crawl_kws)]
        crawl_extra_strengths = [s for s in crawl["strengths"] if any(k in s.lower() for k in _crawl_kws)]

        # Render authority: when Playwright confirmed a signal, suppress contradicting
        # PSI failures and crawl "client-side can't verify" notices for that signal.
        _upgraded = (
            crawl.get("render_comparison", {})
                 .get("upgraded_signals", [])
        )
        if _upgraded:
            _render_suppress = {
                "title":    ("missing page title",       "title"),
                "meta":     ("missing meta description", "meta"),
                "h1":       ("",                         "h1"),
                "canonical":("no canonical",             "canonical"),
                "schema":   ("no structured data",       "schema"),
                "og":       ("no open graph",            "og"),
            }
            suppress_psi_patterns   = []
            suppress_crawl_patterns = []
            confirmed_labels        = []
            for sig in _upgraded:
                if sig in _render_suppress:
                    psi_pat, crawl_pat = _render_suppress[sig]
                    if psi_pat:
                        suppress_psi_patterns.append(psi_pat.lower())
                    if crawl_pat:
                        suppress_crawl_patterns.append(crawl_pat.lower())
                    confirmed_labels.append(sig)

            psi_issues = [
                i for i in psi_issues
                if not any(p in i.lower() for p in suppress_psi_patterns)
            ]
            crawl_extra_issues = [
                i for i in crawl_extra_issues
                if not any(p in i.lower() for p in suppress_crawl_patterns)
            ]
            crawl_extra_strengths = [
                s for s in crawl_extra_strengths
                if "client-side" not in s.lower()
                or not any(p in s.lower() for p in suppress_crawl_patterns)
            ]
            if confirmed_labels:
                psi_strengths = [
                    f"ℹ️ JS-rendered result used as authoritative source for: {', '.join(confirmed_labels)}"
                ] + psi_strengths

        all_issues    = list(dict.fromkeys(psi_issues    + crawl_extra_issues))
        all_strengths = list(dict.fromkeys(psi_strengths + crawl_extra_strengths))

        return {
            "method":              "pagespeed+crawl",
            "score":               lh_seo_score,
            "grade":               _grade(lh_seo_score),
            "performance_score":   lh_perf_score,
            "accessibility_score": lh_acc_score,
            "audit_results":       audit_results,
            "core_web_vitals":     {"fcp": fcp, "lcp": lcp, "cls": cls_, "tbt": tbt},
            "schema_items":        schema_items,
            "has_schema":          has_schema,
            "platform":            self._platform,
            "crawl_signals":       crawl.get("crawl_signals", {}),
            "robots_txt":          self._psi_to_robots_dict(audit_results, audits),
            "sitemap":             crawl["sitemap"],
            "canonical":           crawl["canonical"],
            "open_graph":          crawl["open_graph"],
            "issues":              all_issues,
            "strengths":           all_strengths,
            "data_sources": {
                "public_crawl":  "available" if crawl_ok else "unavailable — site could not be reached",
                "pagespeed_api": "connected — Lighthouse data included",
            },
        }

    def _evaluate_pagespeed(
        self, audit_results: dict, sitemap: dict,
        og: dict, seo_score: int, perf_score: int,
    ) -> Tuple[List[str], List[str]]:
        issues, strengths = [], []

        if seo_score >= 80:
            strengths.append(f"✅ Lighthouse SEO score: {seo_score}/100")
        elif seo_score >= 50:
            issues.append(f"🟡 Lighthouse SEO score: {seo_score}/100 — room to improve")
        else:
            issues.append(f"🔴 Low Lighthouse SEO score: {seo_score}/100")

        if perf_score >= 65:
            strengths.append(f"✅ Performance score: {perf_score}/100")
        elif perf_score >= 35:
            issues.append(f"🟡 Performance score: {perf_score}/100")
        else:
            issues.append(f"🔴 Poor performance score: {perf_score}/100 — hurts Core Web Vitals ranking")

        audit_labels = {
            "robots-txt":       ("🟡 Missing robots.txt",          "✅ robots.txt present"),
            "document-title":   ("🔴 CRITICAL: Missing page title", "✅ Page title present"),
            "meta-description": ("🟡 Missing meta description",     "✅ Meta description present"),
            "viewport":         ("🔴 Missing mobile viewport meta", "✅ Mobile viewport configured"),
            "canonical":        ("🟡 No canonical URL tag",         "✅ Canonical URL implemented"),
            "structured-data":  ("🟡 No structured data / schema markup", "✅ Structured data present"),
            "image-alt":        ("🟡 Images missing alt text",      "✅ Images have alt text"),
        }
        for audit_id, (fail_msg, pass_msg) in audit_labels.items():
            result = audit_results.get(audit_id, {})
            passed = result.get("passed")
            if passed is True:
                strengths.append(pass_msg)
            elif passed is False:
                issues.append(fail_msg)

        if sitemap.get("found"):
            strengths.append("✅ XML sitemap found")
        elif sitemap.get("found") is False:
            issues.append("🔴 No XML sitemap found — search engines may miss pages")

        if og.get("present"):
            strengths.append("✅ Open Graph tags present")
            if og.get("has_og_image"):
                strengths.append("✅ OG image tag present")
            else:
                issues.append("🟡 Missing og:image tag")
        elif og.get("present") is False:
            issues.append("🟡 No Open Graph meta tags")

        return issues, strengths

    def _psi_to_robots_dict(self, audit_results: dict, audits: dict) -> dict:
        robots_passed = audit_results.get("robots-txt", {}).get("passed")
        sitemap_ref   = False
        if robots_passed:
            for item in audits.get("robots-txt", {}).get("details", {}).get("items", []):
                if isinstance(item, dict) and "sitemap" in str(item).lower():
                    sitemap_ref = True
        return {"exists": robots_passed, "sitemap_referenced": sitemap_ref}

    # ─────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────

    def _fetch(self, url: str) -> tuple:
        return fetch_url(url)

    def _check_robots_txt(self) -> Dict:
        url = f"{self.base_url}/robots.txt"
        text, status = fetch_url(url)
        if status == 0 or text is None:
            return {"url": url, "exists": None, "status_code": status, "sitemap_referenced": False}
        exists = status == 200 and "user-agent" in text.lower()
        return {
            "url": url, "exists": exists, "status_code": status,
            "sitemap_referenced": "sitemap:" in text.lower() if text else False,
        }

    def _check_sitemap(self) -> Dict:
        for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap/", "/sitemap1.xml"]:
            url = f"{self.base_url}{path}"
            text, status = fetch_url(url)
            if status == 0 or text is None:
                continue
            if status == 200 and text:
                url_count = text.count("<url>") + text.count("<loc>")
                return {"found": True, "url": url, "url_count_estimate": min(url_count, 9999)}
        return {"found": False, "url": None, "url_count_estimate": 0}

    @staticmethod
    def _empty_signals(crawl_quality: str = "failed") -> Dict:
        vs = {k: "unable" for k in
              ("title", "meta", "h1", "canonical", "schema", "og", "twitter", "indexability")}
        return {
            "title": "", "meta_description": "", "h1s": [], "h2s": [],
            "canonical_url": "", "has_canonical": False, "is_indexable": True,
            "og_tags": [], "has_og_tags": False, "has_og_image": False, "has_og_title": False,
            "twitter_tags": [], "has_twitter_card": False,
            "schema_types": [], "has_schema": False, "has_faq_schema": False,
            "word_count": 0, "platform": "unknown",
            "crawl_quality": crawl_quality, "validation_states": vs,
        }

    @staticmethod
    def _all_empty(signals: Dict) -> bool:
        return (
            not signals.get("title")
            and not signals.get("meta_description")
            and not signals.get("h1s")
            and not signals.get("has_schema")
        )

    @staticmethod
    def _has_missing_criticals(signals: Dict) -> bool:
        """True if any critical signal (title, H1) is missing — render may help."""
        vs = signals.get("validation_states", {})
        return vs.get("title") in ("missing", "unable") or vs.get("h1") in ("missing", "unable")

    @staticmethod
    def _schema_quality(signals: Dict) -> str:
        schema_types = signals.get("schema_types", [])
        if not schema_types:
            return "no"
        if len(schema_types) >= 2:
            return "yes"
        return "partial"

    # ─────────────────────────────────────────────────────────────
    #  Legacy aliases
    # ─────────────────────────────────────────────────────────────

    def _check_canonical(self) -> Dict:
        canon = get_canonical(self._homepage_soup) if self._homepage_soup else ""
        if self._homepage_soup:
            return {"present": bool(canon), "value": canon or None}
        html, status = fetch_url(self.base_url)
        if not html or status == 0:
            return {"present": None, "value": None}
        soup = parse_html(html)
        canon = get_canonical(soup)
        return {"present": bool(canon), "value": canon or None}

    def _check_open_graph(self) -> Dict:
        soup = self._homepage_soup
        if not soup:
            html, status = fetch_url(self.base_url)
            if not html or status == 0:
                return {"present": None, "tags": []}
            soup = parse_html(html)
        return get_og_tags(soup)

    def _run_pagespeed(self) -> Dict[str, Any]:
        return self.run()

    def _run_scraping(self) -> Dict[str, Any]:
        return self._run_public_crawl()
