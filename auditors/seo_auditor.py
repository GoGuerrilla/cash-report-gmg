"""
SEO Auditor
Primary data source: direct public site crawl (always runs first).
Optional enhancement: Google PageSpeed Insights API (Lighthouse scores + CWV).

Design principles
─────────────────
  1. Platform detection first — Wix / WordPress / Shopify / Squarespace / Webflow
     are all SSR and scrape normally.  Next.js is also SSR.  React / Vue SPAs
     render client-side; missing technical signals from those platforms are
     treated as Unable-to-validate, never as failures.

  2. URL normalization — tracking parameters (utm_*, fbclid, gclid …) are
     stripped and HTTP redirects are followed once to resolve www vs non-www
     and http vs https before any audit checks run.  All subsequent fetches
     (robots.txt, sitemap) use the resolved canonical URL.

  3. Three-state checks — every technical signal is one of:
       Found             (True / non-empty)
       Missing           (False / empty, but fetch succeeded)
       Unable-to-validate (None — fetch failed OR JS-SPA detected)
     "Unable-to-validate" is scored neutrally (50); it is never penalised
     as a failure.

  4. All-fail auto-retry — if every technical check (title, meta, H1, schema)
     comes back empty simultaneously, the crawl is retried once after a brief
     pause before scores are finalised.

Data Sources
────────────
  Public crawl (always)    — title tag, meta description, H1/H2 headings,
                             canonical tag, indexability (noindex check),
                             Open Graph tags, Twitter Card tags, schema markup,
                             XML sitemap, robots.txt
  PageSpeed API (optional) — Lighthouse SEO / performance / accessibility scores,
                             Core Web Vitals, individual audit pass/fail results
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
    extract_schema,
    get_title, get_meta_description, get_canonical,
    get_og_tags, get_twitter_card, get_robots_meta,
    get_headings, get_word_count,
)

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

# Maps PageSpeed audit IDs → human-readable labels used in the report
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

# JS-SPA platforms where absent technical signals = unable-to-validate
_JS_SPA_PLATFORMS = {"react", "vue"}


def _grade(score: int) -> str:
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    if score >= 35: return "D"
    return "F"


def _vstate(value: Any, crawl_quality: str, platform: str,
            word_count: int = 0) -> str:
    """
    Determine the validation state for a single signal.

    Returns 'found' | 'missing' | 'unable'.

    Rules:
      - crawl_quality='failed'           → 'unable' (fetch itself failed)
      - JS-SPA platform + sparse HTML    → 'unable' (content is client-side)
      - Otherwise: value truthy → 'found', falsy → 'missing'
    """
    if crawl_quality == "failed":
        return "unable"
    if not value and platform in _JS_SPA_PLATFORMS and word_count < 150:
        return "unable"
    return "found" if value else "missing"


class SEOAuditor:
    def __init__(self, url: str, api_key: str = ""):
        self.base_url        = url.rstrip("/")
        self.domain          = urlparse(url).netloc
        self.api_key         = api_key
        self._homepage_soup: Optional[Any] = None   # cached BeautifulSoup object
        self._homepage_html: Optional[str] = None   # cached raw HTML (from first fetch)
        self._homepage_ok:   Optional[bool] = None  # True = reachable, False = not
        self._platform:      str = "unknown"        # detected CMS platform
        self._retry_done:    bool = False            # guard for all-fail retry

    # ─────────────────────────────────────────────────────────────
    #  Public entry point
    # ─────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """
        Always runs a full public site crawl first.
        Optionally enhances with Google PageSpeed Insights if available.
        GSC is not used by the SEO auditor — see GEO auditor for SERP data.
        """
        # ── Step 0: URL normalisation ───────────────────────────
        # fetch_url_ex follows redirects and strips tracking params, returning
        # (html, status, final_url) in a single request that we cache so the
        # crawl step below doesn't need to re-fetch.
        html, status, final_url = fetch_url_ex(self.base_url)
        if final_url and final_url != self.base_url:
            self.base_url = final_url          # use canonical/redirected URL
            self.domain   = urlparse(final_url).netloc
        # Cache so _fetch_and_parse_homepage() re-uses without a second request
        self._homepage_html = html
        self._homepage_ok   = bool(html and status not in (0, 404))

        # ── Step 1: Public site crawl — always runs ─────────────
        crawl    = self._run_public_crawl()
        crawl_ok = crawl.get("reachable", False)

        # ── Step 2: PageSpeed API — optional enhancement ─────────
        psi_mobile  = self._fetch_pagespeed("mobile")
        psi_desktop = self._fetch_pagespeed("desktop")
        psi = psi_mobile or psi_desktop

        if not psi:
            crawl["data_sources"] = {
                "public_crawl": "available" if crawl_ok else "unavailable — site could not be reached",
                "pagespeed_api": "unavailable — audit uses direct site crawl only",
            }
            return crawl

        return self._merge_psi(crawl, psi, crawl_ok)

    # ─────────────────────────────────────────────────────────────
    #  Public site crawl (always runs)
    # ─────────────────────────────────────────────────────────────

    def _run_public_crawl(self) -> Dict[str, Any]:
        """
        Direct public site crawl — always runs regardless of API availability.
        One homepage fetch covers: title, meta desc, H1/H2, canonical, indexability,
        Open Graph, Twitter Cards, schema markup.
        Separate fetches for: robots.txt, XML sitemap.

        Auto-retry: if every technical signal is absent simultaneously (title +
        meta + H1 + schema all missing) the crawl is retried once after a 2-second
        pause to rule out transient scraping failure before treating them as
        genuine absences.
        """
        signals = self._fetch_and_parse_homepage()

        # ── All-fail auto-retry ───────────────────────────────────
        if (
            signals
            and self._homepage_ok
            and not self._retry_done
            and self._all_empty(signals)
        ):
            time.sleep(2)
            self._retry_done    = True
            self._homepage_html = None   # force re-fetch on retry
            self._homepage_soup = None
            self._homepage_ok   = None
            signals = self._fetch_and_parse_homepage()

        robots  = self._check_robots_txt()
        sitemap = self._check_sitemap()

        reachable = self._homepage_ok is True
        issues, strengths = self._evaluate_public_crawl(signals, robots, sitemap)

        if not reachable:
            score = 50
        else:
            score = max(25, min(85, round(len(strengths) / 10 * 100)))

        # Build legacy-format dicts expected by pdf_generator
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
            # Legacy keys for pdf_generator compatibility
            "robots_txt":    robots,
            "sitemap":       sitemap,
            "canonical":     canon_dict,
            "open_graph":    og_dict,
            "has_schema":    signals.get("has_schema", False) if signals else False,
            "schema_items":  signals.get("schema_types", []) if signals else [],
            "issues":        issues,
            "strengths":     strengths,
        }

    def _fetch_and_parse_homepage(self) -> Optional[Dict]:
        """
        Fetch homepage and extract all on-page signals.

        Uses the cached HTML from fetch_url_ex() in run() when available.
        On retry (all-fail path) the cache is cleared and the page is re-fetched.

        Returns None if the site is unreachable.
        Returns a signals dict where every key may be True/False (found/missing)
        or None (unable-to-validate).  The 'validation_states' sub-dict records
        the explicit state for each technical check.
        """
        if self._homepage_ok is False and self._homepage_html is None:
            return None

        # Use cached HTML from normalization step or re-fetch
        html   = self._homepage_html
        status = 200 if html else 0

        if html is None:
            html, status = fetch_url(self.base_url)
            if not html or status == 0:
                self._homepage_ok = False
                return None
            self._homepage_ok   = True
            self._homepage_html = html

        soup = parse_html(html)
        if not soup:
            # HTML returned but unparseable
            crawl_quality = "failed"
            return self._empty_signals(crawl_quality)

        self._homepage_soup = soup
        self._platform      = detect_platform(soup, html)

        # ── All signals via shared, robust extractors ────────────
        title       = get_title(soup)
        meta_desc   = get_meta_description(soup)
        headings    = get_headings(soup, self._platform)
        canonical   = get_canonical(soup)
        robots_meta = get_robots_meta(soup)
        og          = get_og_tags(soup)
        twitter     = get_twitter_card(soup)
        schema_types, has_faq = extract_schema(soup)
        wc          = get_word_count(soup)

        # Crawl quality: 'ok' for SSR platforms, 'partial' for JS-SPAs
        if self._platform in _JS_SPA_PLATFORMS and wc < 150:
            crawl_quality = "partial"   # likely client-side rendered
        else:
            crawl_quality = "ok"

        # ── Validation states (tri-state per signal) ─────────────
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

    @staticmethod
    def _empty_signals(crawl_quality: str = "failed") -> Dict:
        """Return a signals dict with all states marked Unable-to-validate."""
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
        """
        True when every core technical signal is absent simultaneously —
        suggests a scraping failure rather than genuinely missing content.
        """
        return (
            not signals.get("title")
            and not signals.get("meta_description")
            and not signals.get("h1s")
            and not signals.get("has_schema")
        )

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

        vs = signals.get("validation_states", {})
        platform      = signals.get("platform", "unknown")
        crawl_quality = signals.get("crawl_quality", "ok")

        # ── Platform note for JS-SPA sites ───────────────────────
        if crawl_quality == "partial":
            issues.append(
                f"🔵 Platform detected as client-side {platform.upper()} — some technical signals "
                "could not be validated from raw HTML (content may be JS-rendered)"
            )

        # ── Indexability (critical — check first) ─────────────────
        idx_state = vs.get("indexability", "found")
        if idx_state == "unable":
            pass  # can't determine — neutral
        elif not signals.get("is_indexable"):
            issues.append("🔴 Page has a noindex directive — search engines cannot index this page")
        else:
            strengths.append("✅ Page is indexable — no noindex directive detected")

        # ── Title tag ─────────────────────────────────────────────
        title_state = vs.get("title", "missing")
        title = signals.get("title", "")
        if title_state == "unable":
            issues.append("🔵 Title tag could not be validated (client-side rendering likely)")
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

        # ── Meta description ──────────────────────────────────────
        meta_state = vs.get("meta", "missing")
        meta = signals.get("meta_description", "")
        if meta_state == "unable":
            issues.append("🔵 Meta description could not be validated (client-side rendering likely)")
        elif meta:
            mlen = len(meta)
            if 100 <= mlen <= 165:
                strengths.append(f"✅ Meta description present ({mlen} chars)")
            else:
                issues.append(
                    f"🟡 Meta description length ({mlen} chars) — aim for 140–160 characters"
                )
        else:
            issues.append("🔴 No meta description — missed CTR and AI summary opportunity")

        # ── H1 / H2 headings ─────────────────────────────────────
        h1_state = vs.get("h1", "missing")
        h1s = signals.get("h1s", [])
        h2s = signals.get("h2s", [])
        if h1_state == "unable":
            issues.append("🔵 H1 heading could not be validated (client-side rendering likely)")
        elif not h1s:
            issues.append("🔴 No H1 tag — every page needs a keyword-rich H1 heading")
        elif len(h1s) == 1:
            strengths.append("✅ Single H1 tag (correct heading structure)")
        else:
            issues.append(
                f"🟡 Multiple H1 tags ({len(h1s)}) — consolidate to a single primary H1"
            )
        if h1_state != "unable":
            if len(h2s) >= 2:
                strengths.append(f"✅ {len(h2s)} H2 subheadings — well-structured content")
            elif h1_state != "unable":
                issues.append("🟡 Fewer than 2 H2 tags — use H2s to structure content around key topics")

        # ── Canonical tag ─────────────────────────────────────────
        canon_state = vs.get("canonical", "missing")
        if canon_state == "unable":
            pass  # unable-to-validate — neutral, no issue added
        elif signals.get("has_canonical"):
            strengths.append("✅ Canonical tag present — prevents duplicate content signals")
        else:
            issues.append("🟡 No canonical tag — add a canonical link element to the homepage")

        # ── Schema markup ─────────────────────────────────────────
        schema_state = vs.get("schema", "missing")
        if schema_state == "unable":
            pass  # unable-to-validate — neutral
        elif signals.get("has_schema"):
            strengths.append(
                f"✅ Structured data present ({', '.join(signals['schema_types'][:3])})"
            )

        # ── Robots.txt ────────────────────────────────────────────
        if robots.get("exists") is True:
            strengths.append("✅ robots.txt file present")
            if robots.get("sitemap_referenced"):
                strengths.append("✅ Sitemap referenced in robots.txt")
            else:
                issues.append("🟡 Sitemap not referenced in robots.txt")
        elif robots.get("exists") is False:
            issues.append("🟡 No robots.txt file found")

        # ── XML Sitemap ───────────────────────────────────────────
        if sitemap.get("found"):
            strengths.append("✅ XML sitemap found")
        elif sitemap.get("found") is False:
            issues.append("🔴 No XML sitemap found — search engines may miss pages")

        # ── Open Graph ────────────────────────────────────────────
        og_state = vs.get("og", "missing")
        if og_state == "unable":
            pass  # neutral
        elif signals.get("has_og_tags"):
            strengths.append("✅ Open Graph tags present — optimized for social sharing")
            if signals.get("has_og_image"):
                strengths.append("✅ og:image present")
            else:
                issues.append("🟡 Missing og:image — add an image for richer social sharing previews")
        else:
            issues.append("🟡 No Open Graph tags — add og:title, og:description, og:image")

        # ── Twitter Card ──────────────────────────────────────────
        tw_state = vs.get("twitter", "missing")
        if tw_state == "unable":
            pass  # neutral
        elif signals.get("has_twitter_card"):
            strengths.append("✅ Twitter Card tags present")
        else:
            issues.append(
                "🟡 No Twitter Card tags — add twitter:card and twitter:image for X/Twitter previews"
            )

        return issues, strengths

    # ─────────────────────────────────────────────────────────────
    #  PageSpeed API (optional enhancement)
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
            if r.status_code == 200:
                return r.json()
            return {}
        except Exception:
            return {}

    def _merge_psi(self, crawl: Dict, psi: Dict, crawl_ok: bool) -> Dict[str, Any]:
        """Merge PageSpeed Insights data into the public crawl baseline."""
        lh     = psi.get("lighthouseResult", {})
        cats   = lh.get("categories", {})
        audits = lh.get("audits", {})

        seo_raw  = cats.get("seo", {}).get("score")
        perf_raw = cats.get("performance", {}).get("score")
        acc_raw  = cats.get("accessibility", {}).get("score")

        lh_seo_score  = round(seo_raw  * 100) if seo_raw  is not None else crawl["score"]
        lh_perf_score = round(perf_raw * 100) if perf_raw is not None else 50
        lh_acc_score  = round(acc_raw  * 100) if acc_raw  is not None else 50

        # Individual audit results
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

        # Core Web Vitals
        fcp  = audits.get("first-contentful-paint",    {}).get("displayValue", "—")
        lcp  = audits.get("largest-contentful-paint",  {}).get("displayValue", "—")
        cls_ = audits.get("cumulative-layout-shift",   {}).get("displayValue", "—")
        tbt  = audits.get("total-blocking-time",       {}).get("displayValue", "—")

        # Schema from PSI — supplement crawl detection
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

        # Issues/strengths: PSI evaluation + crawl-only signals PSI doesn't cover
        psi_issues, psi_strengths = self._evaluate_pagespeed(
            audit_results, crawl["sitemap"], crawl["open_graph"],
            lh_seo_score, lh_perf_score,
        )
        _crawl_kws = (
            "canonical", "noindex", "indexable", "h1", "h2",
            "twitter", "heading", "subheading", "client-side",
        )
        crawl_extra_issues    = [
            i for i in crawl["issues"]
            if any(k in i.lower() for k in _crawl_kws)
        ]
        crawl_extra_strengths = [
            s for s in crawl["strengths"]
            if any(k in s.lower() for k in _crawl_kws)
        ]

        all_issues    = list(dict.fromkeys(psi_issues    + crawl_extra_issues))
        all_strengths = list(dict.fromkeys(psi_strengths + crawl_extra_strengths))

        signals = crawl.get("crawl_signals", {})

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
            "crawl_signals":       signals,
            # Legacy keys preserved for pdf_generator compatibility
            "robots_txt":          self._psi_to_robots_dict(audit_results, audits),
            "sitemap":             crawl["sitemap"],
            "canonical":           crawl["canonical"],
            "open_graph":          crawl["open_graph"],
            "issues":              all_issues,
            "strengths":           all_strengths,
            "data_sources": {
                "public_crawl": (
                    "available" if crawl_ok else "unavailable — site could not be reached"
                ),
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
            issues.append(
                f"🔴 Poor performance score: {perf_score}/100 — hurts Core Web Vitals ranking"
            )

        audit_labels = {
            "robots-txt":       ("🟡 Missing robots.txt",           "✅ robots.txt present"),
            "document-title":   ("🔴 CRITICAL: Missing page title",  "✅ Page title present"),
            "meta-description": ("🟡 Missing meta description",      "✅ Meta description present"),
            "viewport":         ("🔴 Missing mobile viewport meta",  "✅ Mobile viewport configured"),
            "canonical":        ("🟡 No canonical URL tag",          "✅ Canonical URL implemented"),
            "structured-data":  ("🟡 No structured data / schema markup", "✅ Structured data present"),
            "image-alt":        ("🟡 Images missing alt text",       "✅ Images have alt text"),
        }
        for audit_id, (fail_msg, pass_msg) in audit_labels.items():
            result = audit_results.get(audit_id, {})
            passed = result.get("passed")
            if passed is True:
                strengths.append(pass_msg)
            elif passed is False:
                issues.append(fail_msg)
            # passed is None → unable-to-validate → skip (neutral)

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
    #  Helpers — all fetches use scrape_utils.fetch_url() with retry
    # ─────────────────────────────────────────────────────────────

    def _fetch(self, url: str) -> tuple:
        """Thin wrapper around scrape_utils.fetch_url() for internal callers."""
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

    def _check_canonical(self) -> Dict:
        """Legacy helper — canonical now extracted in _fetch_and_parse_homepage."""
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
        """Legacy helper — OG tags now extracted in _fetch_and_parse_homepage."""
        soup = self._homepage_soup
        if not soup:
            html, status = fetch_url(self.base_url)
            if not html or status == 0:
                return {"present": None, "tags": []}
            soup = parse_html(html)
        return get_og_tags(soup)

    # ─────────────────────────────────────────────────────────────
    #  Legacy aliases (kept for callers that reference them)
    # ─────────────────────────────────────────────────────────────

    def _run_pagespeed(self) -> Dict[str, Any]:
        """Legacy: use run() instead."""
        return self.run()

    def _run_scraping(self) -> Dict[str, Any]:
        """Legacy: superseded by _run_public_crawl()."""
        return self._run_public_crawl()
