"""
Website Auditor
Crawls the client's homepage (and up to max_pages internal pages) to score
technical health, content quality, and conversion readiness.

Design principles — mirrors SEO auditor conventions
────────────────────────────────────────────────────
  1. Platform detection first — Wix / WordPress / Shopify / Squarespace /
     Webflow are all SSR and scrape normally.  React / Vue SPAs render
     client-side; missing signals on those platforms are Unable-to-validate.

  2. URL normalization — tracking parameters are stripped and HTTP redirects
     are followed once before any checks run, resolving www vs non-www and
     http vs https consistently.

  3. Three-state signals — each technical check returns:
       Found              (value present and verified)
       Missing            (fetch succeeded but value absent)
       Unable-to-validate (fetch failed OR JS-SPA where content is client-side)
     "Unable-to-validate" signals score neutrally (50); they never penalise.

  4. All-fail auto-retry — if title + meta + H1 + schema are ALL absent after
     the first crawl, the homepage is re-fetched once after a 2-second pause
     before scores are finalised.
"""
import re
import time
from urllib.parse import urljoin, urlparse
from typing import Dict, Any, List, Optional

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

# JS-SPA platforms — absent technical signals = Unable-to-validate
_JS_SPA_PLATFORMS = {"react", "vue"}


class WebsiteAuditor:
    def __init__(self, url: str, max_pages: int = 10):
        self.base_url  = url.rstrip("/")
        self.max_pages = max_pages
        self.domain    = urlparse(url).netloc
        self.platform  = "unknown"
        self._retry_done = False

    # ─────────────────────────────────────────────────────────────
    #  Public entry point
    # ─────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        results = {
            "url": self.base_url, "status": "ok", "pages_crawled": 0,
            "load_time_seconds": None,
            "https_enabled": self.base_url.startswith("https"),
            "homepage": {}, "pages": [], "issues": [], "strengths": [],
            "scores": {}, "platform": "unknown",
        }

        if not REQUESTS_OK:
            results["status"] = "skipped"
            return results

        # ── Step 0: URL normalisation (fetch_url_ex follows redirects) ──
        html, status, final_url = fetch_url_ex(self.base_url)
        if final_url and final_url != self.base_url:
            self.base_url    = final_url
            results["url"]   = final_url
            self.domain      = urlparse(final_url).netloc
            results["https_enabled"] = final_url.startswith("https")

        # ── Step 1: Crawl site ───────────────────────────────────
        try:
            pages_data = self._crawl_site(results, cached_html=html, cached_status=status)
            results["pages"]         = pages_data
            results["pages_crawled"] = len(pages_data)
            results["homepage"]      = pages_data[0] if pages_data else {}
        except Exception as e:
            results["status"] = "error"
            results["issues"].append(f"Crawl error: {str(e)}")
            return results

        results["platform"] = self.platform

        # ── Step 2: All-fail auto-retry ──────────────────────────
        hp = results["homepage"]
        if (
            hp
            and not self._retry_done
            and self._all_empty(hp)
        ):
            time.sleep(2)
            self._retry_done = True
            try:
                pages_data = self._crawl_site(results, cached_html=None,
                                               cached_status=None)
                results["pages"]         = pages_data
                results["pages_crawled"] = len(pages_data)
                results["homepage"]      = pages_data[0] if pages_data else {}
            except Exception:
                pass  # keep original result on retry failure

        # ── Step 3: Score and evaluate ───────────────────────────
        results["scores"]    = self._score_site(results)
        results["issues"]    = self._detect_issues(results)
        results["strengths"] = self._detect_strengths(results)
        return results

    # ─────────────────────────────────────────────────────────────
    #  Crawling
    # ─────────────────────────────────────────────────────────────

    def _crawl_site(
        self,
        results: dict,
        cached_html: Optional[str] = None,
        cached_status: Optional[int] = None,
    ) -> List[Dict]:
        """
        Crawl starting from base_url.  The first page uses the cached HTML
        from the normalisation fetch to avoid a duplicate request.
        """
        visited = set()
        to_visit = [self.base_url]
        pages = []

        while to_visit and len(pages) < self.max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            # ── Fetch ─────────────────────────────────────────
            if len(pages) == 0 and cached_html is not None:
                html       = cached_html
                status     = cached_status or 200
                load_time  = None
            else:
                start = time.time()
                html, status = fetch_url(url)
                load_time = round(time.time() - start, 2) if html else None

            if html is None:
                continue

            soup = parse_html(html)
            if soup is None:
                continue

            if len(pages) == 0:
                results["load_time_seconds"] = load_time
                self.platform = detect_platform(soup, html)

            page_data = self._analyze_page(url, soup, html, load_time, status)
            pages.append(page_data)

            # Collect internal links for multi-page crawl
            for a in soup.find_all("a", href=True):
                full_url = urljoin(url, a["href"])
                parsed   = urlparse(full_url)
                if parsed.netloc == self.domain and full_url not in visited:
                    to_visit.append(full_url)

        return pages

    def _analyze_page(
        self, url: str, soup: Any, html: str,
        load_time: Optional[float], status: int,
    ) -> Dict:
        """
        Extract and classify page signals with three-state validation.

        validation_states keys: title, meta, h1, schema, og, twitter, viewport
        Values: 'found' | 'missing' | 'unable'
        """
        platform  = self.platform
        wc        = get_word_count(soup)

        # Determine crawl quality for this page
        if platform in _JS_SPA_PLATFORMS and wc < 150:
            crawl_quality = "partial"
        else:
            crawl_quality = "ok"

        # ── Shared-utils extractors ───────────────────────────────
        title     = get_title(soup)
        meta_desc = get_meta_description(soup)
        headings  = get_headings(soup, platform)
        canonical = get_canonical(soup)
        robots_m  = get_robots_meta(soup)
        og        = get_og_tags(soup)
        twitter   = get_twitter_card(soup)
        schema_types, _ = extract_schema(soup)

        # ── Viewport meta (scrape_utils doesn't have a helper — keep inline) ─
        viewport_tag = soup.find(
            "meta", attrs={"name": lambda x: x and x.lower() == "viewport"}
        )
        has_viewport = bool(viewport_tag)

        # ── Image alt analysis ─────────────────────────────────────
        images = soup.find_all("img")
        images_missing_alt = sum(1 for img in images if not img.get("alt"))

        # ── CTA / contact signals ──────────────────────────────────
        page_text  = soup.get_text(separator=" ").lower()
        cta_kws    = ["buy", "get started", "contact", "free", "sign up",
                      "subscribe", "book", "shop now", "schedule", "call us"]
        cta_count  = sum(page_text.count(kw) for kw in cta_kws)
        has_phone  = bool(re.search(r'\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}', page_text))
        has_email  = bool(re.search(
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', page_text
        ))

        # ── Internal / external links ─────────────────────────────
        all_links     = soup.find_all("a", href=True)
        internal_cnt  = sum(
            1 for a in all_links if self.domain in urljoin(url, a["href"])
        )

        # ── Tri-state helper ──────────────────────────────────────
        def vs(value: Any) -> str:
            if crawl_quality == "failed":
                return "unable"
            if not value and platform in _JS_SPA_PLATFORMS and wc < 150:
                return "unable"
            return "found" if value else "missing"

        validation_states = {
            "title":    vs(title),
            "meta":     vs(meta_desc),
            "h1":       vs(headings["h1s"]),
            "schema":   vs(schema_types),
            "og":       vs(og.get("present")),
            "twitter":  vs(twitter.get("present")),
            "viewport": vs(has_viewport),
        }

        return {
            "url":                  url,
            "status_code":          status,
            "load_time":            load_time,
            "platform":             platform,
            "crawl_quality":        crawl_quality,
            "validation_states":    validation_states,
            # Content signals
            "title":                title,
            "title_length":         len(title),
            "meta_description":     meta_desc,
            "meta_description_length": len(meta_desc),
            "h1_count":             len(headings["h1s"]),
            "h1_text":              headings["h1s"],
            "h2_count":             len(headings["h2s"]),
            "canonical_url":        canonical,
            "is_indexable":         robots_m["is_indexable"],
            "has_og_tags":          og.get("present") or False,
            "has_og_image":         og.get("has_og_image", False),
            "has_twitter_card":     twitter.get("present") or False,
            "schema_types":         schema_types,
            "has_schema_markup":    len(schema_types) > 0,
            "has_viewport_meta":    has_viewport,
            "image_count":          len(images),
            "images_missing_alt":   images_missing_alt,
            "internal_links":       internal_cnt,
            "external_links":       len(all_links) - internal_cnt,
            "cta_count":            cta_count,
            "word_count":           wc,
            "has_phone":            has_phone,
            "has_email_visible":    has_email,
        }

    # ─────────────────────────────────────────────────────────────
    #  Scoring  (three-state: None = unable → neutral, not penalised)
    # ─────────────────────────────────────────────────────────────

    def _score_site(self, results: dict) -> Dict[str, int]:
        hp  = results.get("homepage", {})
        vs  = hp.get("validation_states", {})

        # Technical score
        tech = 100
        if not results["https_enabled"]:
            tech -= 30
        # Only penalise viewport if we could actually verify it
        if vs.get("viewport") == "missing":
            tech -= 20
        if results.get("load_time_seconds") and results["load_time_seconds"] > 3:
            tech -= 20
        if hp.get("images_missing_alt", 0) > 3:
            tech -= 10

        # SEO score — skip penalty for 'unable' states
        seo = 100
        if vs.get("title") == "missing":
            seo -= 25
        elif vs.get("title") == "found":
            tl = hp.get("title_length", 0)
            if tl > 65 or tl < 30:
                seo -= 10
        if vs.get("meta") == "missing":
            seo -= 20
        if vs.get("h1") != "unable":
            if hp.get("h1_count", 0) != 1:
                seo -= 15
        if vs.get("schema") == "missing":
            seo -= 10

        # Content score
        content = 100
        if hp.get("word_count", 0) < 300:
            content -= 30
        if hp.get("cta_count", 0) < 2:
            content -= 20

        # Conversion score
        conv = 100
        if hp.get("cta_count", 0) == 0:
            conv -= 40
        if not hp.get("has_phone") and not hp.get("has_email_visible"):
            conv -= 30

        scores = {
            "technical":  max(25, tech),
            "seo":        max(25, seo),
            "content":    max(25, content),
            "conversion": max(25, conv),
        }
        scores["overall"] = round(sum(scores.values()) / len(scores))
        return scores

    # ─────────────────────────────────────────────────────────────
    #  Issues / Strengths  (three-state aware)
    # ─────────────────────────────────────────────────────────────

    def _detect_issues(self, results: dict) -> List[str]:
        issues   = []
        hp       = results.get("homepage", {})
        vs       = hp.get("validation_states", {})
        crawled  = results["pages_crawled"] > 0
        platform = self.platform

        if not results["https_enabled"]:
            issues.append("🔴 CRITICAL: Site is not using HTTPS")

        if not crawled:
            issues.append(
                "🟡 Site could not be crawled — full technical audit requires manual review"
            )
            return issues

        cq = hp.get("crawl_quality", "ok")
        if cq == "partial":
            issues.append(
                f"🔵 Platform detected as {platform.upper()} — some signals could not be "
                "validated from raw HTML (content may be JS-rendered)"
            )

        # Viewport
        if vs.get("viewport") == "missing":
            issues.append("🔴 CRITICAL: Missing mobile viewport meta tag")

        # Load time
        if results.get("load_time_seconds") and results["load_time_seconds"] > 3:
            issues.append(f"🟡 Slow page load: {results['load_time_seconds']}s")

        # Title
        if vs.get("title") == "unable":
            issues.append("🔵 Page title could not be validated (client-side rendering likely)")
        elif vs.get("title") == "missing":
            issues.append("🔴 CRITICAL: Homepage missing title tag")

        # Meta description
        if vs.get("meta") == "unable":
            issues.append("🔵 Meta description could not be validated")
        elif vs.get("meta") == "missing":
            issues.append("🟡 Missing meta description on homepage")

        # H1
        if vs.get("h1") == "unable":
            issues.append("🔵 H1 heading could not be validated (client-side rendering likely)")
        elif hp.get("h1_count", 0) == 0:
            issues.append("🟡 No H1 heading found on homepage")

        # Images
        if hp.get("images_missing_alt", 0) > 0:
            issues.append(f"🟡 {hp['images_missing_alt']} images missing alt text")

        # Content
        if hp.get("word_count", 0) < 300:
            issues.append(
                f"🟡 Homepage has limited content ({hp.get('word_count', 0)} words)"
            )

        # CTAs
        if hp.get("cta_count", 0) < 2:
            issues.append("🟡 Few calls-to-action on homepage")

        # Schema
        if vs.get("schema") == "unable":
            issues.append("🔵 Structured data could not be validated")
        elif vs.get("schema") == "missing":
            issues.append("🟡 No structured data found")

        return issues

    def _detect_strengths(self, results: dict) -> List[str]:
        strengths = []
        hp        = results.get("homepage", {})
        vs        = hp.get("validation_states", {})
        crawled   = results["pages_crawled"] > 0

        if results["https_enabled"]:
            strengths.append("✅ Site uses HTTPS")

        if not crawled:
            return strengths

        if vs.get("viewport") == "found":
            strengths.append("✅ Mobile viewport configured")

        lt = results.get("load_time_seconds")
        if lt and lt < 2:
            strengths.append(f"✅ Fast page load time ({lt}s)")

        if vs.get("meta") == "found":
            strengths.append("✅ Meta description present")

        if vs.get("h1") == "found" and hp.get("h1_count") == 1:
            strengths.append("✅ Proper single H1 heading")

        if vs.get("schema") == "found":
            types = hp.get("schema_types", [])
            label = f" ({', '.join(types[:2])})" if types else ""
            strengths.append(f"✅ Structured data implemented{label}")

        if hp.get("cta_count", 0) >= 3:
            strengths.append("✅ Strong calls-to-action presence")

        if vs.get("og") == "found":
            strengths.append("✅ Open Graph tags present")

        return strengths

    # ─────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _all_empty(hp: dict) -> bool:
        """
        True when every core technical signal is absent simultaneously —
        suggests a transient scraping failure rather than genuinely missing content.
        """
        return (
            not hp.get("title")
            and not hp.get("meta_description")
            and hp.get("h1_count", 0) == 0
            and not hp.get("has_schema_markup")
        )
