"""
Website Auditor
Crawls the client's site — homepage, about page, and one service/offering page
— scoring technical health, content quality, and conversion readiness from
site-level findings across all audited pages.

Design principles
─────────────────
  1. Platform detection first — adjust expectations for SSR vs SPA.
  2. URL normalization — tracking params stripped, redirects followed.
  3. Target page discovery — deliberately fetches homepage + about + service
     page rather than crawling randomly.
  4. Three-state signals — Found / Missing / Unable-to-validate.
     'Unable' is scored neutrally, never as a failure.
  5. All-fail auto-retry — if homepage signals all blank, re-fetch once.
  6. Data quality — returns a data_quality dict with reliability score 0–100.
"""
import os
import re
import time
import logging
from urllib.parse import urljoin, urlparse
from typing import Dict, Any, List, Optional, Tuple

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

from auditors.scrape_utils import (
    fetch_url, fetch_url_ex, parse_html, detect_platform,
    extract_schema,
    get_title, get_meta_description, get_canonical,
    get_og_tags, get_twitter_card, get_robots_meta,
    get_headings, get_word_count,
)

_JS_SPA_PLATFORMS = {"react", "vue", "wix"}

log = logging.getLogger(__name__)

# Lead magnet / funnel detection — scanned in _analyze_page() on the homepage
_LEAD_MAGNET_HREF_KWS = (
    "cash-report", "free-report", "free-audit", "lead-magnet",
    "checklist", "guide", "download", "report", "get-my",
    "free-guide", "ebook", "webinar", "resource", "audit",
)
_LEAD_MAGNET_TEXT_KWS = (
    "free report", "free audit", "free guide", "free checklist",
    "free download", "get your free", "get my free", "download now",
    "get the report", "cash report", "free resource", "free ebook",
)

# Keywords used to identify the About page
_ABOUT_SLUGS = frozenset({
    "about", "about-us", "about-me", "who-we-are", "our-story",
    "team", "our-team", "company", "mission",
})

# Keywords used to identify a Services / Offerings page
_SERVICE_SLUGS = frozenset({
    "services", "service", "solutions", "solution",
    "what-we-do", "offerings", "offering", "products",
    "work", "packages", "pricing",
})


def _adapt_apify_to_pages(apify_result: dict) -> List[Dict]:
    """Convert apify_content.fetch() output to the per-page dict shape that
    _analyze_page() produces, so all downstream consumers work unchanged."""
    pages_raw = apify_result.get("pages", [])
    all_links = apify_result.get("internal_links", [])
    platform  = (apify_result.get("platform_hints") or ["unknown"])[0]
    adapted: List[Dict] = []

    for i, page in enumerate(pages_raw):
        url      = page.get("url", "")
        title    = page.get("title") or ""
        meta_desc = page.get("meta_description") or ""
        headings = page.get("headings", [])
        h1s      = [h["text"] for h in headings if h["level"] == 1]
        h2s      = [h for h in headings if h["level"] == 2]
        struct   = page.get("structured_data", [])
        forms    = page.get("forms", [])
        ctas     = page.get("ctas", [])
        images   = page.get("images", [])
        text     = page.get("text") or ""

        # page_type — first page is always homepage; others inferred from slug
        if i == 0:
            page_type = "homepage"
        else:
            path_parts = {
                seg.lower().rstrip("/")
                for seg in urlparse(url).path.split("/") if seg
            }
            if path_parts & _ABOUT_SLUGS:
                page_type = "about"
            elif path_parts & _SERVICE_SLUGS:
                page_type = "service"
            else:
                page_type = "other"

        schema_types = [obj.get("@type", "") for obj in struct if obj.get("@type")]

        # lead magnet — scan apify CTAs for href/text keyword matches
        lead_magnet_url = None
        lead_magnet_cta = None
        for cta in ctas:
            href_lc = (cta.get("href") or "").lower()
            text_lc = (cta.get("text") or "").lower()
            if any(kw in href_lc for kw in _LEAD_MAGNET_HREF_KWS):
                lead_magnet_url = (cta.get("href") or "").strip()
                lead_magnet_cta = cta.get("text", "")
                break
            if not lead_magnet_url and any(kw in text_lc for kw in _LEAD_MAGNET_TEXT_KWS):
                lead_magnet_url = (cta.get("href") or "").strip()
                lead_magnet_cta = cta.get("text", "")

        int_links        = sum(1 for lk in all_links if lk.get("from_url") == url)
        has_phone        = bool(re.search(r'\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}', text))
        has_email        = bool(re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text))
        images_missing_alt = sum(1 for img in images if img.get("alt") is None)

        validation_states = {
            "title":    "found" if title    else "missing",
            "meta":     "found" if meta_desc else "missing",
            "h1":       "found" if h1s      else "missing",
            "schema":   "found" if schema_types else "missing",
            "og":       "unable",
            "twitter":  "unable",
            "viewport": "unable",
        }

        adapted.append({
            "url":                     url,
            "page_type":               page_type,
            "status_code":             200,
            "load_time":               None,
            "platform":                platform,
            "crawl_quality":           "ok",
            "validation_states":       validation_states,
            "title":                   title,
            "title_length":            len(title),
            "meta_description":        meta_desc,
            "meta_description_length": len(meta_desc),
            "h1_count":                len(h1s),
            "h1_text":                 h1s,
            "h2_count":                len(h2s),
            "canonical_url":           None,
            "is_indexable":            True,
            "has_og_tags":             False,
            "has_og_image":            False,
            "has_twitter_card":        False,
            "schema_types":            schema_types,
            "has_schema_markup":       bool(schema_types),
            "has_viewport_meta":       False,
            "image_count":             len(images),
            "images_missing_alt":      images_missing_alt,
            "internal_links":          int_links,
            "external_links":          0,
            "cta_count":               len(ctas),
            "word_count":              len(text.split()),
            "has_phone":               has_phone,
            "has_email_visible":       has_email,
            "lead_magnet_url":         lead_magnet_url,
            "lead_magnet_cta":         lead_magnet_cta,
            "has_form":                bool(forms),
            "form_count":              len(forms),
            "has_iframe":              False,
            "iframe_sources":          [],
        })

    return adapted


class WebsiteAuditor:
    def __init__(self, url: str, max_pages: int = 10):
        self.base_url    = url.rstrip("/")
        self._orig_url   = url.rstrip("/")
        self.max_pages   = max_pages
        self.domain      = urlparse(url).netloc
        self.platform    = "unknown"
        self._retry_done = False

    # ─────────────────────────────────────────────────────────────
    #  Public entry point
    # ─────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        results = {
            "url": self.base_url, "status": "ok", "pages_crawled": 0,
            "load_time_seconds": None,
            "https_enabled": self.base_url.startswith("https"),
            "homepage": {}, "pages": [], "pages_detail": [],
            "issues": [], "strengths": [], "scores": {},
            "platform": "unknown",
        }

        if not REQUESTS_OK:
            results["status"] = "skipped"
            return results

        # ── Step 0: URL normalisation ────────────────────────
        html, status, final_url = fetch_url_ex(self.base_url)
        redirects_resolved = (final_url != self.base_url)
        if final_url:
            self.base_url    = final_url
            results["url"]   = final_url
            self.domain      = urlparse(final_url).netloc
            results["https_enabled"] = final_url.startswith("https")

        # ── Step 1: Crawl target pages ───────────────────────
        if os.environ.get("USE_APIFY_CONTENT", "").strip() == "1":
            from auditors import apify_content
            apify_result  = apify_content.fetch(self.base_url)
            self.platform = (apify_result.get("platform_hints") or ["unknown"])[0]
            pages_data    = _adapt_apify_to_pages(apify_result)
            log.info(
                "[APIFY_CONTENT_ON] base_url=%s pages=%d blog_posts=%d "
                "platform_hints=%s data_source=apify_content_crawler",
                self.base_url, len(pages_data),
                len(apify_result.get("blog_posts", [])),
                apify_result.get("platform_hints", []),
            )
        else:
            try:
                pages_data = self._crawl_target_pages(results, cached_html=html,
                                                       cached_status=status)
            except Exception as e:
                results["status"] = "error"
                results["issues"].append(f"Crawl error: {str(e)}")
                self._attach_data_quality(results, redirects_resolved)
                return results

        results["pages"]         = pages_data
        results["pages_crawled"] = len(pages_data)
        results["pages_detail"]  = [
            {"url": p["url"], "type": p.get("page_type", "other"),
             "title": p.get("title", ""), "h1_count": p.get("h1_count", 0),
             "word_count": p.get("word_count", 0),
             "has_schema": p.get("has_schema_markup", False)}
            for p in pages_data
        ]
        results["homepage"] = pages_data[0] if pages_data else {}
        results["platform"] = self.platform

        # ── Step 2: All-fail auto-retry ──────────────────────
        hp = results["homepage"]
        if hp and not self._retry_done and self._all_empty(hp):
            time.sleep(2)
            self._retry_done = True
            try:
                pages_data = self._crawl_target_pages(results, cached_html=None,
                                                       cached_status=None)
                results["pages"]         = pages_data
                results["pages_crawled"] = len(pages_data)
                results["homepage"]      = pages_data[0] if pages_data else {}
            except Exception:
                pass

        # ── Step 3: Score and evaluate ───────────────────────
        results["scores"]    = self._score_site(results)
        results["issues"]    = self._detect_issues(results)
        results["strengths"] = self._detect_strengths(results)
        self._attach_data_quality(results, redirects_resolved)
        return results

    # ─────────────────────────────────────────────────────────────
    #  Target page discovery and crawling
    # ─────────────────────────────────────────────────────────────

    def _crawl_target_pages(
        self,
        results: dict,
        cached_html: Optional[str] = None,
        cached_status: Optional[int] = None,
    ) -> List[Dict]:
        """
        Crawl three specific pages: homepage, about page, service/offering page.
        Falls back to random crawl (up to max_pages) if target pages can't be found.
        """
        pages:   List[Dict] = []
        visited: set        = set()

        # ── Homepage ─────────────────────────────────────────
        hp_html   = cached_html
        hp_status = cached_status or 200

        if hp_html is None:
            start     = time.time()
            hp_html, hp_status = fetch_url(self.base_url)
            lt        = round(time.time() - start, 2) if hp_html else None
        else:
            lt = None

        if hp_html:
            soup = parse_html(hp_html)
            if soup:
                results["load_time_seconds"] = lt
                self.platform = detect_platform(soup, hp_html)
                hp_data = self._analyze_page(
                    self.base_url, soup, hp_html, lt, hp_status, "homepage"
                )
                pages.append(hp_data)
                visited.add(self.base_url)

                # ── Discover target page URLs from homepage links ──
                about_url, service_url = self._find_target_pages(soup)

                # ── About page ───────────────────────────────
                if about_url and about_url not in visited:
                    visited.add(about_url)
                    ab_html, ab_status = fetch_url(about_url)
                    if ab_html:
                        ab_soup = parse_html(ab_html)
                        if ab_soup:
                            ab_data = self._analyze_page(
                                about_url, ab_soup, ab_html, None, ab_status, "about"
                            )
                            pages.append(ab_data)

                # ── Service page ─────────────────────────────
                if service_url and service_url not in visited:
                    visited.add(service_url)
                    sv_html, sv_status = fetch_url(service_url)
                    if sv_html:
                        sv_soup = parse_html(sv_html)
                        if sv_soup:
                            sv_data = self._analyze_page(
                                service_url, sv_soup, sv_html, None, sv_status, "service"
                            )
                            pages.append(sv_data)

        # Fallback: if we only have homepage, crawl a few more links
        if len(pages) < 2 and pages:
            hp_soup = parse_html(hp_html) if hp_html else None
            if hp_soup:
                for a in hp_soup.find_all("a", href=True):
                    if len(pages) >= min(self.max_pages, 4):
                        break
                    full_url = urljoin(self.base_url, a["href"])
                    p = urlparse(full_url)
                    if p.netloc != self.domain or full_url in visited:
                        continue
                    if any(ext in p.path.lower() for ext in (".jpg", ".png", ".pdf", ".css", ".js")):
                        continue
                    visited.add(full_url)
                    ex_html, ex_status = fetch_url(full_url)
                    if ex_html:
                        ex_soup = parse_html(ex_html)
                        if ex_soup:
                            ex_data = self._analyze_page(
                                full_url, ex_soup, ex_html, None, ex_status, "other"
                            )
                            pages.append(ex_data)

        return pages

    def _find_target_pages(
        self, homepage_soup: Any
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Scan homepage links to identify the most likely About and Service pages.

        Matching strategy:
          1. Check href path segments against slug keyword sets
          2. Check visible link text against the same keyword sets

        Returns (about_url, service_url) — either may be None.
        """
        about_url   = None
        service_url = None

        for a in homepage_soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue

            full_url = urljoin(self.base_url, href)
            parsed   = urlparse(full_url)
            if parsed.netloc and parsed.netloc != self.domain:
                continue  # external link

            # Extract path segments and link text for matching
            path_parts = {
                seg.lower().rstrip("/")
                for seg in parsed.path.split("/")
                if seg
            }
            link_text = a.get_text(" ", strip=True).lower()

            if about_url is None and (
                path_parts & _ABOUT_SLUGS
                or any(k in link_text for k in ("about", "who we are", "our story", "our team"))
            ):
                about_url = full_url

            if service_url is None and (
                path_parts & _SERVICE_SLUGS
                or any(k in link_text for k in ("service", "solution", "what we do",
                                                  "offering", "package", "pricing"))
            ):
                service_url = full_url

            if about_url and service_url:
                break

        return about_url, service_url

    # ─────────────────────────────────────────────────────────────
    #  Per-page signal extraction (three-state)
    # ─────────────────────────────────────────────────────────────

    def _analyze_page(
        self, url: str, soup: Any, html: str,
        load_time: Optional[float], status: int, page_type: str,
    ) -> Dict:
        platform  = self.platform
        wc        = get_word_count(soup)
        crawl_quality = "partial" if (platform in _JS_SPA_PLATFORMS and wc < 150) else "ok"

        title     = get_title(soup)
        meta_desc = get_meta_description(soup)
        headings  = get_headings(soup, platform)
        canonical = get_canonical(soup)
        robots_m  = get_robots_meta(soup)
        og        = get_og_tags(soup)
        twitter   = get_twitter_card(soup)
        schema_types, _ = extract_schema(soup)

        viewport_tag = soup.find(
            "meta", attrs={"name": lambda x: x and x.lower() == "viewport"}
        )
        has_viewport = bool(viewport_tag)

        images             = soup.find_all("img")
        images_missing_alt = sum(1 for img in images if not img.get("alt"))

        page_text  = soup.get_text(separator=" ").lower()
        cta_kws    = ["buy", "get started", "contact", "free", "sign up",
                      "subscribe", "book", "shop now", "schedule", "call us"]
        cta_count  = sum(page_text.count(kw) for kw in cta_kws)
        has_phone  = bool(re.search(r'\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}', page_text))
        has_email  = bool(re.search(
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', page_text
        ))

        all_links    = soup.find_all("a", href=True)
        internal_cnt = sum(1 for a in all_links if self.domain in urljoin(url, a["href"]))

        # ── Lead magnet / funnel signal detection ──────────────────────────
        lead_magnet_url = None
        lead_magnet_cta = None

        for a in all_links:
            href    = a.get("href", "")
            href_lc = href.lower()
            text_lc = a.get_text(" ", strip=True).lower()
            if any(kw in href_lc for kw in _LEAD_MAGNET_HREF_KWS):
                lead_magnet_url = href.strip()
                lead_magnet_cta = a.get_text(" ", strip=True)
                break
            if not lead_magnet_url and any(kw in text_lc for kw in _LEAD_MAGNET_TEXT_KWS):
                lead_magnet_url = href.strip()
                lead_magnet_cta = a.get_text(" ", strip=True)

        if not lead_magnet_url:
            for btn in soup.find_all(["button", "input"]):
                btn_text = (btn.get_text(" ", strip=True) or btn.get("value", "")).lower()
                if any(kw in btn_text for kw in _LEAD_MAGNET_TEXT_KWS):
                    lead_magnet_cta = btn.get_text(" ", strip=True) or btn.get("value", "")
                    break

        iframes = soup.find_all("iframe")
        forms   = soup.find_all("form")

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
            "url":                     url,
            "page_type":               page_type,
            "status_code":             status,
            "load_time":               load_time,
            "platform":                platform,
            "crawl_quality":           crawl_quality,
            "validation_states":       validation_states,
            "title":                   title,
            "title_length":            len(title),
            "meta_description":        meta_desc,
            "meta_description_length": len(meta_desc),
            "h1_count":                len(headings["h1s"]),
            "h1_text":                 headings["h1s"],
            "h2_count":                len(headings["h2s"]),
            "canonical_url":           canonical,
            "is_indexable":            robots_m["is_indexable"],
            "has_og_tags":             og.get("present") or False,
            "has_og_image":            og.get("has_og_image", False),
            "has_twitter_card":        twitter.get("present") or False,
            "schema_types":            schema_types,
            "has_schema_markup":       len(schema_types) > 0,
            "has_viewport_meta":       has_viewport,
            "image_count":             len(images),
            "images_missing_alt":      images_missing_alt,
            "internal_links":          internal_cnt,
            "external_links":          len(all_links) - internal_cnt,
            "cta_count":               cta_count,
            "word_count":              wc,
            "has_phone":               has_phone,
            "has_email_visible":       has_email,
            "lead_magnet_url":         lead_magnet_url,
            "lead_magnet_cta":         lead_magnet_cta,
            "has_form":                len(forms) > 0,
            "form_count":              len(forms),
            "has_iframe":              len(iframes) > 0,
            "iframe_sources":          [f.get("src", "")[:80] for f in iframes if f.get("src")][:3],
        }

    # ─────────────────────────────────────────────────────────────
    #  Scoring (site-level — aggregates across all pages)
    # ─────────────────────────────────────────────────────────────

    def _score_site(self, results: dict) -> Dict[str, int]:
        """
        Score from site-level findings across all crawled pages.
        Homepage is the primary reference (weight ~0.6).
        About and service pages contribute secondary signals (+bonuses).
        """
        hp  = results.get("homepage", {})
        vs  = hp.get("validation_states", {})
        all_pages = results.get("pages", [])

        # ── Technical (homepage-primary) ─────────────────────
        tech = 100
        if not results["https_enabled"]:                              tech -= 30
        if vs.get("viewport") == "missing":                           tech -= 20
        lt = results.get("load_time_seconds")
        if lt and lt > 3:                                             tech -= 20
        if hp.get("images_missing_alt", 0) > 3:                      tech -= 10
        # Bonus: multiple pages crawled with valid structure
        valid_pages = sum(1 for p in all_pages if p.get("h1_count", 0) >= 1)
        if valid_pages >= 2:                                          tech += 5

        # ── SEO (site-level: any page with good signals counts) ─
        seo = 100
        # Title: check across all pages
        pages_with_title = sum(1 for p in all_pages
                               if p.get("validation_states", {}).get("title") == "found")
        if pages_with_title == 0:
            if vs.get("title") == "missing":                          seo -= 25
        elif vs.get("title") == "found":
            tl = hp.get("title_length", 0)
            if tl > 65 or tl < 30:                                   seo -= 10

        if vs.get("meta") == "missing":                               seo -= 20
        if vs.get("h1") not in ("unable",) and hp.get("h1_count", 0) != 1:
                                                                      seo -= 15
        if vs.get("schema") == "missing":                             seo -= 10
        # Bonus: schema found on multiple pages
        schema_pages = sum(1 for p in all_pages if p.get("has_schema_markup"))
        if schema_pages >= 2:                                         seo += 5

        # ── Content (site-level) ──────────────────────────────
        content = 100
        avg_wc = (sum(p.get("word_count", 0) for p in all_pages) / len(all_pages)
                  if all_pages else 0)
        if avg_wc < 300:                                              content -= 30
        if hp.get("cta_count", 0) < 2 and hp.get("crawl_quality") != "partial": content -= 20
        # Bonus: about + service pages both found
        page_types = {p.get("page_type") for p in all_pages}
        if "about" in page_types and "service" in page_types:        content += 10

        # ── Conversion ────────────────────────────────────────
        conv = 100
        if hp.get("cta_count", 0) == 0:                              conv -= 40
        if not hp.get("has_phone") and not hp.get("has_email_visible"):
                                                                      conv -= 30

        scores = {
            "technical":  max(25, min(100, tech)),
            "seo":        max(25, min(100, seo)),
            "content":    max(25, min(100, content)),
            "conversion": max(25, min(100, conv)),
        }
        scores["overall"] = round(sum(scores.values()) / len(scores))
        return scores

    # ─────────────────────────────────────────────────────────────
    #  Issues / Strengths (three-state aware, site-level)
    # ─────────────────────────────────────────────────────────────

    def _detect_issues(self, results: dict) -> List[str]:
        issues   = []
        hp       = results.get("homepage", {})
        vs       = hp.get("validation_states", {})
        crawled  = results["pages_crawled"] > 0
        all_pages = results.get("pages", [])

        if not results["https_enabled"]:
            issues.append("🔴 CRITICAL: Site is not using HTTPS")

        if not crawled:
            issues.append("🟡 Site could not be crawled — full technical audit requires manual review")
            return issues

        if hp.get("crawl_quality") == "partial":
            issues.append(
                f"🔵 Platform detected as {self.platform.upper()} — some signals could not be "
                "validated from raw HTML"
            )

        if vs.get("viewport") == "missing":
            issues.append("🔴 CRITICAL: Missing mobile viewport meta tag")

        lt = results.get("load_time_seconds")
        if lt and lt > 3:
            issues.append(f"🟡 Slow page load: {lt}s")

        if vs.get("title") == "unable":
            issues.append("🔵 Page title could not be validated (client-side rendering)")
        elif vs.get("title") == "missing":
            issues.append("🔴 CRITICAL: Homepage missing title tag")

        if vs.get("meta") == "unable":
            issues.append("🔵 Meta description could not be validated")
        elif vs.get("meta") == "missing":
            issues.append("🟡 Missing meta description on homepage")

        if vs.get("h1") == "unable":
            issues.append("🔵 H1 heading could not be validated")
        elif hp.get("h1_count", 0) == 0:
            issues.append("🟡 No H1 heading found on homepage")

        if hp.get("images_missing_alt", 0) > 0:
            issues.append(f"🟡 {hp['images_missing_alt']} images missing alt text on homepage")

        if hp.get("word_count", 0) < 300:
            issues.append(f"🟡 Homepage limited content ({hp.get('word_count',0)} words)")

        if hp.get("cta_count", 0) < 2:
            issues.append("🟡 Few calls-to-action on homepage")

        if vs.get("schema") == "unable":
            issues.append("🔵 Structured data could not be validated")
        elif vs.get("schema") == "missing":
            issues.append("🟡 No structured data found on homepage")

        # Site-level: pages missing titles or H1s
        pages_no_title = [p["url"] for p in all_pages[1:]
                          if p.get("validation_states", {}).get("title") == "missing"]
        if pages_no_title:
            issues.append(f"🟡 {len(pages_no_title)} inner page(s) missing title tags")

        page_types = {p.get("page_type") for p in all_pages}
        if "about" not in page_types:
            issues.append("🟡 About page not found — add /about to build trust and E-E-A-T")
        if "service" not in page_types:
            issues.append("🟡 Services/solutions page not detected — dedicate a page to each offering")

        return issues

    def _detect_strengths(self, results: dict) -> List[str]:
        strengths = []
        hp        = results.get("homepage", {})
        vs        = hp.get("validation_states", {})
        crawled   = results["pages_crawled"] > 0
        all_pages = results.get("pages", [])

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

        # Site-level strengths
        page_types = {p.get("page_type") for p in all_pages}
        if "about" in page_types:
            strengths.append("✅ About page found — supports trust and E-E-A-T signals")
        if "service" in page_types:
            strengths.append("✅ Services/solutions page found — structured offering presentation")
        if len(all_pages) >= 3:
            all_valid = all(p.get("h1_count", 0) >= 1 for p in all_pages)
            if all_valid:
                strengths.append(f"✅ H1 headings present across all {len(all_pages)} audited pages")

        return strengths

    # ─────────────────────────────────────────────────────────────
    #  Data quality metadata
    # ─────────────────────────────────────────────────────────────

    def _attach_data_quality(self, results: dict, redirects_resolved: bool) -> None:
        hp       = results.get("homepage", {})
        vs       = hp.get("validation_states", {})
        schema_t = hp.get("schema_types", [])

        if not schema_t:
            sq = "no"
        elif len(schema_t) >= 2:
            sq = "yes"
        else:
            sq = "partial"

        all_found = all(
            v == "found"
            for k, v in vs.items()
            if k in ("title", "meta", "h1", "schema", "viewport")
        )

        dq = {
            "url_normalized":     True,
            "original_url":       self._orig_url,
            "final_url":          self.base_url,
            "redirects_resolved": redirects_resolved,
            "platform_detected":  self.platform,
            "schema_quality":     sq,
            "render_used":        False,   # website auditor doesn't use Playwright
            "render_status":      "not used",
            "raw_crawl_ok":       results["pages_crawled"] > 0,
            "all_signals_found":  all_found,
            "pages_audited":      results["pages_crawled"],
            "pages_detail":       results.get("pages_detail", []),
        }

        # Reliability score (same formula as SEO auditor)
        s = 0
        if dq["raw_crawl_ok"]:                                   s += 30
        if dq["platform_detected"] != "unknown":                 s += 15
        if sq == "yes":                                          s += 20
        elif sq == "partial":                                    s += 10
        if dq["all_signals_found"]:                              s += 10
        if dq["url_normalized"]:                                 s +=  5
        if dq["redirects_resolved"]:                             s +=  5
        if dq.get("pages_audited", 0) >= 3:                      s += 15  # render-equivalent bonus
        dq["reliability_score"] = min(s, 100)

        results["data_quality"] = dq

    # ─────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _all_empty(hp: dict) -> bool:
        return (
            not hp.get("title")
            and not hp.get("meta_description")
            and hp.get("h1_count", 0) == 0
            and not hp.get("has_schema_markup")
        )
