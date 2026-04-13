"""
SEO Auditor
Primary: Google PageSpeed Insights API (returns Lighthouse SEO score + individual audits).
Fallback: HTML scraping when no API key is supplied.
Unknown / unverifiable checks default to 50 (neutral) rather than 0.
"""
from urllib.parse import urljoin, urlparse
from typing import Dict, Any, List

try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

# Maps PageSpeed audit IDs → human-readable labels used in our report
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


def _grade(score: int) -> str:
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    if score >= 35: return "D"
    return "F"


class SEOAuditor:
    def __init__(self, url: str, api_key: str = ""):
        self.base_url = url.rstrip("/")
        self.domain   = urlparse(url).netloc
        self.api_key  = api_key
        self.headers  = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

    # ── Public entry point ─────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        # Always try PageSpeed Insights first — the endpoint is free with no key
        # (rate-limited to ~25 req/100s without a key; add PAGESPEED_API_KEY for higher limits).
        return self._run_pagespeed()

    # ── PageSpeed Insights path ────────────────────────────────

    def _run_pagespeed(self) -> Dict[str, Any]:
        """Call PageSpeed API for both mobile and desktop, extract SEO data."""
        psi_mobile  = self._fetch_pagespeed("mobile")
        psi_desktop = self._fetch_pagespeed("desktop")

        if not psi_mobile and not psi_desktop:
            print("   ⚠️  PageSpeed API unavailable — falling back to scraping")
            return self._run_scraping()

        psi = psi_mobile or psi_desktop

        # ── Lighthouse SEO score ───────────────────────────────
        lh      = psi.get("lighthouseResult", {})
        cats    = lh.get("categories", {})
        audits  = lh.get("audits", {})
        seo_raw = cats.get("seo", {}).get("score")  # 0-1 float or None
        perf_raw = cats.get("performance", {}).get("score")
        acc_raw  = cats.get("accessibility", {}).get("score")

        lh_seo_score  = round(seo_raw  * 100) if seo_raw  is not None else 50
        lh_perf_score = round(perf_raw * 100) if perf_raw is not None else 50
        lh_acc_score  = round(acc_raw  * 100) if acc_raw  is not None else 50

        # ── Individual audit results ───────────────────────────
        audit_results = {}
        for audit_id, label in PSI_AUDIT_MAP.items():
            a = audits.get(audit_id, {})
            audit_score = a.get("score")  # 1=pass, 0=fail, None=not applicable
            if audit_score is None:
                audit_results[audit_id] = {"label": label, "passed": None, "display": "—"}
            else:
                passed = audit_score >= 0.9
                audit_results[audit_id] = {
                    "label":   label,
                    "passed":  passed,
                    "display": a.get("displayValue", "Pass" if passed else "Fail"),
                }

        # ── Sitemap (PageSpeed doesn't check it directly) ──────
        sitemap = self._check_sitemap()

        # ── Open Graph (PageSpeed doesn't check OG tags) ───────
        og = self._check_open_graph()

        # ── CWV (Core Web Vitals from performance) ─────────────
        fcp  = audits.get("first-contentful-paint", {}).get("displayValue", "—")
        lcp  = audits.get("largest-contentful-paint", {}).get("displayValue", "—")
        cls_ = audits.get("cumulative-layout-shift", {}).get("displayValue", "—")
        tbt  = audits.get("total-blocking-time", {}).get("displayValue", "—")

        # ── Issues / Strengths ─────────────────────────────────
        issues, strengths = self._evaluate_pagespeed(audit_results, sitemap, og,
                                                     lh_seo_score, lh_perf_score)

        # ── Structured data details (passed to GEO auditor) ────
        sd_audit   = audits.get("structured-data", {})
        schema_items = []
        for item in sd_audit.get("details", {}).get("items", []):
            if isinstance(item, dict):
                schema_items.append(item.get("description", str(item)))

        return {
            "method":           "pagespeed",
            "score":            lh_seo_score,
            "grade":            _grade(lh_seo_score),
            "performance_score": lh_perf_score,
            "accessibility_score": lh_acc_score,
            "audit_results":    audit_results,
            "sitemap":          sitemap,
            "open_graph":       og,
            "core_web_vitals":  {"fcp": fcp, "lcp": lcp, "cls": cls_, "tbt": tbt},
            "schema_items":     schema_items,
            "has_schema":       audit_results.get("structured-data", {}).get("passed", False) or False,
            "issues":           issues,
            "strengths":        strengths,
            # Keep legacy keys for docx_generator compatibility
            "robots_txt":       self._psi_to_robots_dict(audit_results, audits),
            "canonical":        {"present": audit_results.get("canonical", {}).get("passed")},
        }

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
            print(f"   ⚠️  PageSpeed API {strategy}: HTTP {r.status_code}")
            return {}
        except Exception as e:
            print(f"   ⚠️  PageSpeed API {strategy}: {e}")
            return {}

    def _psi_to_robots_dict(self, audit_results: dict, audits: dict) -> dict:
        robots_passed = audit_results.get("robots-txt", {}).get("passed")
        sitemap_ref   = False
        if robots_passed:
            # Try to detect sitemap reference in robots.txt content
            robots_details = audits.get("robots-txt", {}).get("details", {})
            items = robots_details.get("items", [])
            for item in items:
                if isinstance(item, dict):
                    val = str(item).lower()
                    if "sitemap" in val:
                        sitemap_ref = True
        return {"exists": robots_passed, "sitemap_referenced": sitemap_ref}

    def _evaluate_pagespeed(self, audit_results: dict, sitemap: dict,
                             og: dict, seo_score: int, perf_score: int) -> tuple:
        issues, strengths = [], []

        # SEO score banner
        if seo_score >= 80:
            strengths.append(f"✅ Lighthouse SEO score: {seo_score}/100")
        elif seo_score >= 50:
            issues.append(f"🟡 Lighthouse SEO score: {seo_score}/100 — room to improve")
        else:
            issues.append(f"🔴 Low Lighthouse SEO score: {seo_score}/100")

        # Performance
        if perf_score >= 65:
            strengths.append(f"✅ Performance score: {perf_score}/100")
        elif perf_score >= 35:
            issues.append(f"🟡 Performance score: {perf_score}/100")
        else:
            issues.append(f"🔴 Poor performance score: {perf_score}/100 — hurts Core Web Vitals ranking")

        # Individual audits
        audit_issue_labels = {
            "robots-txt":       ("🟡 Missing robots.txt",          "✅ robots.txt present"),
            "document-title":   ("🔴 CRITICAL: Missing page title", "✅ Page title present"),
            "meta-description": ("🟡 Missing meta description",     "✅ Meta description present"),
            "viewport":         ("🔴 Missing mobile viewport meta", "✅ Mobile viewport configured"),
            "canonical":        ("🟡 No canonical URL tag",         "✅ Canonical URL implemented"),
            "structured-data":  ("🟡 No structured data / schema markup", "✅ Structured data present"),
            "image-alt":        ("🟡 Images missing alt text",      "✅ Images have alt text"),
        }
        for audit_id, (fail_msg, pass_msg) in audit_issue_labels.items():
            result = audit_results.get(audit_id, {})
            passed = result.get("passed")
            if passed is True:
                strengths.append(pass_msg)
            elif passed is False:
                issues.append(fail_msg)
            # None = not applicable / unknown → skip (counts as 50 neutral)

        # Sitemap
        if sitemap.get("found"):
            strengths.append("✅ XML sitemap found")
        elif sitemap.get("found") is False:
            issues.append("🔴 No XML sitemap found — search engines may miss pages")

        # Open Graph
        if og.get("present"):
            strengths.append("✅ Open Graph tags present")
            if og.get("has_og_image"):
                strengths.append("✅ OG image tag present")
            else:
                issues.append("🟡 Missing og:image tag")
        elif og.get("present") is False:
            issues.append("🟡 No Open Graph meta tags")

        return issues, strengths

    # ── Scraping fallback path ─────────────────────────────────

    def _run_scraping(self) -> Dict[str, Any]:
        robots   = self._check_robots_txt()
        sitemap  = self._check_sitemap()
        canon    = self._check_canonical()
        og       = self._check_open_graph()
        issues, strengths = self._evaluate_scraping(robots, sitemap, canon, og)

        # Score = strengths / 6; neutral 50 when nothing could be reached
        any_reachable = any([
            robots.get("exists") is not None,
            sitemap.get("found") is not None,
            canon.get("present") is not None,
            og.get("present") is not None,
        ])
        if not any_reachable:
            score = 50
        else:
            # Floor at 25: site reachable but all SEO checks failed = confirmed missing (D range)
            score = max(25, round(len(strengths) / 6 * 100))

        return {
            "method":    "scraping",
            "score":     score,
            "grade":     _grade(score),
            "robots_txt": robots,
            "sitemap":    sitemap,
            "canonical":  canon,
            "open_graph": og,
            "has_schema": False,
            "schema_items": [],
            "issues":     issues,
            "strengths":  strengths,
        }

    def _fetch(self, url: str) -> tuple:
        if not REQUESTS_OK:
            return None, 0
        try:
            r = requests.get(url, headers=self.headers, timeout=12, allow_redirects=True)
            return r.text, r.status_code
        except Exception:
            return None, 0

    def _check_robots_txt(self) -> Dict:
        url = f"{self.base_url}/robots.txt"
        text, status = self._fetch(url)
        if status == 0 or text is None:
            return {"url": url, "exists": None, "status_code": status, "sitemap_referenced": False}
        exists = status == 200 and "user-agent" in text.lower()
        return {"url": url, "exists": exists, "status_code": status,
                "sitemap_referenced": "sitemap:" in text.lower() if text else False}

    def _check_sitemap(self) -> Dict:
        for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap/", "/sitemap1.xml"]:
            url = f"{self.base_url}{path}"
            text, status = self._fetch(url)
            if status == 0 or text is None:
                continue
            if status == 200 and text:
                url_count = text.count("<url>") + text.count("<loc>")
                return {"found": True, "url": url, "url_count_estimate": min(url_count, 9999)}
        # Distinguish "checked and not found" from "couldn't connect"
        return {"found": False, "url": None, "url_count_estimate": 0}

    def _check_canonical(self) -> Dict:
        text, status = self._fetch(self.base_url)
        if not text or status == 0:
            return {"present": None, "value": None}
        soup = BeautifulSoup(text, "html.parser") if REQUESTS_OK else None
        if soup:
            tag = soup.find("link", rel="canonical")
            if tag:
                return {"present": True, "value": tag.get("href", "")}
        return {"present": False, "value": None}

    def _check_open_graph(self) -> Dict:
        text, status = self._fetch(self.base_url)
        if not text or status == 0:
            return {"present": None, "tags": []}
        soup = BeautifulSoup(text, "html.parser") if REQUESTS_OK else None
        if not soup:
            return {"present": None, "tags": []}
        og_tags  = soup.find_all("meta", attrs={"property": lambda x: x and x.startswith("og:")})
        tag_list = [t.get("property") or t.get("name") for t in og_tags]
        return {
            "present":      len(og_tags) > 0,
            "tags":         tag_list,
            "has_og_image": any("og:image" in t for t in tag_list),
            "has_og_title": any("og:title" in t for t in tag_list),
        }

    def _evaluate_scraping(self, robots, sitemap, canon, og) -> tuple:
        issues, strengths = [], []

        any_reachable = any([
            robots.get("exists") is not None,
            sitemap.get("found") is not None,
            canon.get("present") is not None,
            og.get("present") is not None,
        ])
        if not any_reachable:
            issues.append("🟡 Site could not be reached for SEO checks — verify URL and try again")
            return issues, strengths

        if robots.get("exists") is None:
            pass  # unknown — neutral
        elif robots["exists"]:
            strengths.append("✅ robots.txt file present")
            if robots.get("sitemap_referenced"):
                strengths.append("✅ Sitemap referenced in robots.txt")
            else:
                issues.append("🟡 Sitemap not referenced in robots.txt")
        else:
            issues.append("🟡 Missing robots.txt file")

        if sitemap.get("found"):
            strengths.append("✅ XML sitemap found")
        elif sitemap.get("found") is False:
            issues.append("🔴 No XML sitemap found")

        if canon.get("present") is None:
            pass
        elif canon["present"]:
            strengths.append("✅ Canonical URL tag implemented")
        else:
            issues.append("🟡 No canonical tag on homepage")

        if og.get("present") is None:
            pass
        elif og["present"]:
            strengths.append("✅ Open Graph tags present")
            if og.get("has_og_image"):
                strengths.append("✅ OG image tag present")
            else:
                issues.append("🟡 Missing og:image tag")
        else:
            issues.append("🟡 No Open Graph meta tags")

        return issues, strengths
