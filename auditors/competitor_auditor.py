"""
Competitor Analysis Auditor — C.A.S.H. Report by GMG

Runs website, SEO, and social presence checks on up to 3 competitor URLs,
then builds a side-by-side comparison table against the client.

Checks per competitor
---------------------
  SEO Score          — PageSpeed Lighthouse SEO category score
  Performance Score  — PageSpeed Lighthouse performance score
  Technical Score    — HTTPS, viewport, load time, image alt text
  Content Score      — word count and CTA presence on homepage
  Conversion Score   — CTAs, phone/email contact presence
  Social Channels    — detected social links on homepage
  GBP Score          — Google Business Profile (client only; competitors via URL)

All checks degrade gracefully: unreachable competitor → 50 neutral with a note.
Only the homepage is crawled (max_pages=1) to keep run-time short.
"""
import re
import time
import logging
from urllib.parse import urlparse
from typing import Dict, Any, List

log = logging.getLogger(__name__)

from config import ClientConfig

try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# Social platform link detection patterns (lowercase href matching)
_SOCIAL_PATTERNS = [
    ("LinkedIn",   r"linkedin\.com/(?:company|in)/"),
    ("Instagram",  r"instagram\.com/"),
    ("Facebook",   r"facebook\.com/"),
    ("YouTube",    r"youtube\.com/(?:channel|c|@|user)/"),
    ("TikTok",     r"tiktok\.com/@"),
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"


def _domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc or url


def _ensure_https(url: str) -> str:
    if not url.startswith("http"):
        return "https://" + url
    return url


class CompetitorAuditor:
    def __init__(
        self,
        config: ClientConfig,
        client_audit_data: Dict[str, Any],
        pagespeed_api_key: str = "",
    ):
        self.config      = config
        self.client_data = client_audit_data
        self.api_key     = pagespeed_api_key

        # Primary: explicit competitor_urls field
        urls = list(config.competitor_urls)

        # Fallback: pull URL-like entries from top_competitors when competitor_urls is empty
        if not urls:
            urls = [
                e for e in getattr(config, "top_competitors", [])
                if "." in e and not e.replace(" ", "").isalpha()
            ]

        self.competitor_urls = [_ensure_https(u) for u in urls[:3]]

    def run(self) -> Dict[str, Any]:
        if not self.competitor_urls:
            return {
                "skipped":    True,
                "note":       "No competitor URLs provided.",
                "competitors": [],
                "comparison":  {},
            }

        competitors = []
        for i, url in enumerate(self.competitor_urls, 1):
            print(f"  Auditing competitor {i}: {_domain(url)} …")
            comp = self._audit_competitor(url)
            competitors.append(comp)
            status_note = comp.get("note", "")
            print(
                f"    SEO={comp['seo_score']}  Perf={comp['performance_score']}  "
                f"Tech={comp['technical_score']}  "
                f"Social={comp['social_channel_count']} channels"
                + (f"  [{status_note}]" if status_note else "")
            )

        comparison = self._build_comparison(competitors)

        return {
            "skipped":               False,
            "biggest_challenge":     self.config.biggest_marketing_challenge,
            "competitors":           competitors,
            "comparison":            comparison,
            "insights":              self._derive_insights(comparison),
        }

    # ── Per-competitor audit ───────────────────────────────────

    def _audit_competitor(self, url: str) -> Dict[str, Any]:
        domain = _domain(url)
        base = {
            "url":                  url,
            "domain":               domain,
            "reachable":            False,
            "data_unavailable":     False,
            "seo_score":            None,
            "performance_score":    None,
            "technical_score":      None,
            "content_score":        None,
            "conversion_score":     None,
            "social_channels":      [],
            "social_channel_count": 0,
            # SEO signal defaults — populated by _extract_seo_signals()
            "has_title":            None,
            "title_text":           "",
            "has_meta_desc":        None,
            "has_h1":               None,
            "has_og_tags":          None,
            "has_schema":           None,
            "has_canonical":        None,
            "has_robots_txt":       None,
            "has_sitemap":          None,
            "note":                 "",
        }

        # ── PageSpeed → SEO + Performance ─────────────────────
        psi = self._fetch_pagespeed(url)
        if psi:
            lh   = psi.get("lighthouseResult", {})
            cats = lh.get("categories", {})
            seo_raw  = cats.get("seo",         {}).get("score")
            perf_raw = cats.get("performance", {}).get("score")
            base["seo_score"]         = round(seo_raw  * 100) if seo_raw  is not None else None
            base["performance_score"] = round(perf_raw * 100) if perf_raw is not None else None
            base["reachable"] = True
        else:
            base["note"] = "PageSpeed unavailable — scores unavailable"

        # ── Homepage scrape → Technical + Content + Conversion + Social + SEO signals ──
        homepage = self._scrape_homepage(url)
        if homepage:
            base["reachable"]        = True
            base["technical_score"]  = self._score_technical(url, homepage)
            base["content_score"]    = self._score_content(homepage)
            base["conversion_score"] = self._score_conversion(homepage)
            base["social_channels"]  = self._detect_social(homepage)
            base["social_channel_count"] = len(base["social_channels"])
            base.update(self._extract_seo_signals(homepage, url))
        elif not psi:
            base["note"]             = "Competitor site unreachable — all scores unavailable"
            base["data_unavailable"] = True

        return base

    def _fetch_pagespeed(self, url: str) -> Dict:
        if not REQUESTS_OK:
            return {}
        params = {
            "url":      url,
            "strategy": "mobile",
            "category": ["seo", "performance"],
        }
        if self.api_key:
            params["key"] = self.api_key
        for attempt in range(2):
            try:
                r = requests.get(PAGESPEED_ENDPOINT, params=params, timeout=60)
                if r.status_code == 200:
                    return r.json()
                log.warning("PageSpeed non-200 for %s: %s", url, r.status_code)
            except Exception as e:
                log.warning("PageSpeed timeout/error for %s: %s", url, e)
            if attempt == 0:
                time.sleep(2)
        return {}

    def _extract_seo_signals(self, soup, url: str) -> Dict[str, Any]:
        """Scrape SEO signals from an already-fetched BeautifulSoup object."""
        signals: Dict[str, Any] = {}

        # Page title
        title_tag = soup.find("title")
        signals["has_title"]  = bool(title_tag and title_tag.get_text(strip=True))
        signals["title_text"] = title_tag.get_text(strip=True) if title_tag else ""

        # Meta description
        meta_desc = soup.find("meta", attrs={"name": lambda v: v and v.lower() == "description"})
        signals["has_meta_desc"] = bool(
            meta_desc and meta_desc.get("content", "").strip()
        )

        # H1
        h1 = soup.find("h1")
        signals["has_h1"] = bool(h1 and h1.get_text(strip=True))

        # Open Graph tags
        og = soup.find("meta", property=lambda v: v and v.startswith("og:"))
        signals["has_og_tags"] = bool(og)

        # Structured data / schema
        schema = soup.find("script", attrs={"type": "application/ld+json"})
        signals["has_schema"] = bool(schema and schema.get_text(strip=True))

        # Canonical tag
        canonical = soup.find("link", attrs={"rel": lambda v: v and "canonical" in v})
        signals["has_canonical"] = bool(canonical and canonical.get("href", "").strip())

        # robots.txt — separate lightweight request
        try:
            parsed     = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            r = requests.get(robots_url, headers=_HEADERS, timeout=8, allow_redirects=True)
            signals["has_robots_txt"] = (
                r.status_code == 200 and "user-agent" in r.text.lower()
            )
        except Exception:
            signals["has_robots_txt"] = None

        # Sitemap — separate lightweight request
        try:
            parsed      = urlparse(url)
            sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
            r = requests.get(sitemap_url, headers=_HEADERS, timeout=8, allow_redirects=True)
            signals["has_sitemap"] = r.status_code == 200 and bool(r.text.strip())
        except Exception:
            signals["has_sitemap"] = None

        return signals

    def _scrape_homepage(self, url: str):
        if not REQUESTS_OK:
            return None
        try:
            r = requests.get(url, headers=_HEADERS, timeout=15, allow_redirects=True)
            if r.status_code < 400:
                return BeautifulSoup(r.text, "html.parser")
        except Exception:
            pass
        return None

    def _score_technical(self, url: str, soup) -> int:
        score = 100
        if not url.startswith("https://"):
            score -= 30
        if not soup.find("meta", attrs={"name": "viewport"}):
            score -= 20
        images = soup.find_all("img")
        missing_alt = sum(1 for img in images if not img.get("alt"))
        if missing_alt > 3:
            score -= 10
        return max(25, score)

    def _score_content(self, soup) -> int:
        score = 100
        page_text = soup.get_text(separator=" ")
        word_count = len(re.findall(r'\b\w+\b', page_text))
        if word_count < 300:
            score -= 30
        cta_keywords = ["get started", "contact", "free", "sign up", "subscribe",
                        "book", "buy", "shop now", "learn more", "get a quote"]
        cta_count = sum(page_text.lower().count(kw) for kw in cta_keywords)
        if cta_count < 2:
            score -= 20
        return max(25, score)

    def _score_conversion(self, soup) -> int:
        score = 100
        page_text = soup.get_text(separator=" ").lower()
        cta_keywords = ["get started", "contact", "free", "sign up", "subscribe",
                        "book", "buy", "shop now", "get a quote", "learn more"]
        cta_count = sum(page_text.count(kw) for kw in cta_keywords)
        if cta_count == 0:
            score -= 40
        has_phone = bool(re.search(r'\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}', page_text))
        has_email = bool(re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', page_text))
        if not has_phone and not has_email:
            score -= 30
        return max(25, score)

    def _detect_social(self, soup) -> List[str]:
        channels = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            for name, pattern in _SOCIAL_PATTERNS:
                if re.search(pattern, href):
                    channels.add(name)
        return sorted(channels)

    # ── Client row builder ─────────────────────────────────────

    def _client_row(self) -> Dict[str, Any]:
        seo_data   = self.client_data.get("seo", {})
        web_scores = self.client_data.get("website", {}).get("scores", {})
        gbp_data   = self.client_data.get("gbp", {})
        vs         = seo_data.get("crawl_signals", {}).get("validation_states", {})
        robots     = seo_data.get("robots_txt", {})
        sitemap    = seo_data.get("sitemap", {})

        def _vs_bool(key):
            state = vs.get(key)
            if state in ("found", "found_rendered"):
                return True
            if state == "missing":
                return False
            return None  # "unable" or absent

        return {
            "label":                self.config.client_name,
            "url":                  self.config.website_url or self.config.linktree_url,
            "domain":               _domain(self.config.website_url or ""),
            "is_client":            True,
            "seo_score":            seo_data.get("score"),
            "performance_score":    seo_data.get("performance_score"),
            "technical_score":      web_scores.get("technical"),
            "content_score":        web_scores.get("content"),
            "conversion_score":     web_scores.get("conversion"),
            "social_channels":      self.config.active_social_channels,
            "social_channel_count": len(self.config.active_social_channels),
            "gbp_score":            gbp_data.get("score"),
            # SEO signals
            "has_title":            _vs_bool("title"),
            "title_text":           seo_data.get("crawl_signals", {}).get("title", ""),
            "has_meta_desc":        _vs_bool("meta"),
            "has_h1":               _vs_bool("h1"),
            "has_og_tags":          _vs_bool("og"),
            "has_schema":           seo_data.get("has_schema"),
            "has_canonical":        _vs_bool("canonical"),
            "has_robots_txt":       robots.get("exists"),
            "has_sitemap":          sitemap.get("found"),
        }

    # ── Comparison builder ─────────────────────────────────────

    def _build_comparison(self, competitors: List[Dict]) -> Dict[str, Any]:
        client = self._client_row()

        metrics = [
            ("seo_score",           "SEO Score",          "score"),
            ("performance_score",   "Performance Score",  "score"),
            ("technical_score",     "Website Technical",  "score"),
            ("content_score",       "Website Content",    "score"),
            ("conversion_score",    "Website Conversion", "score"),
            ("social_channel_count","Social Channels",    "count"),
            ("has_title",           "Page Title",         "bool"),
            ("has_meta_desc",       "Meta Description",   "bool"),
            ("has_h1",              "H1 Tag",             "bool"),
            ("has_og_tags",         "Open Graph Tags",    "bool"),
            ("has_schema",          "Structured Data",    "bool"),
            ("has_canonical",       "Canonical Tag",      "bool"),
            ("has_robots_txt",      "robots.txt",         "bool"),
            ("has_sitemap",         "XML Sitemap",        "bool"),
        ]

        rows = []
        for key, label, kind in metrics:
            client_val = client.get(key)
            comp_vals  = [c.get(key) for c in competitors]

            if kind in ("score", "count"):
                numeric = [v for v in comp_vals if isinstance(v, (int, float))]
                beats = sum(
                    1 for v in numeric
                    if client_val is not None and client_val > v
                )
                ties  = sum(
                    1 for v in numeric
                    if client_val is not None and client_val == v
                ) if kind == "score" else 0
            else:
                beats = 0
                ties  = 0

            rows.append({
                "metric":      label,
                "kind":        kind,
                "client_val":  client_val,
                "comp_vals":   comp_vals,
                "beats":       beats,
                "total_comps": len(competitors),
            })

        return {
            "client":      client,
            "competitors": competitors,
            "rows":        rows,
        }

    # ── Insight generator ──────────────────────────────────────

    def _derive_insights(self, comparison: Dict) -> List[str]:
        insights = []
        client  = comparison.get("client", {})
        comps   = comparison.get("competitors", [])
        rows    = comparison.get("rows", [])

        if not comps:
            return insights

        for row in rows:
            if row.get("kind") not in ("score", "count"):
                continue
            metric     = row["metric"]
            client_val = row["client_val"]
            # Filter None before averaging — never crash on unavailable data
            comp_vals  = [v for v in row["comp_vals"] if isinstance(v, (int, float))]
            if not comp_vals or not isinstance(client_val, (int, float)):
                continue
            avg_comp = round(sum(comp_vals) / len(comp_vals))
            gap      = avg_comp - client_val
            if gap >= 10:
                insights.append(
                    f"🟡 {metric}: client scores {client_val} vs competitor average {avg_comp} "
                    f"(gap: {gap} points) — opportunity to close."
                )
            elif client_val - avg_comp >= 10:
                insights.append(
                    f"✅ {metric}: client leads competitors by {client_val - avg_comp} points "
                    f"(client {client_val} vs avg {avg_comp})."
                )

        # Social presence
        client_social = client.get("social_channel_count") or 0
        comp_social   = [c.get("social_channel_count") or 0 for c in comps]
        if comp_social:
            avg_social = sum(comp_social) / len(comp_social)
            if client_social > avg_social:
                insights.append(
                    f"✅ Social: client has {client_social} channels vs competitor average "
                    f"{avg_social:.1f} — broader footprint."
                )
            elif avg_social > client_social:
                insights.append(
                    f"🟡 Social: client has {client_social} channels vs competitor average "
                    f"{avg_social:.1f} — consider expanding presence."
                )

        return insights[:6]  # cap to keep report concise
