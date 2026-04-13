import re
import time
from urllib.parse import urljoin, urlparse
from typing import Dict, Any, List

try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False


class WebsiteAuditor:
    def __init__(self, url: str, max_pages: int = 10):
        self.base_url = url.rstrip("/")
        self.max_pages = max_pages
        self.domain = urlparse(url).netloc
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }

    def run(self) -> Dict[str, Any]:
        results = {
            "url": self.base_url, "status": "ok", "pages_crawled": 0,
            "load_time_seconds": None, "https_enabled": self.base_url.startswith("https"),
            "homepage": {}, "pages": [], "issues": [], "strengths": [], "scores": {}
        }
        if not REQUESTS_OK:
            results["status"] = "skipped"
            return results
        try:
            pages_data = self._crawl_site(results)
            results["pages"] = pages_data
            results["pages_crawled"] = len(pages_data)
            results["homepage"] = pages_data[0] if pages_data else {}
        except Exception as e:
            results["status"] = "error"
            results["issues"].append(f"Crawl error: {str(e)}")
            return results
        results["scores"] = self._score_site(results)
        results["issues"]    = self._detect_issues(results)
        results["strengths"] = self._detect_strengths(results)
        return results

    def _fetch_page(self, url: str) -> tuple:
        start = time.time()
        try:
            r = requests.get(url, headers=self.headers, timeout=15, allow_redirects=True)
            load_time = round(time.time() - start, 2)
            soup = BeautifulSoup(r.text, "html.parser")
            return soup, load_time, r.status_code
        except Exception:
            return None, None, 0

    def _crawl_site(self, results: dict) -> List[Dict]:
        visited = set()
        to_visit = [self.base_url]
        pages = []
        while to_visit and len(pages) < self.max_pages:
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)
            soup, load_time, status = self._fetch_page(url)
            if soup is None:
                continue
            if len(pages) == 0:
                results["load_time_seconds"] = load_time
            page_data = self._analyze_page(url, soup, load_time, status)
            pages.append(page_data)
            for a in soup.find_all("a", href=True):
                full_url = urljoin(url, a["href"])
                parsed = urlparse(full_url)
                if parsed.netloc == self.domain and full_url not in visited:
                    to_visit.append(full_url)
        return pages

    def _analyze_page(self, url, soup, load_time, status) -> Dict:
        data = {"url": url, "status_code": status, "load_time": load_time}
        title_tag = soup.find("title")
        data["title"] = title_tag.get_text(strip=True) if title_tag else ""
        data["title_length"] = len(data["title"])
        meta_desc = soup.find("meta", attrs={"name": "description"})
        data["meta_description"] = meta_desc["content"] if meta_desc and meta_desc.get("content") else ""
        data["meta_description_length"] = len(data["meta_description"])
        data["h1_count"] = len(soup.find_all("h1"))
        data["h1_text"] = [h.get_text(strip=True) for h in soup.find_all("h1")]
        data["h2_count"] = len(soup.find_all("h2"))
        images = soup.find_all("img")
        data["image_count"] = len(images)
        data["images_missing_alt"] = sum(1 for img in images if not img.get("alt"))
        all_links = soup.find_all("a", href=True)
        data["internal_links"] = sum(1 for a in all_links if self.domain in urljoin(url, a["href"]))
        data["external_links"] = len(all_links) - data["internal_links"]
        cta_keywords = ["buy", "get started", "contact", "free", "sign up", "subscribe", "book", "shop now"]
        page_text = soup.get_text(separator=" ").lower()
        data["cta_count"] = sum(page_text.count(kw) for kw in cta_keywords)
        data["word_count"] = len(re.findall(r'\b\w+\b', page_text))
        data["has_viewport_meta"] = bool(soup.find("meta", attrs={"name": "viewport"}))
        data["has_schema_markup"] = bool(soup.find("script", attrs={"type": "application/ld+json"}))
        data["has_phone"] = bool(re.search(r'\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}', page_text))
        data["has_email_visible"] = bool(re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', page_text))
        return data

    def _score_site(self, results: dict) -> Dict[str, int]:
        scores   = {}
        homepage = results.get("homepage", {})
        tech = 100
        if not results["https_enabled"]: tech -= 30
        if not homepage.get("has_viewport_meta"): tech -= 20
        if results.get("load_time_seconds") and results["load_time_seconds"] > 3: tech -= 20
        if homepage.get("images_missing_alt", 0) > 3: tech -= 10
        scores["technical"] = max(25, tech)
        seo = 100
        if not homepage.get("title"): seo -= 25
        elif homepage.get("title_length", 0) > 65 or homepage.get("title_length", 0) < 30: seo -= 10
        if not homepage.get("meta_description"): seo -= 20
        if homepage.get("h1_count", 0) != 1: seo -= 15
        if not homepage.get("has_schema_markup"): seo -= 10
        scores["seo"] = max(25, seo)
        content = 100
        if homepage.get("word_count", 0) < 300: content -= 30
        if homepage.get("cta_count", 0) < 2: content -= 20
        scores["content"] = max(25, content)
        conv = 100
        if homepage.get("cta_count", 0) == 0: conv -= 40
        if not homepage.get("has_phone") and not homepage.get("has_email_visible"): conv -= 30
        scores["conversion"] = max(25, conv)
        scores["overall"] = round(sum(scores.values()) / len(scores))
        return scores

    def _detect_issues(self, results: dict) -> List[str]:
        issues   = []
        homepage = results.get("homepage", {})
        crawled  = results["pages_crawled"] > 0

        # HTTPS is verifiable without crawling
        if not results["https_enabled"]:
            issues.append("🔴 CRITICAL: Site is not using HTTPS")

        if crawled:
            if not homepage.get("has_viewport_meta"):
                issues.append("🔴 CRITICAL: Missing mobile viewport meta tag")
            if results.get("load_time_seconds") and results["load_time_seconds"] > 3:
                issues.append(f"🟡 Slow page load: {results['load_time_seconds']}s")
            if not homepage.get("title"):
                issues.append("🔴 CRITICAL: Homepage missing title tag")
            if not homepage.get("meta_description"):
                issues.append("🟡 Missing meta description on homepage")
            if homepage.get("h1_count", 0) == 0:
                issues.append("🟡 No H1 heading found on homepage")
            if homepage.get("images_missing_alt", 0) > 0:
                issues.append(f"🟡 {homepage['images_missing_alt']} images missing alt text")
            if homepage.get("word_count", 0) < 300:
                issues.append(f"🟡 Homepage has limited content ({homepage.get('word_count', 0)} words)")
            if homepage.get("cta_count", 0) < 2:
                issues.append("🟡 Few calls-to-action on homepage")
            if not homepage.get("has_schema_markup"):
                issues.append("🟡 No structured data found")
        else:
            issues.append("🟡 Site could not be crawled — full technical audit requires manual review")

        return issues

    def _detect_strengths(self, results: dict) -> List[str]:
        strengths = []
        homepage  = results.get("homepage", {})
        crawled   = results["pages_crawled"] > 0

        if results["https_enabled"]:
            strengths.append("✅ Site uses HTTPS")
        if crawled:
            if homepage.get("has_viewport_meta"):
                strengths.append("✅ Mobile viewport configured")
            if results.get("load_time_seconds") and results["load_time_seconds"] < 2:
                strengths.append(f"✅ Fast page load time ({results['load_time_seconds']}s)")
            if homepage.get("meta_description"):
                strengths.append("✅ Meta description present")
            if homepage.get("h1_count") == 1:
                strengths.append("✅ Proper single H1 heading")
            if homepage.get("has_schema_markup"):
                strengths.append("✅ Structured data implemented")
            if homepage.get("cta_count", 0) >= 3:
                strengths.append("✅ Strong calls-to-action presence")
        return strengths
