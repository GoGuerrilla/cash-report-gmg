"""
Linktree Scraper — live scrape of a Linktree profile.

Strategy (in order):
  1. __NEXT_DATA__ JSON block (Linktree's primary data injection).
  2. Generic JSON script walker — tries every <script> tag containing JSON
     that has URL-like strings; handles future Linktree structure changes.
  3. BeautifulSoup HTML fallback — scans all <a href> for social URLs.
  4. Returns minimal structure with data_verified=False if all fail.

Never uses hardcoded fallback data — every run reflects the live page.
"""
import json
import re
from typing import Dict, Any, List

try:
    import requests
    from bs4 import BeautifulSoup
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False


# Maps URL patterns → canonical platform names
PLATFORM_PATTERNS = {
    "Instagram":  r"instagram\.com/([^/?&#\"']+)",
    "LinkedIn":   r"linkedin\.com/(?:company|in)/([^/?&#\"']+)",
    "YouTube":    r"youtube\.com/(?:@|channel/|c/)?([^/?&#\"']+)",
    "Facebook":   r"facebook\.com/([^/?&#\"']+)",
    "TikTok":     r"tiktok\.com/@([^/?&#\"']+)",
    "Discord":    r"discord\.(?:gg|com/invite)/([^/?&#\"']+)",
    "Pinterest":  r"pinterest\.com/([^/?&#\"']+)",
    "Spotify":    r"open\.spotify\.com/([^/?&#\"']+)",
    "Email":      r"mailto:([^?&\"']+)",
}

# Linktree link-type → canonical platform name
_LINKTYPE_MAP = {
    "INSTAGRAM":         "Instagram",
    "INSTAGRAM_PROFILE": "Instagram",
    "YOUTUBE":           "YouTube",
    "FACEBOOK":          "Facebook",
    "TIKTOK":            "TikTok",
    "DISCORD":           "Discord",
    "LINKEDIN":          "LinkedIn",
    "PINTEREST":         "Pinterest",
    "SPOTIFY":           "Spotify",
    "EMAIL":             "Email",
    "EMAIL_ADDRESS":     "Email",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _empty_result(url: str, status: str) -> Dict[str, Any]:
    return {
        "source_url":       url,
        "profile_name":     "",
        "bio":              "",
        "raw_links":        [],
        "classified_links": {},
        "website_url":      "",
        "email":            "",
        "platforms_found":  [],
        "data_verified":    False,
        "scrape_status":    status,
    }


def _classify_url(href: str) -> str:
    """Return canonical platform name for a URL, or 'Website' if unmatched."""
    for platform, pattern in PLATFORM_PATTERNS.items():
        if re.search(pattern, href, re.I):
            return platform
    if href.startswith("http") and "linktr.ee" not in href:
        return "Website"
    return ""


def _extract_urls_from_obj(obj, found: list):
    """Recursively collect all URL-like strings from a JSON object."""
    if isinstance(obj, str):
        if obj.startswith("http") or obj.startswith("mailto:"):
            found.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            _extract_urls_from_obj(v, found)
    elif isinstance(obj, list):
        for item in obj:
            _extract_urls_from_obj(item, found)


def _build_result(url: str, profile_name: str, bio: str,
                  classified: Dict[str, List[str]],
                  raw_links: List[Dict],
                  status: str) -> Dict[str, Any]:
    # De-duplicate within each platform
    for k in classified:
        seen, deduped = set(), []
        for v in classified[k]:
            if v not in seen:
                seen.add(v)
                deduped.append(v)
        classified[k] = deduped

    website_url = (classified.get("Website") or [""])[0]
    email       = (classified.get("Email")   or [""])[0]

    return {
        "source_url":       url,
        "profile_name":     profile_name,
        "bio":              bio,
        "raw_links":        raw_links,
        "classified_links": classified,
        "website_url":      website_url,
        "email":            email,
        "platforms_found":  list(classified.keys()),
        "data_verified":    True,
        "scrape_status":    status,
    }


class LinktreeScraper:
    def __init__(self, linktree_url: str):
        self.url = linktree_url

    # ── Public entry point ─────────────────────────────────────

    def scrape(self) -> Dict[str, Any]:
        if not REQUESTS_OK:
            return _empty_result(self.url, "skipped_no_requests")

        try:
            resp = requests.get(self.url, headers=_HEADERS, timeout=20,
                                allow_redirects=True)
        except Exception as e:
            return _empty_result(self.url, f"network_error: {e}")

        if resp.status_code != 200:
            return _empty_result(self.url, f"http_{resp.status_code}")

        # Strategy 1: __NEXT_DATA__ JSON (structured, preferred)
        result = self._parse_next_data(resp.text)
        if result:
            return result

        # Strategy 2: Generic JSON script walker (future-proof fallback)
        result = self._parse_json_scripts(resp.text)
        if result:
            return result

        # Strategy 3: BeautifulSoup HTML link scan
        result = self._parse_html(resp.text)
        if result:
            return result

        return _empty_result(self.url, "parse_failed")

    # ── Strategy 1: __NEXT_DATA__ ──────────────────────────────

    def _parse_next_data(self, html: str) -> Dict[str, Any]:
        match = re.search(
            r'<script\s+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        if not match:
            return {}

        try:
            nd = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            return {}

        props = nd.get("props", {}).get("pageProps", {})
        acct  = props.get("account", {})

        profile_name = (
            props.get("pageTitle") or
            acct.get("pageTitle") or
            acct.get("username") or ""
        ).strip()

        bio = (
            acct.get("description") or
            props.get("description") or
            props.get("metaDescription") or ""
        ).strip()

        raw_link_objs: List[Dict] = props.get("links") or acct.get("links") or []
        social_link_objs: List[Dict] = props.get("socialLinks") or acct.get("socialLinks") or []

        classified: Dict[str, List[str]] = {}
        raw_links: List[Dict] = []

        def _add_link(href: str, label: str, link_type: str = ""):
            if not href:
                return
            href = href.strip()
            type_upper = link_type.upper()

            # Resolve platform: Linktree type first, then URL pattern
            platform = ""
            for prefix, canonical in _LINKTYPE_MAP.items():
                if type_upper.startswith(prefix):
                    platform = canonical
                    break
            if not platform:
                platform = _classify_url(href)
            if not platform:
                return

            if platform == "Email":
                href = href.replace("mailto:", "")

            classified.setdefault(platform, []).append(href)
            raw_links.append({"url": href, "label": label, "platform": platform})

        for lnk in raw_link_objs:
            _add_link(lnk.get("url", ""), lnk.get("title", ""), lnk.get("type", ""))

        for sl in social_link_objs:
            _add_link(sl.get("url", ""), sl.get("type", ""), sl.get("type", ""))

        if not classified:
            return {}

        return _build_result(self.url, profile_name, bio, classified, raw_links,
                             "ok_next_data")

    # ── Strategy 2: Generic JSON script walker ─────────────────

    def _parse_json_scripts(self, html: str) -> Dict[str, Any]:
        """
        Try every <script> block that contains parseable JSON.
        Collect all URLs found, classify them, return if ≥2 social platforms found.
        Handles future Linktree structure changes without code updates.
        """
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)

        classified: Dict[str, List[str]] = {}
        raw_links: List[Dict] = []

        for script in scripts:
            script = script.strip()
            if not script or not script.startswith("{"):
                continue
            try:
                obj = json.loads(script)
            except (json.JSONDecodeError, ValueError):
                continue

            urls: List[str] = []
            _extract_urls_from_obj(obj, urls)

            for href in urls:
                platform = _classify_url(href)
                if not platform:
                    continue
                if platform == "Email":
                    href = href.replace("mailto:", "")
                classified.setdefault(platform, []).append(href)
                raw_links.append({"url": href, "label": "", "platform": platform})

        # Require at least 2 social platforms (not just Website) to consider valid
        social_keys = [k for k in classified if k not in ("Website", "Email")]
        if len(social_keys) < 2:
            return {}

        return _build_result(self.url, "", "", classified, raw_links,
                             "ok_json_script_walk")

    # ── Strategy 3: BeautifulSoup HTML fallback ────────────────

    def _parse_html(self, html: str) -> Dict[str, Any]:
        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception:
            return {}

        profile_name = ""
        for tag in soup.find_all(["h1", "h2"],
                                  class_=re.compile(r"(name|title|profile)", re.I)):
            profile_name = tag.get_text(strip=True)
            if profile_name:
                break

        bio = ""
        for tag in soup.find_all(["p", "span"],
                                  class_=re.compile(r"(bio|description|subtitle)", re.I)):
            bio = tag.get_text(strip=True)
            if bio:
                break

        classified: Dict[str, List[str]] = {}
        raw_links: List[Dict] = []

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            label = a.get_text(strip=True)
            platform = _classify_url(href)
            if not platform:
                continue
            if platform == "Email":
                href = href.replace("mailto:", "")
            classified.setdefault(platform, []).append(href)
            raw_links.append({"url": href, "label": label, "platform": platform})

        if not classified:
            return {}

        return _build_result(self.url, profile_name, bio, classified, raw_links,
                             "ok_html_fallback")
