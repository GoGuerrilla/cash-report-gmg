"""
scrape_utils.py — Shared scraping utilities for the CASH audit system.

Design principles
─────────────────
  • All public functions fail gracefully — never raise, return neutral on error.
  • fetch_url() retries with a Googlebot UA when primary request is blocked.
  • extract_schema() handles every real-world JSON-LD format (see below).
  • Meta-tag helpers are case-insensitive and check both property= and name=
    attributes so they work across Wix, Squarespace, Shopify, WordPress,
    Webflow, and custom React/Next.js sites.

JSON-LD formats handled by extract_schema()
────────────────────────────────────────────
  Single object  {"@type": "Organization", ...}
  Array          [{"@type": "Organization"}, {"@type": "FAQPage", ...}]
  @graph         {"@graph": [{"@type": "WebSite"}, {"@type": "BreadcrumbList"}]}
  @type as list  {"@type": ["Organization", "LocalBusiness"], ...}
  Multiple tags  multiple <script type="application/ld+json"> blocks
  Nested deep    @graph inside @graph, mainEntity with FAQPage items, etc.
  Encoding edge  BOM prefix (\\ufeff), surrounding whitespace, escaped chars

Platform-specific notes
────────────────────────
  Wix         — SSR HTML; all meta tags and schema in static source; H1/H2 present.
               May require Chrome UA to pass Cloudflare checks.
  Squarespace — SSR HTML; OG tags sometimes use name= instead of property=.
               Googlebot fallback usually passes.
  Shopify     — SSR HTML; schema frequently in @graph format via Organization +
               WebSite + WebPage nodes.  Chrome UA normally sufficient.
  WordPress   — PHP-rendered; standard HTML; Yoast/RankMath emit @graph schemas
               with multiple types per script block.
  Webflow     — SSR HTML; clean structure, no special handling needed.
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
    from bs4 import BeautifulSoup, Tag
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ─────────────────────────────────────────────────────────────────────────────
#  HTTP fetch helpers
# ─────────────────────────────────────────────────────────────────────────────

# Primary: realistic Chrome on macOS headers (passes most Cloudflare checks)
_HEADERS_CHROME: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

# Fallback: Googlebot — most sites explicitly allow this UA
_HEADERS_GOOGLEBOT: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}

_FETCH_TIMEOUT = 15  # seconds


def fetch_url(url: str, timeout: int = _FETCH_TIMEOUT) -> Tuple[Optional[str], int]:
    """
    Fetch a URL safely with retry logic.

    Attempt order:
      1. Chrome UA  → if 403/429/503: retry with Googlebot UA
      2. Googlebot UA

    Returns (html_text, status_code).
    Returns (None, 0) on network error, timeout, or both attempts failing.
    Never raises.
    """
    if not REQUESTS_OK or not url:
        return None, 0

    for headers in (_HEADERS_CHROME, _HEADERS_GOOGLEBOT):
        try:
            r = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            )
            # On anti-scraping block codes, try the next UA
            if r.status_code in (403, 429, 503) and headers is _HEADERS_CHROME:
                continue
            return r.text, r.status_code
        except requests.exceptions.Timeout:
            return None, 0
        except Exception:
            return None, 0

    return None, 0


def parse_html(html: str) -> Optional[Any]:
    """
    Parse an HTML string into a BeautifulSoup object.
    Returns None on any parse failure.
    """
    if not html or not REQUESTS_OK:
        return None
    try:
        return BeautifulSoup(html, "html.parser")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Platform detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_platform(soup: Optional[Any], html: str = "") -> str:
    """
    Detect the CMS / website platform from HTML fingerprints.
    Returns one of: 'wix' | 'squarespace' | 'shopify' | 'wordpress'
                    | 'webflow' | 'unknown'

    Used to adjust scraping expectations (e.g., Wix embeds H1 in static HTML
    despite being a visual page builder).
    """
    html_l = html.lower() if html else ""

    # Wix
    if (
        "wixstatic.com" in html_l
        or "wix.com/_api" in html_l
        or "wix-code" in html_l
        or "wix.viewer" in html_l
    ):
        return "wix"

    # Squarespace
    if (
        "squarespace.com" in html_l
        or "sqsp.net" in html_l
        or "squarespace-cdn.com" in html_l
    ):
        return "squarespace"

    # Shopify
    if (
        "myshopify.com" in html_l
        or "shopifycdn.com" in html_l
        or "cdn.shopify.com" in html_l
    ):
        return "shopify"

    # Webflow
    if "webflow.com" in html_l or "webflow.io" in html_l:
        return "webflow"

    # WordPress — generator meta or common path patterns
    if soup:
        gen = soup.find("meta", attrs={"name": lambda x: x and x.lower() == "generator"})
        if gen and "wordpress" in (gen.get("content") or "").lower():
            return "wordpress"
    if "wp-content" in html_l or "wp-includes" in html_l or "wp-json" in html_l:
        return "wordpress"

    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
#  Schema / JSON-LD extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_schema(soup: Optional[Any]) -> Tuple[List[str], bool]:
    """
    Extract all @type values from every <script type="application/ld+json"> block.

    Handles all common real-world formats:
      - Single object:   {"@type": "Organization"}
      - Top-level array: [{"@type": "Organization"}, {"@type": "FAQPage"}]
      - @graph:          {"@graph": [{"@type": "WebSite"}, ...]}
      - Nested @graph:   @graph items that themselves contain @graph
      - @type as list:   {"@type": ["Organization", "LocalBusiness"]}
      - Multiple blocks: several <script type="application/ld+json"> tags
      - Encoding issues: BOM (\\ufeff), leading/trailing whitespace

    Returns:
      schema_types  — deduplicated list of @type strings found
      has_faq       — True if "FAQPage" appears anywhere in the tree
    """
    if not soup:
        return [], False

    found_types: List[str] = []
    has_faq = False

    def _collect(obj: Any, depth: int = 0) -> None:
        nonlocal has_faq
        if depth > 10 or obj is None:
            return
        if isinstance(obj, list):
            for item in obj:
                _collect(item, depth + 1)
            return
        if not isinstance(obj, dict):
            return

        # Extract @type — may be a string or a list of strings
        raw_type = obj.get("@type")
        if raw_type is not None:
            types = raw_type if isinstance(raw_type, list) else [raw_type]
            for t in types:
                if isinstance(t, str) and t.strip():
                    clean = t.strip()
                    if clean not in found_types:
                        found_types.append(clean)
                    if clean == "FAQPage":
                        has_faq = True

        # Recurse into known nested containers
        _NESTED_KEYS = (
            "@graph",
            "mainEntity",
            "mainEntityOfPage",
            "itemListElement",
            "hasPart",
            "about",
            "breadcrumb",
            "potentialAction",
            "publisher",
            "author",
        )
        for key in _NESTED_KEYS:
            val = obj.get(key)
            if val:
                _collect(val, depth + 1)

    for script in soup.find_all("script", type="application/ld+json"):
        # Prefer .string (lxml/html.parser preserves raw text there)
        # Fall back to .get_text() for edge-case parsers
        raw: str = script.string or script.get_text(strip=False) or ""
        raw = raw.strip()
        if not raw:
            continue

        # Try parsing — strip BOM on failure
        data = None
        for candidate in (raw, raw.lstrip("\ufeff")):
            try:
                data = json.loads(candidate)
                break
            except (ValueError, TypeError):
                continue

        if data is None:
            continue  # unparseable — skip silently

        _collect(data)

    return found_types, has_faq


# ─────────────────────────────────────────────────────────────────────────────
#  Meta-tag helpers (case-insensitive, attribute-agnostic)
# ─────────────────────────────────────────────────────────────────────────────

def get_title(soup: Optional[Any]) -> str:
    """Return the page <title> text, or empty string."""
    if not soup:
        return ""
    tag = soup.find("title")
    return tag.get_text().strip() if tag else ""


def get_meta_description(soup: Optional[Any]) -> str:
    """
    Return meta description content.
    Checks name='description' first, then property='description'
    (some CMSs use property= incorrectly).
    Case-insensitive on both attributes.
    """
    if not soup:
        return ""
    for attr in ("name", "property"):
        tag = soup.find(
            "meta",
            attrs={attr: lambda x: x and x.lower() == "description"},
        )
        if tag:
            return (tag.get("content") or "").strip()
    return ""


def get_canonical(soup: Optional[Any]) -> str:
    """
    Return the canonical URL href.
    Handles rel='canonical' with list or string value (BeautifulSoup
    sometimes returns rel as a list).
    """
    if not soup:
        return ""
    for tag in soup.find_all("link"):
        rel = tag.get("rel", [])
        # rel may be a list ['canonical'] or string 'canonical'
        if isinstance(rel, list):
            rel_str = " ".join(rel).lower()
        else:
            rel_str = str(rel).lower()
        if "canonical" in rel_str:
            return (tag.get("href") or "").strip()
    return ""


def get_og_tags(soup: Optional[Any]) -> Dict:
    """
    Extract Open Graph tags.

    Checks both:
      - property='og:*'  (standard)
      - name='og:*'      (used by some CMSs, e.g. some Squarespace themes)

    Returns dict with keys: present, tags (list of property names), has_og_image,
    has_og_title, has_og_description.
    """
    if not soup:
        return {
            "present": None, "tags": [],
            "has_og_image": False, "has_og_title": False, "has_og_description": False,
        }

    seen_ids: set = set()
    all_tags: list = []

    for attr in ("property", "name"):
        for tag in soup.find_all(
            "meta", attrs={attr: lambda x: x and x.lower().startswith("og:")}
        ):
            tid = id(tag)
            if tid not in seen_ids:
                seen_ids.add(tid)
                all_tags.append(tag)

    props = [
        (t.get("property") or t.get("name") or "").lower()
        for t in all_tags
    ]
    return {
        "present":          len(all_tags) > 0,
        "tags":             props,
        "has_og_image":     any("og:image" in p for p in props),
        "has_og_title":     any("og:title" in p for p in props),
        "has_og_description": any("og:description" in p for p in props),
    }


def get_twitter_card(soup: Optional[Any]) -> Dict:
    """
    Extract Twitter Card tags.

    Checks both:
      - name='twitter:*'      (standard)
      - property='twitter:*'  (used by some WordPress plugins)

    Returns dict with keys: present, tags (list of name values).
    """
    if not soup:
        return {"present": None, "tags": []}

    seen_ids: set = set()
    all_tags: list = []

    for attr in ("name", "property"):
        for tag in soup.find_all(
            "meta", attrs={attr: lambda x: x and x.lower().startswith("twitter:")}
        ):
            tid = id(tag)
            if tid not in seen_ids:
                seen_ids.add(tid)
                all_tags.append(tag)

    names = [
        (t.get("name") or t.get("property") or "").lower()
        for t in all_tags
    ]
    return {
        "present": len(all_tags) > 0,
        "tags":    names,
    }


def get_robots_meta(soup: Optional[Any]) -> Dict:
    """
    Check meta robots noindex directives.
    Checks both name='robots' and name='googlebot'.

    Returns dict with keys: is_indexable (bool), content (str).
    Defaults to indexable=True when no meta robots tag is found
    (absence of noindex ≠ noindex).
    """
    if not soup:
        return {"is_indexable": True, "content": ""}

    for name in ("robots", "googlebot"):
        tag = soup.find(
            "meta",
            attrs={"name": lambda x: x and x.lower() == name},
        )
        if tag:
            content = (tag.get("content") or "").lower()
            return {
                "is_indexable": "noindex" not in content,
                "content":      content,
            }
    return {"is_indexable": True, "content": ""}


def get_headings(soup: Optional[Any], platform: str = "unknown") -> Dict:
    """
    Extract H1 and H2 headings from the parsed page.

    Note: Wix, Squarespace, Shopify, and WordPress all use server-side
    rendering — H1/H2 headings appear in the static HTML source that
    requests.get() fetches, so normal BeautifulSoup extraction works.

    Returns dict with keys: h1s (list), h2s (list), detected (bool).
    """
    if not soup:
        return {"h1s": [], "h2s": [], "detected": False}
    try:
        h1s = [h.get_text(" ", strip=True) for h in soup.find_all("h1")
               if h.get_text(strip=True)]
        h2s = [h.get_text(" ", strip=True) for h in soup.find_all("h2")
               if h.get_text(strip=True)][:12]
        return {"h1s": h1s, "h2s": h2s, "detected": True}
    except Exception:
        return {"h1s": [], "h2s": [], "detected": False}


def get_word_count(soup: Optional[Any]) -> int:
    """Estimate visible word count from page body text."""
    if not soup:
        return 0
    try:
        text = soup.get_text(separator=" ")
        return len(re.findall(r"\b\w{2,}\b", text))
    except Exception:
        return 0
