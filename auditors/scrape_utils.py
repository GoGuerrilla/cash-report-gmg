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
  Wix         — Client-side rendered SPA (same treatment as react/vue). Raw HTML is a
               JS shell; use render_page() for real DOM content. /blog-feed.xml
               provides RSS ground truth for blog post count/dates.
  Squarespace — SSR HTML; OG tags sometimes use name= instead of property=.
               Googlebot fallback usually passes.
  Shopify     — SSR HTML; schema frequently in @graph format via Organization +
               WebSite + WebPage nodes.  Chrome UA normally sufficient.
  WordPress   — PHP-rendered; standard HTML; Yoast/RankMath emit @graph schemas
               with multiple types per script block.
  Webflow     — SSR HTML; clean structure, no special handling needed.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)

try:
    import requests
    from bs4 import BeautifulSoup, Tag
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_OK = True
except ImportError:
    PLAYWRIGHT_OK = False

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

# ── Tracking / noise query parameters to strip before auditing ────────────────
_TRACKING_PARAMS: frozenset = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_source_platform", "fbclid", "gclid", "dclid", "gbraid",
    "wbraid", "msclkid", "twclid", "li_fat_id", "mc_eid", "mc_cid",
    "yclid", "_ga", "_gl", "igshid", "s_cid", "ref", "source",
})


def _strip_tracking_params(url: str) -> str:
    """Strip known tracking query parameters and fragments from a URL."""
    try:
        from urllib.parse import urlparse, urlencode, urlunparse, parse_qsl
        p = urlparse(url)
        qs = urlencode(
            [(k, v) for k, v in parse_qsl(p.query)
             if k.lower() not in _TRACKING_PARAMS]
        )
        return urlunparse(p._replace(query=qs, fragment=""))
    except Exception:
        return url


def normalize_url(url: str, timeout: int = 10) -> str:
    """
    Normalize a URL before auditing:
      1. Strip tracking query parameters (utm_*, fbclid, gclid, etc.)
      2. Remove URL fragments (#...)
      3. Follow HTTP redirects via a HEAD request (falls back to GET if the
         server rejects HEAD) to resolve www vs non-www, http vs https, and
         301/302 canonical targets.

    Returns the cleaned, redirected URL. Never raises.
    """
    url = _strip_tracking_params(url)
    if not REQUESTS_OK or not url:
        return url.rstrip("/")

    for headers in (_HEADERS_CHROME, _HEADERS_GOOGLEBOT):
        try:
            r = requests.head(
                url, headers=headers, timeout=timeout, allow_redirects=True
            )
            if r.status_code in (403, 429, 503) and headers is _HEADERS_CHROME:
                continue
            if r.status_code == 405:
                # Server rejects HEAD — stream GET to get final URL cheaply
                rg = requests.get(
                    url, headers=headers, timeout=timeout,
                    allow_redirects=True, stream=True,
                )
                rg.close()
                return _strip_tracking_params(rg.url).rstrip("/")
            return _strip_tracking_params(r.url).rstrip("/")
        except Exception:
            break

    return url.rstrip("/")


def fetch_url_ex(
    url: str, timeout: int = _FETCH_TIMEOUT
) -> Tuple[Optional[str], int, str]:
    """
    Like fetch_url() but also returns the final URL after following redirects.
    Tracking parameters are stripped from the final URL.

    Attempt order:
      1. Chrome UA  → if 403/429/503: retry with Googlebot UA
      2. Googlebot UA

    Returns (html_text, status_code, final_url).
    Returns (None, 0, original_url) on network error or both attempts failing.
    Never raises.
    """
    if not REQUESTS_OK or not url:
        return None, 0, url

    for headers in (_HEADERS_CHROME, _HEADERS_GOOGLEBOT):
        try:
            r = requests.get(
                url, headers=headers, timeout=timeout, allow_redirects=True
            )
            if r.status_code in (403, 429, 503) and headers is _HEADERS_CHROME:
                continue
            final = _strip_tracking_params(r.url)
            return r.text, r.status_code, final
        except requests.exceptions.Timeout:
            return None, 0, url
        except Exception:
            return None, 0, url

    return None, 0, url


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

    Returns one of:
      'wix' | 'squarespace' | 'shopify' | 'wordpress' | 'webflow'
      'nextjs'  — Next.js (SSR/ISR — content is in HTML, scraping works)
      'react'   — Create-React-App or other client-side-only React SPA
                  (content is JS-rendered; technical signals may be absent
                  from raw HTML and should be treated as Unable-to-validate)
      'vue'     — Vue SPA (same caveat as 'react')
      'unknown'

    Scraping behaviour notes:
      Squarespace / Shopify / WordPress / Webflow / Next.js — server-side
      rendering; H1, meta tags, and schema appear in raw HTML.
      wix / react / vue — client-side SPAs; raw HTML is a JS shell; missing
      technical signals should be Unable-to-validate, not scored as failures.
    """
    html_l = html.lower() if html else ""

    # Wix
    if (
        "wixstatic.com" in html_l
        or "wix.com/_api" in html_l
        or "wix-code" in html_l
        or "wix.viewer" in html_l
        or "<!-- wix:site" in html_l
        or (soup and soup.find("meta", attrs={"name": "wix-site-id"}))
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

    # Next.js — SSR/ISR; content is present in HTML, scraping works normally
    if "__next_data__" in html_l or '"next"' in html_l or "/_next/static" in html_l:
        return "nextjs"

    # Pure React SPA — content rendered client-side; missing tags = unable-to-validate
    # Key signal: root div with no meaningful child content
    if soup:
        root = soup.find(id="root") or soup.find(attrs={"data-reactroot": True})
        if root is not None and len(root.get_text(strip=True)) < 50:
            return "react"
    if "data-reactroot" in html_l:
        return "react"

    # Vue SPA — same client-side caveat as React
    if soup:
        app_div = soup.find(id="app")
        if app_div is not None and len(app_div.get_text(strip=True)) < 50:
            if "vue" in html_l or "vuejs" in html_l:
                return "vue"
    if "__vue_app__" in html_l or "vue.runtime" in html_l:
        return "vue"

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


# ─────────────────────────────────────────────────────────────────────────────
#  Visible text extraction (filters noise — nav/footer/scripts/JSON blobs)
# ─────────────────────────────────────────────────────────────────────────────

# Tags whose entire content subtree is excluded from primary body text
_NOISE_TAGS: frozenset = frozenset({
    "script", "style", "noscript", "iframe",
    "nav", "footer", "header",
    "aside", "menu", "form",
})

# Inline-style patterns that indicate hidden / zero-opacity elements
_HIDDEN_STYLE_RE = re.compile(
    r"display\s*:\s*none|visibility\s*:\s*hidden|opacity\s*:\s*0", re.I
)

# Heuristic: a text node that looks like a JSON blob (not human-readable prose)
_JSON_BLOB_RE = re.compile(r'^\s*[\[{]', re.S)


def get_visible_text(soup: Optional[Any]) -> str:
    """
    Extract primary visible body text, filtering out:
      - <script>, <style>, <noscript>, <iframe> content
      - <nav>, <footer>, <header>, <aside>, <menu>, <form> chrome/repetition
      - Elements with display:none / visibility:hidden / aria-hidden="true"
      - JSON blobs embedded as inline text (common in Next.js / React hydration)

    Uses the text-node walk (find_all(string=True)) to avoid mutating the
    original soup and without an expensive deep-copy.

    Returns a plain-text string suitable for word counting and content analysis.
    """
    if not soup:
        return ""
    try:
        parts: List[str] = []
        for string in soup.find_all(string=True):
            node_text = str(string).strip()
            if not node_text:
                continue

            # Walk ancestors — skip if any ancestor is a noise tag or hidden
            skip = False
            for anc in string.parents:
                tag_name = getattr(anc, "name", None)
                if tag_name in _NOISE_TAGS:
                    skip = True
                    break
                style = anc.get("style", "") if hasattr(anc, "get") else ""
                if style and _HIDDEN_STYLE_RE.search(style):
                    skip = True
                    break
                aria = anc.get("aria-hidden", "") if hasattr(anc, "get") else ""
                if aria == "true":
                    skip = True
                    break
            if skip:
                continue

            # Drop JSON / data blobs (>80 chars, starts with { or [)
            if len(node_text) > 80 and _JSON_BLOB_RE.match(node_text):
                continue

            parts.append(node_text)

        return " ".join(parts)
    except Exception:
        # Fallback to raw text on any error
        return soup.get_text(separator=" ") if soup else ""


def get_word_count(soup: Optional[Any]) -> int:
    """
    Estimate visible word count from primary page body text.

    Uses get_visible_text() to exclude scripts, nav/footer chrome, hidden
    elements, and embedded JSON blobs — producing a count that reflects only
    human-readable content.
    """
    if not soup:
        return 0
    try:
        text = get_visible_text(soup)
        return len(re.findall(r"\b[a-zA-Z]{2,}\b", text))
    except Exception:
        return 0


# ─────────────────────────────────────────────────────────────────────────────
#  Headless browser rendering (Playwright — optional)
# ─────────────────────────────────────────────────────────────────────────────

def render_page(
    url: str,
    timeout_ms: int = 20_000,
) -> Tuple[Optional[str], str]:
    """
    Render a page with a headless Chromium browser (Playwright) and return
    the fully-rendered HTML after JavaScript execution.

    Call this when raw-HTML scraping returns empty technical signals on a
    site suspected of client-side rendering (React / Vue SPA detected).

    Attempt sequence:
      1. Navigate to URL (Chrome UA, 1280×800 viewport)
      2. Wait for DOMContentLoaded
      3. Best-effort networkidle wait (5 s cap — times out silently)
      4. Capture page.content() (post-JS DOM)

    Returns (rendered_html, "ok") on success.
    Returns (None, error_message) if Playwright is unavailable or the render
    fails.  Never raises.
    """
    if not PLAYWRIGHT_OK:
        return None, "playwright not installed"
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=_HEADERS_CHROME["User-Agent"],
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=5_000)
            except Exception:
                pass   # networkidle timeout is acceptable — use current DOM
            html = page.content()
            browser.close()
        return html, "ok"
    except Exception as exc:
        return None, str(exc)


def fetch_wix_blog_rss(base_url: str) -> Optional[List[Dict[str, Any]]]:
    """
    Fetch /blog-feed.xml on a Wix site and parse as RSS.

    Returns:
      List[Dict] — feed reachable; dicts have keys title, published, url.
                   An empty list means the blog feature is enabled but has no posts.
      None       — feed unreachable (404 = blog not enabled; other = fetch/parse
                   failure). The specific reason is logged at INFO/WARNING level.

    Never raises. Uses stdlib xml.etree only — no new dependencies.
    """
    import xml.etree.ElementTree as ET

    feed_url = base_url.rstrip("/") + "/blog-feed.xml"
    html, status = fetch_url(feed_url)

    if status == 404:
        _log.info("Wix blog RSS 404 — blog not enabled: %s", feed_url)
        return None
    if not html or status not in (200,):
        _log.info("Wix blog RSS unavailable: %s (status=%s)", feed_url, status)
        return None

    try:
        root = ET.fromstring(html)
    except ET.ParseError as exc:
        _log.warning("Wix blog RSS parse error: %s — %s", feed_url, exc)
        return None

    posts: List[Dict[str, Any]] = []
    channel = root.find("channel")           # RSS 2.0
    if channel is not None:
        for item in channel.findall("item"):
            title_el = item.find("title")
            pub_el   = item.find("pubDate")
            link_el  = item.find("link")
            posts.append({
                "title":     (title_el.text or "").strip() if title_el is not None else "",
                "published": (pub_el.text   or "").strip() if pub_el   is not None else "",
                "url":       (link_el.text  or "").strip() if link_el  is not None else "",
            })
    else:
        # Atom 1.0 fallback
        ns = "http://www.w3.org/2005/Atom"
        for entry in root.findall(f"{{{ns}}}entry"):
            title_el = entry.find(f"{{{ns}}}title")
            pub_el   = (entry.find(f"{{{ns}}}published")
                        or entry.find(f"{{{ns}}}updated"))
            link_el  = entry.find(f"{{{ns}}}link")
            posts.append({
                "title":     (title_el.text or "").strip() if title_el is not None else "",
                "published": (pub_el.text   or "").strip() if pub_el   is not None else "",
                "url":       link_el.get("href", "")       if link_el  is not None else "",
            })

    _log.info("Wix blog RSS: %d post(s) at %s", len(posts), feed_url)
    return posts
