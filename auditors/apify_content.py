"""
Probe 2 — Apify Website Content Crawler

JS-rendered depth-2 crawl of client website. Replaces all static content
fetching in website_auditor.py. Platform-invisible: Wix, WordPress, Webflow,
React, and static sites produce identical output contract.

── saveHtmlAsFile ────────────────────────────────────────────────────────────
Omitting htmlTransformer uses the default "readableText" transformer, which
returns url/title/text/metadata/crawl per item but NO html field.
saveHtmlAsFile=True stores each page's rendered HTML in the Apify KV store
and adds htmlUrl to each dataset item. _fetch_html_url() retrieves that HTML
so BeautifulSoup can extract headings, structured_data, forms, CTAs, images,
and internal_links from the fully-rendered DOM.
_extract_text() is the canonical text source (Decision 5-A) — apify_text is
never used for the text or body contract fields.

── HOLD PATTERN — step-5 implementer (website_auditor.py) ───────────────────
fetch() raises RuntimeError whose message starts with "apify_failed".
The caller must handle it as follows — do not deviate from this pattern:

    from datetime import datetime, timezone

    try:
        probe2 = apify_content.fetch(base_url, sitemap_urls=sitemap_urls)
    except RuntimeError as exc:
        if str(exc).startswith("apify_failed"):
            _ts = datetime.now(timezone.utc).isoformat()
            log.error(
                "[CASH HOLD] reason=apify_failed url=%s timestamp=%s",
                base_url, _ts,
            )
            send_hold_warning_email(config, contact_email, str(exc))
            return          # skip client-facing PDF and report email

The [CASH HOLD] line is the greppable signal in Railway logs.
send_hold_warning_email fires the internal notification to gmg@goguerrilla.xyz.
The client never receives a PDF or email when a HOLD fires.
"""

import json
import logging
import os
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup
    _BS4_OK = True
except ImportError:
    _BS4_OK = False

_log = logging.getLogger(__name__)

# ── Apify actor endpoint ──────────────────────────────────────────────────────

_ACTOR_URL = (
    "https://api.apify.com/v2/acts/apify~website-content-crawler"
    "/run-sync-get-dataset-items"
)

# ── Call parameters ───────────────────────────────────────────────────────────

_MAX_PAGES  = 15
_MAX_DEPTH  = 2
_TIMEOUT    = 150   # s — cold start 30-60s + render time for 15 pages
_RETRY_WAIT = 10    # s — wait before single retry (actor call and htmlUrl fetch)

# ── Page selection — priority path groups ────────────────────────────────────
# Within each group the first path that HEAD-responds < 400 wins.
# One URL per group is added to startUrls, in the order below.

_PRIORITY_GROUPS = [
    ["/services", "/service", "/solutions", "/work"],
    ["/about", "/about-us", "/team"],
    ["/faq", "/faqs", "/help"],
    ["/contact", "/contact-us", "/book", "/schedule"],
    # Blog landing — so depth-2 crawl reaches individual posts. Without this,
    # Wix sites whose sitemap doesn't surface /blog never have their posts
    # discovered (observed on goguerrilla.xyz: pages=16 blog_posts=0 despite
    # an active blog section).
    ["/blog", "/articles", "/insights", "/news", "/resources",
     "/marketing-insights", "/posts", "/journal", "/learn"],
]

# ── Blog URL patterns and JSON-LD types ──────────────────────────────────────

_BLOG_SEGS  = ("/post/", "/blog/", "/articles/", "/news/", "/insights/")
_BLOG_TYPES = frozenset({"BlogPosting", "Article", "NewsArticle"})

# ── CTA keyword set ───────────────────────────────────────────────────────────

_CTA_KWS = frozenset({
    # Direct purchase / engagement
    "buy", "get started", "start", "sign up", "subscribe", "book", "schedule",
    "contact", "call us", "shop", "try", "join", "download", "get my",
    "free", "demo", "consult", "hire", "work with", "apply", "learn more",
    "request", "claim", "register",
    # Service / advisory CTAs missed in earlier vocab — added per Swift Profit
    # Systems beta feedback 2026-05-06 ("Find My $50K-$500K", "Take the Audit",
    # "Discovery Call", "Profit Audit" all read as CTAs to humans but slipped
    # the prior keyword list).
    "audit", "guide", "tour", "explore", "discover", "find out", "find my",
    "see how", "see if", "tell me", "yes", "i want", "next step", "next steps",
    "talk to", "talk with", "speak with", "reach out", "ask", "i'm in",
    "take the", "take my", "claim my", "claim your", "get the",
    "watch", "play", "listen", "read more", "preview",
    "estimate", "quote", "proposal", "pricing", "plans", "view plans",
    "discovery", "strategy session", "intro call", "kick-off",
})

# ── Form type signals and precedence ─────────────────────────────────────────
# contact > optin > booking > other

_FORM_SIGNALS: Dict[str, List[str]] = {
    "contact": ["contact", "get in touch", "send message", "reach out", "enquire"],
    "optin":   ["subscribe", "sign up", "newsletter", "opt in", "join",
                "get updates", "free guide", "lead magnet"],
    "booking": ["book", "schedule", "appointment", "reserve",
                "consultation", "discovery"],
}
_FORM_TYPE_ORDER = ["contact", "optin", "booking"]

# ── Platform detection signals ────────────────────────────────────────────────
# Precedence: wix, squarespace, webflow, shopify, wordpress, unknown.

_PLATFORM_SIGNALS = [
    ("wix",         ["wixstatic.com", "wix.com/_api", "wix-code",
                     "wix.viewer", "<!-- wix:site"]),
    ("squarespace", ["squarespace.com", "sqsp.net", "squarespace-cdn.com"]),
    ("webflow",     ["webflow.com", "webflow.io"]),
    ("shopify",     ["myshopify.com", "cdn.shopify.com", "shopifycdn.com"]),
    ("wordpress",   ["wp-content", "wp-includes", "wp-json"]),
]


# ═════════════════════════════════════════════════════════════════════════════
#  Page selection helpers
# ═════════════════════════════════════════════════════════════════════════════

def _head_ok(url: str, timeout: int = 3) -> bool:
    """Return True if URL responds HTTP < 400."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status < 400
    except urllib.error.HTTPError as exc:
        return exc.code < 400
    except Exception:
        return False


def _build_start_urls(
    base_url: str,
    sitemap_urls: Optional[List[str]],
) -> List[Dict]:
    """
    Build deterministic ordered startUrls list per page selection rule.

    Order:
      1. Homepage (always first)
      2. One URL per priority group (first path per group that HEAD-responds < 400)
      3. Sitemap URLs in sitemap order, deduped against already-added
    Total capped at _MAX_PAGES.

    Same site → same pages every run as long as site structure is unchanged.
    """
    base    = base_url.rstrip("/")
    ordered = [base_url]
    seen    = {base}

    for group in _PRIORITY_GROUPS:
        for path in group:
            candidate = base + path
            if _head_ok(candidate) and candidate.rstrip("/") not in seen:
                seen.add(candidate.rstrip("/"))
                ordered.append(candidate)
                break   # one URL per group; first hit wins

    for url in (sitemap_urls or []):
        if len(ordered) >= _MAX_PAGES:
            break
        norm = url.rstrip("/")
        if norm not in seen:
            seen.add(norm)
            ordered.append(url)

    return [{"url": u} for u in ordered[:_MAX_PAGES]]


# ═════════════════════════════════════════════════════════════════════════════
#  Apify HTTP layer
# ═════════════════════════════════════════════════════════════════════════════

def _apify_call(start_urls: List[Dict], api_key: str) -> List[Dict]:
    """POST to Apify sync endpoint. Raises on any failure."""
    payload = json.dumps({
        "startUrls":          start_urls,
        "maxCrawlPages":      _MAX_PAGES,
        "maxCrawlDepth":      _MAX_DEPTH,
        "crawlerType":        "playwright:chrome",
        "saveHtmlAsFile":     True,
        "proxyConfiguration": {"useApifyProxy": True},
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{_ACTOR_URL}?token={api_key}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_html_url(html_url: str) -> str:
    """
    Fetch rendered HTML from Apify KV store URL.
    One retry after _RETRY_WAIT seconds on failure.
    Raises on persistent failure — caller applies HOLD policy per BLOCKER 2.
    """
    last_exc: Optional[Exception] = None
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(html_url)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            last_exc = exc
            _log.warning(
                "apify_content: htmlUrl fetch attempt %d failed: %s — %s",
                attempt, html_url, exc,
            )
            if attempt == 1:
                time.sleep(_RETRY_WAIT)
    raise RuntimeError(f"htmlUrl fetch failed: {html_url}") from last_exc


# ═════════════════════════════════════════════════════════════════════════════
#  Platform detection
# ═════════════════════════════════════════════════════════════════════════════

def _detect_platform_hints(html: str) -> List[str]:
    """
    Detect platform from rendered homepage HTML.
    Returns list[str] — first element is primary platform for display.
    Precedence: wix, squarespace, webflow, shopify, wordpress, unknown.
    """
    if not html:
        return ["unknown"]
    html_l = html.lower()
    hits = [name for name, sigs in _PLATFORM_SIGNALS
            if any(s in html_l for s in sigs)]
    if "cloudflare" in html_l or "__cf_bm" in html_l:
        hits.append("cloudflare")
    return hits if hits else ["unknown"]


# ═════════════════════════════════════════════════════════════════════════════
#  HTML parsers — operate on full DOM; none mutate the soup object
# ═════════════════════════════════════════════════════════════════════════════

def _parse_headings(soup) -> List[Dict]:
    """Extract h1-h3 as [{"level": int, "text": str}]."""
    result = []
    for tag in soup.find_all(["h1", "h2", "h3"]):
        text = tag.get_text(" ", strip=True)
        if text:
            result.append({"level": int(tag.name[1]), "text": text})
    return result


def _parse_structured_data(soup) -> List[Dict]:
    """
    Extract all JSON-LD blocks. Returns list of fully parsed dicts.
    Handles single objects, arrays, and @graph wrappers.
    """
    results: List[Dict] = []
    for script in soup.find_all("script", type="application/ld+json"):
        raw = (script.string or "").strip().lstrip("﻿")
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(obj, list):
            results.extend(o for o in obj if isinstance(o, dict))
        elif isinstance(obj, dict):
            graph = obj.get("@graph")
            if isinstance(graph, list):
                results.extend(o for o in graph if isinstance(o, dict))
            else:
                results.append(obj)
    return results


def _classify_form(action: str, fields: List[str], btn: str) -> str:
    """Classify form type. Precedence: contact > optin > booking > other."""
    combined = " ".join([action, " ".join(fields), btn]).lower()
    for ftype in _FORM_TYPE_ORDER:
        if any(s in combined for s in _FORM_SIGNALS[ftype]):
            return ftype
    return "other"


def _parse_forms(soup, page_url: str) -> List[Dict]:
    """Extract form elements. Operates on full DOM — footer forms included."""
    forms = []
    for form in soup.find_all("form"):
        raw_action = form.get("action") or ""
        action = (
            urljoin(page_url, raw_action)
            if raw_action and not raw_action.startswith("http")
            else raw_action or None
        )

        fields: List[str] = []
        for inp in form.find_all(["input", "textarea", "select"]):
            if inp.get("type") in ("hidden", "submit", "button", "image", "reset"):
                continue
            label = (
                inp.get("placeholder")
                or inp.get("name")
                or inp.get("aria-label")
                or inp.get("id")
                or ""
            ).strip()
            if label:
                fields.append(label)

        btn_el = form.find(
            ["button", "input"],
            attrs={"type": lambda t: t in ("submit", "button", None)},
        )
        btn_text: Optional[str] = None
        if btn_el:
            btn_text = (
                btn_el.get_text(" ", strip=True) or btn_el.get("value") or None
            )

        forms.append({
            "action_url":  action,
            "fields":      fields,
            "button_text": btn_text,
            "form_type":   _classify_form(action or "", fields, btn_text or ""),
        })
    return forms


def _infer_cta_location(element) -> str:
    """Walk ancestors to infer section context for a CTA element."""
    for parent in element.parents:
        if not hasattr(parent, "get"):
            continue
        tag  = getattr(parent, "name", "") or ""
        cls  = " ".join(parent.get("class") or []).lower()
        role = (parent.get("role") or "").lower()
        if tag == "header" or "header" in cls:
            return "header"
        if tag == "footer" or "footer" in cls:
            return "footer"
        if "hero" in cls or "banner" in cls:
            return "hero"
        if "sidebar" in cls or role == "complementary":
            return "sidebar"
        if tag in ("main", "article", "section") or "main" in cls:
            return "main"
    return "main"


def _parse_ctas(soup) -> List[Dict]:
    """
    Extract CTA buttons and links from full DOM — sitewide CTAs included.
    Deduplicates by lowercased text prefix. Capped at 20 per page.
    """
    ctas: List[Dict] = []
    seen: set         = set()

    for btn in soup.find_all("button"):
        text = btn.get_text(" ", strip=True)
        if not text or len(text) > 80:
            continue
        if any(kw in text.lower() for kw in _CTA_KWS):
            key = text.lower()[:40]
            if key not in seen:
                seen.add(key)
                ctas.append({
                    "text":         text,
                    "href":         None,
                    "location":     _infer_cta_location(btn),
                    "element_type": "button",
                })

    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        if not text or len(text) > 80:
            continue
        if any(kw in text.lower() for kw in _CTA_KWS):
            key = text.lower()[:40]
            if key not in seen:
                seen.add(key)
                ctas.append({
                    "text":         text,
                    "href":         a.get("href"),
                    "location":     _infer_cta_location(a),
                    "element_type": "link",
                })

    return ctas[:20]


def _parse_images(soup, page_url: str) -> List[Dict]:
    """
    Extract img elements from full DOM.
    alt=None when attribute is absent (truly missing).
    alt="" when present but empty (decorative). Never fabricated.
    """
    images = []
    for img in soup.find_all("img", src=True):
        src = img.get("src", "").strip()
        if not src or src.startswith("data:"):
            continue
        if not src.startswith(("http://", "https://")):
            src = urljoin(page_url, src)
        images.append({
            "src":      src,
            "alt":      img.get("alt"),
            "page_url": page_url,
        })
    return images


def _parse_internal_links(soup, page_url: str, domain: str) -> List[Dict]:
    """Extract internal links from full DOM. Deduplicates by to_url."""
    links = []
    seen: set = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        full   = urljoin(page_url, href)
        parsed = urlparse(full)
        if parsed.netloc and parsed.netloc != domain:
            continue
        if full in seen:
            continue
        seen.add(full)
        links.append({
            "from_url":    page_url,
            "to_url":      full,
            "anchor_text": a.get_text(" ", strip=True) or None,
        })
    return links


def _parse_external_links(soup, page_url: str, domain: str) -> List[Dict]:
    """Extract OFF-domain links (Maps, social, review pages, etc.) from full
    DOM. Required by GBP/social auditors to detect signals like a Google
    review CTA or Maps embed that live on inner pages — not just the
    homepage. Deduplicates by to_url."""
    links = []
    seen: set = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        full   = urljoin(page_url, href)
        parsed = urlparse(full)
        if not parsed.netloc or parsed.netloc == domain:
            continue
        if full in seen:
            continue
        seen.add(full)
        links.append({
            "from_url":    page_url,
            "to_url":      full,
            "anchor_text": a.get_text(" ", strip=True) or None,
        })
    return links


# ═════════════════════════════════════════════════════════════════════════════
#  Blog item detection
# ═════════════════════════════════════════════════════════════════════════════

def _is_blog_item(url: str, struct_data: List[Dict]) -> bool:
    """
    True if URL matches a blog path segment OR JSON-LD @type is a blog/article
    type. JSON-LD check is the authoritative secondary signal — a page with
    @type BlogPosting is a blog post regardless of URL structure.
    """
    if any(seg in url.lower() for seg in _BLOG_SEGS):
        return True
    for obj in struct_data:
        t     = obj.get("@type")
        types: List[str] = (
            [t] if isinstance(t, str) else (t if isinstance(t, list) else [])
        )
        if any(bt in _BLOG_TYPES for bt in types):
            return True
    return False


# ═════════════════════════════════════════════════════════════════════════════
#  Page text extraction — separate fresh parse so headings are excluded cleanly
# ═════════════════════════════════════════════════════════════════════════════

def _extract_text(html: str, url: str = "") -> Optional[str]:
    """
    Canonical text source per Decision 5(A). BS4 over apify_text.

    Extracts main content text from rendered HTML.
    Headings go in headings[] per contract — removed here before extraction.
    Tries <main> first; falls back to <body>.
    Strips nav, footer, scripts, cookie banners, and dialogs.
    """
    if not html or not _BS4_OK:
        return None
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.find_all(["nav", "footer", "script", "style", "noscript"]):
        tag.decompose()
    for el in soup.select(".cookie-banner, .modal"):
        el.decompose()
    for el in soup.find_all(attrs={"role": "dialog"}):
        el.decompose()

    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        tag.decompose()

    root = soup.find("main") or soup.find("body") or soup
    if not root or not list(root.descendants):
        _log.warning(
            "apify_content: _extract_text found no usable root — url=%s", url
        )
        return None

    text = root.get_text(separator=" ", strip=True)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text if text else None


# ═════════════════════════════════════════════════════════════════════════════
#  Blog post signals
#  Nullability rule: None when not detected. Never fabricated.
# ═════════════════════════════════════════════════════════════════════════════

def _extract_published(soup, struct_data: List[Dict]) -> Optional[str]:
    """
    Return ISO8601 published date or None.
    Priority: JSON-LD datePublished → <time datetime> → OG article:published_time.
    Never uses today's date as fallback.
    """
    for obj in struct_data:
        pub = obj.get("datePublished") or obj.get("dateCreated")
        if pub and isinstance(pub, str):
            return pub.strip()

    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        dt = (time_el.get("datetime") or "").strip()
        if dt:
            return dt

    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta:
        content = (meta.get("content") or "").strip()
        if content:
            return content

    return None


def _extract_author(soup, struct_data: List[Dict]) -> Optional[str]:
    """
    Return author name or None.
    Priority: JSON-LD author.name → <meta name="author">.
    Never fabricated.
    """
    for obj in struct_data:
        author = obj.get("author")
        if isinstance(author, dict):
            name = (author.get("name") or "").strip()
            if name:
                return name
        elif isinstance(author, str) and author.strip():
            return author.strip()
        elif isinstance(author, list):
            for a in author:
                if isinstance(a, dict):
                    name = (a.get("name") or "").strip()
                    if name:
                        return name

    meta = soup.find("meta", attrs={"name": "author"})
    if meta:
        content = (meta.get("content") or "").strip()
        if content:
            return content

    return None


# ═════════════════════════════════════════════════════════════════════════════
#  Public entry point
# ═════════════════════════════════════════════════════════════════════════════

def fetch(base_url: str, sitemap_urls: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Probe 2: JS-rendered content crawl via Apify website-content-crawler.

    Returns Probe 2 data contract dict on success.
    Raises RuntimeError whose message starts with "apify_failed" on
    unrecoverable failure. Caller (website_auditor.py step 5) must catch
    and trigger HOLD — see module docstring for exact log format and sequence.

    Args:
        base_url:     Client website homepage URL.
        sitemap_urls: Optional list of URLs from Probe 1 sitemap parse.
                      Used to fill slots after priority paths up to _MAX_PAGES.
    """
    api_key = os.environ.get("APIFY_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("apify_failed — APIFY_API_KEY not set")
    if not _BS4_OK:
        raise RuntimeError("apify_failed — beautifulsoup4 not installed")

    # ── Page selection (deterministic) ────────────────────────────────────────
    start_urls = _build_start_urls(base_url, sitemap_urls)
    _log.info(
        "apify_content: startUrls=%d for %s — %s",
        len(start_urls), base_url,
        [d["url"] for d in start_urls],
    )

    # ── Actor call — one retry after _RETRY_WAIT seconds ─────────────────────
    last_exc: Optional[Exception] = None
    items: Optional[List[Dict]]   = None

    for attempt in (1, 2):
        try:
            items = _apify_call(start_urls, api_key)
            _log.info(
                "apify_content: actor attempt %d succeeded — %d items for %s",
                attempt, len(items), base_url,
            )
            break
        except Exception as exc:
            last_exc = exc
            _log.warning(
                "apify_content: actor attempt %d failed for %s — %s",
                attempt, base_url, exc,
            )
            if attempt == 1:
                time.sleep(_RETRY_WAIT)

    if items is None:
        raise RuntimeError("apify_failed") from last_exc

    if not isinstance(items, list) or not items:
        raise RuntimeError(
            f"apify_failed — empty or malformed response for {base_url}"
        )

    # ── Post-process dataset items ────────────────────────────────────────────
    domain      = urlparse(base_url).netloc
    pages:      List[Dict] = []
    blog_posts: List[Dict] = []
    all_links:  List[Dict] = []
    # Accumulate platform hints across every crawled page — a CDN-cached or
    # JS-stripped homepage often hides Wix/Squarespace fingerprints that show
    # up on inner pages (blog posts, /about, /contact). Only fall back to
    # "unknown" if no page across the entire crawl shows a signal.
    platform_hint_set: set = set()
    max_depth              = 0

    for item in items:
        url = (item.get("url") or item.get("loadedUrl") or "").strip()
        if not url:
            continue

        title      = item.get("title") or None
        metadata   = item.get("metadata") or {}
        meta_desc  = metadata.get("description") or None
        crawl_meta = item.get("crawl") or {}
        depth      = int(crawl_meta.get("depth") or 0)
        if depth > max_depth:
            max_depth = depth

        is_homepage = url.rstrip("/") == base_url.rstrip("/")

        # ── Fetch rendered HTML ───────────────────────────────────────────────
        # Homepage failure → HOLD. Inner-page failure → skip entirely.
        # Never append a page with empty structural fields.
        html_url = (item.get("htmlUrl") or "").strip()

        if not html_url:
            if is_homepage:
                raise RuntimeError(
                    f"apify_failed — homepage htmlUrl missing: {url}"
                )
            _log.warning("apify_content: no htmlUrl for %s — skipping", url)
            continue

        try:
            html = _fetch_html_url(html_url)
        except Exception as exc:
            if is_homepage:
                raise RuntimeError(
                    f"apify_failed — homepage htmlUrl fetch failed: {url}"
                ) from exc
            _log.warning(
                "apify_content: skipping %s — htmlUrl fetch failed: %s", url, exc
            )
            continue

        # ── Parse — structural parsers use full DOM ───────────────────────────
        soup = BeautifulSoup(html, "html.parser")

        # Union platform hints from every page — first hit wins for ordering
        # but we keep collecting so a homepage that drops the fingerprint
        # doesn't leave the audit with platform="unknown" when later pages
        # clearly identify it.
        for hint in _detect_platform_hints(html):
            if hint != "unknown":
                platform_hint_set.add(hint)

        headings    = _parse_headings(soup)
        struct_data = _parse_structured_data(soup)
        int_links   = _parse_internal_links(soup, url, domain)
        all_links.extend(int_links)

        # ── Route to blog_posts or pages ──────────────────────────────────────
        if _is_blog_item(url, struct_data):
            blog_posts.append({
                "title":     title,
                "body":      _extract_text(html, url),
                "published": _extract_published(soup, struct_data),
                "author":    _extract_author(soup, struct_data),
                "url":       url,
            })
        else:
            pages.append({
                "url":              url,
                "title":            title,
                "text":             _extract_text(html, url),
                "headings":         headings,
                "meta_description": meta_desc,
                "structured_data":  struct_data,
                "forms":            _parse_forms(soup, url),
                "ctas":             _parse_ctas(soup),
                "images":           _parse_images(soup, url),
                # External hrefs feed the GBP/social signal-upgrade pass so
                # auditors can detect a Maps link, Google review CTA, or
                # social profile that lives on an inner page (e.g. /about)
                # rather than the homepage.
                "external_links":   _parse_external_links(soup, url, domain),
                # Iframe srcs feed GBP Maps-embed detection on inner pages.
                "iframe_srcs":      [
                    iframe.get("src", "").strip()
                    for iframe in soup.find_all("iframe", src=True)
                    if iframe.get("src", "").strip()
                ],
            })

    # Order hints by precedence so the first element is the primary platform
    # for display. Falls back to ["unknown"] only when no page across the
    # entire crawl matched any signature.
    _PRECEDENCE = ["wix", "squarespace", "webflow", "shopify", "wordpress",
                   "cloudflare"]
    if platform_hint_set:
        platform_hints = ([p for p in _PRECEDENCE if p in platform_hint_set]
                          + sorted(platform_hint_set
                                   - set(_PRECEDENCE)))
    else:
        platform_hints = ["unknown"]

    _log.info(
        "apify_content: %s — pages=%d  blog_posts=%d  links=%d  "
        "platform=%s  depth=%d",
        base_url, len(pages), len(blog_posts), len(all_links),
        platform_hints, max_depth,
    )

    return {
        "pages":          pages,
        "blog_posts":     blog_posts,
        "internal_links": all_links,
        "crawl_depth":    max_depth,
        "platform_hints": platform_hints,
        "data_source":    "apify_content_crawler",
        "crawled_at":     datetime.now(timezone.utc).isoformat(),
    }
