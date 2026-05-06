"""
Google search lookup — best-effort profile discovery.

Per Dave 2026-05-06: when the website crawl fails to surface a LinkedIn /
Facebook / GBP profile and intake didn't provide one, the report has been
emitting "no LinkedIn presence" findings on businesses that genuinely have
LinkedIn — they just haven't linked it from their footer. This module does
a single Google search for the business name plus the target domain and
returns the first matching profile URL.

Failure modes
-------------
Google rate-limits / serves a CAPTCHA roughly 30-50% of the time on direct
unauthenticated GETs. This module always fails silently — it never raises,
never penalises any score, and returns "" when nothing is found. It is a
*supplementary* signal layered on top of the website crawl, not a primary
data source.

Usage
-----
    from auditors import google_lookup
    li_url = google_lookup.find_profile("Swift Profit Systems", "linkedin")
    fb_url = google_lookup.find_profile("Swift Profit Systems", "facebook")
    gbp    = google_lookup.find_profile("Swift Profit Systems", "gbp")
"""
import logging
import re
import urllib.parse
from typing import Optional

try:
    import requests
    _OK = True
except ImportError:
    _OK = False

log = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Per-platform: (search-query suffix, URL pattern that confirms a real profile)
_PLATFORM_SPECS = {
    "linkedin": (
        "site:linkedin.com",
        re.compile(r"linkedin\.com/(?:in|company|school)/[A-Za-z0-9_\-\.%]+", re.I),
    ),
    "facebook": (
        "site:facebook.com",
        re.compile(r"facebook\.com/(?!sharer|tr|plugins|permalink|story|hashtag)"
                   r"[A-Za-z0-9_\-\.%]+", re.I),
    ),
    "instagram": (
        "site:instagram.com",
        re.compile(r"instagram\.com/(?!p/|reel/|stories/|tv/|explore/)"
                   r"[A-Za-z0-9_\-\.%]+", re.I),
    ),
    "twitter": (
        "site:twitter.com OR site:x.com",
        re.compile(r"(?:twitter\.com|x\.com)/(?!share|intent|hashtag|search|home)"
                   r"[A-Za-z0-9_]+", re.I),
    ),
    "youtube": (
        "site:youtube.com",
        re.compile(r"youtube\.com/(?:@[A-Za-z0-9_\-]+|c/[A-Za-z0-9_\-]+|"
                   r"channel/UC[A-Za-z0-9_\-]+|user/[A-Za-z0-9_\-]+)", re.I),
    ),
    "gbp": (
        "site:google.com/maps/place",
        re.compile(r"google\.com/maps/place/[^\"'\s<>]+", re.I),
    ),
}


def find_profile(business_name: str, platform: str,
                 timeout: int = 10) -> str:
    """
    Search Google for `<business_name> site:<platform>` and return the first
    profile URL that matches the platform's confirming pattern. Returns ""
    on any failure (rate-limit, CAPTCHA, network, no result, etc.) — the
    pipeline must never block on this lookup.
    """
    if not _OK or not business_name:
        return ""
    spec = _PLATFORM_SPECS.get(platform.lower())
    if not spec:
        return ""
    suffix, pattern = spec

    query = f'"{business_name}" {suffix}'
    url   = f"https://www.google.com/search?q={urllib.parse.quote_plus(query)}"

    try:
        r = requests.get(url, headers=_HEADERS, timeout=timeout)
        if r.status_code != 200 or not r.text:
            log.info("google_lookup: %s (%s) — status=%d, no result",
                     platform, business_name, r.status_code)
            return ""
        html = r.text

        # Skip if Google served a consent / CAPTCHA / no-results page
        if "Our systems have detected" in html or "captcha" in html.lower()[:8000]:
            log.info("google_lookup: %s (%s) — captcha/consent wall",
                     platform, business_name)
            return ""

        for match in pattern.finditer(html):
            candidate = match.group(0)
            # Strip Google's URL-rewrite prefix if present
            candidate = candidate.lstrip("/")
            # Reject the share / login pages that occasionally slip through
            lower = candidate.lower()
            if any(skip in lower for skip in (
                "/share", "/login", "/help", "/about", "policies",
            )):
                continue
            log.info("google_lookup: %s found for %r → %s",
                     platform, business_name, candidate)
            # Normalise — prepend https:// if missing
            if not candidate.startswith(("http://", "https://")):
                candidate = "https://" + candidate
            return candidate
    except Exception as exc:
        log.info("google_lookup: %s (%s) failed silently: %s",
                 platform, business_name, exc)
    return ""


def discover_missing_socials(business_name: str,
                             config_obj,
                             platforms: Optional[list] = None) -> dict:
    """
    For each platform in `platforms` whose corresponding config attribute is
    empty, run a Google profile lookup and update the config in place.
    Returns a dict of {platform: discovered_url} for the entries that were
    populated. Safe to call when requests is missing — returns {}.
    """
    if not _OK or not business_name:
        return {}
    targets = platforms or ["linkedin", "facebook", "instagram"]
    populated: dict = {}

    for plat in targets:
        attr = {
            "linkedin":  "linkedin_url",
            "facebook":  "facebook_page_url",
            "instagram": "instagram_handle",
            "twitter":   "twitter_handle",
            "youtube":   "youtube_channel_url",
        }.get(plat)
        if not attr:
            continue
        existing = getattr(config_obj, attr, "") or ""
        if existing:
            continue
        url = find_profile(business_name, plat)
        if not url:
            continue
        # For handle-style fields, extract just the handle from the URL
        if attr in ("instagram_handle", "twitter_handle"):
            m = re.search(r"\.(?:com|net)/(?:@?)([A-Za-z0-9_]+)", url)
            value = ("@" + m.group(1)) if m else url
        else:
            value = url
        setattr(config_obj, attr, value)
        populated[plat] = value
        log.info("google_lookup: populated config.%s = %r", attr, value)

    return populated
