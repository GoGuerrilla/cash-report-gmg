"""
LinkedIn Company Page Scraper
Extracts post timestamps and follower count from the public company page
HTML without requiring any API key or authentication.

LinkedIn embeds JSON-LD structured data (<script type="application/ld+json">)
containing `datePublished` for each visible post and follower count in the
page's og/meta tags. This works for public company pages served to
unauthenticated visitors.

Returns a dict compatible with preloaded_channel_data["linkedin"]:
  {
    "followers":            int | None,
    "posts_per_week":       float | None,
    "days_since_last_post": int | None,
    "is_active":            bool | None,
    "recent_headlines":     list[str],
    "post_dates":           list[str],   # ISO date strings
    "data_source":          str,
  }
"""
import logging
import os
import re
import json
import random
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional


# Try these user agents in order — LinkedIn sometimes blocks one but not another
_USER_AGENTS = [
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"
    ),
    "LinkedInBot/1.0 (compatible; Mozilla/5.0; Apache-HttpClient)",
]

_ISO_RE          = re.compile(r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})')
_HEADLINE_RE     = re.compile(r'"headline"\s*:\s*"([^"]{10,120})"')
_FOLLOWER_RE     = re.compile(r'([\d,]+)\s*follower', re.I)
_LI_COMPANY_RE   = re.compile(r'(https?://(?:www\.)?linkedin\.com/company/[^/?#]+)')
_TIMEOUT         = 15

_log = logging.getLogger(__name__)

_PROXYCURL_URL = "https://nubela.co/proxycurl/api/v2/linkedin/company"

_PROXYCURL_FALLBACK: Dict[str, Any] = {
    "followers":      None,
    "company_size":   None,
    "founded_year":   None,
    "employee_count": None,
}


def _normalize_linkedin_url(url: str) -> str:
    """Strip /admin/, /feed/, query strings, fragments — return canonical company URL."""
    if not url:
        return url
    m = _LI_COMPANY_RE.match(url)
    if not m:
        return url
    return m.group(1) + "/"


def _proxycurl_enrich(linkedin_url: str, result: Dict[str, Any]) -> None:
    """
    Backfills follower_count, company_size, founded_year, employee_count via
    Proxycurl when the HTML scrape loaded the page but couldn't find followers.
    Mutates `result` in-place. Falls back to None values + flag on any failure.
    """
    api_key = os.environ.get("PROXYCURL_API_KEY", "")
    if not api_key:
        result.update(_PROXYCURL_FALLBACK)
        result["data_source"] = "linkedin_reachable_fallback"
        return

    params = urllib.parse.urlencode({"url": _normalize_linkedin_url(linkedin_url)})
    req = urllib.request.Request(
        f"{_PROXYCURL_URL}?{params}",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        follower_count = data.get("follower_count")
        company_size   = data.get("company_size")
        founded_year   = data.get("founded_year")
        employee_count = data.get("employee_count")

        if follower_count is not None:
            result["followers"] = int(follower_count)

        if isinstance(company_size, dict):
            s, e = company_size.get("start"), company_size.get("end")
            result["company_size"] = f"{s}–{e}" if s and e else str(s or e or "")
        elif company_size:
            result["company_size"] = str(company_size)

        if founded_year is not None:
            result["founded_year"] = int(founded_year)

        if employee_count is not None:
            result["employee_count"] = int(employee_count)

        result["data_source"] = "proxycurl"

    except Exception:
        result.update(_PROXYCURL_FALLBACK)
        result["data_source"] = "linkedin_reachable_fallback"


def _fetch(url: str) -> Optional[str]:
    """Attempt to fetch a URL, trying multiple user agents."""
    time.sleep(random.uniform(2, 3))
    for ua in _USER_AGENTS:
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent":                ua,
                    "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language":           "en-US,en;q=0.5",
                    "DNT":                       "1",
                    "Connection":                "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest":            "document",
                    "Sec-Fetch-Mode":            "navigate",
                    "Sec-Fetch-Site":            "none",
                    "Sec-Fetch-User":            "?1",
                    "Cache-Control":             "max-age=0",
                },
            )
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                html = resp.read().decode("utf-8", errors="replace")
                if len(html) > 5000:   # reject empty/redirect shells
                    return html
        except (urllib.error.URLError, OSError):
            continue
    return None


def _parse_dates(html: str) -> List[datetime]:
    """Extract and parse all datePublished timestamps from JSON-LD."""
    raw  = _ISO_RE.findall(html)
    seen = set()
    dts  = []
    for s in raw:
        if s not in seen:
            seen.add(s)
            try:
                dts.append(datetime.fromisoformat(s).replace(tzinfo=timezone.utc))
            except ValueError:
                pass
    dts.sort(reverse=True)   # newest first
    return dts


def _parse_followers(html: str) -> Optional[int]:
    m = _FOLLOWER_RE.search(html)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_headlines(html: str) -> List[str]:
    return list(dict.fromkeys(_HEADLINE_RE.findall(html)))[:5]


def _posts_per_week(dates: List[datetime], window_days: int = 30) -> Optional[float]:
    """
    Count posts within `window_days` and convert to a weekly rate.
    Needs at least 2 posts to compute a meaningful rate.
    """
    if not dates:
        return None
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    recent = [d for d in dates if d >= cutoff]
    if len(recent) < 2:
        # Only 1 post in window — can't compute rate; return 1/week as floor
        return 1.0 if recent else None
    # Use the span between newest and oldest recent post
    span_days = max((recent[0] - recent[-1]).days, 1)
    return round(len(recent) / (span_days / 7), 1)


def scrape(linkedin_url: str) -> Dict[str, Any]:
    """
    Main entry point. Accepts a LinkedIn company page URL.
    Returns a dict compatible with preloaded_channel_data["linkedin"].
    """
    base = _normalize_linkedin_url(linkedin_url).rstrip("/")
    result: Dict[str, Any] = {
        "followers":            None,
        "posts_per_week":       None,
        "days_since_last_post": None,
        "is_active":            None,
        "recent_headlines":     [],
        "post_dates":           [],
        "data_source":          "scrape_failed",
        "content_topics":       [],
        "post_themes":          [],
        "services_listed":      [],
        "engagement_level":     None,
        "company_size":         None,
        "founded_year":         None,
        "employee_count":       None,
    }

    # Try overview page (embeds the most posts in JSON-LD)
    html = _fetch(base) or _fetch(base + "/posts/?feedView=all")
    if not html:
        result["is_active"] = None
        return result

    dates     = _parse_dates(html)
    followers = _parse_followers(html)
    headlines = _parse_headlines(html)

    now = datetime.now(timezone.utc)

    if dates:
        days_since = (now - dates[0]).days
        ppw        = _posts_per_week(dates)
        result.update({
            "days_since_last_post": days_since,
            "posts_per_week":       ppw,
            "is_active":            days_since <= 30,
            "post_dates":           [d.strftime("%Y-%m-%d") for d in dates],
            "data_source":          "linkedin_html",
        })
    else:
        # Page loaded but no post dates found — mark reachable but unverified
        result["is_active"]   = True
        result["data_source"] = "linkedin_reachable"

    if followers is not None:
        result["followers"] = followers

    if headlines:
        result["recent_headlines"] = headlines

    if result["data_source"] == "linkedin_reachable" and result["followers"] is None:
        _proxycurl_enrich(base, result)

    _log.info(
        "linkedin_scrape_result url=%s data_source=%s followers=%s",
        base,
        result.get("data_source"),
        result.get("followers"),
    )
    return result
