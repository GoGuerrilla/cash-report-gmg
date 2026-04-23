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
import re
import random
import time
import urllib.request
import urllib.error
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

_ISO_RE      = re.compile(r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})')
_HEADLINE_RE = re.compile(r'"headline"\s*:\s*"([^"]{10,120})"')
_FOLLOWER_RE = re.compile(r'([\d,]+)\s*follower', re.I)
_TIMEOUT     = 15


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
    base = linkedin_url.rstrip("/")
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

    return result
