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

_APIFY_ACTOR_URL = (
    "https://api.apify.com/v2/acts/harvestapi~linkedin-company-posts"
    "/run-sync-get-dataset-items"
)


def _normalize_linkedin_url(url: str) -> str:
    """Strip /admin/, /feed/, query strings, fragments — return canonical company URL."""
    if not url:
        return url
    m = _LI_COMPANY_RE.match(url)
    if not m:
        return url
    return m.group(1) + "/"


def _is_valid_linkedin_company_url(url: str) -> bool:
    """
    True if URL is a valid PUBLIC LinkedIn company page URL.
    Rejects: /admin/ paths, /in/ personal profiles, numeric-ID-only
    company URLs, slugs shorter than 3 chars, missing matches.

    Used at the auto-discovery boundary in
    intake/questionnaire.py:_classified_to_platforms to prevent
    bad URLs (e.g., /company/12628998/admin/ pasted into a client's
    website by mistake) from reaching the scraper and producing
    misleading "data unavailable" output downstream.
    """
    if not url:
        return False
    if "/admin" in url.lower():
        return False
    m = _LI_COMPANY_RE.match(url)
    if not m:
        return False
    slug = m.group(1).rstrip("/").rsplit("/", 1)[-1]
    if slug.isdigit():
        return False
    if len(slug) < 3:
        return False
    return True


def _apify_enrich(linkedin_url: str, result: Dict[str, Any]) -> None:
    """
    Fetches post data via Apify harvestapi/linkedin-company-posts (sync run, max 10 posts).
    Computes post cadence and engagement from returned metadata.
    Follower count is not available from this actor and remains None.
    Mutates `result` in-place.
    """
    api_key = os.environ.get("APIFY_API_KEY", "")
    if not api_key:
        raise RuntimeError("apify_enrich: APIFY_API_KEY not set")

    payload = json.dumps({
        "url":   _normalize_linkedin_url(linkedin_url),
        "count": 10,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{_APIFY_ACTOR_URL}?token={api_key}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        posts = json.loads(resp.read().decode("utf-8"))

    if not isinstance(posts, list) or not posts:
        raise RuntimeError(
            f"apify_enrich: empty or malformed response for {linkedin_url}"
        )

    dates: List[datetime] = []
    headlines: List[str]  = []
    total_likes = total_comments = total_reactions = 0

    for post in posts:
        pub = post.get("pubDate") or post.get("publishedAt") or post.get("date")
        if pub:
            try:
                dt = datetime.fromisoformat(
                    str(pub).replace("Z", "+00:00")
                ).replace(tzinfo=timezone.utc)
                dates.append(dt)
            except ValueError:
                pass

        total_likes     += int(post.get("likes",     0) or 0)
        total_comments  += int(post.get("comments",  0) or 0)
        total_reactions += int(post.get("reactions", 0) or 0)

        text = post.get("text") or post.get("content") or post.get("title") or ""
        if len(text) >= 10:
            headlines.append(text[:120])

    dates.sort(reverse=True)
    now = datetime.now(timezone.utc)

    if dates:
        days_since = (now - dates[0]).days
        result.update({
            "days_since_last_post": days_since,
            "posts_per_week":       _posts_per_week(dates),
            "is_active":            days_since <= 30,
            "post_dates":           [d.strftime("%Y-%m-%d") for d in dates],
        })

    if headlines:
        result["recent_headlines"] = headlines[:5]

    n = len(posts)
    if n:
        result["avg_likes"]     = round(total_likes     / n, 1)
        result["avg_comments"]  = round(total_comments  / n, 1)
        result["avg_reactions"] = round(total_reactions / n, 1)

    result["data_source"] = "apify_linkedin_posts"


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

    if result["data_source"] == "linkedin_reachable":
        try:
            _apify_enrich(base, result)
        except Exception as exc:
            _log.warning(
                "apify_enrich failed for %s — falling back: %s", base, exc
            )
            result["data_source"] = "linkedin_reachable_fallback"

    _log.info(
        "linkedin_scrape_result url=%s data_source=%s followers=%s",
        base,
        result.get("data_source"),
        result.get("followers"),
    )
    return result
