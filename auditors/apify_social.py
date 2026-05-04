"""
auditors/apify_social.py — social platform scrapers via Apify.

Phase 1: profile-level data per platform. Each fetch_<platform>() function takes
a handle/URL and returns a normalized dict of follower count, post cadence, and
recent posts. Raises RuntimeError("apify_social_failed — <platform>: <reason>")
on unrecoverable failure so callers can fall back gracefully (per Push 4 pattern:
positive evidence wins, missing data does not pretend to be a confirmed negative).

Cost: ~$0.05-0.20 per platform per audit. Caller MUST skip when no handle/URL is
available — no point spending Apify budget on empty inputs.

Actor slugs (Phase 1, approved 2026-05-03):
  Instagram   apify/instagram-scraper
  TikTok      clockworks/tiktok-scraper
  X / Twitter apidojo/twitter-scraper-lite
  Facebook    apify/facebook-posts-scraper
  FB Comments apify/facebook-comments-scraper        (Phase 2 — not wired yet)

Output shape (all fetchers normalize to this):
    {
      "platform":            "Instagram" | "TikTok" | "X" | "Facebook",
      "handle":              "@username" or page slug,
      "url":                 canonical profile URL,
      "followers":           int or None,
      "post_count":          int or None,
      "recent_posts":        list of {url, text, published, engagement, ...},
      "posts_per_week":      float or None,
      "days_since_last_post": int or None,
      "bio":                 str (may be empty),
      "data_source":         "apify_<actor_slug>",
      "scraped_at":          ISO8601 UTC,
      "raw_count":           int — count of items returned by actor,
    }
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Actor slugs — Apify URL form uses ~ instead of /
# ─────────────────────────────────────────────────────────────────────────────

_ACTOR_INSTAGRAM           = "apify~instagram-scraper"
_ACTOR_IG_FOLLOWERS        = "apify~instagram-followers-count-scraper"
_ACTOR_IG_POSTS            = "apify~instagram-post-scraper"
_ACTOR_TIKTOK              = "clockworks~tiktok-scraper"
# Replaced apidojo/twitter-scraper-lite (returned only {'demo': ...} placeholder)
# and then apidojo/twitter-user-scraper (also returned demo) with mikolabs/
# tweets-x-scraper — Nitter-backed scraper Dave verified working 2026-05-03 21:51.
_ACTOR_TWITTER             = "mikolabs~tweets-x-scraper"
_ACTOR_TWITTER_FOLLOWERS   = "kaitoeasyapi~premium-x-follower-scraper-following-data"
_ACTOR_FB_POSTS            = "apify~facebook-posts-scraper"
_ACTOR_FB_FOLLOWERS        = "apify~facebook-followers-following-scraper"
_ACTOR_FB_COMMENTS         = "apify~facebook-comments-scraper"  # Phase 2
_ACTOR_LINKEDIN_FOLLOWERS  = "data_link_miner~linkedin-company-followers-scraper"
_ACTOR_YOUTUBE             = "streamers~youtube-scraper"

# ─────────────────────────────────────────────────────────────────────────────
# Call parameters
# ─────────────────────────────────────────────────────────────────────────────

_RESULTS_LIMIT = 25     # Per Dave 2026-05-03: 25 posts is enough for cadence/engagement signal
_TIMEOUT       = 90     # s — most social actors complete in 30-60s
_RETRY_WAIT    = 10     # s — wait before single retry on failure


# ─────────────────────────────────────────────────────────────────────────────
# HTTP layer
# ─────────────────────────────────────────────────────────────────────────────

def _api_key() -> str:
    key = os.environ.get("APIFY_API_KEY", "").strip()
    if not key:
        raise RuntimeError("apify_social_failed — APIFY_API_KEY not set")
    return key


def _apify_call(actor_slug: str, payload: dict, api_key: str) -> List[Dict]:
    """POST to Apify run-sync-get-dataset-items endpoint. Returns dataset items."""
    url = (
        f"https://api.apify.com/v2/acts/{actor_slug}"
        f"/run-sync-get-dataset-items?token={api_key}"
    )
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _retry_call(actor_slug: str, payload: dict, label: str) -> List[Dict]:
    """One retry on failure. Returns items or raises RuntimeError."""
    api_key = _api_key()
    last_exc: Optional[Exception] = None
    for attempt in (1, 2):
        try:
            items = _apify_call(actor_slug, payload, api_key)
            log.info(
                "apify_social: %s attempt %d → %d items",
                label, attempt, len(items),
            )
            return items
        except Exception as exc:
            last_exc = exc
            log.warning(
                "apify_social: %s attempt %d failed: %s",
                label, attempt, exc,
            )
            if attempt == 1:
                time.sleep(_RETRY_WAIT)
    raise RuntimeError(f"apify_social_failed — {label}") from last_exc


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log_schema_sample(label: str, items: List[Dict], missing_fields: List[str]) -> None:
    """
    When a normalizer can't extract expected fields, log a one-line schema
    sample so the next iteration knows which actor field names to use.
    Truncates the dump to keep log lines readable.
    """
    if not items or not missing_fields:
        return
    first = items[0] if isinstance(items[0], dict) else {}
    top_keys = sorted(first.keys()) if isinstance(first, dict) else []
    nested = {}
    # If actor stores author/profile data nested, surface those keys too
    for k in ("author", "user", "userInfo", "pageInfo", "ownerProfilePicUrl",
              "authorMeta", "metadata"):
        v = first.get(k) if isinstance(first, dict) else None
        if isinstance(v, dict):
            nested[k] = sorted(v.keys())[:15]
    log.info(
        "apify_social schema-sample [%s] missing=%s top_keys=%s nested=%s",
        label, missing_fields, top_keys[:25], nested,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Cadence helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_iso(value: Any) -> Optional[datetime]:
    """Best-effort ISO8601 parse. Returns None on failure."""
    if not value:
        return None
    try:
        s = str(value).replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def _cadence_from_dates(published_dates: List[datetime]) -> Dict[str, Optional[float]]:
    """
    Compute posts_per_week + days_since_last_post from a list of post dates.
    Returns {"posts_per_week": float, "days_since_last_post": int} or None values.
    """
    valid = [d for d in published_dates if d is not None]
    if not valid:
        return {"posts_per_week": None, "days_since_last_post": None}

    now = datetime.now(timezone.utc)
    valid.sort(reverse=True)  # newest first
    most_recent = valid[0]
    days_since = max(0, (now - most_recent).days)

    # Derive ppw from the span between oldest and most recent post in the sample
    if len(valid) > 1:
        oldest = valid[-1]
        span_days = max(1, (most_recent - oldest).days)
        ppw = round(len(valid) / span_days * 7, 2)
    else:
        # Only one post sampled — can't compute frequency reliably
        ppw = None

    return {"posts_per_week": ppw, "days_since_last_post": days_since}


# ─────────────────────────────────────────────────────────────────────────────
# Public fetchers — one per platform
# ─────────────────────────────────────────────────────────────────────────────

def fetch_instagram(handle_or_url: str) -> Dict[str, Any]:
    """
    Scrape an Instagram profile + recent posts via apify/instagram-scraper.

    Args:
        handle_or_url: '@goguerrilla' or 'goguerrilla' or full URL.
    """
    if not (handle_or_url or "").strip():
        raise RuntimeError("apify_social_failed — instagram: empty input")

    # Normalize to handle (strip @, trailing slash, URL prefix)
    raw = handle_or_url.strip().lstrip("@").rstrip("/")
    handle = raw.split("/")[-1] if "/" in raw else raw
    profile_url = f"https://www.instagram.com/{handle}/"

    payload = {
        "directUrls":  [profile_url],
        "resultsType": "posts",
        "resultsLimit": _RESULTS_LIMIT,
        "addParentData": True,    # include profile metadata alongside posts
    }
    items = _retry_call(_ACTOR_INSTAGRAM, payload, f"instagram:{handle}")
    return _normalize_instagram(items, handle, profile_url)


def fetch_tiktok(handle_or_url: str) -> Dict[str, Any]:
    """
    Scrape a TikTok profile + recent videos via clockworks/tiktok-scraper.
    """
    if not (handle_or_url or "").strip():
        raise RuntimeError("apify_social_failed — tiktok: empty input")

    raw = handle_or_url.strip().lstrip("@").rstrip("/")
    handle = raw.split("/")[-1] if "/" in raw else raw
    profile_url = f"https://www.tiktok.com/@{handle}"

    payload = {
        "profiles":             [handle],
        "resultsPerPage":       _RESULTS_LIMIT,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
    }
    items = _retry_call(_ACTOR_TIKTOK, payload, f"tiktok:{handle}")
    return _normalize_tiktok(items, handle, profile_url)


def fetch_twitter(handle_or_url: str) -> Dict[str, Any]:
    """
    Scrape an X (Twitter) profile + recent tweets via mikolabs/tweets-x-scraper.

    Verified input schema 2026-05-03 (per Dave's working test). Uses Nitter as
    the scraping backend with residential proxies for reliability. Setting
    scrapeProfileInfo=True so the response includes profile metadata
    (followers, post count, bio) alongside the tweet sample.
    """
    if not (handle_or_url or "").strip():
        raise RuntimeError("apify_social_failed — twitter: empty input")

    raw = handle_or_url.strip().lstrip("@").rstrip("/")
    handle = raw.split("/")[-1] if "/" in raw else raw
    # Strip any whitespace inside the handle just in case
    handle = handle.strip()
    profile_url = f"https://x.com/{handle}"

    payload = {
        "twitterHandles":        [handle],
        "scrapeProfileInfo":     True,
        "includeReplies":        False,
        "includeRetweets":       True,
        "includeNativeRetweets": True,
        "includeLinks":          False,
        "latestTweets":          False,
        "mediaOnly":             False,
        "onlyImages":            False,
        "onlyQuotes":            False,
        "onlyVerified":          False,
        "onlyVideos":            False,
        "safeSearch":            False,
        "useResidentialProxy":   True,
        "nitterInstance":        "https://nitter.net",
        "proxyConfiguration": {
            "useApifyProxy":     True,
            "apifyProxyGroups":  ["RESIDENTIAL"],
        },
    }
    items = _retry_call(_ACTOR_TWITTER, payload, f"twitter:{handle}")
    return _normalize_twitter(items, handle, profile_url)


def fetch_facebook_posts(page_url: str) -> Dict[str, Any]:
    """
    Scrape a Facebook Page + recent posts via apify/facebook-posts-scraper.
    Takes the full page URL (not just a handle) since FB pages have varied URL forms.
    """
    if not (page_url or "").strip():
        raise RuntimeError("apify_social_failed — facebook: empty input")

    page_url = page_url.strip().rstrip("/")
    page_slug = page_url.rsplit("/", 1)[-1] or page_url

    payload = {
        "startUrls":    [{"url": page_url}],
        "resultsLimit": _RESULTS_LIMIT,
    }
    items = _retry_call(_ACTOR_FB_POSTS, payload, f"facebook:{page_slug}")
    return _normalize_facebook(items, page_slug, page_url)


def fetch_instagram_followers(handle_or_url: str) -> Dict[str, Any]:
    """
    Dedicated IG follower-count fetcher via apify/instagram-followers-count-scraper.

    Use as a backfill when the main fetch_instagram() returned no follower count
    (the bundled scraper occasionally drops ownerFollowersCount on profiles
    where the post sample is small or the profile is private).

    Returns: {handle, followers, raw_count, data_source, scraped_at}
    """
    if not (handle_or_url or "").strip():
        raise RuntimeError("apify_social_failed — instagram_followers: empty input")

    raw = handle_or_url.strip().lstrip("@").rstrip("/")
    handle = raw.split("/")[-1] if "/" in raw else raw

    payload = {
        "usernames":      [handle],
        "instagramHandles": [handle],
    }
    items = _retry_call(_ACTOR_IG_FOLLOWERS, payload, f"instagram_followers:{handle}")

    first = items[0] if items else {}
    followers = (first.get("followers")
                 or first.get("followersCount")
                 or first.get("followerCount")
                 or first.get("followers_count")
                 or first.get("count") or None)

    if followers is None:
        _log_schema_sample(f"instagram_followers:{handle}", items, ["followers"])

    return {
        "handle":      f"@{handle}",
        "followers":   followers,
        "data_source": "apify_instagram_followers_count_scraper",
        "scraped_at":  _now_iso(),
        "raw_count":   len(items),
    }


def fetch_instagram_posts(handle_or_url: str) -> Dict[str, Any]:
    """
    IG post-detail fetcher via apify/instagram-post-scraper.

    Returns the standard recent_posts shape so the main fetch_instagram() can
    be supplemented when the bundled scraper returns thin post data.
    """
    if not (handle_or_url or "").strip():
        raise RuntimeError("apify_social_failed — instagram_posts: empty input")

    raw = handle_or_url.strip().lstrip("@").rstrip("/")
    handle = raw.split("/")[-1] if "/" in raw else raw
    profile_url = f"https://www.instagram.com/{handle}/"

    payload = {
        "directUrls":   [profile_url],
        "username":     [handle],
        "resultsLimit": _RESULTS_LIMIT,
    }
    items = _retry_call(_ACTOR_IG_POSTS, payload, f"instagram_posts:{handle}")

    recent_posts: List[Dict] = []
    pub_dates: List[datetime] = []
    for it in items[:_RESULTS_LIMIT]:
        if not isinstance(it, dict):
            continue
        published = it.get("timestamp") or it.get("takenAtTimestamp") or it.get("date")
        d = _parse_iso(published)
        if d:
            pub_dates.append(d)
        recent_posts.append({
            "url":        it.get("url") or it.get("postUrl"),
            "text":       it.get("caption") or "",
            "published":  published,
            "likes":      it.get("likesCount"),
            "comments":   it.get("commentsCount"),
            "type":       it.get("type"),
        })

    cadence = _cadence_from_dates(pub_dates)

    if not recent_posts:
        _log_schema_sample(f"instagram_posts:{handle}", items, ["recent_posts"])

    return {
        "handle":               f"@{handle}",
        "recent_posts":         recent_posts,
        "posts_per_week":       cadence["posts_per_week"],
        "days_since_last_post": cadence["days_since_last_post"],
        "data_source":          "apify_instagram_post_scraper",
        "scraped_at":           _now_iso(),
        "raw_count":            len(items),
    }


def fetch_twitter_followers(handle_or_url: str) -> Dict[str, Any]:
    """
    Premium X follower-count fetcher via kaitoeasyapi/premium-x-follower-scraper-following-data.

    Verified input schema 2026-05-03 (per Dave's successful test):
        {
          "user_names": ["handle"],
          "getFollowers":   true,
          "getFollowing":   true,
          "maxFollowers":   200,
          "maxFollowings":  200,
        }

    Actor returns a list of follower / following user records. For audit
    purposes we just need the count, so we cap at 200 follower records (the
    actor's default sample size) and use len(items) as the floor count. If
    the actor exposes total_count / totalFollowers metadata on any record we
    prefer that over the sample size.
    """
    if not (handle_or_url or "").strip():
        raise RuntimeError("apify_social_failed — twitter_followers: empty input")

    raw = handle_or_url.strip().lstrip("@").rstrip("/")
    handle = raw.split("/")[-1] if "/" in raw else raw
    # Actor uses lowercase usernames per spec
    user_name = handle.lower()

    payload = {
        "user_names":     [user_name],
        "getFollowers":   True,
        "getFollowing":   False,   # save cost — we only need the follower count
        "maxFollowers":   200,
        "maxFollowings":  0,
    }
    items = _retry_call(_ACTOR_TWITTER_FOLLOWERS, payload, f"twitter_followers:{handle}")

    # Try to find an explicit count field on any returned record; otherwise
    # fall back to len(items) as the lower-bound follower count.
    followers = None
    for it in items[:5]:
        if not isinstance(it, dict):
            continue
        for key in ("followers_count", "followersCount", "totalFollowers",
                    "total_followers", "followerCount"):
            v = it.get(key)
            if isinstance(v, int) and v > 0:
                followers = v
                break
        if followers:
            break

    if followers is None and items:
        # Each item is a follower record — count them as a floor estimate
        followers = len(items)

    if followers is None:
        _log_schema_sample(f"twitter_followers:{handle}", items, ["followers"])

    return {
        "handle":      f"@{handle}",
        "followers":   followers,
        "following":   None,   # not requested
        "data_source": "apify_kaitoeasyapi_premium_x_follower_scraper",
        "scraped_at":  _now_iso(),
        "raw_count":   len(items),
    }


def fetch_facebook_followers(page_url: str) -> Dict[str, Any]:
    """
    Page-level follower-count fetcher via apify/facebook-followers-following-scraper.

    Verified actor output 2026-05-03: returns a LIST of follower-person records,
    each with keys like __typename, facebookId, facebookUrl, followType,
    followersId, image, subtitle_text, title, url. Per-record subtitle_text
    sometimes contains 'X followers' on the subject page; fallback is len(items)
    as a floor count when the response only includes the sample list.

    Returns: {page_url, followers, raw_count, data_source, scraped_at}
    """
    if not (page_url or "").strip():
        raise RuntimeError("apify_social_failed — facebook_followers: empty input")

    page_url = page_url.strip().rstrip("/")
    page_slug = page_url.rsplit("/", 1)[-1] or page_url

    payload = {
        "startUrls":  [{"url": page_url}],
        "maxItems":   1,
    }
    items = _retry_call(_ACTOR_FB_FOLLOWERS, payload, f"facebook_followers:{page_slug}")

    followers = None

    # Try direct count fields first (in case the actor surfaces totals)
    for it in items[:5]:
        if not isinstance(it, dict):
            continue
        for key in ("followers", "followersCount", "followerCount",
                    "pageFollowers", "totalFollowers"):
            v = it.get(key)
            if isinstance(v, int) and v > 0:
                followers = v
                break
        if followers:
            break

    # Parse follower count from subtitle_text on any record (e.g. "12K followers")
    if followers is None:
        for it in items[:5]:
            sub = (it.get("subtitle_text") or "") if isinstance(it, dict) else ""
            m = re.search(r"([\d.,]+)\s*([KkMm]?)\s*followers?", sub, re.I)
            if m:
                num_str = m.group(1).replace(",", "")
                try:
                    n = float(num_str)
                    suffix = m.group(2).upper()
                    if suffix == "K":
                        n *= 1_000
                    elif suffix == "M":
                        n *= 1_000_000
                    followers = int(n)
                    break
                except ValueError:
                    continue

    # Last resort: use the count of returned follower records as a lower bound
    if followers is None and items:
        followers = len(items)

    if followers is None:
        _log_schema_sample(f"facebook_followers:{page_slug}", items, ["followers"])

    return {
        "page_url":    page_url,
        "followers":   followers,
        "data_source": "apify_facebook_followers_following_scraper",
        "scraped_at":  _now_iso(),
        "raw_count":   len(items),
    }


def fetch_linkedin_followers(company_url: str) -> Dict[str, Any]:
    """
    LinkedIn company-page follower-count fetcher via
    data_link_miner/linkedin-company-followers-scraper.

    Use as a backup/enrichment when the existing linkedin_scraper returns no
    follower count or fails. Returns the same shape as the other follower
    fetchers: {url, followers, raw_count, data_source, scraped_at}.
    """
    if not (company_url or "").strip():
        raise RuntimeError("apify_social_failed — linkedin_followers: empty input")

    company_url = company_url.strip().rstrip("/")
    slug = company_url.rsplit("/", 1)[-1] or company_url

    payload = {
        "startUrls":     [{"url": company_url}],
        "companyUrls":   [company_url],
        "maxItems":      1,
    }
    items = _retry_call(_ACTOR_LINKEDIN_FOLLOWERS, payload, f"linkedin_followers:{slug}")

    first = items[0] if items else {}
    followers = (first.get("followers")
                 or first.get("followersCount")
                 or first.get("followerCount")
                 or first.get("companyFollowers") or None)

    if followers is None:
        _log_schema_sample(f"linkedin_followers:{slug}", items, ["followers"])

    return {
        "url":         company_url,
        "followers":   followers,
        "data_source": "apify_data_link_miner_linkedin_followers",
        "scraped_at":  _now_iso(),
        "raw_count":   len(items),
    }


def fetch_youtube(channel_url: str) -> Dict[str, Any]:
    """
    YouTube channel + recent videos via streamers/youtube-scraper.

    Backup/alternative to the existing YouTube Data API v3 path — useful when
    YOUTUBE_API_KEY is not set or the API returns no usable data.

    Returns the standard social-fetcher shape (followers = subscriber count).
    """
    if not (channel_url or "").strip():
        raise RuntimeError("apify_social_failed — youtube: empty input")

    channel_url = channel_url.strip().rstrip("/")
    handle = channel_url.rsplit("/", 1)[-1].lstrip("@") or channel_url

    payload = {
        "startUrls":      [{"url": channel_url}],
        "channelUrls":    [channel_url],
        "maxResults":     _RESULTS_LIMIT,
        "maxVideos":      _RESULTS_LIMIT,
    }
    items = _retry_call(_ACTOR_YOUTUBE, payload, f"youtube:{handle}")

    first = items[0] if items else {}
    # Try multiple common YouTube actor field names
    followers   = (first.get("subscriberCount")
                   or first.get("subscribers")
                   or first.get("channelSubscriberCount")
                   or (first.get("channel") or {}).get("subscriberCount")
                   or None)
    post_count  = (first.get("videoCount")
                   or first.get("totalVideos")
                   or (first.get("channel") or {}).get("videoCount")
                   or None)
    bio         = (first.get("description")
                   or (first.get("channel") or {}).get("description")
                   or "").strip()

    recent_posts: List[Dict] = []
    pub_dates: List[datetime] = []
    # Videos may be on first item directly or in a nested .videos array
    videos = first.get("videos") or items
    for v in videos[:_RESULTS_LIMIT]:
        if not isinstance(v, dict):
            continue
        published = (v.get("uploadDate") or v.get("publishedAt")
                     or v.get("publishDate") or v.get("date"))
        d = _parse_iso(published)
        if d:
            pub_dates.append(d)
        recent_posts.append({
            "url":        v.get("url") or v.get("videoUrl") or v.get("watchUrl"),
            "text":       v.get("title") or "",
            "published":  published,
            "views":      v.get("viewCount") or v.get("views"),
            "likes":      v.get("likeCount") or v.get("likes"),
            "comments":   v.get("commentCount"),
        })

    cadence = _cadence_from_dates(pub_dates)

    _missing = [name for name, val in (
        ("followers", followers),
        ("post_count", post_count),
        ("posts_per_week", cadence["posts_per_week"]),
    ) if val is None]
    if _missing:
        _log_schema_sample(f"youtube:{handle}", items, _missing)

    return {
        "platform":             "YouTube",
        "handle":               handle,
        "url":                  channel_url,
        "followers":            followers,
        "post_count":           post_count,
        "recent_posts":         recent_posts,
        "posts_per_week":       cadence["posts_per_week"],
        "days_since_last_post": cadence["days_since_last_post"],
        "bio":                  bio,
        "data_source":          "apify_streamers_youtube_scraper",
        "scraped_at":           _now_iso(),
        "raw_count":            len(items),
    }


def fetch_facebook_comments(post_urls: List[str]) -> Dict[str, Any]:
    """
    Phase 2 — scrape comments for a list of FB post URLs via apify/facebook-comments-scraper.
    Not wired into the main flow yet. Caller passes the post URLs returned by
    fetch_facebook_posts() once we decide which posts deserve deep-dive analysis.
    """
    if not post_urls:
        raise RuntimeError("apify_social_failed — facebook_comments: empty input")

    payload = {
        "startUrls":    [{"url": u} for u in post_urls],
        "resultsLimit": 50,    # 50 comments per post is a reasonable depth
    }
    items = _retry_call(
        _ACTOR_FB_COMMENTS, payload,
        f"facebook_comments:{len(post_urls)} posts",
    )
    return {
        "platform":     "Facebook",
        "post_urls":    post_urls,
        "comments":     items,
        "raw_count":    len(items),
        "data_source":  "apify_facebook_comments_scraper",
        "scraped_at":   _now_iso(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-platform normalizers — Apify actor outputs vary; fold them into the
# common output shape documented in the module docstring.
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_instagram(
    items: List[Dict], handle: str, profile_url: str,
) -> Dict[str, Any]:
    """
    apify/instagram-scraper with resultsType=posts + addParentData=True returns
    a list of post dicts each carrying an ownerUsername / ownerFullName /
    ownerFollowersCount via the parent-data attachment.
    """
    if not isinstance(items, list):
        items = []

    # Profile-level fields are duplicated on each post — pick from the first item
    first = items[0] if items else {}
    followers   = (first.get("ownerFollowersCount")
                   or first.get("followersCount") or None)
    post_count  = (first.get("ownerPostsCount")
                   or first.get("postsCount") or None)
    bio         = (first.get("ownerBiography") or first.get("biography") or "").strip()

    recent_posts: List[Dict] = []
    pub_dates: List[datetime] = []
    for it in items[:_RESULTS_LIMIT]:
        published = it.get("timestamp") or it.get("takenAtTimestamp")
        d = _parse_iso(published)
        if d:
            pub_dates.append(d)
        recent_posts.append({
            "url":        it.get("url") or it.get("postUrl"),
            "text":       it.get("caption") or "",
            "published":  published,
            "likes":      it.get("likesCount"),
            "comments":   it.get("commentsCount"),
            "type":       it.get("type"),
        })

    cadence = _cadence_from_dates(pub_dates)
    return {
        "platform":             "Instagram",
        "handle":               f"@{handle}",
        "url":                  profile_url,
        "followers":            followers,
        "post_count":           post_count,
        "recent_posts":         recent_posts,
        "posts_per_week":       cadence["posts_per_week"],
        "days_since_last_post": cadence["days_since_last_post"],
        "bio":                  bio,
        "data_source":          "apify_instagram_scraper",
        "scraped_at":           _now_iso(),
        "raw_count":            len(items),
    }


def _normalize_tiktok(
    items: List[Dict], handle: str, profile_url: str,
) -> Dict[str, Any]:
    """
    clockworks/tiktok-scraper returns a list of video dicts each carrying
    authorMeta.{name, fans, video} and createTime / playCount / diggCount.
    """
    if not isinstance(items, list):
        items = []

    first = items[0] if items else {}
    author = first.get("authorMeta") or {}
    followers  = author.get("fans") or first.get("followers") or None
    post_count = author.get("video") or first.get("videoCount") or None
    bio        = (author.get("signature") or "").strip()

    recent_posts: List[Dict] = []
    pub_dates: List[datetime] = []
    for it in items[:_RESULTS_LIMIT]:
        # createTime is unix epoch seconds on TikTok actor output
        ts = it.get("createTime") or it.get("createTimeISO")
        if isinstance(ts, (int, float)):
            d: Optional[datetime] = datetime.fromtimestamp(ts, tz=timezone.utc)
        else:
            d = _parse_iso(ts)
        if d:
            pub_dates.append(d)
        recent_posts.append({
            "url":        it.get("webVideoUrl") or it.get("url"),
            "text":       it.get("text") or "",
            "published":  d.isoformat() if d else None,
            "plays":      it.get("playCount"),
            "likes":      it.get("diggCount"),
            "comments":   it.get("commentCount"),
            "shares":     it.get("shareCount"),
        })

    cadence = _cadence_from_dates(pub_dates)
    return {
        "platform":             "TikTok",
        "handle":               f"@{handle}",
        "url":                  profile_url,
        "followers":            followers,
        "post_count":           post_count,
        "recent_posts":         recent_posts,
        "posts_per_week":       cadence["posts_per_week"],
        "days_since_last_post": cadence["days_since_last_post"],
        "bio":                  bio,
        "data_source":          "apify_clockworks_tiktok_scraper",
        "scraped_at":           _now_iso(),
        "raw_count":            len(items),
    }


def _normalize_twitter(
    items: List[Dict], handle: str, profile_url: str,
) -> Dict[str, Any]:
    """
    apidojo/twitter-scraper-lite returns a list of tweet dicts, with author info
    on each tweet under .author or .user.
    """
    if not isinstance(items, list):
        items = []

    first = items[0] if items else {}
    author = first.get("author") or first.get("user") or {}
    followers  = author.get("followers") or author.get("followersCount") or None
    post_count = author.get("statusesCount") or author.get("tweetCount") or None
    bio        = (author.get("description") or author.get("bio") or "").strip()

    recent_posts: List[Dict] = []
    pub_dates: List[datetime] = []
    for it in items[:_RESULTS_LIMIT]:
        published = it.get("createdAt") or it.get("created_at") or it.get("timestamp")
        d = _parse_iso(published)
        if d:
            pub_dates.append(d)
        recent_posts.append({
            "url":        it.get("url") or it.get("twitterUrl"),
            "text":       it.get("text") or it.get("fullText") or "",
            "published":  published,
            "likes":      it.get("likeCount") or it.get("favoriteCount"),
            "retweets":   it.get("retweetCount"),
            "replies":    it.get("replyCount"),
            "views":      it.get("viewCount"),
        })

    cadence = _cadence_from_dates(pub_dates)

    # Diagnostic: surface actor's actual schema when expected fields came back None
    _missing = [name for name, val in (
        ("followers", followers),
        ("post_count", post_count),
        ("posts_per_week", cadence["posts_per_week"]),
        ("days_since_last_post", cadence["days_since_last_post"]),
    ) if val is None]
    if _missing:
        _log_schema_sample(f"twitter:{handle}", items, _missing)

    return {
        "platform":             "X",
        "handle":               f"@{handle}",
        "url":                  profile_url,
        "followers":            followers,
        "post_count":           post_count,
        "recent_posts":         recent_posts,
        "posts_per_week":       cadence["posts_per_week"],
        "days_since_last_post": cadence["days_since_last_post"],
        "bio":                  bio,
        "data_source":          "apify_mikolabs_tweets_x_scraper",
        "scraped_at":           _now_iso(),
        "raw_count":            len(items),
    }


def _normalize_facebook(
    items: List[Dict], page_slug: str, page_url: str,
) -> Dict[str, Any]:
    """
    apify/facebook-posts-scraper returns post-level dicts ONLY — no page-level
    metadata (no follower count, no page bio). Verified actor schema 2026-05-03:
      top-level: collaborators, facebookId, facebookUrl, feedbackId, inputUrl,
                 likes, media, pageAdLibrary, pageName, postId, shares, text,
                 time, timestamp, topLevelUrl, topReactionsCount, url, user
      nested user: {id, name, profilePic, profileUrl}
    For follower count, run apify/facebook-pages-scraper as a separate actor.
    """
    if not isinstance(items, list):
        items = []

    first = items[0] if items else {}
    # Followers + post_count are NOT available from this actor — leave None.
    # The actor is post-only; the report renderer accepts None gracefully.
    followers  = None
    post_count = None
    # Page name is the closest thing to a "bio" we can surface
    bio        = (first.get("pageName") or "").strip()

    recent_posts: List[Dict] = []
    pub_dates: List[datetime] = []
    for it in items[:_RESULTS_LIMIT]:
        published = it.get("time") or it.get("timestamp")
        d = _parse_iso(published)
        if d:
            pub_dates.append(d)
        recent_posts.append({
            "url":        it.get("url") or it.get("topLevelUrl"),
            "text":       it.get("text") or "",
            "published":  published,
            "likes":      it.get("likes"),
            "reactions":  it.get("topReactionsCount"),
            "shares":     it.get("shares"),
        })

    cadence = _cadence_from_dates(pub_dates)
    return {
        "platform":             "Facebook",
        "handle":               page_slug,
        "url":                  page_url,
        "followers":            followers,
        "post_count":           post_count,
        "recent_posts":         recent_posts,
        "posts_per_week":       cadence["posts_per_week"],
        "days_since_last_post": cadence["days_since_last_post"],
        "bio":                  bio,
        "data_source":          "apify_facebook_posts_scraper",
        "scraped_at":           _now_iso(),
        "raw_count":            len(items),
    }
