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
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Actor slugs — Apify URL form uses ~ instead of /
# ─────────────────────────────────────────────────────────────────────────────

_ACTOR_INSTAGRAM   = "apify~instagram-scraper"
_ACTOR_TIKTOK      = "clockworks~tiktok-scraper"
_ACTOR_TWITTER     = "apidojo~twitter-scraper-lite"
_ACTOR_FB_POSTS    = "apify~facebook-posts-scraper"
_ACTOR_FB_COMMENTS = "apify~facebook-comments-scraper"  # Phase 2

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
    Scrape an X (Twitter) profile + recent tweets via apidojo/twitter-scraper-lite.
    """
    if not (handle_or_url or "").strip():
        raise RuntimeError("apify_social_failed — twitter: empty input")

    raw = handle_or_url.strip().lstrip("@").rstrip("/")
    handle = raw.split("/")[-1] if "/" in raw else raw
    profile_url = f"https://x.com/{handle}"

    payload = {
        "twitterHandles": [handle],
        "maxTweets":      _RESULTS_LIMIT,
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
        "data_source":          "apify_apidojo_twitter_scraper_lite",
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
