"""
Meta Graph API Auditor — C.A.S.H. Report by GMG

Pulls live metrics for a Facebook Page and linked Instagram Business account
using the Meta Graph API v19.0.

Authentication tiers
--------------------
  Tier 1 — App Access Token (app_id|app_secret)
    Always available. Grants public page data:
      • Page name, fan_count, followers_count, category, about
      • Public feed posts (for posts/week + days-since-last-post)
    No page-level permissions needed.

  Tier 2 — Page Access Token (META_PAGE_ACCESS_TOKEN in .env)
    Optional. Unlocks:
      • Page Insights: reach, impressions, engagement totals (28-day window)
      • Instagram Business Account: followers, media_count, recent media
      • Per-post engagement (likes + comments on IG)

    Required OAuth scopes (current Meta API — deprecated manage_pages removed):
      pages_show_list       — list pages the user manages (replaces manage_pages)
      pages_read_engagement — read Page likes, comments, impressions, reach
      instagram_basic       — read Instagram Business Account profile + media
      read_insights         — read Page Insights and Instagram Insights metrics

    How to generate a Page Access Token via Graph API Explorer:
      1. Go to https://developers.facebook.com/tools/explorer/
      2. Select your App (App ID: 4305565303095234) from the Application dropdown
      3. Click "Generate Access Token"
      4. Add these permissions in the Permissions panel:
           pages_show_list, pages_read_engagement,
           instagram_basic, read_insights
      5. Authorize the app — this gives you a User Access Token
      6. In the Explorer, run:
           GET /me/accounts
         Copy the "access_token" value for your Page from the response
      7. That is your Page Access Token — paste it into .env:
           META_PAGE_ACCESS_TOKEN=<token>
      Note: User/Page tokens from the Explorer expire in ~1 hour.
      For production use, exchange for a Long-Lived Token:
        GET /oauth/access_token
            ?grant_type=fb_exchange_token
            &client_id={app_id}
            &client_secret={app_secret}
            &fb_exchange_token={short_lived_token}
      Long-lived Page tokens do not expire as long as the user
      remains an admin of the Page.

Output
------
  .fetch() returns a dict with two top-level keys:
    facebook  — metrics dict (merges into channel_data["facebook"])
    instagram — metrics dict (merges into channel_data["instagram"])
  Both contain:
    followers, posts_per_week, days_since_last_post, is_active,
    engagement_rate, reach_28d, data_source
  Plus audit-level lists: issues[], strengths[], recommendations[]
"""
import json
import logging as _logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

_log = _logging.getLogger(__name__)

GRAPH_VERSION = "v19.0"
BASE_URL      = f"https://graph.facebook.com/{GRAPH_VERSION}"


class MetaAuditor:
    def __init__(
        self,
        app_id:              str,
        app_secret:          str,
        facebook_page_id:    str = "",   # vanity name OR numeric page ID
        instagram_handle:    str = "",   # handle without @
        page_access_token:   str = "",   # optional — unlocks insights + IG
    ):
        self.app_id           = app_id
        self.app_secret       = app_secret
        self.page_id          = facebook_page_id.strip().strip("/")
        self.ig_handle        = instagram_handle.lstrip("@").strip()
        self.page_token       = page_access_token.strip()
        self._app_token       = f"{app_id}|{app_secret}"

    # ══════════════════════════════════════════════════════════
    #  Token exchange
    # ══════════════════════════════════════════════════════════

    def _exchange_for_long_lived_token(self) -> None:
        """
        Exchange a short-lived Page Access Token for a 60-day long-lived token.
        Mutates self.page_token in-place on success. Never raises.
        Safe to call with an already-long-lived token — Meta returns the
        existing token with remaining TTL, so self.page_token is refreshed.
        """
        if not (self.page_token and self.app_id and self.app_secret):
            return
        try:
            params = urllib.parse.urlencode({
                "grant_type":        "fb_exchange_token",
                "client_id":         self.app_id,
                "client_secret":     self.app_secret,
                "fb_exchange_token": self.page_token,
            })
            url = "https://graph.facebook.com/oauth/access_token?" + params
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            if "error" in body:
                _log.warning("Meta: token exchange error — %s (using original token)",
                             body["error"].get("message", "unknown"))
                return
            new_token  = body.get("access_token", "")
            expires_in = body.get("expires_in")   # seconds, typically ~5,183,944 (~60 days)
            if not new_token:
                return
            self.page_token = new_token
            if expires_in:
                expiry = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
                _log.info("Meta: long-lived token active — expires %s (%d days)",
                          expiry.strftime("%Y-%m-%d"), int(expires_in) // 86400)
            else:
                _log.info("Meta: long-lived token active (no expiry in response)")
        except Exception as exc:
            _log.warning("Meta: token exchange failed — using original token (%s)", exc)

    # ══════════════════════════════════════════════════════════
    #  Public entry point
    # ══════════════════════════════════════════════════════════

    def fetch(self) -> Dict[str, Any]:
        """
        Run all available checks and return a unified result dict.
        Never raises — every sub-call is individually guarded.
        """
        self._exchange_for_long_lived_token()
        result = {
            "facebook":       self._empty_platform("Facebook"),
            "instagram":      self._empty_platform("Instagram"),
            "has_page_token": bool(self.page_token),
            "issues":         [],
            "strengths":      [],
            "recommendations": [],
        }

        # ── Facebook ──────────────────────────────────────────
        if self.page_id:
            fb = self._fetch_facebook()
            result["facebook"] = fb
            result["issues"]    += fb.get("issues", [])
            result["strengths"] += fb.get("strengths", [])

        # ── Instagram ─────────────────────────────────────────
        if self.page_token and self.page_id:
            ig = self._fetch_instagram_via_page()
            result["instagram"] = ig
            result["issues"]    += ig.get("issues", [])
            result["strengths"] += ig.get("strengths", [])
        else:
            # No Page Access Token — score as 50 neutral, do not penalise
            ig = result["instagram"]
            ig["handle"]       = self.ig_handle or ""
            ig["data_source"]  = "meta_no_page_token"
            ig["neutral_score"] = True
            ig["neutral_note"]  = (
                "Enhanced Meta insights available after Page Access Token setup"
            )
            result["instagram"] = ig

        # ── Cross-platform recommendations ────────────────────
        result["recommendations"] = self._recommendations(result)

        return result

    # ══════════════════════════════════════════════════════════
    #  Facebook fetch
    # ══════════════════════════════════════════════════════════

    def _fetch_facebook(self) -> Dict[str, Any]:
        data    = self._empty_platform("Facebook")
        token   = self.page_token or self._app_token

        # ── Basic page info ───────────────────────────────────
        try:
            page = self._get(
                f"/{self.page_id}",
                {"fields": "id,name,fan_count,followers_count,category,about,website",
                 "access_token": token},
            )
            data["page_id"]           = page.get("id", "")
            data["page_name"]         = page.get("name", "")
            data["fan_count"]         = int(page.get("fan_count") or 0)
            data["followers"]         = int(page.get("followers_count") or page.get("fan_count") or 0)
            data["category"]          = page.get("category", "")
            data["about"]             = (page.get("about") or "")[:200]
            data["data_source"]       = "meta_graph_api"
            data["is_active"]         = True
        except Exception as exc:
            data["error"]       = str(exc)[:120]
            data["data_source"] = "meta_graph_api_error"
            data["issues"].append(f"🔴 Facebook API error fetching page info: {exc!s:.80}")
            return data

        if data["followers"] > 0:
            data["strengths"].append(
                f"✅ Facebook Page: {data['followers']:,} followers"
            )

        # ── Recent posts → posts/week + days since last ───────
        try:
            feed = self._get(
                f"/{self.page_id}/posts",
                {"fields": "created_time,message",
                 "limit":  "50",
                 "access_token": token},
            )
            posts = feed.get("data", [])
            data["total_posts_fetched"] = len(posts)
            ppw, days_since = self._analyse_posts(posts)
            data["posts_per_week"]       = ppw
            data["days_since_last_post"] = days_since
            if days_since is not None:
                data["is_active"] = days_since <= 30
        except Exception:
            pass   # non-fatal — freshness unknown

        # ── Page Insights (requires Page token) ───────────────
        if self.page_token:
            try:
                insights = self._get(
                    f"/{self.page_id}/insights",
                    {"metric":  "page_impressions_unique,"
                                "page_post_engagements,"
                                "page_fans",
                     "period":  "days_28",
                     "access_token": self.page_token},
                )
                for item in insights.get("data", []):
                    name   = item.get("name", "")
                    values = item.get("values", [])
                    if not values:
                        continue
                    latest = values[-1].get("value", 0)
                    if name == "page_impressions_unique":
                        data["reach_28d"] = int(latest)
                    elif name == "page_post_engagements":
                        data["engagements_28d"] = int(latest)
                    elif name == "page_fans":
                        data["fans_28d_change"] = int(latest) - data.get("fan_count", 0)

                # Engagement rate = engagements / reach * 100
                reach = data.get("reach_28d", 0)
                eng   = data.get("engagements_28d", 0)
                if reach and reach > 0:
                    data["engagement_rate"] = round(eng / reach * 100, 2)
            except Exception:
                pass   # non-fatal — insights need page token with correct permissions
        else:
            # No Page token — mark insights fields as neutral, do not penalise scoring
            data["engagement_rate"]  = None
            data["reach_28d"]        = None
            data["engagements_28d"]  = None
            data["insights_neutral"] = True
            data["insights_note"]    = (
                "Enhanced Meta insights available after Page Access Token setup"
            )

        # ── Issues & strengths ────────────────────────────────
        ppw   = data.get("posts_per_week")
        days  = data.get("days_since_last_post")
        er    = data.get("engagement_rate")
        fol   = data.get("followers", 0)

        if days is not None:
            if days <= 7:
                data["strengths"].append(f"✅ Facebook: Posted within the last {days} day(s)")
            elif days <= 30:
                data["strengths"].append(f"✅ Facebook: Last post {days} days ago — still active")
            elif days <= 90:
                data["issues"].append(f"🟡 Facebook: Last post {days} days ago — going stale")
            else:
                data["issues"].append(f"🔴 Facebook: No post in {days}+ days — channel appears abandoned")

        if ppw is not None:
            if ppw >= 5:
                data["strengths"].append(f"✅ Facebook: Posting {ppw:.1f}x/week — high frequency")
            elif ppw >= 3:
                data["strengths"].append(f"✅ Facebook: Posting {ppw:.1f}x/week — solid cadence")
            elif ppw >= 1:
                data["issues"].append(f"🟡 Facebook: Posting {ppw:.1f}x/week — below recommended 3–5x/week")
            else:
                data["issues"].append("🔴 Facebook: Posting less than once per week")

        if er is not None and not data.get("insights_neutral"):
            if er >= 3.0:
                data["strengths"].append(f"✅ Facebook engagement rate {er}% — above average (>3%)")
            elif er >= 1.0:
                data["issues"].append(f"🟡 Facebook engagement rate {er}% — below strong benchmark (3%+)")
            else:
                data["issues"].append(f"🔴 Facebook engagement rate {er}% — critically low (<1%)")
        elif data.get("insights_neutral"):
            data["strengths"].append(
                f"ℹ️ Facebook engagement & reach: {data['insights_note']}"
            )

        if fol < 500:
            data["issues"].append(f"🟡 Facebook: {fol:,} followers — audience growth needed")
        elif fol < 2000:
            data["issues"].append(f"🟡 Facebook: {fol:,} followers — growing but below industry average for B2B agencies")
        elif fol >= 5000:
            data["strengths"].append(f"✅ Facebook: {fol:,} followers — strong social proof")

        return data

    # ══════════════════════════════════════════════════════════
    #  Instagram fetch (via linked Facebook Page)
    # ══════════════════════════════════════════════════════════

    def _fetch_instagram_via_page(self) -> Dict[str, Any]:
        data = self._empty_platform("Instagram")

        # Step 1: get the IG Business Account ID from the FB Page
        try:
            page_ig = self._get(
                f"/{self.page_id}",
                {"fields":       "instagram_business_account",
                 "access_token": self.page_token},
            )
            ig_acct = page_ig.get("instagram_business_account", {})
            ig_id   = ig_acct.get("id") if ig_acct else None
        except Exception as exc:
            data["error"]       = str(exc)[:120]
            data["data_source"] = "meta_graph_api_error"
            data["issues"].append("🟡 Instagram: Could not retrieve IG Business Account ID from Facebook Page")
            return data

        if not ig_id:
            data["issues"].append(
                "🟡 Instagram: No Instagram Business Account linked to this Facebook Page. "
                "Link the Instagram account in Meta Business Suite to unlock live metrics."
            )
            data["data_source"] = "meta_no_ig_linked"
            return data

        # Step 2: IG account profile
        try:
            profile = self._get(
                f"/{ig_id}",
                {"fields":       "username,followers_count,media_count,biography,website",
                 "access_token": self.page_token},
            )
            data["handle"]         = profile.get("username", self.ig_handle)
            data["followers"]      = int(profile.get("followers_count") or 0)
            data["total_posts"]    = int(profile.get("media_count") or 0)
            data["biography"]      = (profile.get("biography") or "")[:200]
            data["is_active"]      = True
            data["data_source"]    = "meta_graph_api"
        except Exception as exc:
            data["error"]       = str(exc)[:120]
            data["data_source"] = "meta_graph_api_error"
            data["issues"].append(f"🟡 Instagram: Profile fetch failed — {exc!s:.80}")
            return data

        # Step 3: Recent media → posts/week, days since last, engagement
        try:
            media_resp = self._get(
                f"/{ig_id}/media",
                {"fields":       "timestamp,like_count,comments_count,media_type",
                 "limit":        "30",
                 "access_token": self.page_token},
            )
            posts = media_resp.get("data", [])
            ppw, days_since = self._analyse_posts(posts)
            data["posts_per_week"]       = ppw
            data["days_since_last_post"] = days_since
            if days_since is not None:
                data["is_active"] = days_since <= 30

            # Engagement rate = avg (likes + comments) / followers * 100
            if posts and data["followers"]:
                total_eng = sum(
                    int(p.get("like_count") or 0) + int(p.get("comments_count") or 0)
                    for p in posts
                )
                avg_eng = total_eng / len(posts)
                data["engagement_rate"]    = round(avg_eng / data["followers"] * 100, 2)
                data["avg_likes_per_post"] = round(
                    sum(int(p.get("like_count") or 0) for p in posts) / len(posts), 1
                )
                data["avg_comments_per_post"] = round(
                    sum(int(p.get("comments_count") or 0) for p in posts) / len(posts), 1
                )
                data["posts_analyzed"] = len(posts)
        except Exception:
            pass   # non-fatal

        # Step 4: Issues & strengths
        fol  = data.get("followers", 0)
        ppw  = data.get("posts_per_week")
        days = data.get("days_since_last_post")
        er   = data.get("engagement_rate")

        if fol > 0:
            data["strengths"].append(f"✅ Instagram: {fol:,} followers")

        if fol < 1000:
            data["issues"].append(f"🟡 Instagram: {fol:,} followers — audience growth needed")
        elif fol < 5000:
            data["issues"].append(f"🟡 Instagram: {fol:,} followers — below 5K milestone")
        elif fol >= 10000:
            data["strengths"].append(f"✅ Instagram: {fol:,} followers — strong authority signal")

        if days is not None:
            if days <= 7:
                data["strengths"].append(f"✅ Instagram: Posted within the last {days} day(s)")
            elif days <= 30:
                data["strengths"].append(f"✅ Instagram: Last post {days} days ago — active")
            elif days <= 60:
                data["issues"].append(f"🟡 Instagram: Last post {days} days ago — algorithm will suppress")
            else:
                data["issues"].append(f"🔴 Instagram: No post detected in {days}+ days — channel inactive")

        if ppw is not None:
            if ppw >= 4:
                data["strengths"].append(f"✅ Instagram: Posting {ppw:.1f}x/week — at ideal frequency")
            elif ppw >= 2:
                data["issues"].append(f"🟡 Instagram: Posting {ppw:.1f}x/week — Reels/Stories daily recommended")
            else:
                data["issues"].append(f"🔴 Instagram: Posting {ppw:.1f}x/week — severely under minimum")

        if er is not None:
            if er >= 3.0:
                data["strengths"].append(f"✅ Instagram engagement rate {er}% — above average (>3%)")
            elif er >= 1.0:
                data["issues"].append(f"🟡 Instagram engagement rate {er}% — below benchmark 3%+")
            else:
                data["issues"].append(f"🔴 Instagram engagement rate {er}% — critically low (<1%)")

        return data

    # ══════════════════════════════════════════════════════════
    #  Helpers
    # ══════════════════════════════════════════════════════════

    def _analyse_posts(self, posts: List[Dict]) -> tuple:
        """
        Given a list of post dicts (each with a 'timestamp' or 'created_time' field),
        return (posts_per_week: float|None, days_since_last_post: int|None).
        Looks back up to 90 days.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=90)
        dates  = []

        for p in posts:
            ts_raw = p.get("timestamp") or p.get("created_time", "")
            if not ts_raw:
                continue
            try:
                # Graph API returns ISO-8601 with +0000 offset
                ts = datetime.fromisoformat(ts_raw.replace("+0000", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    dates.append(ts)
            except ValueError:
                continue

        if not dates:
            return None, None

        dates.sort(reverse=True)
        most_recent     = dates[0]
        days_since      = (now - most_recent).days
        window_days     = min(90, (now - dates[-1]).days + 1) or 1
        posts_per_week  = round(len(dates) / window_days * 7, 1)

        return posts_per_week, days_since

    def _get(self, path: str, params: dict) -> dict:
        """HTTP GET against the Graph API. Raises on non-200 or API error."""
        url  = BASE_URL + path + "?" + urllib.parse.urlencode(params)
        req  = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw  = exc.read().decode("utf-8", errors="replace")
            try:
                err_body = json.loads(raw)
                msg = err_body.get("error", {}).get("message", raw[:120])
            except Exception:
                msg = raw[:120]
            raise RuntimeError(msg) from exc

        if "error" in body:
            raise RuntimeError(body["error"].get("message", str(body["error"]))[:120])
        return body

    @staticmethod
    def _empty_platform(name: str) -> Dict[str, Any]:
        return {
            "platform":              name,
            "followers":             None,
            "fan_count":             None,
            "posts_per_week":        None,
            "days_since_last_post":  None,
            "is_active":             None,
            "engagement_rate":       None,
            "reach_28d":             None,
            "engagements_28d":       None,
            "data_source":           "not_fetched",
            "insights_neutral":      False,
            "insights_note":         "",
            "neutral_score":         False,
            "neutral_note":          "",
            "issues":                [],
            "strengths":             [],
        }

    def _recommendations(self, result: Dict) -> List[Dict]:
        recs = []
        fb   = result.get("facebook", {})
        ig   = result.get("instagram", {})

        fb_fol         = fb.get("followers") or 0
        ig_fol         = ig.get("followers") or 0
        fb_er          = fb.get("engagement_rate")
        ig_er          = ig.get("engagement_rate")
        fb_ppw         = fb.get("posts_per_week")
        ig_ppw         = ig.get("posts_per_week")
        fb_insights_ok = not fb.get("insights_neutral")
        ig_live        = not ig.get("neutral_score")

        if fb_fol and fb_fol < 2000:
            recs.append({
                "platform": "Facebook",
                "priority": "HIGH",
                "action":   "Run targeted Facebook follower growth campaign",
                "detail":   "Boost top-performing organic posts ($5–10/day) to reach ICP audiences",
                "timeline": "30 days",
            })
        if ig_live and ig_fol < 5000:
            recs.append({
                "platform": "Instagram",
                "priority": "HIGH",
                "action":   "Increase Reels output to 4–5x/week",
                "detail":   "Reels reach non-followers 6x more than static posts",
                "timeline": "Ongoing",
            })
        if fb_insights_ok and fb_er is not None and fb_er < 1.0:
            recs.append({
                "platform": "Facebook",
                "priority": "HIGH",
                "action":   "Audit and restructure Facebook content mix",
                "detail":   "Replace promotional posts with value-led content (tips, case studies, polls)",
                "timeline": "2 weeks",
            })
        if ig_live and ig_er is not None and ig_er < 1.0:
            recs.append({
                "platform": "Instagram",
                "priority": "MEDIUM",
                "action":   "Add engagement prompts (questions, CTAs) to every post",
                "detail":   "Comments and saves signal quality to the algorithm more than likes",
                "timeline": "Immediate",
            })
        if not result.get("has_page_token"):
            recs.append({
                "platform": "Meta",
                "priority": "LOW",
                "action":   "Set up Page Access Token for enhanced Meta insights",
                "detail":   (
                    "Enhanced Meta insights available after Page Access Token setup. "
                    "Unlocks Page reach, engagement rate, and live Instagram metrics. "
                    "Scopes needed: pages_show_list, pages_read_engagement, "
                    "instagram_basic, read_insights."
                ),
                "timeline": "1 hour setup",
            })
        if fb_ppw is not None and fb_ppw < 3:
            recs.append({
                "platform": "Facebook",
                "priority": "MEDIUM",
                "action":   "Increase posting cadence to 3–5x/week",
                "detail":   "Consistent posting is required for algorithm reach recovery",
                "timeline": "Ongoing",
            })
        return recs
