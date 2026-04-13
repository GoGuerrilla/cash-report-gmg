"""
YouTube Data API v3 client for C.A.S.H. Report.

Fetches live channel metrics for a YouTube channel handle:
  - Subscriber count
  - Total video count
  - Videos uploaded in the last 30 days
  - Average views per video (total_views / total_videos)
  - Most viewed video title
  - Channel description
  - Days since last upload (for freshness scoring)
  - Posts/week rate (based on 30-day window)

Quota cost: ~204 units per run (well within 10,000/day default).
"""
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional


class YouTubeAuditor:
    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, channel_handle: str, api_key: str):
        # Accept @goguerrilla or goguerrilla
        self.handle  = channel_handle.lstrip("@")
        self.api_key = api_key

    # ── Internal HTTP helper ───────────────────────────────────

    def _get(self, endpoint: str, params: dict) -> dict:
        params["key"] = self.api_key
        url = f"{self.BASE_URL}/{endpoint}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))

    # ── Channel resolution ─────────────────────────────────────

    def _resolve_channel_id(self) -> Optional[str]:
        """Resolve a @handle to a channel ID via the channels.list endpoint."""
        data = self._get("channels", {
            "part":      "id",
            "forHandle": self.handle,
        })
        items = data.get("items", [])
        return items[0]["id"] if items else None

    # ── Public entry point ─────────────────────────────────────

    def fetch(self) -> Dict[str, Any]:
        """
        Returns a dict ready to merge into preloaded_channel_data["youtube"].
        On any failure, returns a minimal dict with error details so the
        freshness auditor falls back to neutral-50 scoring.
        """
        try:
            channel_id = self._resolve_channel_id()
            if not channel_id:
                return {
                    "is_active":   False,
                    "error":       f"Channel @{self.handle} not found via YouTube API",
                    "data_source": "youtube_api_v3_error",
                }

            # ── Channel statistics + description ───────────────
            ch_data = self._get("channels", {
                "part": "statistics,snippet",
                "id":   channel_id,
            })
            ch       = ch_data.get("items", [{}])[0]
            stats    = ch.get("statistics", {})
            snippet  = ch.get("snippet", {})

            subscriber_count = int(stats.get("subscriberCount",  0))
            total_videos     = int(stats.get("videoCount",       0))
            total_views      = int(stats.get("viewCount",        0))
            description      = snippet.get("description", "")

            # Average views per video (channel-level stat — no extra quota)
            avg_views = round(total_views / total_videos) if total_videos else 0

            # ── Videos uploaded in the last 30 days ───────────
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=30)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            recent_search = self._get("search", {
                "part":           "id",
                "channelId":      channel_id,
                "type":           "video",
                "publishedAfter": cutoff,
                "maxResults":     50,
                "order":          "date",
            })
            recent_ids        = [
                item["id"]["videoId"]
                for item in recent_search.get("items", [])
                if item.get("id", {}).get("videoId")
            ]
            videos_last_30    = len(recent_ids)
            posts_per_week    = round(videos_last_30 / 4.33, 2) if videos_last_30 else 0.0

            # ── Days since last upload ─────────────────────────
            days_since_last_post = None

            if recent_ids:
                # Most-recent video is first in date-ordered results
                vid_resp = self._get("videos", {
                    "part": "snippet",
                    "id":   recent_ids[0],
                })
                if vid_resp.get("items"):
                    pub_str = vid_resp["items"][0]["snippet"].get("publishedAt", "")
                    if pub_str:
                        pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                        days_since_last_post = (datetime.now(timezone.utc) - pub_dt).days
            else:
                # No uploads in 30 days — find the most recent overall
                latest_search = self._get("search", {
                    "part":      "id",
                    "channelId": channel_id,
                    "type":      "video",
                    "maxResults": 1,
                    "order":     "date",
                })
                latest_ids = [
                    item["id"]["videoId"]
                    for item in latest_search.get("items", [])
                    if item.get("id", {}).get("videoId")
                ]
                if latest_ids:
                    vid_resp = self._get("videos", {
                        "part": "snippet",
                        "id":   latest_ids[0],
                    })
                    if vid_resp.get("items"):
                        pub_str = vid_resp["items"][0]["snippet"].get("publishedAt", "")
                        if pub_str:
                            pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                            days_since_last_post = (datetime.now(timezone.utc) - pub_dt).days

            # ── Most viewed video ──────────────────────────────
            most_viewed_title = None
            top_search = self._get("search", {
                "part":      "snippet",
                "channelId": channel_id,
                "type":      "video",
                "maxResults": 1,
                "order":     "viewCount",
            })
            if top_search.get("items"):
                most_viewed_title = top_search["items"][0]["snippet"].get("title", "")

            return {
                "is_active":              True,
                "channel_id":             channel_id,
                "subscriber_count":       subscriber_count,
                "total_video_count":      total_videos,
                "total_view_count":       total_views,
                "videos_last_30_days":    videos_last_30,
                "posts_per_week":         posts_per_week,
                "avg_views_per_video":    avg_views,
                "most_viewed_video_title": most_viewed_title,
                "description":            description,
                "days_since_last_post":   days_since_last_post,
                "data_source":            "youtube_api_v3",
            }

        except Exception as exc:
            return {
                "is_active":   None,
                "error":       str(exc),
                "data_source": "youtube_api_v3_error",
            }
