import json
import logging
import os
import urllib.request

_APIFY_CRAWLER_URL = (
    "https://api.apify.com/v2/acts/apify~website-content-crawler"
    "/run-sync-get-dataset-items"
)
_BLOG_PATH_SEGS = ("/post/", "/blog/")
_TIMEOUT = 90

_log = logging.getLogger(__name__)


def apify_render(base_url: str, exclude_urls: list = None) -> list:
    """
    Renders the site via Apify website-content-crawler (sync, max 10 pages).
    Returns a list of {url, title, text} dicts for non-blog pages.
    Returns an empty list on failure or missing API key.
    """
    api_key = os.environ.get("APIFY_API_KEY", "")
    if not api_key:
        _log.warning("apify_render: APIFY_API_KEY not set — skipping")
        return []

    exclude_set = set(exclude_urls or [])

    payload = json.dumps({
        "startUrls":          [{"url": base_url}],
        "maxCrawlPages":      10,
        "maxCrawlDepth":      2,
        "proxyConfiguration": {"useApifyProxy": True},
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{_APIFY_CRAWLER_URL}?token={api_key}",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            items = json.loads(resp.read().decode("utf-8"))

        results = []
        for item in items:
            url = item.get("url", "")
            if any(seg in url.lower() for seg in _BLOG_PATH_SEGS):
                continue
            if url in exclude_set:
                continue
            results.append({
                "url":   url,
                "title": item.get("title", ""),
                "text":  item.get("text", ""),
            })
        return results

    except Exception as exc:
        _log.warning("apify_render failed: %s", exc)
        return []
