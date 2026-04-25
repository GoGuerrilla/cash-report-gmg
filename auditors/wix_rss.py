import feedparser
from urllib.parse import urljoin


def fetch_wix_blog(base_url: str) -> dict:
    feed_url = urljoin(base_url.rstrip("/") + "/", "blog-feed.xml")
    parsed = feedparser.parse(feed_url)
    if parsed.bozo or not parsed.entries:
        return {"posts": [], "fetched": False, "feed_url": feed_url}
    return {
        "posts": [
            {
                "title":     e.get("title", ""),
                "summary":   e.get("summary", ""),
                "link":      e.get("link", ""),
                "published": e.get("published", ""),
                "author":    e.get("author", ""),
            }
            for e in parsed.entries
        ],
        "fetched":  True,
        "feed_url": feed_url,
    }
