"""
Google Business Profile (GBP) Auditor — Free implementation
No paid API or Places API required.

Strategy
--------
Three tiers of evidence, each usable independently:

TIER 1 — Client website signals (primary, works for any business type)
  The strongest free proxy for GBP health: businesses with good profiles
  actively link to them, embed Maps, and feature review CTAs on their site.
  Signals checked:
    • Google Maps URL (/maps/place/, g.page/, goo.gl/maps) in any href → listing exists
    • Google Maps iframe embed → listing claimed and promoted
    • Google review link (google.com/...reviews or Maps review URL) → reviews promoted
    • Review CTA text ("leave a review", "write a review", "google reviews")
    • Phone number visible on page (regex)
    • Address visible via schema.org LocalBusiness JSON-LD or regex
    • Schema.org LocalBusiness structured data quality

TIER 2 — Google Maps search HTML (best-effort)
  A HEAD/GET request to maps.google.com/search — if the business name appears
  in the response we gain a weak confirmation signal. This succeeds ~60% of
  the time depending on IP/region; treated as a bonus only.

TIER 3 — OpenStreetMap Nominatim (free, no key, works for physical locations)
  Reliable for restaurants, retail, and other physical businesses.
  Not used for pure service businesses (they rarely appear in OSM).

Scoring rubric (100 pts total)
-------------------------------
  GBP listing confirmed          25 pts  — Maps URL on website (25) or embed (18)
                                           or Maps HTML confirms (10, additive)
  Reviews promoted               25 pts  — Google review link (25) or CTA text (15)
  NAP on website                 25 pts  — phone +12, address +13
  Schema.org LocalBusiness       15 pts  — full (15), partial (8), absent (0)
  NAP self-consistency           10 pts  — schema phone matches page-text phone

  Floor: 0 when no listing confirmed. Listing confirmed but sparse → 35.
"""
import logging
import os
import re
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Any, List, Optional, Tuple

try:
    import requests
    from bs4 import BeautifulSoup
    _SCRAPING_OK = True
except ImportError:
    _SCRAPING_OK = False

log = logging.getLogger(__name__)

# Apify Maps actor — Tier 3 verified data source. Per Dave 2026-05-09:
# the regex-scraped review counts from Tier 2 (Maps HTML) were always
# rendered as "Unverified" in the PDF because we couldn't be sure a
# parenthesized number near the business name was actually a review
# count vs ZIP / pixel / etc. The Compass account maintains a Maps
# scraper that returns the actual GMB record (rating, reviewsCount,
# address, hours, place URL), costing roughly $0.005-0.01 per call.
# Default-on when APIFY_API_KEY is set; kill switch via USE_APIFY_MAPS=0.
#
# Slug fallback chain — Apify periodically renames or republishes the
# Compass Maps actors (saw HTTP 404 on `compass~google-maps-scraper` in
# Dave's 2026-05-09 19:00 UTC smoke test). Try the older / more stable
# slug first, fall through to the newer slug if Apify returns 404 or
# similar "actor not found" conditions. First success wins.
_APIFY_MAPS_ACTORS = (
    "compass~crawler-google-places",      # original, widely used since 2020
    "compass~google-maps-scraper",        # newer rebrand, may not exist
    "compass~Google-Maps-Scraper",        # case-variant fallback
)
_APIFY_MAPS_TIMEOUT  = 120   # s — Maps actor cold-starts in 30-60s
_APIFY_MAPS_RETRY_WAIT = 8

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_PHONE_RE   = re.compile(r"\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}")
_RATING_RE  = re.compile(r"(\d\.\d)\s*(?:out of 5|/5|stars?)", re.I)
_REVIEW_RE  = re.compile(r"([\d,]+)\s*(?:Google\s+)?review", re.I)

# Patterns that confirm a Google Maps / GBP listing link
_MAPS_LINK_PATTERNS = [
    r"google\.com/maps/place/",
    r"maps\.app\.goo\.gl/",
    r"goo\.gl/maps/",
    r"g\.page/",
    r"maps\.google\.com/",
]
_MAPS_LINK_RE = re.compile("|".join(_MAPS_LINK_PATTERNS), re.I)

# Patterns for Google review links
_REVIEW_LINK_PATTERNS = [
    r"google\.com/maps/place/[^\"']+/reviews",
    r"search\.google\.com/local/reviews",
    r"g\.page/[^\"']+\?share",
    r"google\.com/search\?[^\"']*[+&]source=reviews",
    r"maps\.google\.com/[^\"']*reviews",
]
_REVIEW_LINK_RE = re.compile("|".join(_REVIEW_LINK_PATTERNS), re.I)

# Review CTA text patterns. Per Swift Profit Systems beta feedback 2026-05-06:
# the prior regex only caught direct "leave a review" / "google review" copy
# and missed real-world phrasings like "what clients say", "client wins", or
# star markers that signal the page IS promoting reviews / social proof.
# Widened to include the most common copy variants and unicode star markers.
_REVIEW_CTA_RE = re.compile(
    r"(?:leave|write|post|give|submit)\s+(?:us\s+)?(?:a\s+)?review"
    r"|google\s+review"
    r"|review\s+us\s+on\s+google"
    r"|what\s+(?:our\s+)?clients?\s+(?:are\s+)?say"
    r"|hear\s+from\s+(?:our\s+)?clients?"
    r"|client\s+(?:wins|results|outcomes|testimonials|stories|reviews)"
    r"|see\s+what\s+(?:our\s+)?clients?\s+say"
    r"|(?:five|5)\s*[-\s]?\s*star"
    r"|★|⭐",
    re.I,
)

# Schema.org types that indicate a local/service business
_SCHEMA_TYPES = {
    "LocalBusiness", "Organization", "Corporation", "ProfessionalService",
    "Restaurant", "Store", "MedicalBusiness", "LegalService", "AccountingService",
    "FinancialService", "RealEstateAgent", "HomeAndConstructionBusiness",
    "SportsActivityLocation", "FoodEstablishment",
}


def _normalise_phone(raw: str) -> str:
    return re.sub(r"\D", "", raw)


def _domain(url: str) -> str:
    url = re.sub(r"https?://", "", url, flags=re.I)
    return url.split("/")[0].lstrip("www.")


class GBPAuditor:
    def __init__(
        self,
        business_name:    str,
        website_url:      str = "",
        api_key:          str = "",   # accepted for backwards compatibility, not used
        pinned_place_id:  str = "",   # Google Maps placeId from a prior confirmed run
    ):
        self.name           = business_name.strip()
        self.website        = website_url.strip().rstrip("/")
        self.pinned_place_id = pinned_place_id.strip()

    # ── Public entry point ─────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        if not _SCRAPING_OK:
            return self._neutral(
                "requests/BeautifulSoup not installed — GBP scored at 50 neutral. "
                "Run: pip3 install requests beautifulsoup4"
            )

        signals: Dict[str, Any] = {
            # Tier 1 — website signals
            "maps_link_on_site":    False,   # direct Maps URL in any <a href>
            "maps_link_url":        "",
            "maps_embed_on_site":   False,   # <iframe src="...maps...">
            "review_link_on_site":  False,   # Google review link in any <a href>
            "review_cta_on_site":   False,   # "leave a review" / "Google reviews" text
            "site_phone":           "",
            "site_address":         "",
            "schema_quality":       0,       # 0=absent, 1=partial, 2=full
            "schema_phone":         "",
            "schema_address":       "",
            # Tier 2 — Maps HTML best-effort
            "maps_html_confirmed":  False,
            "maps_rating":          None,
            "maps_review_count":    0,
            "maps_phone":           "",
            # Review count reliability flags (regex extraction is approximate)
            "review_count_verified": True,   # set to False when extracted by regex
            "review_count_method":   "none", # "regex_scrape" when extracted
            # Derived
            "nap_consistent":       False,
            # Apify-confirmed Google Maps place ID (ChIJ... format); stored for PIN reuse
            "place_id":             "",
        }

        # ── Tier 1: website ────────────────────────────────────
        if self.website:
            soup = self._fetch(self.website)
            if soup:
                self._parse_website(soup, signals)

            # Also try the /contact page if homepage didn't find Maps link
            if not signals["maps_link_on_site"] and not signals["maps_embed_on_site"]:
                contact_soup = self._fetch_contact_page(soup) if soup else None
                if contact_soup:
                    self._parse_website(contact_soup, signals)

        # ── Tier 2: Google Maps HTML (best-effort) ─────────────
        self._try_maps_search(signals)

        # ── Tier 3: Apify Maps actor (verified GMB data) ───────
        # Promotes regex-scraped values to verified ones when the Apify
        # actor returns a confirmed match. Fires only when APIFY_API_KEY
        # is set, USE_APIFY_MAPS != "0" (kill switch), and we have both
        # a name and a website to disambiguate (Apify Maps queries by
        # search string and a bare brand name returns random listings).
        self._try_apify_maps(signals)

        # ── NAP consistency ────────────────────────────────────
        phones = [p for p in (
            signals["schema_phone"],
            signals["site_phone"],
            signals["maps_phone"],
        ) if p]
        if len(phones) >= 2:
            normed = [_normalise_phone(p) for p in phones]
            signals["nap_consistent"] = len(set(normed)) == 1

        return self._build_result(signals)

    # ── Tier 1: website parsing ────────────────────────────────

    def _fetch(self, url: str) -> Optional["BeautifulSoup"]:
        if not url.startswith("http"):
            url = "https://" + url
        try:
            r = requests.get(url, headers=_HEADERS, timeout=15, allow_redirects=True)
            if r.status_code < 400:
                return BeautifulSoup(r.text, "html.parser")
        except Exception:
            pass
        return None

    def _fetch_contact_page(self, home_soup: "BeautifulSoup") -> Optional["BeautifulSoup"]:
        """Try to find and fetch a /contact page linked from the homepage."""
        if not self.website:
            return None
        base = self.website.rstrip("/")
        for a in home_soup.find_all("a", href=True):
            href = a["href"].lower().strip()
            if re.search(r"\bcontact\b", href):
                if href.startswith("http"):
                    url = href
                elif href.startswith("/"):
                    url = base + href
                else:
                    continue
                return self._fetch(url)
        # Try common paths
        for path in ("/contact", "/contact-us", "/about"):
            result = self._fetch(base + path)
            if result:
                return result
        return None

    def _parse_website(self, soup: "BeautifulSoup", signals: dict) -> None:
        full_text = soup.get_text(" ", strip=True)

        # ── Maps links and embeds ──────────────────────────────
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if _MAPS_LINK_RE.search(href):
                signals["maps_link_on_site"] = True
                if not signals["maps_link_url"]:
                    signals["maps_link_url"] = href
            if _REVIEW_LINK_RE.search(href):
                signals["review_link_on_site"] = True

        for iframe in soup.find_all("iframe", src=True):
            src = iframe["src"]
            if "google.com/maps" in src or "maps.google.com" in src:
                signals["maps_embed_on_site"] = True

        # ── Review CTA text ────────────────────────────────────
        if _REVIEW_CTA_RE.search(full_text):
            signals["review_cta_on_site"] = True

        # ── Phone ───────────────────────────────────────────────
        if not signals["site_phone"]:
            m = _PHONE_RE.search(full_text)
            if m:
                signals["site_phone"] = m.group(0)

        # ── Schema.org LocalBusiness JSON-LD ───────────────────
        if signals["schema_quality"] == 0:
            self._parse_schema(soup, signals)

        # ── Address fallback (regex near "address" keyword) ────
        if not signals["site_address"] and not signals["schema_address"]:
            addr_ctx = re.search(
                r"(?:address|location)[:\s]{0,5}"
                r"(\d{1,5}[^<\n]{5,80}(?:st|ave|blvd|dr|rd|ln|way|street|avenue)[^<\n]{0,40})",
                full_text, re.I,
            )
            if addr_ctx:
                signals["site_address"] = addr_ctx.group(1).strip()[:120]

    def _parse_schema(self, soup: "BeautifulSoup", signals: dict) -> None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = json.loads(script.string or "")
                items = raw if isinstance(raw, list) else [raw]
                if isinstance(raw, dict) and "@graph" in raw:
                    items = raw["@graph"]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    stype = item.get("@type", "")
                    if any(t in stype for t in _SCHEMA_TYPES):
                        tel   = item.get("telephone", "")
                        addr  = item.get("address", {})
                        name  = item.get("name", "")
                        url   = item.get("url", "")
                        if tel:
                            signals["schema_phone"] = tel
                        if isinstance(addr, dict):
                            parts = [addr.get("streetAddress",""),
                                     addr.get("addressLocality",""),
                                     addr.get("addressRegion","")]
                            signals["schema_address"] = ", ".join(p for p in parts if p)
                        elif isinstance(addr, str) and addr:
                            signals["schema_address"] = addr

                        # Quality score: 0=absent, 1=partial, 2=full
                        fields_present = sum(bool(x) for x in (name, tel, addr, url))
                        if fields_present >= 3:
                            signals["schema_quality"] = 2
                        elif fields_present >= 1:
                            signals["schema_quality"] = 1
                        return   # first match wins
            except Exception:
                continue

    # ── Tier 2: Google Maps HTML ───────────────────────────────

    def _try_maps_search(self, signals: dict) -> None:
        """
        Best-effort fetch of Google Maps search page.
        Extracts business name confirmation, rating, and review count
        from the embedded JavaScript data if present.
        Fails silently — never penalises score.

        Per Dave 2026-05-07 (GMG smoke test): a bare business name
        (e.g. "GMG") is too short to be uniquely identifiable in a
        Maps search — Google returns random unrelated listings and
        our regex never finds a match. Combine the business name
        with the website domain (e.g. "GMG goguerrilla.xyz") which
        is unique enough to disambiguate even short abbreviations.
        Falls back to name-only if the website-qualified search
        returns nothing.
        """
        if not self.name:
            return

        # Build the query candidates in fall-back order. Most specific
        # (name + domain) first; bare-name second only if the qualified
        # search returned no html / non-200.
        query_candidates: List[str] = []
        domain = _domain(self.website) if self.website else ""
        if domain:
            query_candidates.append(f"{self.name} {domain}")
        query_candidates.append(self.name)

        html = ""
        used_query = ""
        for cand in query_candidates:
            try:
                q = urllib.parse.quote_plus(cand)
                r = requests.get(
                    f"https://www.google.com/maps/search/{q}",
                    headers=_HEADERS, timeout=12,
                )
                if r.status_code == 200 and r.text:
                    html = r.text
                    used_query = cand
                    # Stop on the first successful response — even if it
                    # doesn't end up confirming the listing, we don't want
                    # to ping Google twice and risk a CAPTCHA on retry.
                    break
            except Exception:
                continue
        if not html:
            return

        try:
            # Confirm business name appears in response
            if self.name.lower() in html.lower():
                signals["maps_html_confirmed"] = True

            # Try to extract rating — looks for ,X.X, patterns near the business name
            name_pos = html.lower().find(self.name.lower())
            if name_pos > 0:
                ctx = html[name_pos: name_pos + 1000]
                m = re.search(r",(\d\.\d),", ctx)
                if m:
                    val = float(m.group(1))
                    if 1.0 <= val <= 5.0:
                        signals["maps_rating"] = val

                # Try review count — "(N)" parenthesized format is most reliable in Maps JS
                m_paren = re.search(r"\((\d{1,5})\)", ctx)
                if m_paren and int(m_paren.group(1)) >= 2:
                    signals["maps_review_count"] = int(m_paren.group(1))
                    signals["review_count_verified"] = False
                    signals["review_count_method"]   = "regex_scrape"
                else:
                    # Fallback: comma-bounded number within 300 chars of the rating,
                    # max 4 digits to avoid matching ZIP codes or CSS pixel values
                    rating_end = m.end() if m else 0
                    m2 = re.search(r",(\d{2,4}),", ctx[rating_end: rating_end + 300])
                    if m2:
                        signals["maps_review_count"]  = int(m2.group(1))
                        signals["review_count_verified"] = False
                        signals["review_count_method"]   = "regex_scrape"

            # Phone from Maps HTML
            phones = _PHONE_RE.findall(html)
            if phones:
                signals["maps_phone"] = phones[0]

        except Exception:
            pass

    # ── Tier 3: Apify Maps actor (verified GMB data) ───────────

    def _try_apify_maps(self, signals: dict) -> None:
        """
        Apify compass/google-maps-scraper actor — Tier 3 verified data.

        Promotes the regex-scraped Tier-2 fields to verified values when
        the actor returns a confirmed match. Returns rating, reviewsCount,
        address, phone, hours, and the canonical Maps place URL — all
        sourced directly from Google's GMB record, not regex-scraped from
        a search results page.

        Cost: ~$0.005-0.01 per call via Apify. Fires only when:
          - APIFY_API_KEY is set
          - USE_APIFY_MAPS != "0" (kill switch)
          - Both name and website are available (need both to disambiguate
            short brand abbreviations like "GMG")

        Failure-silent — never penalises the score, never raises.
        """
        if os.environ.get("USE_APIFY_MAPS", "1").strip() == "0":
            return
        api_key = os.environ.get("APIFY_API_KEY", "").strip()
        if not api_key or not self.name or not self.website:
            return

        domain = _domain(self.website)
        if not domain:
            return

        if self.pinned_place_id:
            # Pinned mode: go directly to the confirmed listing — fully
            # deterministic, no text search, no ranking variance between runs.
            # placeIds input is supported by compass~crawler-google-places.
            payload = {
                "placeIds":          [self.pinned_place_id],
                "language":          "en",
                "scrapeReviews":     False,
                "scrapeContacts":    False,
                "scrapeImageAuthors": False,
                "exportPlaceUrls":   False,
            }
            log.info("apify_maps: using pinned placeId=%r for %r", self.pinned_place_id, self.name)
        else:
            # Text-search mode: "<name> <domain>" disambiguation.
            # Ask for top 5 because Google Maps ranking varies by
            # Apify-worker geography — right listing is often in positions 2-5.
            query = f"{self.name} {domain}"
            payload = {
                "searchStringsArray":          [query],
                "maxCrawledPlacesPerSearch":   5,
                "language":                    "en",
                "scrapeReviews":               False,
                "scrapeContacts":              False,
                "scrapeImageAuthors":          False,
                "exportPlaceUrls":             False,
            }

        # Iterate the slug-fallback chain — Apify periodically renames or
        # republishes the Compass Maps actors. On HTTP 404 (actor not found
        # at this slug), advance to the next candidate; on a transient error
        # (5xx, timeout, JSON parse), retry once with the same slug. First
        # success wins. Per Dave 2026-05-09 19:00 UTC: GMG smoke saw HTTP
        # 404 on the prior single-slug call — this fallback prevents a
        # rebrand from killing the Tier 3 path.
        items: Optional[List[Dict]] = None
        last_exc: Optional[Exception] = None
        import time as _time

        for actor_slug in _APIFY_MAPS_ACTORS:
            url = (
                f"https://api.apify.com/v2/acts/{actor_slug}"
                f"/run-sync-get-dataset-items?token={api_key}"
            )
            slug_404 = False
            for attempt in (1, 2):
                try:
                    req = urllib.request.Request(
                        url,
                        data=json.dumps(payload).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=_APIFY_MAPS_TIMEOUT) as resp:
                        items = json.loads(resp.read().decode("utf-8"))
                    log.info(
                        "apify_maps: actor=%r attempt %d → %d items for %r",
                        actor_slug, attempt, len(items or []), query,
                    )
                    break
                except urllib.error.HTTPError as exc:
                    last_exc = exc
                    if exc.code == 404:
                        # Wrong slug — don't retry, move to next slug.
                        log.info(
                            "apify_maps: actor=%r returned 404 — trying next slug",
                            actor_slug,
                        )
                        slug_404 = True
                        break
                    log.warning(
                        "apify_maps: actor=%r attempt %d HTTP %d for %r: %s",
                        actor_slug, attempt, exc.code, query, exc,
                    )
                    if attempt == 1:
                        _time.sleep(_APIFY_MAPS_RETRY_WAIT)
                except Exception as exc:
                    last_exc = exc
                    log.warning(
                        "apify_maps: actor=%r attempt %d failed for %r: %s",
                        actor_slug, attempt, query, exc,
                    )
                    if attempt == 1:
                        _time.sleep(_APIFY_MAPS_RETRY_WAIT)
            # Outer slug-loop control: stop trying further slugs as soon as
            # we got a successful response (items != None) OR a non-404
            # error (auth issue, quota, etc. won't be fixed by another slug).
            if items is not None or not slug_404:
                break

        if not items:
            return

        if not isinstance(items, list) or not items:
            return

        # Iterate the top-N Apify results and pick the first one that
        # matches by website domain (strongest signal) or by name
        # (legal-entity-stripped). Horizon Advisers 2026-05-18:
        # increased from top-1 to top-5 because Google Maps ranking
        # varies between runs — the right listing is often in
        # positions 2-5 even when position 1 is a different business.
        nm         = self.name.lower().strip()
        own_domain = _domain(self.website).lower() if self.website else ""
        _LEGAL = re.compile(
            r"[,.]?\s*\b(llc|l\.l\.c\.|inc|inc\.|corp|corp\.|ltd|"
            r"ltd\.|co|co\.|company|llp|pllc|plc|pc|p\.c\.)\b\.?",
            re.I,
        )
        nm_stripped = _LEGAL.sub("", nm).strip()

        place: Optional[Dict] = None
        title         = ""
        place_website = ""
        place_domain  = ""
        accept_reason = ""
        rejected: List[str] = []

        # Phone from website scrape (already populated by Tier 1 before we get here)
        own_phone = _normalise_phone(signals.get("site_phone") or signals.get("schema_phone") or "")

        # Two-pass matching: domain match takes priority over name match.
        # Pass 1 scans ALL candidates for a domain match before accepting
        # any name-only result — prevents a wrong same-name business in
        # position 1 from blocking the correct listing in position 2-5.
        # Pass 2 falls back to name+phone match only if no domain match found.
        name_match_candidate: Optional[Dict] = None

        for candidate in items:
            if not isinstance(candidate, dict):
                continue
            c_title   = (candidate.get("title")   or "").lower().strip()
            c_website = (candidate.get("website") or "").lower().strip()
            c_domain  = _domain(c_website).lower() if c_website else ""
            if own_domain and c_domain and own_domain == c_domain:
                place         = candidate
                title         = c_title
                place_website = c_website
                place_domain  = c_domain
                accept_reason = "website_match"
                break
            # Stash the first viable name match as fallback (phone-checked below)
            if name_match_candidate is None:
                c_title_stripped = _LEGAL.sub("", c_title).strip()
                if c_title and nm and (
                    nm_stripped in c_title_stripped or c_title_stripped in nm_stripped
                ):
                    c_phone = _normalise_phone(str(candidate.get("phone") or ""))
                    if own_phone and c_phone and own_phone != c_phone:
                        log.info(
                            "apify_maps: name match stash rejected — phone mismatch "
                            "(site=%r apify=%r) title=%r",
                            own_phone, c_phone, c_title,
                        )
                        rejected.append(f"{c_title!r}/{c_domain or '—'} [phone_mismatch]")
                    else:
                        name_match_candidate = candidate

        # Accept the stashed name match only if Pass 1 found no domain match
        if place is None and name_match_candidate is not None:
            c_title   = (name_match_candidate.get("title")   or "").lower().strip()
            c_website = (name_match_candidate.get("website") or "").lower().strip()
            c_domain  = _domain(c_website).lower() if c_website else ""
            place         = name_match_candidate
            title         = c_title
            place_website = c_website
            place_domain  = c_domain
            accept_reason = "name_match"

        if place is None:
            log.info(
                "apify_maps: no match across %d candidates — "
                "rejected=%s name=%r website=%r",
                len(items), rejected, nm, own_domain or "—",
            )
            return

        if accept_reason == "website_match":
            log.info(
                "apify_maps: website match (candidate %d/%d) — "
                "title=%r site=%r (skipped %d wrong-business results)",
                items.index(place) + 1, len(items),
                title, place_domain, len(rejected),
            )
        else:
            log.info(
                "apify_maps: name match (legal-entity-stripped, "
                "candidate %d/%d) — title=%r name=%r "
                "(skipped %d wrong-business results)",
                items.index(place) + 1, len(items),
                title, nm, len(rejected),
            )

        # Capture the Google Maps place ID for future pinned runs.
        if place.get("placeId"):
            signals["place_id"] = str(place["placeId"]).strip()
            log.info("apify_maps: captured placeId=%r for future PIN", signals["place_id"])

        # Promote regex-scraped fields to verified values.
        if place.get("totalScore") is not None:
            try:
                signals["maps_rating"] = float(place["totalScore"])
            except (TypeError, ValueError):
                pass

        if place.get("reviewsCount") is not None:
            try:
                signals["maps_review_count"]      = int(place["reviewsCount"])
                signals["review_count_verified"]  = True
                signals["review_count_method"]    = "apify_google_maps_scraper"
            except (TypeError, ValueError):
                pass

        if place.get("phone"):
            signals["maps_phone"] = str(place["phone"]).strip()

        if place.get("address"):
            # Use Apify's canonical address as a backstop when the
            # website didn't surface one. Don't overwrite an existing
            # site_address — that's a different (website-derived) field.
            if not signals.get("site_address") and not signals.get("schema_address"):
                signals["site_address"] = str(place["address"]).strip()

        if place.get("url"):
            # If the website never linked to Maps but Apify confirmed the
            # listing exists, store the canonical Maps URL so the report
            # can render it as the "Place URL" field — without flipping
            # maps_link_on_site (which is specifically about whether the
            # WEBSITE links out, used in Tier-1 scoring).
            if not signals.get("maps_link_url"):
                signals["maps_link_url"] = str(place["url"])
            signals["maps_html_confirmed"] = True

        signals["data_source_apify_maps"]    = True
        signals["apify_match_type"]          = accept_reason  # "website_match" or "name_match"

    # ── Score builder ──────────────────────────────────────────

    def _build_result(self, s: dict) -> Dict[str, Any]:
        score = 0

        # 1. GBP listing confirmed (35 pts)
        # Weighted by how actively the business promotes it
        if s["maps_link_on_site"]:
            listing_score = 35   # actively links to GBP — strongest signal
        elif s["maps_embed_on_site"]:
            listing_score = 28   # embeds map — location prominent
        elif s["maps_html_confirmed"]:
            listing_score = 20   # listing exists but not promoted on website
        else:
            listing_score = 0
        score += listing_score

        # 2. NAP on website (25 pts)
        effective_phone   = s["schema_phone"] or s["site_phone"] or s["maps_phone"]
        effective_address = s["schema_address"] or s["site_address"]
        phone_score   = 12 if effective_phone   else 0
        address_score = 13 if effective_address else 0
        score        += phone_score + address_score

        # 3. Reviews signal (25 pts)
        if s["review_link_on_site"]:
            review_score = 25   # direct Google review link — actively collecting
        elif s["review_cta_on_site"]:
            review_score = 18   # CTA text present
        elif s["maps_rating"] is not None:
            review_score = 12 if s["maps_rating"] >= 4.5 else 8
        else:
            review_score = 0
        score += review_score

        # 4. Schema.org LocalBusiness (15 pts)
        schema_score = {0: 0, 1: 8, 2: 15}[s["schema_quality"]]
        score       += schema_score

        # 5. NAP consistency (bonus, 5 pts — small since data is limited)
        nap_score = 5 if s["nap_consistent"] else 0
        score    += nap_score

        # Score honesty: only floor when an actual GBP listing was confirmed
        # (Tier 1 maps link/embed or Tier 2 maps_html name match). For
        # businesses with zero detectable GBP signal, the score must reflect
        # that reality — no synthetic baseline. Per Dave 2026-05-06: a 25-pt
        # floor was masking businesses that genuinely have no presence.
        listing_confirmed = listing_score > 0
        if listing_confirmed:
            score = max(35, score)
        score = max(0, min(100, score))

        # Completeness proxy for display
        completeness_fields = {
            "listing": listing_confirmed,
            "phone":   bool(effective_phone),
            "address": bool(effective_address),
            "reviews": s["review_link_on_site"] or s["review_cta_on_site"],
            "schema":  s["schema_quality"] > 0,
        }
        completeness_pct = round(sum(completeness_fields.values()) / len(completeness_fields) * 100)

        issues, strengths = self._evaluate(s, listing_score, effective_phone, effective_address)

        # Per Dave 2026-05-09: when Apify Maps actor returned a verified
        # match, surface that in data_source so the PDF / DOCX renderers
        # can drop the "we don't use Places API" caveat and show the
        # values as Apify-verified. PDF generator's `is_verified_gbp`
        # check accepts both "google_places_api" and the new
        # "apify_google_maps" tag — see pdf_generator.py:1572.
        ds_tag = ("apify_google_maps" if s.get("data_source_apify_maps")
                  else "website_scrape_maps_html")

        return {
            "score":              score,
            "grade":              self._grade(score),
            "data_source":        ds_tag,
            "found":              listing_confirmed,
            "business_name":      self.name,
            "address":            effective_address,
            "phone":              effective_phone,
            "rating":                s["maps_rating"],
            "review_count":          s["maps_review_count"],
            "review_count_verified": s["review_count_verified"],
            "review_count_method":   s["review_count_method"],
            "photo_count":           0,        # requires paid API
            "hours_listed":       False,        # requires paid API
            "is_likely_verified": listing_confirmed and s["schema_quality"] >= 1,
            "website_listed":     s["maps_link_url"],
            "place_url":          s["maps_link_url"],
            "services_listed":    [],
            "last_post_date":     None,
            "post_note":          "GBP posts require Google My Business API (OAuth2).",
            "place_id":           s.get("place_id", ""),
            "apify_match_type":   s.get("apify_match_type", ""),
            "completeness_pct":   completeness_pct,
            "nap_consistent":     s["nap_consistent"],
            "site_phone":         s["site_phone"],
            "schema_quality":     s["schema_quality"],
            "maps_html_confirmed": s["maps_html_confirmed"],
            "score_breakdown": {
                "listing_confirmed": listing_score,
                "nap_on_website":    phone_score + address_score,
                "reviews_promoted":  review_score,
                "schema_markup":     schema_score,
                "nap_consistent":    nap_score,
            },
            "issues":    issues,
            "strengths": strengths,
        }

    # ── Evaluation ─────────────────────────────────────────────

    def _evaluate(
        self,
        s: dict,
        listing_score: int,
        effective_phone: str,
        effective_address: str,
    ) -> Tuple[List[str], List[str]]:
        issues, strengths = [], []

        # Listing
        if listing_score == 0:
            issues.append(
                "🔴 No Google Maps link found on website and listing not confirmed in Maps search. "
                "Claim and verify at business.google.com — then add a Maps link to your website footer."
            )
        elif s["maps_link_on_site"]:
            strengths.append("✅ Google Maps listing link found on website — GBP confirmed.")
        elif s["maps_embed_on_site"]:
            strengths.append(
                "✅ Google Maps embed found on website. "
                "Also add a direct link to your GBP listing so visitors can leave reviews."
            )
        else:
            strengths.append("✅ Business name found in Google Maps search results — listing exists.")

        # Reviews
        if s["review_link_on_site"]:
            strengths.append("✅ Google review link on website — actively driving review collection.")
        elif s["review_cta_on_site"]:
            strengths.append("✅ Review CTA text on website — encouraging customers to leave reviews.")
        elif s["maps_rating"]:
            strengths.append(f"✅ Rating {s['maps_rating']}/5 detected in Maps search results.")
        else:
            issues.append(
                "🔴 No Google review link or review CTA found on website. "
                "Add a 'Leave us a Google review' link to your homepage, footer, or thank-you emails. "
                "Reviews are the #1 local trust signal."
            )

        if s["maps_rating"] and s["maps_rating"] < 4.0:
            issues.append(
                f"🟡 Rating {s['maps_rating']}/5 — below average. "
                "Proactively request reviews and respond to every negative review within 24 hours."
            )

        # NAP
        if not effective_phone and not effective_address:
            issues.append(
                "🔴 No phone number or address detected on website. "
                "Add NAP (Name, Address, Phone) to your footer and contact page "
                "for local SEO and GBP consistency."
            )
        else:
            if effective_phone:
                strengths.append(f"✅ Phone number present on website ({effective_phone}).")
            else:
                issues.append(
                    "🟡 No phone number detected on website — "
                    "add a visible phone number to improve trust and GBP NAP consistency."
                )
            if effective_address:
                strengths.append("✅ Business address found on website.")
            else:
                issues.append(
                    "🟡 No address detected on website — "
                    "add your address to the contact page and footer to improve local SEO."
                )

        # Schema
        if s["schema_quality"] == 0:
            issues.append(
                "🟡 No schema.org LocalBusiness markup found. "
                "Adding structured data helps Google understand your NAP and improves rich results."
            )
        elif s["schema_quality"] == 1:
            issues.append(
                "🟡 Partial schema.org LocalBusiness markup — add telephone, address, "
                "and url fields for full structured data benefit."
            )
        else:
            strengths.append("✅ Schema.org LocalBusiness structured data present and complete.")

        # NAP consistency
        if s["nap_consistent"]:
            strengths.append("✅ Phone number is consistent across website and Maps data.")
        elif s["site_phone"] or s["schema_phone"]:
            issues.append(
                "🟡 NAP data found from multiple sources but phone numbers don't match — "
                "ensure your phone number is identical on your website, GBP, and all directories."
            )

        # GBP listing confidence — flag when we matched by name only (no domain confirmation).
        # This means the GBP listing may not have the website URL filled in, which hurts
        # local SEO and makes review/rating data less reliable across audits.
        if s.get("apify_match_type") == "name_match":
            issues.append(
                "🟡 Your Google Business Profile listing was matched by name only — your website URL "
                "may not be set in your GBP profile. Log into business.google.com, go to Edit Profile "
                "→ Contact, and add your website URL. This improves local SEO and ensures audit data "
                "stays accurate across reports."
            )

        return issues, strengths

    # ── Neutral fallback ───────────────────────────────────────

    def _neutral(self, note: str) -> Dict[str, Any]:
        return {
            "score":              50,
            "grade":              "C",
            "data_source":        "not_available",
            "found":              False,
            "note":               note,
            "business_name":      self.name,
            "address":            "",
            "phone":              "",
            "rating":                None,
            "review_count":          0,
            "review_count_verified": True,
            "review_count_method":   "none",
            "photo_count":           0,
            "hours_listed":       False,
            "is_likely_verified": False,
            "website_listed":     "",
            "place_url":          "",
            "services_listed":    [],
            "last_post_date":     None,
            "completeness_pct":   0,
            "nap_consistent":     False,
            "site_phone":         "",
            "schema_quality":     0,
            "maps_html_confirmed": False,
            "score_breakdown":    {},
            "issues":             [],
            "strengths":          [],
        }

    @staticmethod
    def _grade(score: int) -> str:
        if score >= 90: return "A"
        if score >= 80: return "B"
        if score >= 70: return "C"
        if score >= 60: return "D"
        return "F"


def upgrade_with_pages(
    gbp_result: Dict[str, Any],
    pages:      List[Dict[str, Any]],
    business_name: str = "",
) -> Dict[str, Any]:
    """
    Re-scan all crawled pages for GBP signals the original GBPAuditor missed.

    The auditor's own scrape only checks the homepage and one heuristic
    secondary path (/contact, /contact-us, /about). Sites with custom
    slugs (Swift Profit Systems' /about-elliot, 2026-05-06) leave review
    CTAs and Maps links on pages the auditor never visited, producing
    false "no review CTA" findings. This pass piggy-backs on the Apify
    content crawl so we read what's already on disk — no new requests.

    Returns an updated copy of gbp_result with rescored signals. Issues
    and strengths are recomputed from the upgraded signal set.
    """
    if not gbp_result or not pages:
        return gbp_result

    # Reconstruct the signals dict from the existing result. Anything that
    # was already True stays True (we only upgrade — never downgrade — to
    # match the "rescan finds more, never less" expectation).
    s: Dict[str, Any] = {
        "maps_link_on_site":   bool(gbp_result.get("place_url")
                                    or gbp_result.get("website_listed")),
        "maps_link_url":       gbp_result.get("place_url", "") or "",
        "maps_embed_on_site":  False,  # not preserved in result; rederive
        "review_link_on_site": False,
        "review_cta_on_site":  False,
        "site_phone":          gbp_result.get("site_phone", "")
                                or gbp_result.get("phone", "") or "",
        "site_address":        gbp_result.get("address", "") or "",
        "schema_quality":      gbp_result.get("schema_quality", 0) or 0,
        "schema_phone":        "",
        "schema_address":      gbp_result.get("address", "") or "",
        "maps_html_confirmed": gbp_result.get("maps_html_confirmed", False),
        "maps_rating":         gbp_result.get("rating"),
        "maps_review_count":   gbp_result.get("review_count", 0) or 0,
        "maps_phone":          "",
        "review_count_verified": gbp_result.get("review_count_verified", True),
        "review_count_method":   gbp_result.get("review_count_method", "none"),
        "nap_consistent":      gbp_result.get("nap_consistent", False),
        "apify_match_type":    gbp_result.get("apify_match_type", ""),
    }

    # Walk every page — text, external_links, iframe_srcs, structured_data
    pages_scanned = 0
    for p in pages:
        if not isinstance(p, dict):
            continue
        pages_scanned += 1

        text = p.get("text") or ""

        # 1. Review CTA text — Dave 2026-05-06: SPR has a Google review CTA
        # on /about-elliot that the homepage-only scan missed.
        if not s["review_cta_on_site"] and text and _REVIEW_CTA_RE.search(text):
            s["review_cta_on_site"] = True

        # 2. Phone in page text — fill in if not already detected
        if not s["site_phone"] and text:
            m = _PHONE_RE.search(text)
            if m:
                s["site_phone"] = m.group(0)

        # 3. External links — Maps URL + Google review URL detection
        # Static-first pages store external_links as an int count, not a
        # list of dicts (website_auditor._analyze_page line 836). Guard
        # against both shapes: only iterate when it's a list. Horizon
        # Advisers 2026-05-15 crashed here with "'int' object is not
        # iterable" because `5 or []` returns 5, not [].
        ext_links = p.get("external_links")
        if isinstance(ext_links, list):
            for link in ext_links:
                if not isinstance(link, dict):
                    continue
                href = (link.get("to_url") or "")
                if not href:
                    continue
                if not s["maps_link_on_site"] and _MAPS_LINK_RE.search(href):
                    s["maps_link_on_site"] = True
                    if not s["maps_link_url"]:
                        s["maps_link_url"] = href
                if not s["review_link_on_site"] and _REVIEW_LINK_RE.search(href):
                    s["review_link_on_site"] = True

        # 4. Iframe Maps embeds — Apify shape uses "iframe_srcs" (list of
        # URL strings); static-first shape uses "iframe_sources" (also a
        # list of strings, but capped at 3 and truncated to 80 chars).
        # Check both keys.
        iframe_srcs = p.get("iframe_srcs") or p.get("iframe_sources")
        if isinstance(iframe_srcs, list):
            for src in iframe_srcs:
                if not isinstance(src, str):
                    continue
                if "google.com/maps" in src or "maps.google.com" in src:
                    s["maps_embed_on_site"] = True
                    break

        # 5. Schema.org LocalBusiness on inner pages
        structured = p.get("structured_data")
        if not isinstance(structured, list):
            structured = []
        for item in structured:
            if not isinstance(item, dict):
                continue
            stype = item.get("@type", "")
            if not any(t in str(stype) for t in _SCHEMA_TYPES):
                continue
            tel  = item.get("telephone", "")
            addr = item.get("address", {})
            name = item.get("name", "")
            url  = item.get("url", "")
            if tel and not s["schema_phone"]:
                s["schema_phone"] = tel
            if isinstance(addr, dict) and not s["schema_address"]:
                parts = [addr.get("streetAddress",""),
                         addr.get("addressLocality",""),
                         addr.get("addressRegion","")]
                joined = ", ".join(p_ for p_ in parts if p_)
                if joined:
                    s["schema_address"] = joined
            elif isinstance(addr, str) and addr and not s["schema_address"]:
                s["schema_address"] = addr
            fields_present = sum(bool(x) for x in (name, tel, addr, url))
            new_quality = 2 if fields_present >= 3 else (1 if fields_present >= 1 else 0)
            if new_quality > s["schema_quality"]:
                s["schema_quality"] = new_quality

    # NAP consistency re-check across the upgraded signal set
    phones = [p_ for p_ in (s["schema_phone"], s["site_phone"], s["maps_phone"]) if p_]
    if len(phones) >= 2:
        normed = [_normalise_phone(p_) for p_ in phones]
        s["nap_consistent"] = len(set(normed)) == 1

    # Re-score using the same rubric as the original auditor
    score = 0
    if s["maps_link_on_site"]:
        listing_score = 35
    elif s["maps_embed_on_site"]:
        listing_score = 28
    elif s["maps_html_confirmed"]:
        listing_score = 20
    else:
        listing_score = 0
    score += listing_score

    effective_phone   = s["schema_phone"] or s["site_phone"] or s["maps_phone"]
    effective_address = s["schema_address"] or s["site_address"]
    phone_score   = 12 if effective_phone   else 0
    address_score = 13 if effective_address else 0
    score += phone_score + address_score

    if s["review_link_on_site"]:
        review_score = 25
    elif s["review_cta_on_site"]:
        review_score = 18
    elif s["maps_rating"] is not None:
        review_score = 12 if s["maps_rating"] >= 4.5 else 8
    else:
        review_score = 0
    score += review_score

    schema_score = {0: 0, 1: 8, 2: 15}[s["schema_quality"]]
    score += schema_score

    nap_score = 5 if s["nap_consistent"] else 0
    score += nap_score

    listing_confirmed = listing_score > 0
    if listing_confirmed:
        score = max(35, score)
    score = max(0, min(100, score))

    completeness_fields = {
        "listing": listing_confirmed,
        "phone":   bool(effective_phone),
        "address": bool(effective_address),
        "reviews": s["review_link_on_site"] or s["review_cta_on_site"],
        "schema":  s["schema_quality"] > 0,
    }
    completeness_pct = round(
        sum(completeness_fields.values()) / len(completeness_fields) * 100
    )

    # Recompute issues/strengths against the upgraded signals
    auditor_for_eval = GBPAuditor(business_name=business_name)
    issues, strengths = auditor_for_eval._evaluate(
        s, listing_score, effective_phone, effective_address
    )

    upgraded = dict(gbp_result)
    upgraded.update({
        "score":            score,
        "grade":            GBPAuditor._grade(score),
        "found":            listing_confirmed,
        "address":          effective_address,
        "phone":            effective_phone,
        "is_likely_verified": listing_confirmed and s["schema_quality"] >= 1,
        "website_listed":   s["maps_link_url"],
        "place_url":        s["maps_link_url"],
        "completeness_pct": completeness_pct,
        "nap_consistent":   s["nap_consistent"],
        "site_phone":       s["site_phone"],
        "schema_quality":   s["schema_quality"],
        "maps_html_confirmed": s["maps_html_confirmed"],
        "score_breakdown": {
            "listing_confirmed": listing_score,
            "nap_on_website":    phone_score + address_score,
            "reviews_promoted":  review_score,
            "schema_markup":     schema_score,
            "nap_consistent":    nap_score,
        },
        "issues":              issues,
        "strengths":           strengths,
        "data_source":         (gbp_result.get("data_source") or "")
                                + "+inner_page_rescan",
        "pages_rescanned":     pages_scanned,
    })
    return upgraded
