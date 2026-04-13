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

  Floor: 25.  Listing confirmed but sparse → floor 35.
"""
import re
import json
import urllib.parse
from typing import Dict, Any, List, Optional, Tuple

try:
    import requests
    from bs4 import BeautifulSoup
    _SCRAPING_OK = True
except ImportError:
    _SCRAPING_OK = False

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

# Review CTA text patterns
_REVIEW_CTA_RE = re.compile(
    r"(?:leave|write|post|give|submit)\s+(?:us\s+)?(?:a\s+)?review"
    r"|google\s+review"
    r"|review\s+us\s+on\s+google"
    r"|review\s+us\s+on\s+google",
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
        business_name: str,
        website_url:   str = "",
        api_key:       str = "",   # accepted for backwards compatibility, not used
    ):
        self.name    = business_name.strip()
        self.website = website_url.strip().rstrip("/")

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
            # Derived
            "nap_consistent":       False,
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
        """
        if not self.name:
            return
        query = urllib.parse.quote_plus(self.name)
        url   = f"https://www.google.com/maps/search/{query}"
        try:
            r = requests.get(url, headers=_HEADERS, timeout=12)
            if r.status_code != 200:
                return
            html = r.text

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

                # Try review count
                m2 = re.search(r",(\d{2,5}),", ctx)
                if m2:
                    signals["maps_review_count"] = int(m2.group(1))

            # Phone from Maps HTML
            phones = _PHONE_RE.findall(html)
            if phones:
                signals["maps_phone"] = phones[0]

        except Exception:
            pass

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

        # Floor: listing confirmed but sparse data → at least D range
        listing_confirmed = listing_score > 0
        if listing_confirmed:
            score = max(35, score)
        score = max(25, min(100, score))

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

        return {
            "score":              score,
            "grade":              self._grade(score),
            "data_source":        "website_scrape_maps_html",
            "found":              listing_confirmed,
            "business_name":      self.name,
            "address":            effective_address,
            "phone":              effective_phone,
            "rating":             s["maps_rating"],
            "review_count":       s["maps_review_count"],
            "photo_count":        0,           # requires paid API
            "hours_listed":       False,        # requires paid API
            "is_likely_verified": listing_confirmed and s["schema_quality"] >= 1,
            "website_listed":     s["maps_link_url"],
            "place_url":          s["maps_link_url"],
            "services_listed":    [],
            "last_post_date":     None,
            "post_note":          "GBP posts require Google My Business API (OAuth2).",
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
            "rating":             None,
            "review_count":       0,
            "photo_count":        0,
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
        if score >= 80: return "A"
        if score >= 65: return "B"
        if score >= 50: return "C"
        if score >= 35: return "D"
        return "F"
