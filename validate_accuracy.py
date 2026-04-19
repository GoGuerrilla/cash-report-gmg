"""
validate_accuracy.py — Independent CASH Audit Validator

Independently verifies website data points using only public tools.
Designed to run after every C.A.S.H. audit and append a
Validation Confidence Score to the report.

Usage:
    python validate_accuracy.py https://example.com
    python validate_accuracy.py https://example.com --compare cash_report.json
    python validate_accuracy.py https://example.com --out results.json

Environment:
    PAGESPEED_API_KEY — Google PageSpeed Insights API key (optional)
                        If not set, PageSpeed check is skipped.
"""

import argparse
import json
import os
import re
import socket
import ssl
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# ── Constants ─────────────────────────────────────────────────────────────────

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
REQUEST_TIMEOUT = 15
USER_AGENT = (
    "Mozilla/5.0 (compatible; CASHValidator/1.0; +https://goguerrilla.com)"
)
SOCIAL_PATTERNS = {
    "linkedin":  r"linkedin\.com",
    "facebook":  r"facebook\.com",
    "youtube":   r"youtube\.com",
    "instagram": r"instagram\.com",
    "twitter_x": r"(twitter\.com|x\.com)",
}

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(url: str, **kwargs) -> "requests.Response | None":
    """GET with shared headers and timeout. Returns None on any error."""
    headers = {"User-Agent": USER_AGENT}
    try:
        return requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, **kwargs)
    except requests.RequestException:
        return None


def _origin(url: str) -> str:
    """Return scheme + netloc, e.g. https://example.com"""
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ── Individual checks ─────────────────────────────────────────────────────────

def check_page(url: str) -> dict:
    """
    Single fetch of the homepage. Returns all HTML-derived checks:
      - meta title, H1, meta description, viewport
      - Open Graph tags
      - JSON-LD structured data
      - social links
      - page load status code
    """
    resp = _get(url, allow_redirects=True)

    error_payload = {
        "status_code":      None,
        "meta_title":       {"present": False, "value": None},
        "h1":               {"present": False, "value": None},
        "meta_description": {"present": False, "value": None},
        "mobile_viewport":  {"present": False},
        "open_graph":       {"present": False, "tags": {}},
        "schema_jsonld":    {"present": False, "types": []},
        "social_links":     {k: False for k in SOCIAL_PATTERNS},
        "_fetch_error":     True,
    }

    if resp is None:
        return {**error_payload, "error": "Request failed — could not reach URL"}
    if resp.status_code != 200:
        return {**error_payload, "status_code": resp.status_code,
                "error": f"Non-200 response: HTTP {resp.status_code}"}

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Status code
    status_code = resp.status_code

    # ── Meta title
    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True) if title_tag else None

    # ── H1
    h1_tag = soup.find("h1")
    h1_text = h1_tag.get_text(strip=True) if h1_tag else None

    # ── Meta description
    desc_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    desc_text = (desc_tag.get("content", "") or "").strip() or None

    # ── Mobile viewport
    viewport_tag = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})
    viewport_content = viewport_tag.get("content", "").strip() if viewport_tag else None

    # ── Open Graph tags — collect all og: properties
    og_tags = {}
    for tag in soup.find_all("meta", property=re.compile(r"^og:", re.I)):
        prop = tag.get("property", "").lower()
        content = tag.get("content", "").strip()
        if prop and content:
            og_tags[prop] = content
    # Also check name="og:*" variant (some CMSes use this)
    for tag in soup.find_all("meta", attrs={"name": re.compile(r"^og:", re.I)}):
        prop = tag.get("name", "").lower()
        content = tag.get("content", "").strip()
        if prop and content and prop not in og_tags:
            og_tags[prop] = content

    # ── JSON-LD structured data
    ld_tags = soup.find_all("script", attrs={"type": "application/ld+json"})
    schema_types = []
    for tag in ld_tags:
        try:
            data = json.loads(tag.string or "")
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict):
                    t = item.get("@type")
                    if t:
                        schema_types.append(t if isinstance(t, str) else str(t))
        except (json.JSONDecodeError, TypeError):
            pass

    # ── Social links — scan all <a href> values in one pass
    all_hrefs = " ".join(a.get("href", "") for a in soup.find_all("a", href=True))
    social_found = {
        name: bool(re.search(pattern, all_hrefs, re.I))
        for name, pattern in SOCIAL_PATTERNS.items()
    }

    return {
        "status_code":      status_code,
        "meta_title":       {"present": bool(title_text), "value": title_text},
        "h1":               {"present": bool(h1_text), "value": h1_text},
        "meta_description": {"present": bool(desc_text), "value": desc_text},
        "mobile_viewport":  {"present": bool(viewport_tag), "content": viewport_content},
        "open_graph":       {"present": bool(og_tags), "tags": og_tags},
        "schema_jsonld":    {"present": bool(schema_types), "types": schema_types},
        "social_links":     social_found,
    }


def check_pagespeed(url: str) -> dict:
    """Fetch Google PageSpeed Insights performance score (mobile strategy)."""
    api_key = os.environ.get("PAGESPEED_API_KEY", "").strip()
    if not api_key:
        return {
            "skipped": True,
            "score":   None,
            "reason":  "PageSpeed skipped — no API key (set PAGESPEED_API_KEY)",
        }

    resp = _get(PAGESPEED_ENDPOINT, params={"url": url, "key": api_key, "strategy": "mobile"})

    if resp is None:
        return {"skipped": False, "score": None, "error": "Request failed"}
    if resp.status_code != 200:
        return {"skipped": False, "score": None, "error": f"HTTP {resp.status_code}"}

    try:
        data = resp.json()
        score = (
            data
            .get("lighthouseResult", {})
            .get("categories", {})
            .get("performance", {})
            .get("score")
        )
        return {
            "skipped":  False,
            "score":    round(score * 100) if score is not None else None,
            "strategy": "mobile",
        }
    except (ValueError, KeyError, TypeError) as exc:
        return {"skipped": False, "score": None, "error": str(exc)}


def check_ssl(url: str) -> dict:
    """Verify SSL certificate validity and return expiry date."""
    parsed = urlparse(url)
    if parsed.scheme != "https":
        return {"valid": False, "reason": "URL does not use HTTPS"}

    hostname = parsed.hostname
    port = parsed.port or 443
    ctx = ssl.create_default_context()

    try:
        with socket.create_connection((hostname, port), timeout=REQUEST_TIMEOUT) as sock:
            with ctx.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert()
                expire_str = cert.get("notAfter", "")
                try:
                    expire_dt = datetime.strptime(expire_str, "%b %d %H:%M:%S %Y %Z")
                    expire_iso = expire_dt.strftime("%Y-%m-%d")
                    days_left = (expire_dt - datetime.utcnow()).days
                except ValueError:
                    expire_iso = expire_str
                    days_left = None
                return {"valid": True, "expires": expire_iso, "days_remaining": days_left}
    except ssl.SSLCertVerificationError as exc:
        return {"valid": False, "reason": f"Certificate verification failed: {exc}"}
    except (socket.timeout, ConnectionRefusedError, OSError) as exc:
        return {"valid": False, "reason": f"Connection error: {exc}"}


def check_sitemap(url: str) -> dict:
    """Check whether /sitemap.xml exists and returns HTTP 200."""
    sitemap_url = _origin(url) + "/sitemap.xml"
    resp = _get(sitemap_url, allow_redirects=True)
    if resp is None:
        return {"present": False, "url": sitemap_url, "error": "Request failed"}
    return {"present": resp.status_code == 200, "url": sitemap_url, "status_code": resp.status_code}


def check_robots(url: str) -> dict:
    """Check whether /robots.txt exists and returns HTTP 200."""
    robots_url = _origin(url) + "/robots.txt"
    resp = _get(robots_url, allow_redirects=True)
    if resp is None:
        return {"present": False, "url": robots_url, "error": "Request failed"}
    return {"present": resp.status_code == 200, "url": robots_url, "status_code": resp.status_code}


# ── Main validator ────────────────────────────────────────────────────────────

def validate(url: str) -> dict:
    """
    Run all independent checks against a URL.
    Returns a structured report dict.
    """
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    page = check_page(url)

    findings = {
        # Meta / HTML
        "status_code":      page.get("status_code"),
        "meta_title":       page.get("meta_title"),
        "h1":               page.get("h1"),
        "meta_description": page.get("meta_description"),
        "mobile_viewport":  page.get("mobile_viewport"),
        "open_graph":       page.get("open_graph"),
        "schema_jsonld":    page.get("schema_jsonld"),
        # Social
        "social_links":     page.get("social_links"),
        # Technical
        "ssl":              check_ssl(url),
        "sitemap":          check_sitemap(url),
        "robots_txt":       check_robots(url),
        # Performance
        "pagespeed":        check_pagespeed(url),
    }

    return {
        "url":        url,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "findings":   findings,
    }


# ── compare_with_cash_report() ────────────────────────────────────────────────

# Maps known CASH report keys to extractor lambdas against findings
_CASH_KEY_MAP = {
    "meta_title":       lambda f: f["meta_title"]["present"],
    "h1":               lambda f: f["h1"]["present"],
    "meta_description": lambda f: f["meta_description"]["present"],
    "mobile_viewport":  lambda f: f["mobile_viewport"]["present"],
    "open_graph":       lambda f: f["open_graph"]["present"],
    "schema_jsonld":    lambda f: f["schema_jsonld"]["present"],
    "ssl_valid":        lambda f: f["ssl"]["valid"],
    "sitemap":          lambda f: f["sitemap"]["present"],
    "robots_txt":       lambda f: f["robots_txt"]["present"],
    "pagespeed_score":  lambda f: f["pagespeed"].get("score"),
    "linkedin":         lambda f: f["social_links"].get("linkedin"),
    "facebook":         lambda f: f["social_links"].get("facebook"),
    "youtube":          lambda f: f["social_links"].get("youtube"),
    "instagram":        lambda f: f["social_links"].get("instagram"),
    "twitter_x":        lambda f: f["social_links"].get("twitter_x"),
}

_NUMERIC_KEYS      = {"pagespeed_score"}
_NUMERIC_TOLERANCE = 5   # points — CASH score within ±5 of independent = match


def compare_with_cash_report(findings_or_report: "dict | str", cash_report: "dict | str") -> dict:
    """
    Compare independent validator findings against a CASH report summary.

    Args:
        findings_or_report: The full validate() report dict, OR just its
                            'findings' sub-dict, OR a file path to a
                            validate() JSON output file.
        cash_report:        A dict of CASH report values, OR a file path
                            to a CASH report JSON file.

    Returns:
        {
            "matches":          [...],
            "mismatches":       [...],
            "skipped":          [...],
            "confidence_score": float   # 0.0–1.0
        }

    Also prints a formatted comparison table to stdout.
    """
    # ── Resolve findings input
    if isinstance(findings_or_report, str):
        with open(findings_or_report, "r", encoding="utf-8") as fh:
            findings_or_report = json.load(fh)
    findings = findings_or_report.get("findings", findings_or_report)

    # ── Resolve CASH report input
    if isinstance(cash_report, str):
        with open(cash_report, "r", encoding="utf-8") as fh:
            cash_report = json.load(fh)

    matches    = []
    mismatches = []
    skipped    = []

    for key, extractor in _CASH_KEY_MAP.items():
        if key not in cash_report:
            skipped.append({"check": key, "reason": "Not present in CASH report"})
            continue

        cash_val = cash_report[key]

        try:
            independent_val = extractor(findings)
        except (KeyError, TypeError):
            skipped.append({"check": key, "reason": "Could not extract from findings"})
            continue

        if independent_val is None:
            skipped.append({"check": key, "reason": "Independent check returned no value"})
            continue

        if key in _NUMERIC_KEYS:
            try:
                matched = abs(float(cash_val) - float(independent_val)) <= _NUMERIC_TOLERANCE
            except (TypeError, ValueError):
                matched = cash_val == independent_val
        else:
            # Normalise PASS/FAIL/yes/no strings from CASH reports to bool
            if isinstance(cash_val, str):
                cash_val = cash_val.strip().lower() in ("true", "yes", "pass", "1", "found")
            matched = cash_val == independent_val

        entry = {"check": key, "cash_value": cash_val, "independent_value": independent_val}
        (matches if matched else mismatches).append(entry)

    total      = len(matches) + len(mismatches)
    confidence = round(len(matches) / total, 3) if total else 0.0

    result = {
        "matches":          matches,
        "mismatches":       mismatches,
        "skipped":          skipped,
        "confidence_score": confidence,
    }

    _print_comparison_table(result)
    return result


def _print_comparison_table(result: dict) -> None:
    col_w  = [22, 22, 22, 14]
    header = ["Check", "CASH Says", "Independent", "Match?"]
    sep    = "─" * (sum(col_w) + 3 * len(col_w))

    def row(*cols):
        return "  ".join(str(c).ljust(w) for c, w in zip(cols, col_w))

    print()
    print(sep)
    print(row(*header))
    print(sep)

    all_rows = sorted(
        [(r, True)  for r in result["matches"]] +
        [(r, False) for r in result["mismatches"]],
        key=lambda x: x[0]["check"],
    )
    for entry, matched in all_rows:
        symbol = "✓" if matched else "✗  MISMATCH"
        print(row(entry["check"], entry["cash_value"], entry["independent_value"], symbol))

    for s in result["skipped"]:
        print(row(s["check"], "—", "—", "(skipped)"))

    checked = len(result["matches"]) + len(result["mismatches"])
    print(sep)
    print(f"  Validation Confidence Score:  {result['confidence_score']:.0%}  "
          f"({len(result['matches'])}/{checked} checks matched)")
    print(sep)
    print()


# ── Human-readable summary ────────────────────────────────────────────────────

def print_summary(report: dict) -> None:
    """Print a readable summary table of validate() findings."""
    f   = report["findings"]
    url = report["url"]
    ts  = report["checked_at"]

    sep = "─" * 60

    def status(val: bool) -> str:
        return "PASS" if val else "FAIL"

    print()
    print(sep)
    print(f"  CASH Validator — {url}")
    print(f"  Checked: {ts}")
    print(sep)

    print(f"  Status code         {f['status_code']}")
    print(f"  SSL valid           {status(f['ssl']['valid'])}"
          + (f"  (expires {f['ssl'].get('expires','?')}, "
             f"{f['ssl'].get('days_remaining','?')} days)" if f['ssl']['valid'] else ""))
    print(f"  sitemap.xml         {status(f['sitemap']['present'])}")
    print(f"  robots.txt          {status(f['robots_txt']['present'])}")
    print(sep)
    print(f"  Meta title          {status(f['meta_title']['present'])}"
          + (f"  \"{f['meta_title']['value'][:50]}\"" if f['meta_title']['present'] else ""))
    print(f"  Meta description    {status(f['meta_description']['present'])}")
    print(f"  H1 tag              {status(f['h1']['present'])}"
          + (f"  \"{f['h1']['value'][:50]}\"" if f['h1']['present'] else ""))
    print(f"  Mobile viewport     {status(f['mobile_viewport']['present'])}")
    print(f"  Open Graph tags     {status(f['open_graph']['present'])}"
          + (f"  ({len(f['open_graph']['tags'])} tags)" if f['open_graph']['present'] else ""))
    print(f"  JSON-LD schema      {status(f['schema_jsonld']['present'])}"
          + (f"  {f['schema_jsonld']['types']}" if f['schema_jsonld']['present'] else ""))
    print(sep)

    sl      = f["social_links"]
    found   = [k for k, v in sl.items() if v]
    missing = [k for k, v in sl.items() if not v]
    print(f"  Social found        {', '.join(found) or 'none'}")
    print(f"  Social missing      {', '.join(missing) or 'none'}")
    print(sep)

    ps = f["pagespeed"]
    if ps.get("skipped"):
        print(f"  PageSpeed           {ps['reason']}")
    elif ps.get("score") is not None:
        print(f"  PageSpeed (mobile)  {ps['score']}/100")
    else:
        print(f"  PageSpeed           ERROR — {ps.get('error','unknown')}")
    print(sep)
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Independently validate a website against CASH audit findings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("url",
                   help="Website URL to validate (e.g. https://example.com)")
    p.add_argument("--compare", metavar="CASH_REPORT.json",
                   help="Path to a CASH report JSON file to compare against")
    p.add_argument("--out",     metavar="OUTPUT.json",
                   help="Write full findings JSON to this file")
    return p


def main():
    args = _build_parser().parse_args()

    print(f"\nValidating: {args.url}")
    print("Running independent checks…")

    report = validate(args.url)
    print_summary(report)
    print(json.dumps(report, indent=2))

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nFindings written to: {args.out}")

    if args.compare:
        print(f"\nComparing against CASH report: {args.compare}")
        compare_with_cash_report(report, args.compare)


if __name__ == "__main__":
    main()
