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

# ── Constants ────────────────────────────────────────────────────────────────

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


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def _get(url: str, **kwargs) -> requests.Response | None:
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

def check_html_tags(url: str) -> dict:
    """Scrape homepage and check meta title, H1, meta description."""
    resp = _get(url)
    if resp is None or resp.status_code != 200:
        return {
            "meta_title":       {"present": False, "value": None, "error": "Could not fetch page"},
            "h1":               {"present": False, "value": None, "error": "Could not fetch page"},
            "meta_description": {"present": False, "value": None, "error": "Could not fetch page"},
            "mobile_viewport":  {"present": False, "error": "Could not fetch page"},
            "schema_jsonld":    {"present": False, "types": [], "error": "Could not fetch page"},
            "social_links":     {k: False for k in SOCIAL_PATTERNS},
            "_html_fetch_error": True,
        }

    soup = BeautifulSoup(resp.text, "html.parser")

    # Meta title
    title_tag = soup.find("title")
    title_text = title_tag.get_text(strip=True) if title_tag else None

    # H1
    h1_tag = soup.find("h1")
    h1_text = h1_tag.get_text(strip=True) if h1_tag else None

    # Meta description
    desc_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    desc_text = desc_tag.get("content", "").strip() if desc_tag else None
    if not desc_text:
        desc_text = None

    # Mobile viewport
    viewport_tag = soup.find("meta", attrs={"name": re.compile(r"^viewport$", re.I)})

    # JSON-LD schema
    ld_tags = soup.find_all("script", attrs={"type": "application/ld+json"})
    schema_types = []
    for tag in ld_tags:
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, dict):
                t = data.get("@type")
                if t:
                    schema_types.append(t if isinstance(t, str) else str(t))
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        t = item.get("@type")
                        if t:
                            schema_types.append(t if isinstance(t, str) else str(t))
        except (json.JSONDecodeError, TypeError):
            pass

    # Social links — scan all <a href> attributes
    all_hrefs = " ".join(
        a.get("href", "") for a in soup.find_all("a", href=True)
    )
    social_found = {
        name: bool(re.search(pattern, all_hrefs, re.I))
        for name, pattern in SOCIAL_PATTERNS.items()
    }

    return {
        "meta_title":       {"present": bool(title_text), "value": title_text},
        "h1":               {"present": bool(h1_text), "value": h1_text},
        "meta_description": {"present": bool(desc_text), "value": desc_text},
        "mobile_viewport":  {"present": bool(viewport_tag)},
        "schema_jsonld":    {"present": bool(schema_types), "types": schema_types},
        "social_links":     social_found,
    }


def check_pagespeed(url: str) -> dict:
    """Fetch Google PageSpeed Insights score (performance category)."""
    api_key = os.environ.get("PAGESPEED_API_KEY", "").strip()
    if not api_key:
        return {
            "skipped": True,
            "reason": "PageSpeed skipped — no API key (set PAGESPEED_API_KEY)",
        }

    params = {"url": url, "key": api_key, "strategy": "mobile"}
    resp = _get(PAGESPEED_ENDPOINT, params=params)

    if resp is None:
        return {"skipped": False, "score": None, "error": "Request failed"}
    if resp.status_code != 200:
        return {"skipped": False, "score": None, "error": f"HTTP {resp.status_code}"}

    try:
        data = resp.json()
        categories = data.get("lighthouseResult", {}).get("categories", {})
        perf = categories.get("performance", {})
        score = perf.get("score")
        score_pct = round(score * 100) if score is not None else None
        return {"skipped": False, "score": score_pct, "strategy": "mobile"}
    except (ValueError, KeyError, TypeError) as exc:
        return {"skipped": False, "score": None, "error": str(exc)}


def check_ssl(url: str) -> dict:
    """Verify SSL certificate is valid and get expiry date."""
    parsed = urlparse(url)
    hostname = parsed.hostname
    port = parsed.port or 443

    if parsed.scheme != "https":
        return {"valid": False, "reason": "URL does not use HTTPS"}

    ctx = ssl.create_default_context()
    try:
        with socket.create_connection((hostname, port), timeout=REQUEST_TIMEOUT) as sock:
            with ctx.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert()
                expire_str = cert.get("notAfter", "")
                # Format: "Nov  1 00:00:00 2026 GMT"
                try:
                    expire_dt = datetime.strptime(expire_str, "%b %d %H:%M:%S %Y %Z")
                    expire_iso = expire_dt.strftime("%Y-%m-%d")
                    days_left = (expire_dt - datetime.utcnow()).days
                except ValueError:
                    expire_iso = expire_str
                    days_left = None
                return {
                    "valid": True,
                    "expires": expire_iso,
                    "days_remaining": days_left,
                }
    except ssl.SSLCertVerificationError as exc:
        return {"valid": False, "reason": f"Certificate verification failed: {exc}"}
    except (socket.timeout, ConnectionRefusedError, OSError) as exc:
        return {"valid": False, "reason": f"Connection error: {exc}"}


def check_sitemap(url: str) -> dict:
    """Check if /sitemap.xml exists and returns 200."""
    sitemap_url = _origin(url) + "/sitemap.xml"
    resp = _get(sitemap_url, allow_redirects=True)
    if resp is None:
        return {"present": False, "url": sitemap_url, "error": "Request failed"}
    return {
        "present": resp.status_code == 200,
        "url": sitemap_url,
        "status_code": resp.status_code,
    }


def check_robots(url: str) -> dict:
    """Check if /robots.txt exists and returns 200."""
    robots_url = _origin(url) + "/robots.txt"
    resp = _get(robots_url, allow_redirects=True)
    if resp is None:
        return {"present": False, "url": robots_url, "error": "Request failed"}
    return {
        "present": resp.status_code == 200,
        "url": robots_url,
        "status_code": resp.status_code,
    }


# ── Main validator ────────────────────────────────────────────────────────────

def validate(url: str) -> dict:
    """
    Run all independent checks against a URL.
    Returns a structured findings dict.
    """
    # Normalise — ensure scheme is present
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    html_results = check_html_tags(url)

    findings = {
        "meta_title":       html_results.get("meta_title"),
        "h1":               html_results.get("h1"),
        "meta_description": html_results.get("meta_description"),
        "mobile_viewport":  html_results.get("mobile_viewport"),
        "schema_jsonld":    html_results.get("schema_jsonld"),
        "social_links":     html_results.get("social_links"),
        "pagespeed":        check_pagespeed(url),
        "ssl":              check_ssl(url),
        "sitemap":          check_sitemap(url),
        "robots_txt":       check_robots(url),
    }

    report = {
        "url":        url,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "findings":   findings,
    }

    return report


# ── compare() ─────────────────────────────────────────────────────────────────

# Maps CASH report keys → how to extract a comparable value from findings
_CASH_KEY_MAP = {
    "meta_title":       lambda f: f["meta_title"]["present"],
    "h1":               lambda f: f["h1"]["present"],
    "meta_description": lambda f: f["meta_description"]["present"],
    "mobile_viewport":  lambda f: f["mobile_viewport"]["present"],
    "schema_jsonld":    lambda f: f["schema_jsonld"]["present"],
    "ssl_valid":        lambda f: f["ssl"]["valid"],
    "sitemap":          lambda f: f["sitemap"]["present"],
    "robots_txt":       lambda f: f["robots_txt"]["present"],
    "pagespeed_score":  lambda f: f["pagespeed"].get("score"),  # numeric
    "linkedin":         lambda f: f["social_links"].get("linkedin"),
    "facebook":         lambda f: f["social_links"].get("facebook"),
    "youtube":          lambda f: f["social_links"].get("youtube"),
    "instagram":        lambda f: f["social_links"].get("instagram"),
    "twitter_x":        lambda f: f["social_links"].get("twitter_x"),
}

# Checks where we do fuzzy numeric comparison instead of exact bool match
_NUMERIC_KEYS = {"pagespeed_score"}
_NUMERIC_TOLERANCE = 5  # points


def compare(findings: dict, cash_report: "dict | str") -> dict:
    """
    Compare independent validator findings against a CASH report summary.

    Args:
        findings:    The 'findings' dict from validate() output,
                     OR the full validate() report dict (auto-detected).
        cash_report: Either a dict of CASH report values,
                     or a file path string pointing to a JSON file.

    Returns:
        {
            "matches":    [ {check, cash_value, independent_value}, ... ],
            "mismatches": [ {check, cash_value, independent_value}, ... ],
            "skipped":    [ {check, reason}, ... ],
            "confidence_score": float  # 0.0–1.0
        }

    Prints a formatted comparison table to stdout.
    """
    # Accept either the full report dict or just findings
    if "findings" in findings:
        findings = findings["findings"]

    # Load cash_report from file if a path string was given
    if isinstance(cash_report, str):
        path = cash_report
        try:
            with open(path, "r", encoding="utf-8") as fh:
                cash_report = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Could not load CASH report from '{path}': {exc}") from exc

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

        # Numeric comparison with tolerance
        if key in _NUMERIC_KEYS:
            try:
                match = abs(float(cash_val) - float(independent_val)) <= _NUMERIC_TOLERANCE
            except (TypeError, ValueError):
                match = cash_val == independent_val
        else:
            # Normalise bool-like strings from CASH reports
            if isinstance(cash_val, str):
                cash_val = cash_val.strip().lower() in ("true", "yes", "pass", "1")
            match = cash_val == independent_val

        entry = {
            "check":             key,
            "cash_value":        cash_val,
            "independent_value": independent_val,
        }

        if match:
            matches.append(entry)
        else:
            mismatches.append(entry)

    total_checked = len(matches) + len(mismatches)
    confidence = round(len(matches) / total_checked, 3) if total_checked else 0.0

    result = {
        "matches":          matches,
        "mismatches":       mismatches,
        "skipped":          skipped,
        "confidence_score": confidence,
    }

    _print_comparison_table(result)
    return result


def _print_comparison_table(result: dict) -> None:
    col_w = [26, 20, 20, 10]
    header = ["Check", "CASH Says", "Independent", "Match?"]
    sep = "─" * (sum(col_w) + 3 * 3 + 1)

    def row(cols):
        return "  ".join(str(c).ljust(w) for c, w in zip(cols, col_w))

    print()
    print(sep)
    print(row(header))
    print(sep)

    all_rows = (
        [(r, True)  for r in result["matches"]] +
        [(r, False) for r in result["mismatches"]]
    )
    all_rows.sort(key=lambda x: x[0]["check"])

    for entry, matched in all_rows:
        symbol = "✓" if matched else "✗ MISMATCH"
        print(row([
            entry["check"],
            str(entry["cash_value"]),
            str(entry["independent_value"]),
            symbol,
        ]))

    for s in result["skipped"]:
        print(row([s["check"], "—", "—", f"skipped: {s['reason']}"]))

    print(sep)
    checked = len(result["matches"]) + len(result["mismatches"])
    print(f"  Validation Confidence Score: {result['confidence_score']:.0%}  "
          f"({len(result['matches'])}/{checked} checks matched)")
    print(sep)
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Independently validate a website against CASH audit findings.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("url", help="Website URL to validate (e.g. https://example.com)")
    p.add_argument(
        "--compare",
        metavar="CASH_REPORT.json",
        help="Path to a CASH report JSON file to compare against",
    )
    p.add_argument(
        "--out",
        metavar="OUTPUT.json",
        help="Write full findings JSON to this file",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        default=True,
        help="Pretty-print JSON output (default: on)",
    )
    return p


def main():
    parser = _build_parser()
    args = parser.parse_args()

    print(f"\nValidating: {args.url}")
    print("Running independent checks…\n")

    report = validate(args.url)

    indent = 2 if args.pretty else None
    print(json.dumps(report, indent=indent))

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=indent)
        print(f"\nFindings written to: {args.out}")

    if args.compare:
        print(f"\nComparing against CASH report: {args.compare}")
        compare(report, args.compare)


if __name__ == "__main__":
    main()
