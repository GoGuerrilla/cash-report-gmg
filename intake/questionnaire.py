"""
C.A.S.H. Report by GMG — 7-Question Intake Form
Target: under 2 minutes end-to-end.

Questions
---------
1. Client name & business type
2. Website URL or Linktree URL (auto-detected)
3. Social handles / URLs (paste anything — platform auto-detected)
4. Target market & ideal customer profile
5. Monthly ad budget
6. Email list size
7. Email send frequency

Saves answers to intake/last_intake.json for re-runs.
"""
import json
import os
import re
import sys
from typing import Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import ClientConfig
from auditors.industry_benchmarks import INDUSTRIES, INDUSTRY_GROUPS
from auditors.linkedin_scraper import _is_valid_linkedin_company_url
from intake.client_db import save_intake_record

INTAKE_SAVE_PATH = os.path.join(os.path.dirname(__file__), "last_intake.json")

# ── Terminal colours ───────────────────────────────────────────
BOLD  = "\033[1m"
TEAL  = "\033[36m"
GOLD  = "\033[33m"
GREEN = "\033[32m"
DIM   = "\033[2m"
RESET = "\033[0m"
LINE  = "─" * 56


# ── Low-level input helpers ────────────────────────────────────

def _ask(prompt: str, hint: str = "", required: bool = False) -> str:
    hint_str = f"  {DIM}{hint}{RESET}\n" if hint else ""
    while True:
        try:
            sys.stdout.write(f"{hint_str}  {GOLD}→{RESET} ")
            sys.stdout.flush()
            val = input().strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if val:
            return val
        if not required:
            return ""
        print(f"  {DIM}Required — please enter a value.{RESET}")


def _ask_number(prompt: str, default: float = 0.0, is_int: bool = False):
    print(f"\n  {BOLD}{prompt}{RESET}")
    print(f"  {DIM}Press Enter for 0{RESET}")
    while True:
        try:
            sys.stdout.write(f"  {GOLD}→{RESET} ")
            sys.stdout.flush()
            raw = input().strip().replace(",", "").replace("$", "").replace("k", "000")
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not raw:
            return int(default) if is_int else default
        try:
            val = float(raw)
            return int(val) if is_int else val
        except ValueError:
            print(f"  {DIM}Enter a number (e.g. 500 or 1200){RESET}")


def _q(n: int, text: str, total: int = 12):
    print(f"\n  {BOLD}{TEAL}[{n}/{total}]{RESET}  {BOLD}{text}{RESET}")


# ── Platform auto-detection ────────────────────────────────────

_PLATFORM_PATTERNS = [
    # (field_key,          regex to match in lowercase token)
    ("linkedin_url",        r"linkedin\.com"),
    ("instagram_handle",    r"instagram\.com"),
    ("youtube_channel_url", r"youtube\.com|youtu\.be"),
    ("facebook_page_url",   r"facebook\.com|fb\.com"),
    ("tiktok_handle",       r"tiktok\.com"),
    # twitter.com URLs still common; x.com is the rebrand domain
    ("twitter_handle",      r"twitter\.com|^x\.com|/x\.com|//x\.com"),
    ("discord_url",         r"discord\.gg|discord\.com/invite"),
    ("linktree_url",        r"linktr\.ee"),
]

# Handles that must be extracted from the URL path
_HANDLE_EXTRACT = {
    "instagram_handle":  r"instagram\.com/([^/?#\s]+)",
    "tiktok_handle":     r"tiktok\.com/@?([^/?#\s]+)",
    "twitter_handle":    r"(?:twitter\.com|x\.com)/@?([^/?#\s]+)",
}


def _normalise(token: str) -> str:
    """Strip protocol and trailing slashes for cleaner storage."""
    t = token.strip().rstrip("/")
    if not re.match(r"https?://", t, re.I) and "." in t:
        t = "https://" + t
    return t


def _detect_platforms(raw: str) -> Dict[str, Any]:
    """
    Parse a freeform blob of handles/URLs and return detected platform fields.
    Unrecognised tokens are collected in 'unmatched'.
    """
    detected: Dict[str, str] = {k: "" for k, _ in _PLATFORM_PATTERNS}
    unmatched = []

    # Split on whitespace, commas, and newlines — URLs have no internal spaces
    tokens = [t.strip().strip(",;") for t in re.split(r"[\s,;]+", raw) if t.strip()]

    for token in tokens:
        tl = token.lower()
        matched = False
        for field, pattern in _PLATFORM_PATTERNS:
            if re.search(pattern, tl):
                if field in _HANDLE_EXTRACT:
                    m = re.search(_HANDLE_EXTRACT[field], tl)
                    detected[field] = m.group(1).lstrip("@") if m else _normalise(token)
                else:
                    detected[field] = _normalise(token)
                matched = True
                break
        if not matched:
            if re.match(r"https?://", tl) and "." in tl:
                unmatched.append(token)
            # other bare words ignored

    detected["unmatched"] = unmatched
    return detected


def _platform_summary(d: Dict[str, Any]) -> str:
    """One-line human-readable summary of detected platforms."""
    labels = {
        "linkedin_url":        "LinkedIn",
        "instagram_handle":    "Instagram",
        "youtube_channel_url": "YouTube",
        "facebook_page_url":   "Facebook",
        "tiktok_handle":       "TikTok",
        "twitter_handle":      "X",
        "discord_url":         "Discord",
        "linktree_url":        "Linktree",
    }
    found = [labels[k] for k, _ in _PLATFORM_PATTERNS if d.get(k)]
    unmatched = d.get("unmatched", [])
    parts = []
    if found:
        parts.append(f"{GREEN}Detected:{RESET} {', '.join(found)}")
    if unmatched:
        parts.append(f"{DIM}Unmatched: {', '.join(unmatched)}{RESET}")
    return "  " + "  |  ".join(parts) if parts else f"  {DIM}No platforms detected{RESET}"


# ── Q2 URL classifier ──────────────────────────────────────────

def _classify_url(url: str):
    """Return (website_url, linktree_url) tuple."""
    if "linktr.ee" in url.lower():
        return "", _normalise(url)
    return _normalise(url), ""


def _classified_to_platforms(classified: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a {PlatformName: [url, ...]} dict (from Linktree or website scrape)
    into the platforms dict that ClientConfig expects.
    Extracts bare handles for Instagram and TikTok.
    """
    platforms: Dict[str, str] = {k: "" for k, _ in _PLATFORM_PATTERNS}

    if classified.get("LinkedIn"):
        for url in classified["LinkedIn"]:
            if _is_valid_linkedin_company_url(url):
                platforms["linkedin_url"] = url
                break
        # If no valid URL found, platforms["linkedin_url"] stays ""
        # and the LinkedIn scraper is skipped downstream
        # (per webhook_server.py: if config.linkedin_url:)

    if classified.get("Instagram"):
        u = classified["Instagram"][0]
        m = re.search(r"instagram\.com/([^/?#\s]+)", u, re.I)
        platforms["instagram_handle"] = m.group(1).strip("/") if m else u

    if classified.get("TikTok"):
        u = classified["TikTok"][0]
        m = re.search(r"tiktok\.com/@?([^/?#\s]+)", u, re.I)
        platforms["tiktok_handle"] = m.group(1).strip("/") if m else u

    # Linktree scraper uses canonical key "Twitter" (legacy + x.com URLs both
    # classify here); _scrape_website_socials uses "X". Accept either.
    _x_links = classified.get("X") or classified.get("Twitter")
    if _x_links:
        u = _x_links[0]
        m = re.search(r"(?:twitter\.com|x\.com)/@?([^/?#\s]+)", u, re.I)
        platforms["twitter_handle"] = m.group(1).strip("/") if m else u

    if classified.get("YouTube"):
        platforms["youtube_channel_url"] = classified["YouTube"][0]

    if classified.get("Facebook"):
        platforms["facebook_page_url"] = classified["Facebook"][0]

    if classified.get("Discord"):
        platforms["discord_url"] = classified["Discord"][0]

    if classified.get("Website"):
        platforms["_website_from_linktree"] = classified["Website"][0]

    platforms["unmatched"] = []
    return platforms


def _scrape_website_socials(url: str) -> Dict[str, Any]:
    """
    Fetch a website homepage and extract all social media profile links
    found in <a href> tags (typically in header/footer/nav).
    Returns a {PlatformName: [url, ...]} classified dict.
    """
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return {}

        soup = BeautifulSoup(resp.text, "html.parser")
        social_patterns = [
            ("LinkedIn",   r"linkedin\.com"),
            ("Instagram",  r"instagram\.com"),
            ("YouTube",    r"youtube\.com|youtu\.be"),
            ("Facebook",   r"facebook\.com|fb\.com"),
            ("TikTok",     r"tiktok\.com"),
            ("X",          r"twitter\.com|^x\.com|/x\.com|//x\.com"),
            ("Discord",    r"discord\.gg|discord\.com/invite"),
        ]
        classified: Dict[str, list] = {}
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            for platform, pattern in social_patterns:
                if re.search(pattern, href, re.I):
                    classified.setdefault(platform, [])
                    if href not in classified[platform]:
                        classified[platform].append(href)
                    break
        return classified
    except Exception:
        return {}


# ── Email frequency helpers ────────────────────────────────────

def _parse_email_freq(raw: str):
    """Return (email_send_frequency str, has_active_newsletter bool, has_email_marketing bool)."""
    r = raw.lower().strip()
    if any(x in r for x in ("never", "no", "none", "0", "don't", "dont")):
        return "never", False, False
    if any(x in r for x in ("daily",)):
        return "daily", True, True
    # biweekly: must check before "week" to avoid partial match
    if any(x in r for x in ("biweekly", "bi-weekly", "bi weekly",
                             "every 2 week", "every two week",
                             "twice a month", "2x a month", "2x month",
                             "twice per month")):
        return "biweekly", True, True
    if any(x in r for x in ("week",)):
        return "weekly", True, True
    if any(x in r for x in ("month",)):
        return "monthly", True, True
    if any(x in r for x in ("quarter",)):
        return "quarterly", True, False
    return raw, bool(raw and r not in ("0", "never", "no")), bool(raw)


# ── Main intake runner ─────────────────────────────────────────

def run_intake(skip_if_saved: bool = False) -> ClientConfig:
    """
    Run the 7-question intake and return a populated ClientConfig.
    If skip_if_saved=True and last_intake.json exists, loads that instead.
    """
    if skip_if_saved and os.path.exists(INTAKE_SAVE_PATH):
        print(f"\n{DIM}  Loading saved intake → {INTAKE_SAVE_PATH}{RESET}")
        return _load_intake()

    # ── Header ─────────────────────────────────────────────────
    print(f"\n{BOLD}{'═' * 56}{RESET}")
    print(f"{BOLD}{TEAL}  C.A.S.H. REPORT BY GMG — CLIENT INTAKE{RESET}")
    print(f"{DIM}  12 questions · ~3 minutes · press Ctrl-C to quit{RESET}")
    print(f"{BOLD}{'═' * 56}{RESET}")

    # ── Q1: Client name & industry ────────────────────────────
    _q(1, "Client name")
    client_name = _ask("", required=True)

    # ── Industry: Step 1 — choose parent group ────────────────
    groups = list(INDUSTRY_GROUPS.keys())
    print(f"\n  {BOLD}Client category  (enter number):{RESET}")
    for i, grp in enumerate(groups, 1):
        subs_preview = ", ".join(INDUSTRY_GROUPS[grp][:2]) + "…"
        print(f"  {DIM}{i}.{RESET}  {grp}  {DIM}({subs_preview}){RESET}")

    while True:
        sys.stdout.write(f"  {GOLD}→{RESET} ")
        sys.stdout.flush()
        try:
            choice = input().strip()
        except (EOFError, KeyboardInterrupt):
            print(); sys.exit(0)
        if choice.isdigit() and 1 <= int(choice) <= len(groups):
            client_category = groups[int(choice) - 1]
            break
        matched_grp = next((g for g in groups if g.lower().startswith(choice.lower())), None)
        if matched_grp:
            client_category = matched_grp
            break
        print(f"  {DIM}Enter a number 1–{len(groups)}{RESET}")

    # ── Industry: Step 2 — choose subcategory ─────────────────
    subcategories = INDUSTRY_GROUPS[client_category]
    print(f"\n  {BOLD}{client_category} — business type  (enter number):{RESET}")
    for i, sub in enumerate(subcategories, 1):
        print(f"  {DIM}{i}.{RESET}  {sub}")

    while True:
        sys.stdout.write(f"  {GOLD}→{RESET} ")
        sys.stdout.flush()
        try:
            choice = input().strip()
        except (EOFError, KeyboardInterrupt):
            print(); sys.exit(0)
        if choice.isdigit() and 1 <= int(choice) <= len(subcategories):
            industry_category = subcategories[int(choice) - 1]
            client_industry   = industry_category
            break
        matched_sub = next((s for s in subcategories if s.lower().startswith(choice.lower())), None)
        if matched_sub:
            industry_category = matched_sub
            client_industry   = matched_sub
            break
        print(f"  {DIM}Enter a number 1–{len(subcategories)}{RESET}")

    print(f"  {GREEN}✓{RESET} {industry_category}  {DIM}({client_category}){RESET}")

    # ── Q2: Website or Linktree URL ────────────────────────────
    _q(2, "Website URL or Linktree URL")
    raw_q2 = _ask("", hint="e.g. https://acme.com  or  https://linktr.ee/acme")
    website_url, linktree_url = _classify_url(raw_q2) if raw_q2 else ("", "")

    # ── Q3: Social channels — auto-detect from Linktree or website ──
    platforms: Dict[str, Any] = {k: "" for k, _ in _PLATFORM_PATTERNS}
    platforms["unmatched"] = []
    auto_detected = False

    if linktree_url:
        _q(3, "Social channels — scanning Linktree")
        print(f"  {DIM}Fetching {linktree_url} …{RESET}")
        try:
            from auditors.linktree_scraper import LinktreeScraper
            lt_data = LinktreeScraper(linktree_url).scrape()
            if lt_data.get("data_verified") and lt_data.get("platforms_found"):
                platforms = _classified_to_platforms(lt_data["classified_links"])
                # Pull website URL discovered inside Linktree if Q2 gave us only a Linktree
                if not website_url:
                    website_url = platforms.pop("_website_from_linktree", "") or website_url
                else:
                    platforms.pop("_website_from_linktree", None)
                auto_detected = True
                print(f"  {GREEN}✓{RESET} Auto-detected from Linktree ({lt_data['scrape_status']}):")
                print(_platform_summary(platforms))
            else:
                print(f"  {DIM}Could not parse Linktree ({lt_data.get('scrape_status', '?')}) — enter social URLs manually below{RESET}")
        except Exception as exc:
            print(f"  {DIM}Linktree scrape error: {exc}{RESET}")

    elif website_url:
        _q(3, "Social channels — scanning website")
        print(f"  {DIM}Scanning {website_url} for social links …{RESET}")
        classified = _scrape_website_socials(website_url)
        if classified:
            platforms = _classified_to_platforms(classified)
            platforms.pop("_website_from_linktree", None)
            auto_detected = True
            print(f"  {GREEN}✓{RESET} Found on website:")
            print(_platform_summary(platforms))
        else:
            print(f"  {DIM}No social links found on website — enter below{RESET}")

    # Manual entry if auto-detect failed or no URL was given
    if not auto_detected:
        if not linktree_url and not website_url:
            _q(3, "Social handles & URLs  (paste everything — auto-detected)")
        else:
            print(f"\n  {BOLD}[3/{12}]{RESET}  {BOLD}Add missing social handles / URLs{RESET}")
        print(f"  {DIM}Paste handles or profile URLs — any format, any order{RESET}")
        print(f"  {DIM}e.g.  linkedin.com/company/acme  @acme_co  youtube.com/@acme{RESET}")
        try:
            sys.stdout.write(f"  {GOLD}→{RESET} ")
            sys.stdout.flush()
            social_raw = input().strip()
        except (EOFError, KeyboardInterrupt):
            print(); sys.exit(0)
        if social_raw:
            platforms = _detect_platforms(social_raw)
            platforms.setdefault("unmatched", [])
            # Linktree URL pasted in social field
            if not linktree_url and platforms.get("linktree_url"):
                linktree_url = platforms["linktree_url"]
            print(_platform_summary(platforms))

    # Always offer "add anything missing?" when auto-detected
    elif auto_detected:
        print(f"  {DIM}Add any missing handles/URLs, or press Enter to continue:{RESET}")
        try:
            sys.stdout.write(f"  {GOLD}→{RESET} ")
            sys.stdout.flush()
            extra = input().strip()
        except (EOFError, KeyboardInterrupt):
            print(); sys.exit(0)
        if extra:
            extra_p = _detect_platforms(extra)
            for k, _ in _PLATFORM_PATTERNS:
                if extra_p.get(k) and not platforms.get(k):
                    platforms[k] = extra_p[k]
            print(_platform_summary(platforms))

    # ── Q4: Target market / ICP ────────────────────────────────
    _q(4, "Target market — who is your ideal client?")
    print(f"  {DIM}Be specific: industry, role, size, geography, pain point{RESET}")
    stated_target_market = _ask(
        "",
        hint="e.g.  Financial advisors and CPAs at RIA firms with $50M+ AUM",
        required=True,
    )

    # ── Q5: Monthly ad budget ──────────────────────────────────
    _q(5, "Monthly paid ad budget")
    monthly_ad_budget = _ask_number("", default=0.0)

    # ── Q6: Email list size ────────────────────────────────────
    _q(6, "Email list size  (0 if none)")
    email_list_size = _ask_number("", default=0.0, is_int=True)

    # ── Q7: Email send frequency ───────────────────────────────
    _q(7, "How often do you email your list?")
    raw_freq = _ask("", hint="e.g.  weekly · monthly · biweekly · never")
    email_send_frequency, has_active_newsletter, has_email_marketing = _parse_email_freq(raw_freq)

    # ── Q8: Competitor URLs ────────────────────────────────────
    _q(8, "Top competitor websites  (up to 3 — press Enter to skip)")
    print(f"  {DIM}Paste one per line, or comma-separate on one line{RESET}")
    competitor_urls = []
    while len(competitor_urls) < 3:
        try:
            sys.stdout.write(f"  {GOLD}→{RESET} ")
            sys.stdout.flush()
            line = input().strip().strip(",;")
        except (EOFError, KeyboardInterrupt):
            print(); sys.exit(0)
        if not line:
            break
        # Handle comma-separated on one line
        for tok in re.split(r"[\s,;]+", line):
            tok = tok.strip()
            if tok and len(competitor_urls) < 3:
                url = _normalise(tok) if "." in tok else tok
                if url:
                    competitor_urls.append(url)
        # If user pasted everything on one line, stop after parsing it
        if competitor_urls:
            break
    if competitor_urls:
        print(f"  {GREEN}✓{RESET} {len(competitor_urls)} competitor(s): {', '.join(competitor_urls)}")

    # ── Q9: Biggest marketing challenge ───────────────────────
    _q(9, "Biggest marketing challenge right now?")
    biggest_marketing_challenge = _ask(
        "",
        hint="e.g.  Not enough leads · Hard to stand out · No consistent content · Referrals only",
    )

    # ── Q10: Contact email (required) ─────────────────────────
    _q(10, "Client email address")
    _EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
    while True:
        contact_email = _ask("", hint="e.g.  jane@acme.com", required=True)
        if _EMAIL_RE.match(contact_email):
            break
        print(f"  {DIM}Please enter a valid email address.{RESET}")

    # ── Rate limit check (runs after email is known) ───────────
    try:
        from intake.rate_limiter import RateLimiter, get_public_ip as _get_ip
        _rl = RateLimiter()
        _ip = _get_ip() if not _rl.bypass else None
        _allowed, _reason = _rl.check(
            email       = contact_email,
            website_url = website_url or linktree_url,
            ip_address  = _ip,
        )
        if not _allowed:
            print(f"\n{'─'*56}")
            print(f"  {RED}⛔  AUDIT NOT AVAILABLE{RESET}")
            print(f"{'─'*56}")
            for _line in _reason.split("\n"):
                print(f"  {_line}")
            print(f"{'─'*56}\n")
            sys.exit(0)
    except SystemExit:
        raise
    except Exception as _rl_err:
        pass   # rate limiter failure never blocks an audit

    # ── Q11: Phone number (optional) ──────────────────────────
    _q(11, "Client phone number  (optional — press Enter to skip)")
    phone_number = _ask("", hint="e.g.  (555) 867-5309  or  +1 555 867 5309")

    # ── Q12: Marketing consent (required) ─────────────────────
    _q(12, "Marketing consent — may GMG contact this client about future services?")
    print(f"  {DIM}This is required to store the record.{RESET}")
    while True:
        raw_consent = _ask("", hint="y / n", required=True).lower()
        if raw_consent in ("y", "yes"):
            marketing_consent = True
            print(f"  {GREEN}✓{RESET} Consent recorded: Yes")
            break
        if raw_consent in ("n", "no"):
            marketing_consent = False
            print(f"  {GREEN}✓{RESET} Consent recorded: No")
            break
        print(f"  {DIM}Please enter  y  or  n{RESET}")

    # ── Confirmation ────────────────────────────────────────────
    print(f"\n{BOLD}{'═' * 56}{RESET}")
    print(f"{BOLD}{GREEN}  INTAKE COMPLETE{RESET}")
    print(f"{LINE}")
    print(f"  Client   : {client_name}  ({industry_category} · {client_category})")
    print(f"  Email    : {contact_email}")
    if phone_number:
        print(f"  Phone    : {phone_number}")
    print(f"  Consent  : {'Yes' if marketing_consent else 'No'}")
    print(f"  Website  : {website_url or linktree_url or '—'}")
    detected_names = [
        lbl for k, lbl in [
            ("linkedin_url","LinkedIn"), ("instagram_handle","Instagram"),
            ("youtube_channel_url","YouTube"), ("facebook_page_url","Facebook"),
            ("tiktok_handle","TikTok"), ("twitter_handle","X"),
            ("discord_url","Discord"),
        ] if platforms.get(k)
    ]
    print(f"  Channels : {', '.join(detected_names) or '—'}")
    print(f"  ICP      : {stated_target_market}")
    print(f"  Ad budget: ${monthly_ad_budget:,.0f}/month")
    print(f"  List     : {email_list_size:,} subscribers · {email_send_frequency or 'not set'}")
    if competitor_urls:
        print(f"  Compete  : {', '.join(competitor_urls)}")
    if biggest_marketing_challenge:
        print(f"  Challenge: {biggest_marketing_challenge[:80]}")
    print(f"{BOLD}{'═' * 56}{RESET}\n")

    # ── Build data dict ─────────────────────────────────────────
    data = {
        "client_name":             client_name,
        "client_industry":         client_industry,
        "industry_category":       industry_category,
        "client_category":         client_category,
        "website_url":             website_url,
        "linktree_url":            linktree_url,
        "stated_target_market":    stated_target_market,
        "target_audience":         stated_target_market,
        "stated_icp_industry":     client_industry,
        "stated_value_prop":       "",
        "primary_goal":            f"Generate qualified leads from: {stated_target_market}",
        "monthly_ad_budget":       monthly_ad_budget,
        "email_list_size":         email_list_size,
        "email_send_frequency":    email_send_frequency,
        "has_email_marketing":     has_email_marketing,
        "has_active_newsletter":   has_active_newsletter,
        # Social
        "linkedin_url":            platforms.get("linkedin_url", ""),
        "instagram_handle":        platforms.get("instagram_handle", ""),
        "youtube_channel_url":     platforms.get("youtube_channel_url", ""),
        "facebook_page_url":       platforms.get("facebook_page_url", ""),
        "tiktok_handle":           platforms.get("tiktok_handle", ""),
        "twitter_handle":          platforms.get("twitter_handle", ""),
        "discord_url":             platforms.get("discord_url", ""),
        # Contact & consent
        "contact_email":           contact_email,
        "phone_number":            phone_number,
        "marketing_consent":       marketing_consent,
        # Defaults for fields not asked
        "agency_name":             "C.A.S.H. Report by GMG",
        "current_client_count":    0,
        "current_client_types":    "",
        "has_referral_system":     False,
        "referral_system_description": "",
        "has_lead_magnet":         False,
        "booking_tool":            "",
        "team_hourly_rate":        0.0,
        "team_size":               1,
        "platform_posting_frequency": {},
        "top_competitors":         [],
        "competitor_urls":         competitor_urls,
        "biggest_marketing_challenge": biggest_marketing_challenge,
        "intake_completed":        True,
    }

    # ── Save to JSON for re-runs ────────────────────────────────
    try:
        with open(INTAKE_SAVE_PATH, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

    # ── Save to client database ─────────────────────────────────
    try:
        row_id = save_intake_record(
            client_name=client_name,
            email=contact_email,
            phone_number=phone_number,
            marketing_consent=marketing_consent,
            business_type=industry_category,
            website=website_url or linktree_url,
        )
        print(f"  {GREEN}✓ Client record saved to cash_clients.db  (id={row_id}){RESET}\n")
    except Exception as exc:
        print(f"  {DIM}DB save skipped: {exc}{RESET}\n")

    return _dict_to_config(data)


# ── Load / convert helpers ─────────────────────────────────────

def _load_intake() -> ClientConfig:
    with open(INTAKE_SAVE_PATH) as f:
        data = json.load(f)
    return _dict_to_config(data)


def _dict_to_config(data: dict) -> ClientConfig:
    return ClientConfig(
        client_name=data.get("client_name", ""),
        contact_email=data.get("contact_email", ""),
        phone_number=data.get("phone_number", ""),
        marketing_consent=bool(data.get("marketing_consent", False)),
        client_industry=data.get("client_industry", "General"),
        industry_category=data.get("industry_category", "Other"),
        client_category=data.get("client_category", ""),
        website_url=data.get("website_url", ""),
        linktree_url=data.get("linktree_url", ""),
        agency_name=data.get("agency_name", "C.A.S.H. Report by GMG"),
        stated_target_market=data.get("stated_target_market", ""),
        stated_icp_industry=data.get("stated_icp_industry", ""),
        stated_value_prop=data.get("stated_value_prop", ""),
        primary_goal=data.get("primary_goal", ""),
        current_client_count=int(data.get("current_client_count", 0)),
        current_client_types=data.get("current_client_types", ""),
        email_list_size=int(data.get("email_list_size", 0)),
        email_send_frequency=data.get("email_send_frequency", ""),
        has_email_marketing=bool(data.get("has_email_marketing", False)),
        has_active_newsletter=bool(data.get("has_active_newsletter", False)),
        has_referral_system=bool(data.get("has_referral_system", False)),
        referral_system_description=data.get("referral_system_description", ""),
        has_lead_magnet=bool(data.get("has_lead_magnet", False)),
        booking_tool=data.get("booking_tool", ""),
        team_hourly_rate=float(data.get("team_hourly_rate", 0.0)),
        platform_posting_frequency=data.get("platform_posting_frequency", {}),
        monthly_ad_budget=float(data.get("monthly_ad_budget", 0)),
        team_size=int(data.get("team_size", 1)),
        linkedin_url=data.get("linkedin_url", ""),
        instagram_handle=data.get("instagram_handle", ""),
        youtube_channel_url=data.get("youtube_channel_url", ""),
        facebook_page_url=data.get("facebook_page_url", ""),
        discord_url=data.get("discord_url", ""),
        tiktok_handle=data.get("tiktok_handle", ""),
        twitter_handle=data.get("twitter_handle", ""),
        top_competitors=data.get("top_competitors", []),
        competitor_urls=data.get("competitor_urls", []),
        biggest_marketing_challenge=data.get("biggest_marketing_challenge", ""),
        target_audience=data.get("target_audience", ""),
        intake_completed=True,
    )


# ── Backwards-compat alias used by run_goguerrilla.py ─────────
save_client_to_db = None  # import guard — not used by new flow


if __name__ == "__main__":
    cfg = run_intake()
    print(f"\n  Config built for: {cfg.client_name}")
    print(f"  ICP: {cfg.stated_target_market}")
    print(f"  Channels: {cfg.active_social_channels}")
