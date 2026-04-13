"""
Industry-Specific Scoring Benchmarks for the C.A.S.H. Report

Four client categories, each with subcategories:

  1. Professional Services
       Financial Advisory · Legal · Healthcare & Medical · Accounting & CPA

  2. Local & Consumer Business
       Restaurant & Food Service · Retail & E-commerce · Home Services & Trades
       Real Estate · Beauty & Wellness

  3. Brands, Founders & Entrepreneurs
       Personal Brand & Creator · Coach, Speaker & Author · Startup & Early-stage

  4. B2B & Service Companies
       Agency & Consulting · SaaS & Tech · Non-profit & Cause
       Professional B2B Services

Platform weights
----------------
  Weight ≥ 1.3  — primary channel  (absence flagged as CRITICAL)
  Weight 0.9–1.2 — standard / recommended
  Weight ≤ 0.8  — low relevance (absence is informational only, not penalised)

  "GBP" key controls how heavily Google Business Profile is weighted
  in GEO / Authority scoring.

Posting benchmarks
------------------
  Per-subcategory min / ideal / max posts per week.
  Rule: reaching "ideal" = excellent; at/above "min" = meets standard.
  Professional / B2B: LinkedIn 2×/week = excellent per C.A.S.H. spec.
  Consumer / local: Instagram and Facebook are the primary channels.
  Restaurants and retail are NOT evaluated on LinkedIn cadence.
  Law firms and financial advisors are NOT penalised for low Instagram cadence.
"""
from typing import Dict, Any

# ── Four client category groups ────────────────────────────────
INDUSTRY_GROUPS: Dict[str, list] = {
    "Professional Services": [
        "Financial Advisory",
        "Legal",
        "Healthcare & Medical",
        "Accounting & CPA",
    ],
    "Local & Consumer Business": [
        "Restaurant & Food Service",
        "Retail & E-commerce",
        "Home Services & Trades",
        "Real Estate",
        "Beauty & Wellness",
    ],
    "Brands, Founders & Entrepreneurs": [
        "Personal Brand & Creator",
        "Coach, Speaker & Author",
        "Startup & Early-stage",
    ],
    "B2B & Service Companies": [
        "Agency & Consulting",
        "SaaS & Tech",
        "Non-profit & Cause",
        "Professional B2B Services",
    ],
}

# ── Flat industry list (all subcategories + "Other") ───────────
# Used in dropdown menus and anywhere INDUSTRIES is iterated.
INDUSTRIES: list = [
    sub
    for subs in INDUSTRY_GROUPS.values()
    for sub in subs
] + ["Other"]

# ── Reverse map: subcategory → parent group ────────────────────
_SUBCATEGORY_TO_GROUP: Dict[str, str] = {
    sub: grp
    for grp, subs in INDUSTRY_GROUPS.items()
    for sub in subs
}

# ── Aliases for old industry names (backwards-compat) ─────────
# Handles JSON files saved with the old 9-category schema.
_INDUSTRY_ALIASES: Dict[str, str] = {
    "Financial Services":  "Financial Advisory",
    "B2B Services":        "Agency & Consulting",
    "Non-profit":          "Non-profit & Cause",
    "Restaurant":          "Restaurant & Food Service",
    "Retail":              "Retail & E-commerce",
    "Healthcare":          "Healthcare & Medical",
    "Real Estate":         "Real Estate",   # unchanged, listed for explicitness
    "Legal":               "Legal",
}

# ── Platform importance weights by subcategory ─────────────────
# 1.5 = critical (flag absence as critical in social auditor)
# 1.0 = standard
# 0.5 = low relevance (do not penalise absence)
PLATFORM_WEIGHTS: Dict[str, Dict[str, float]] = {

    # ── Professional Services ──────────────────────────────────

    "Financial Advisory": {
        "LinkedIn":  1.5,   # primary B2B credibility channel
        "YouTube":   1.3,   # education / market-update authority
        "Facebook":  0.7,
        "Instagram": 0.5,   # not how HNW clients find advisors
        "TikTok":    0.4,
        "Discord":   0.3,
        "GBP":       0.6,   # local discovery minor for advisors
    },
    "Legal": {
        "LinkedIn":  1.5,   # primary professional-services channel
        "YouTube":   1.1,   # explainer videos, Q&A
        "Facebook":  0.7,
        "Instagram": 0.5,   # not how clients find attorneys
        "TikTok":    0.3,
        "Discord":   0.2,
        "GBP":       1.2,   # local clients search Maps for attorneys
    },
    "Healthcare & Medical": {
        "LinkedIn":  0.9,   # B2B referrals only
        "YouTube":   1.2,   # patient education
        "Facebook":  1.3,   # patient communities, local groups
        "Instagram": 1.1,   # wellness aesthetics
        "TikTok":    0.8,   # #HealthTok growing fast
        "Discord":   0.3,
        "GBP":       1.5,   # critical — patients use Maps to find providers
    },
    "Accounting & CPA": {
        "LinkedIn":  1.4,   # B2B referrals and thought leadership
        "YouTube":   1.0,   # tax tips, explainers
        "Facebook":  0.8,   # local business owner groups
        "Instagram": 0.4,   # not how clients find CPAs
        "TikTok":    0.3,
        "Discord":   0.2,
        "GBP":       0.9,   # local CPA discovery via Maps is real
    },

    # ── Local & Consumer Business ──────────────────────────────

    "Restaurant & Food Service": {
        "LinkedIn":  0.3,   # irrelevant for restaurants
        "YouTube":   0.6,
        "Facebook":  1.3,   # events, community, local ads
        "Instagram": 1.5,   # primary — food photos drive visits
        "TikTok":    1.4,   # viral food content
        "Discord":   0.3,
        "GBP":       1.5,   # critical — diners rely on Maps ratings/hours/menus
    },
    "Retail & E-commerce": {
        "LinkedIn":  0.4,   # not a retail discovery channel
        "YouTube":   0.9,   # product demos and reviews
        "Facebook":  1.2,   # paid ads + community groups
        "Instagram": 1.5,   # primary — product discovery
        "TikTok":    1.4,   # TikTok Shop + viral products
        "Discord":   0.4,
        "GBP":       1.3,   # local retail in-store discovery
    },
    "Home Services & Trades": {
        "LinkedIn":  0.4,   # not how homeowners find contractors
        "YouTube":   1.0,   # how-to content builds authority
        "Facebook":  1.4,   # Nextdoor-style local groups, retargeting ads
        "Instagram": 0.8,   # before/after project photos
        "TikTok":    0.7,   # project reveals / DIY
        "Discord":   0.2,
        "GBP":       1.5,   # critical — primary way customers find tradespeople
    },
    "Real Estate": {
        "LinkedIn":  0.9,
        "YouTube":   1.4,   # property tours, market updates
        "Facebook":  1.2,   # local buyer/seller groups
        "Instagram": 1.3,   # listing photos, reels
        "TikTok":    1.2,   # growing fast for listings
        "Discord":   0.3,
        "GBP":       1.2,   # local agent discovery
    },
    "Beauty & Wellness": {
        "LinkedIn":  0.4,   # not a beauty discovery channel
        "YouTube":   0.9,   # tutorials
        "Facebook":  1.2,   # local community groups, retargeting ads
        "Instagram": 1.5,   # primary — visual before/after
        "TikTok":    1.4,   # beauty trends, tutorials
        "Discord":   0.3,
        "GBP":       1.4,   # local discovery critical for salons/spas/gyms
    },

    # ── Brands, Founders & Entrepreneurs ──────────────────────

    "Personal Brand & Creator": {
        "LinkedIn":  1.1,
        "YouTube":   1.4,   # long-form is the core content engine
        "Facebook":  0.8,
        "Instagram": 1.3,   # primary visual platform
        "TikTok":    1.3,   # viral growth
        "Discord":   0.9,   # community building
        "GBP":       0.4,
    },
    "Coach, Speaker & Author": {
        "LinkedIn":  1.4,   # primary — B2B clients discover coaches here
        "YouTube":   1.3,   # speaking clips, long-form content
        "Facebook":  1.0,   # groups / community
        "Instagram": 1.1,   # quotes, behind-the-scenes
        "TikTok":    0.9,
        "Discord":   0.7,
        "GBP":       0.5,
    },
    "Startup & Early-stage": {
        "LinkedIn":  1.3,   # investor + customer discovery
        "YouTube":   1.1,   # product demos, founder story
        "Facebook":  0.8,
        "Instagram": 1.0,
        "TikTok":    0.8,
        "Discord":   1.0,   # community + early adopters
        "GBP":       0.5,
    },

    # ── B2B & Service Companies ────────────────────────────────

    "Agency & Consulting": {
        "LinkedIn":  1.5,   # primary — B2B buyers find agencies on LinkedIn
        "YouTube":   1.3,   # thought leadership, demos
        "Facebook":  0.7,
        "Instagram": 0.8,
        "TikTok":    0.5,
        "Discord":   0.6,
        "GBP":       0.5,
    },
    "SaaS & Tech": {
        "LinkedIn":  1.4,   # B2B discovery and content
        "YouTube":   1.3,   # product walkthroughs, tutorials
        "Facebook":  0.7,
        "Instagram": 0.8,
        "TikTok":    0.6,
        "Discord":   1.1,   # developer/user communities
        "GBP":       0.4,
    },
    "Non-profit & Cause": {
        "LinkedIn":  1.1,   # corporate partnerships, volunteer recruitment
        "YouTube":   1.1,   # mission storytelling
        "Facebook":  1.4,   # primary — donor and volunteer community
        "Instagram": 1.1,   # visual mission storytelling
        "TikTok":    0.9,
        "Discord":   0.8,
        "GBP":       0.8,
    },
    "Professional B2B Services": {
        "LinkedIn":  1.5,   # primary
        "YouTube":   1.2,
        "Facebook":  0.7,
        "Instagram": 0.7,
        "TikTok":    0.4,
        "Discord":   0.5,
        "GBP":       0.6,
    },

    # ── Fallback ───────────────────────────────────────────────
    "Other": {p: 1.0 for p in
              ["LinkedIn", "YouTube", "Facebook", "Instagram",
               "TikTok", "Discord", "GBP"]},
}

# ── Posting frequency benchmarks (posts/week) ─────────────────
# Key rule: each subcategory is calibrated to its own primary channels.
# Restaurants are not benchmarked on LinkedIn cadence.
# Law firms / financial advisors are not benchmarked on Instagram cadence.
POSTING_BENCHMARKS: Dict[str, Dict[str, Dict[str, float]]] = {

    # ── Professional Services ──────────────────────────────────

    "Financial Advisory": {
        "LinkedIn":  {"min": 1,    "ideal": 2,   "max": 5},    # 2×/week = excellent
        "YouTube":   {"min": 0.25, "ideal": 0.5, "max": 1},    # monthly long-form is fine
        "Facebook":  {"min": 1,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 1,    "ideal": 3,   "max": 7},    # secondary channel
        "TikTok":    {"min": 1,    "ideal": 3,   "max": 7},
        "Discord":   {"min": 1,    "ideal": 3,   "max": 7},
    },
    "Legal": {
        "LinkedIn":  {"min": 1,    "ideal": 2,   "max": 5},    # 2×/week = excellent
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 2},
        "Facebook":  {"min": 1,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 1,    "ideal": 3,   "max": 7},    # secondary channel
        "TikTok":    {"min": 1,    "ideal": 3,   "max": 7},
        "Discord":   {"min": 1,    "ideal": 3,   "max": 7},
    },
    "Healthcare & Medical": {
        "LinkedIn":  {"min": 2,    "ideal": 3,   "max": 7},
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 2},
        "Facebook":  {"min": 3,    "ideal": 5,   "max": 14},
        "Instagram": {"min": 3,    "ideal": 5,   "max": 14},
        "TikTok":    {"min": 2,    "ideal": 5,   "max": 14},
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "Accounting & CPA": {
        "LinkedIn":  {"min": 1,    "ideal": 2,   "max": 5},    # 2×/week = excellent
        "YouTube":   {"min": 0.25, "ideal": 0.5, "max": 1},    # monthly explainers
        "Facebook":  {"min": 1,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 1,    "ideal": 2,   "max": 5},    # very secondary
        "TikTok":    {"min": 1,    "ideal": 3,   "max": 7},
        "Discord":   {"min": 1,    "ideal": 3,   "max": 7},
    },

    # ── Local & Consumer Business ──────────────────────────────

    "Restaurant & Food Service": {
        "LinkedIn":  {"min": 0.25, "ideal": 0.5, "max": 1},    # nearly irrelevant
        "YouTube":   {"min": 0.25, "ideal": 0.5, "max": 1},
        "Facebook":  {"min": 3,    "ideal": 5,   "max": 14},
        "Instagram": {"min": 5,    "ideal": 10,  "max": 21},   # daily+ is table stakes
        "TikTok":    {"min": 5,    "ideal": 10,  "max": 21},   # viral food content
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "Retail & E-commerce": {
        "LinkedIn":  {"min": 0.5,  "ideal": 1,   "max": 3},    # low relevance
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 3},
        "Facebook":  {"min": 3,    "ideal": 5,   "max": 14},
        "Instagram": {"min": 5,    "ideal": 10,  "max": 21},   # primary sales channel
        "TikTok":    {"min": 5,    "ideal": 10,  "max": 21},
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "Home Services & Trades": {
        "LinkedIn":  {"min": 0.25, "ideal": 0.5, "max": 1},    # nearly irrelevant
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 2},    # project walkthrough videos
        "Facebook":  {"min": 3,    "ideal": 5,   "max": 14},   # local group presence
        "Instagram": {"min": 2,    "ideal": 4,   "max": 10},   # before/after
        "TikTok":    {"min": 2,    "ideal": 5,   "max": 14},
        "Discord":   {"min": 1,    "ideal": 3,   "max": 7},
    },
    "Real Estate": {
        "LinkedIn":  {"min": 2,    "ideal": 3,   "max": 7},
        "YouTube":   {"min": 1,    "ideal": 2,   "max": 4},    # property tour videos
        "Facebook":  {"min": 3,    "ideal": 5,   "max": 14},
        "Instagram": {"min": 4,    "ideal": 7,   "max": 14},
        "TikTok":    {"min": 3,    "ideal": 7,   "max": 14},
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "Beauty & Wellness": {
        "LinkedIn":  {"min": 0.25, "ideal": 0.5, "max": 1},    # nearly irrelevant
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 3},    # tutorials
        "Facebook":  {"min": 3,    "ideal": 5,   "max": 10},
        "Instagram": {"min": 5,    "ideal": 10,  "max": 21},   # primary visual channel
        "TikTok":    {"min": 5,    "ideal": 10,  "max": 21},   # beauty trends
        "Discord":   {"min": 1,    "ideal": 3,   "max": 7},
    },

    # ── Brands, Founders & Entrepreneurs ──────────────────────

    "Personal Brand & Creator": {
        "LinkedIn":  {"min": 2,    "ideal": 4,   "max": 7},
        "YouTube":   {"min": 1,    "ideal": 2,   "max": 4},    # core content engine
        "Facebook":  {"min": 2,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 5,    "ideal": 10,  "max": 21},
        "TikTok":    {"min": 5,    "ideal": 14,  "max": 28},
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "Coach, Speaker & Author": {
        "LinkedIn":  {"min": 2,    "ideal": 3,   "max": 7},    # primary discovery
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 2},    # speaking clips
        "Facebook":  {"min": 2,    "ideal": 3,   "max": 7},    # group content
        "Instagram": {"min": 3,    "ideal": 5,   "max": 14},
        "TikTok":    {"min": 2,    "ideal": 5,   "max": 14},
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "Startup & Early-stage": {
        "LinkedIn":  {"min": 2,    "ideal": 4,   "max": 7},
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 3},
        "Facebook":  {"min": 2,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 2,    "ideal": 4,   "max": 10},
        "TikTok":    {"min": 2,    "ideal": 5,   "max": 14},
        "Discord":   {"min": 3,    "ideal": 7,   "max": 14},
    },

    # ── B2B & Service Companies ────────────────────────────────

    "Agency & Consulting": {
        "LinkedIn":  {"min": 2,    "ideal": 4,   "max": 7},    # primary
        "YouTube":   {"min": 1,    "ideal": 2,   "max": 4},
        "Facebook":  {"min": 2,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 2,    "ideal": 4,   "max": 10},
        "TikTok":    {"min": 2,    "ideal": 5,   "max": 14},
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "SaaS & Tech": {
        "LinkedIn":  {"min": 2,    "ideal": 4,   "max": 7},
        "YouTube":   {"min": 1,    "ideal": 2,   "max": 4},    # product walkthroughs
        "Facebook":  {"min": 2,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 2,    "ideal": 4,   "max": 10},
        "TikTok":    {"min": 2,    "ideal": 5,   "max": 14},
        "Discord":   {"min": 3,    "ideal": 7,   "max": 14},
    },
    "Non-profit & Cause": {
        "LinkedIn":  {"min": 2,    "ideal": 3,   "max": 7},
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 2},
        "Facebook":  {"min": 3,    "ideal": 5,   "max": 14},   # primary
        "Instagram": {"min": 3,    "ideal": 5,   "max": 14},
        "TikTok":    {"min": 2,    "ideal": 5,   "max": 14},
        "Discord":   {"min": 2,    "ideal": 5,   "max": 14},
    },
    "Professional B2B Services": {
        "LinkedIn":  {"min": 2,    "ideal": 3,   "max": 7},    # primary
        "YouTube":   {"min": 0.5,  "ideal": 1,   "max": 3},
        "Facebook":  {"min": 2,    "ideal": 3,   "max": 7},
        "Instagram": {"min": 2,    "ideal": 3,   "max": 7},
        "TikTok":    {"min": 1,    "ideal": 3,   "max": 7},
        "Discord":   {"min": 1,    "ideal": 3,   "max": 7},
    },
}

# Generic fallback
_GENERIC_BENCHMARKS: Dict[str, Dict[str, float]] = {
    "LinkedIn":  {"min": 2,   "ideal": 4,  "max": 7},
    "Instagram": {"min": 3,   "ideal": 7,  "max": 14},
    "YouTube":   {"min": 1,   "ideal": 2,  "max": 5},
    "Facebook":  {"min": 3,   "ideal": 5,  "max": 14},
    "TikTok":    {"min": 5,   "ideal": 14, "max": 28},
    "Discord":   {"min": 3,   "ideal": 7,  "max": 14},
}
POSTING_BENCHMARKS["Other"] = _GENERIC_BENCHMARKS

# ── Primary platforms per subcategory ─────────────────────────
# Absence flagged as CRITICAL in social auditor.
PRIMARY_PLATFORMS: Dict[str, list] = {
    # Professional Services
    "Financial Advisory":          ["LinkedIn"],
    "Legal":                       ["LinkedIn"],
    "Healthcare & Medical":        ["Facebook", "Instagram"],
    "Accounting & CPA":            ["LinkedIn"],
    # Local & Consumer Business
    "Restaurant & Food Service":   ["Instagram", "Facebook"],
    "Retail & E-commerce":         ["Instagram"],
    "Home Services & Trades":      ["Facebook"],
    "Real Estate":                 ["Instagram", "YouTube"],
    "Beauty & Wellness":           ["Instagram"],
    # Brands, Founders & Entrepreneurs
    "Personal Brand & Creator":    ["YouTube", "Instagram"],
    "Coach, Speaker & Author":     ["LinkedIn"],
    "Startup & Early-stage":       ["LinkedIn"],
    # B2B & Service Companies
    "Agency & Consulting":         ["LinkedIn"],
    "SaaS & Tech":                 ["LinkedIn"],
    "Non-profit & Cause":          ["Facebook"],
    "Professional B2B Services":   ["LinkedIn"],
    # Fallback
    "Other":                       [],
}

# ── Recommended platforms per subcategory ─────────────────────
# Absence flagged as a warning (not critical).
RECOMMENDED_PLATFORMS: Dict[str, list] = {
    # Professional Services
    "Financial Advisory":          ["YouTube", "LinkedIn"],
    "Legal":                       ["YouTube", "LinkedIn"],
    "Healthcare & Medical":        ["Google Business Profile", "Facebook", "Instagram"],
    "Accounting & CPA":            ["YouTube", "LinkedIn"],
    # Local & Consumer Business
    "Restaurant & Food Service":   ["Instagram", "TikTok", "Google Business Profile"],
    "Retail & E-commerce":         ["Instagram", "TikTok", "Facebook"],
    "Home Services & Trades":      ["Facebook", "Instagram", "YouTube"],
    "Real Estate":                 ["YouTube", "Instagram", "Facebook"],
    "Beauty & Wellness":           ["Instagram", "TikTok", "Facebook"],
    # Brands, Founders & Entrepreneurs
    "Personal Brand & Creator":    ["YouTube", "Instagram", "TikTok"],
    "Coach, Speaker & Author":     ["LinkedIn", "YouTube", "Instagram"],
    "Startup & Early-stage":       ["LinkedIn"],
    # B2B & Service Companies
    "Agency & Consulting":         ["LinkedIn", "YouTube"],
    "SaaS & Tech":                 ["LinkedIn", "YouTube"],
    "Non-profit & Cause":          ["Facebook", "Instagram"],
    "Professional B2B Services":   ["LinkedIn", "YouTube"],
    # Fallback
    "Other":                       [],
}

# ── GBP importance descriptions per subcategory ───────────────
GBP_IMPORTANCE: Dict[str, str] = {
    # Professional Services
    "Financial Advisory":
        "Moderate — some clients search locally for financial advisors; "
        "not the primary discovery channel for most advisors.",
    "Legal":
        "High — local clients frequently search Maps for attorneys.",
    "Healthcare & Medical":
        "Critical — patients consistently use Maps to find providers.",
    "Accounting & CPA":
        "Moderate-High — local businesses do search Maps for CPAs and bookkeepers.",
    # Local & Consumer Business
    "Restaurant & Food Service":
        "Critical — diners rely on Maps for ratings, hours, menus, and photos.",
    "Retail & E-commerce":
        "High — local shoppers check GBP hours, directions, photos, and reviews.",
    "Home Services & Trades":
        "Critical — the primary channel homeowners use to find tradespeople.",
    "Real Estate":
        "High — buyers and sellers search Maps for local agents and offices.",
    "Beauty & Wellness":
        "High — local discovery critical for salons, spas, gyms, and med spas.",
    # Brands, Founders & Entrepreneurs
    "Personal Brand & Creator":
        "Low — personal brands and creators are not discovered via Maps.",
    "Coach, Speaker & Author":
        "Low-Moderate — national or virtual coaches rarely need GBP.",
    "Startup & Early-stage":
        "Low — early-stage companies are found via digital channels, not Maps.",
    # B2B & Service Companies
    "Agency & Consulting":
        "Low — B2B buyers rarely discover agencies via Maps.",
    "SaaS & Tech":
        "Low — software products are found via search and LinkedIn, not Maps.",
    "Non-profit & Cause":
        "Moderate — donors and volunteers may search locally for the organisation.",
    "Professional B2B Services":
        "Low-Moderate — depends on whether clients are geographically local.",
    # Fallback
    "Other": "Moderate.",
}


# ── Public helpers ─────────────────────────────────────────────

def get_posting_benchmarks(platform: str, industry: str) -> Dict[str, float]:
    """Return min/ideal/max posts per week for this platform × subcategory."""
    canon = industry_label(industry)
    industry_map = POSTING_BENCHMARKS.get(canon, POSTING_BENCHMARKS["Other"])
    return industry_map.get(platform, _GENERIC_BENCHMARKS.get(
        platform, {"min": 1, "ideal": 3, "max": 7}
    ))


def get_platform_weight(platform: str, industry: str) -> float:
    """Return the importance multiplier (0.3–1.5) for this platform × subcategory."""
    canon = industry_label(industry)
    return PLATFORM_WEIGHTS.get(canon, PLATFORM_WEIGHTS["Other"]).get(platform, 1.0)


def get_primary_platforms(industry: str) -> list:
    """Platforms whose absence is a CRITICAL issue for this subcategory."""
    return PRIMARY_PLATFORMS.get(industry_label(industry), [])


def get_recommended_platforms(industry: str) -> list:
    """Platforms whose absence is a warning for this subcategory."""
    return RECOMMENDED_PLATFORMS.get(industry_label(industry), [])


def get_gbp_importance(industry: str) -> str:
    """Human-readable GBP importance note for this subcategory."""
    canon = industry_label(industry)
    return GBP_IMPORTANCE.get(canon, GBP_IMPORTANCE["Other"])


def get_industry_group(industry: str) -> str:
    """Return the parent group name for a subcategory, or 'Other'."""
    canon = industry_label(industry)
    return _SUBCATEGORY_TO_GROUP.get(canon, "Other")


def get_subcategories(group: str) -> list:
    """Return the list of subcategories for a parent group."""
    return INDUSTRY_GROUPS.get(group, [])


def is_local_business(industry: str) -> bool:
    """Return True if GBP is a primary signal for this subcategory (weight ≥ 1.3)."""
    return get_platform_weight("GBP", industry) >= 1.3


def is_b2b(industry: str) -> bool:
    """Return True if this subcategory is primarily B2B-oriented."""
    canon = industry_label(industry)
    group = _SUBCATEGORY_TO_GROUP.get(canon, "")
    if group in ("B2B & Service Companies", "Professional Services"):
        return True
    if canon in ("Startup & Early-stage", "Coach, Speaker & Author"):
        return True
    return False


def industry_label(industry: str) -> str:
    """
    Normalise free-text or legacy industry name to a canonical subcategory label.
    Falls back to 'Other' if no match found.
    """
    if not industry:
        return "Other"
    # Direct match (already canonical)
    if industry in INDUSTRIES:
        return industry
    # Legacy alias map
    if industry in _INDUSTRY_ALIASES:
        return _INDUSTRY_ALIASES[industry]
    # Fuzzy: check if any canonical name contains or is contained in the input
    il = industry.lower().strip()
    for canonical in INDUSTRIES:
        if canonical.lower() in il or il in canonical.lower():
            return canonical
    return "Other"
