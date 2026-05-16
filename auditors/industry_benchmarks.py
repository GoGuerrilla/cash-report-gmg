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


# ── Retention / referral framing per industry ──────────────────
#
# Horizon Advisers 2026-05-15: the synthesis produced "one free month
# for every referral client who signs" for a financial advisory firm.
# Financial advisors earn on AUM (% of assets) or fixed planning fees —
# they have no monthly subscription product. The "free month" framing
# is a SaaS pattern the model defaults to when retention guidance isn't
# industry-anchored. This table lets the prompt explicitly forbid
# wrong-flavor retention mechanics per industry and provide native
# alternatives — with a "skip rather than fabricate" fallback for
# industries where the model genuinely doesn't have a confident
# industry-appropriate pricing mechanic.
#
# Each entry has three keys consumed by the synthesis prompt builder:
#   revenue_model — one-line description of how the industry earns
#                   (so the model understands the constraint)
#   acceptable    — comma-separated framings that fit the revenue model
#   forbidden     — comma-separated framings that would damage credibility
#                   if used for this industry

_RETENTION_FRAMING: Dict[str, Dict[str, str]] = {
    "Financial Advisory": {
        "revenue_model": "fee-only or AUM-based (% of assets managed) — NOT subscription",
        "acceptable":    "complimentary planning review for the referrer, advisory-fee credit (X bps off next year's fee), referral acknowledgement gift, annual portfolio-review touchpoint, quarterly market-commentary newsletter",
        "forbidden":     "Offer 1 free month for every referred client who signs, Offer a free month per referral, Free month for closed referral, free trial of advisory services, monthly subscription discount, free credit hours, $X off your next subscription",
    },
    "Legal": {
        "revenue_model": "hourly billing, retainers, or contingency — NOT subscription",
        "acceptable":    "complimentary initial consultation, retainer-credit for referrals, fee credit on next engagement, annual legal-checkup outreach, structured client-anniversary touchpoint",
        "forbidden":     "free month, free trial, monthly subscription discount, 'free billable hours'",
    },
    "Accounting & CPA": {
        "revenue_model": "engagement-based or hourly — NOT subscription",
        "acceptable":    "complimentary tax-planning consultation, fee credit on next return, referral acknowledgement, quarterly tax-update touchpoint, year-end planning session",
        "forbidden":     "free month, free trial, monthly subscription discount",
    },
    "Healthcare & Medical": {
        "revenue_model": "per-visit fees or insurance-billed — referral kickbacks are REGULATED (Stark / anti-kickback)",
        "acceptable":    "patient-education resources, structured follow-up cadence (post-visit at 1/4/12 weeks), appointment-reminder system, annual wellness-check outreach",
        "forbidden":     "ANY monetary referral incentive (regulatory risk), free month, free trial, 'send a friend, get $X', patient bonus per referral",
    },
    "Real Estate": {
        "revenue_model": "commission-based per closed transaction",
        "acceptable":    "closing-gift program, past-client home-anniversary outreach, referral-network gifting, structured CRM cadence for past clients (annual market update + tax-time check-in)",
        "forbidden":     "free month, free trial, subscription discount",
    },
    "Home Services & Trades": {
        "revenue_model": "per-project or maintenance-plan based",
        "acceptable":    "'$X off your next service' for referrals, maintenance-plan upsell, post-job 30/60/90-day follow-up, annual maintenance reminder, loyalty pricing on repeat work",
        "forbidden":     "free month (only acceptable if a recurring service-plan product is verified in the audit data — do NOT assume one exists)",
    },
    "Restaurant & Food Service": {
        "revenue_model": "transaction-based; loyalty programs common",
        "acceptable":    "loyalty-program enrollment, birthday reward, referral-credit toward next visit, frequency-based VIP tier, win-back at 30-day-no-visit",
        "forbidden":     "(none — most retention framings are industry-appropriate; just confirm tooling exists before recommending)",
    },
    "Retail & E-commerce": {
        "revenue_model": "transaction-based; subscription only if subscription-box product",
        "acceptable":    "referral credit toward next order, loyalty-program enrollment, anniversary discount, post-purchase email cadence, win-back campaigns",
        "forbidden":     "free month / free trial framings UNLESS the audit data confirms a subscription product",
    },
    "Beauty & Wellness": {
        "revenue_model": "appointment-based; memberships common in some sub-verticals",
        "acceptable":    "membership-tier upsell, package-of-X pricing, referral credit toward next appointment, birthday reward, win-back at 60-day-no-visit",
        "forbidden":     "free month framing only acceptable if a membership product is verified in audit data",
    },
    "Personal Brand & Creator": {
        "revenue_model": "varies — courses, sponsorships, products, services",
        "acceptable":    "community-building rituals, email-list nurture cadence, audience-engagement loops, paid-community retention, repeat-buyer email sequence",
        "forbidden":     "subscription framings unless a verified membership / subscription product is in the audit data",
    },
    "Coach, Speaker & Author": {
        "revenue_model": "program-based, course sales, retainers, or one-off engagements",
        "acceptable":    "alumni community access, post-program nurture sequence, structured check-in cadence, advanced-program upsell, book-club / community ritual",
        "forbidden":     "free month / free trial (programs are priced per-cohort or per-engagement, not monthly)",
    },
    "Startup & Early-stage": {
        "revenue_model": "highly variable — DO NOT assume a revenue model without intake confirmation",
        "acceptable":    "structured customer-success cadence, advisory-call retention, founder-led check-ins, NPS feedback loops",
        "forbidden":     "specific pricing-mechanic retention without first confirming the client's revenue model from intake data — fabricated mechanics damage credibility",
    },
    "SaaS & Tech": {
        "revenue_model": "subscription-based (MRR / ARR) — free month / trial framings ARE native to this industry",
        "acceptable":    "free month for a closed referral, annual-upgrade discount, expansion-account credit, in-product engagement nudges, customer-success QBRs",
        "forbidden":     "(none — subscription-style framings fit this industry)",
    },
    "Agency & Consulting": {
        "revenue_model": "retainer or project-based — NOT consumer-subscription",
        "acceptable":    "QBR cadence with each client, expansion-scope conversations, alumni-referral program with retainer credit, case-study co-marketing as retention touchpoint",
        "forbidden":     "free month, free trial, consumer-subscription framings (retainer credit on the NEXT engagement is the right shape)",
    },
    "Non-profit & Cause": {
        "revenue_model": "donor-based; grants and sponsorships",
        "acceptable":    "donor-stewardship cadence, impact-report touchpoint, recurring-giving conversion, donor-appreciation event",
        "forbidden":     "free month, free trial — these are commercial framings that don't fit donor retention",
    },
    "Professional B2B Services": {
        "revenue_model": "retainer, project, or hourly — NOT consumer-subscription",
        "acceptable":    "QBR cadence, executive-briefing program, expansion-scope conversations, referral acknowledgement, annual strategy-review touchpoint",
        "forbidden":     "free month, free trial, consumer-subscription framings",
    },
}


def get_retention_framing(industry: str) -> Dict[str, str]:
    """Return retention/referral framing guidance for the given industry.

    Returns dict with keys ``revenue_model``, ``acceptable``, ``forbidden``.
    Returns an empty dict for unknown / "Other" industries — the caller
    should fall back to operational cadence retention (communication
    frequency, follow-up windows) rather than fabricate a pricing
    mechanic the AI can't ground in industry knowledge.
    """
    canon = industry_label(industry)
    return _RETENTION_FRAMING.get(canon, {})


# ── Compliance / regulatory framing per industry ───────────────
#
# Horizon Advisers 2026-05-15 sweep surfaced three compliance-risky
# recommendations for a SEC/FINRA-registered investment adviser:
#   - "Build a case study page... before/after metrics is worth 100
#      generic testimonials" (regulated by SEC Marketing Rule 206(4)-1)
#   - "Add visible pricing or 'starting from' range" (fee schedules
#      live in ADV Part 2A; AUM-based fees aren't flat-rate)
#   - Plain "case study with named attribution" framing without any
#      compliance caveat
#
# Several industries operate under advertising / endorsement rules that
# make these recommendations actively harmful: doing what we suggested
# could expose the operator to regulatory action. This table lets the
# synthesis prompt forbid the risky framings per industry and substitute
# compliant alternatives, with a "skip rather than fabricate" fallback
# for industries without specific compliance constraints on file.
#
# Each entry consumed by the synthesis prompt builder:
#   regulator    — one-line name of the governing body / rule
#   restrictions — one-paragraph "why this matters" for the model
#   forbidden    — comma-separated risky framings (prompted as ❌)
#   acceptable   — comma-separated compliant alternatives (prompted as ✅)

_COMPLIANCE_FRAMING: Dict[str, Dict[str, str]] = {
    "Financial Advisory": {
        "regulator":    "SEC Marketing Rule (Investment Advisers Act Rule 206(4)-1) / FINRA / state securities regulators",
        "restrictions": "Client testimonials and endorsements are heavily regulated — the SEC Marketing Rule (effective Nov 2022) requires specific disclosures, oversight, and ineligibility checks. Performance claims with 'before/after metrics' or specific outcome numbers trigger advertising-rule scrutiny. Flat-rate or 'starting from' pricing is rarely displayed publicly because AUM/fee-only firms compute fees per client and fee schedules live in Form ADV Part 2A. Form CRS is required client-relationship disclosure for SEC- and state-registered advisers.",
        "forbidden":    "Publish before/after client metrics on the website, Post client-specific case studies with outcome numbers, Add 'Starting from $X/month' pricing, Show flat-rate pricing on the homepage, Display testimonials without required SEC Marketing Rule disclosures, Advertise specific portfolio returns or performance",
        "acceptable":   "Publish anonymized educational case-style narratives with no client identification and no performance claims, Reference ADV Part 2A for the fee schedule rather than displaying fees on the homepage, Surface FINRA / SEC registration and any CFP / CFA credentials, Link to Form CRS, Add disclosure language reviewed by a compliance officer before publishing any testimonial",
    },
    "Legal": {
        "regulator":    "state bar advertising rules (varies by jurisdiction)",
        "restrictions": "Client testimonials are regulated by state bar — most jurisdictions require disclaimer language (e.g. 'past results do not guarantee future outcomes'). Pricing disclosures vary by practice area — flat-fee marketing is common only in narrow contexts (estate planning, traffic, immigration consults). Solicitation rules differ between jurisdictions.",
        "forbidden":    "Post client testimonials without state-bar-compliant disclaimers, Claim guaranteed outcomes or 'we win every case' framing, Advertise flat fees outside permitted practice areas, Publish before/after case results without ethics-compliant framing, Use 'specialist' / 'expert' language unless certified by the state bar",
        "acceptable":   "Publish testimonials with the appropriate state-bar disclaimer language, Reference free initial consultation when the practice area allows, Surface bar admissions, practice areas, and verifiable experience, Anonymized matter summaries with disclaimer",
    },
    "Healthcare & Medical": {
        "regulator":    "HIPAA / FDA / state medical board rules / Stark Law / Anti-Kickback Statute",
        "restrictions": "Patient testimonials must comply with HIPAA — no PHI without explicit written authorization. Outcome claims are constrained by FDA and state medical board rules. Monetary referral incentives between healthcare providers are prohibited under Stark / anti-kickback. Off-label promotion of FDA-regulated products is barred.",
        "forbidden":    "Post patient testimonials containing identifying information without HIPAA authorization, Claim specific success rates without FDA-permitted clinical data, Offer monetary referral incentives between providers, Advertise prescription medication discounts in non-permitted contexts, Promote off-label uses of FDA-regulated products",
        "acceptable":   "Anonymized patient stories with explicit written HIPAA authorization on file, Board certifications and practice credentials, Accepted insurance plans and appointment availability, Educational content reviewed by a clinical compliance officer",
    },
    "Accounting & CPA": {
        "regulator":    "AICPA Code of Conduct / state board of accountancy advertising rules",
        "restrictions": "Client testimonials with named attribution have ethics constraints under the AICPA Code. CPA firms have additional restrictions when soliciting audit-engagement clients via certain advertising. State boards regulate use of 'CPA' designation.",
        "forbidden":    "Advertise specific client tax-savings outcomes with named attribution, Claim guaranteed tax savings or specific refund amounts, Promise audit-fee refunds, Use 'CPA' designation in contexts where state licensure isn't confirmed",
        "acceptable":   "Anonymized success summaries with no client identification, CPA license number / firm registration, Year-end planning availability, Niche industry expertise statements, Continuing-education credentials",
    },
}


def get_compliance_framing(industry: str) -> Dict[str, str]:
    """Return regulatory / compliance framing for the given industry.

    Returns dict with keys ``regulator``, ``restrictions``, ``forbidden``,
    ``acceptable``. Returns an empty dict for industries without recorded
    compliance constraints — the caller should NOT emit a compliance
    directive block in that case (don't fabricate compliance rules).
    """
    canon = industry_label(industry)
    return _COMPLIANCE_FRAMING.get(canon, {})


# ── Self-reference framing per industry ────────────────────────
#
# Horizon Advisers 2026-05-15: the synthesis produced "Build a 5-email
# welcome sequence — Introduce agency, share case study, book discovery
# call" for a financial advisory firm. Horizon Advisers is NOT an
# agency — the model fell back to a generic B2B agency template.
#
# This table gives the model the industry-appropriate self-reference
# (what to call the client's own business) so it doesn't default to
# "agency" / "the brand" / "the company" boilerplate when a more
# precise term exists.

_SELF_REFERENCE: Dict[str, str] = {
    "Financial Advisory":        "the advisory practice / the firm",
    "Legal":                     "the practice / the firm / the law firm",
    "Accounting & CPA":          "the firm / the CPA firm / the practice",
    "Healthcare & Medical":      "the practice / the clinic",
    "Restaurant & Food Service": "the restaurant",
    "Retail & E-commerce":       "the shop / the store / the brand",
    "Home Services & Trades":    "the business / the company",
    "Real Estate":               "the brokerage / the practice",
    "Beauty & Wellness":         "the studio / the salon / the spa",
    "Personal Brand & Creator":  "your platform / your audience-business",
    "Coach, Speaker & Author":   "the practice / the program / the coaching business",
    "Startup & Early-stage":     "the startup / the company",
    "SaaS & Tech":               "the company / the platform / the product",
    "Agency & Consulting":       "the agency / the consultancy / the firm",
    "Non-profit & Cause":        "the organization / the nonprofit",
    "Professional B2B Services": "the firm",
}


def get_self_reference(industry: str) -> str:
    """Return the industry-appropriate way to refer to the client's business.

    Falls back to a generic "the business" when industry is unknown.
    The synthesis prompt uses this to forbid generic / wrong-industry
    self-references like "agency" for a financial advisor or "the
    brand" for a law firm.
    """
    canon = industry_label(industry)
    return _SELF_REFERENCE.get(canon, "the business")
