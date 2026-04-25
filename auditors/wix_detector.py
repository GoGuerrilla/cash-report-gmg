import re

_WIX_SIGNALS = (
    re.compile(r'<meta[^>]+name=["\']generator["\'][^>]+content=["\']Wix', re.I),
    re.compile(r'X-Wix-', re.I),
    re.compile(r'static\.wixstatic\.com', re.I),
)


def is_wix(html: str) -> bool:
    if not html:
        return False
    return any(p.search(html) for p in _WIX_SIGNALS)
