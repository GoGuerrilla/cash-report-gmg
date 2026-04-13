"""
C.A.S.H. Report — 1-Page Teaser PDF Generator
Produces a single-page summary PDF after every audit.

Layout (top → bottom on one letter-size page)
----------------------------------------------
  Header banner   : C.A.S.H. REPORT branding + client name
  Score hero      : Large overall score + grade + 4 CASH component scores
  Two-column body : Top 3 Strengths (green) | Top 3 Improvements (red)
  Closing callout : Fixed "thank you / next steps" message in navy box
  Footer strip    : GMG contact line

Saved as:  reports/<clientname_slug>_cash_teaser.pdf
"""
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas as rl_canvas

from config import ClientConfig

# ── Dark tech palette — Bloomberg terminal / cybersecurity theme ─
NAVY       = colors.HexColor("#0A0A0A")   # near-black page background
NAVY_LIGHT = colors.HexColor("#111827")   # dark charcoal card background
GOLD       = colors.HexColor("#C9A84C")   # GMG gold (unchanged)
GOLD_LIGHT = colors.HexColor("#D4A843")   # brighter gold for text accents
WHITE      = colors.white                 # white text and accents
OFF_WHITE  = colors.HexColor("#111827")   # alias for card background
GREEN      = colors.HexColor("#00FF88")   # neon green for strengths
GREEN_BG   = colors.HexColor("#051A0F")   # dark green card background
RED        = colors.HexColor("#FF4444")   # neon red for issues
RED_BG     = colors.HexColor("#1E0505")   # dark red card background
MID_GRAY   = colors.HexColor("#94A3B8")   # cool gray secondary text
DARK_TEXT  = colors.HexColor("#FFFFFF")   # body text = white on dark bg
ELEC_BLUE  = colors.HexColor("#00AEEF")   # electric blue for labels / accents

W, H = letter   # 612 × 792 pts

CLOSING_MESSAGE = (
    "Thank you for completing your C.A.S.H. Assessment!\n\n"
    "Your full C.A.S.H. Report including 15+ pages of detailed analysis, "
    "competitor comparison, GEO visibility score and a personalized 90-day action "
    "plan is being prepared by your GMG Representative.\n\n"
    "Look for your complete report in the next 12-24 hours.\n\n"
    "Questions? Contact us at gmg@goguerrilla.xyz — The GMG Team"
)


def _slug(name: str) -> str:
    """Convert a client name to a safe filename slug."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _grade_color(grade: str):
    mapping = {
        "A": colors.HexColor("#1A8C4E"),
        "B": colors.HexColor("#2E86C1"),
        "C": colors.HexColor("#D4AC0D"),
        "D": colors.HexColor("#CA6F1E"),
        "F": colors.HexColor("#C0392B"),
    }
    return mapping.get(grade, MID_GRAY)


def _strip_emoji(text: str) -> str:
    """Remove leading emoji + whitespace from audit finding strings."""
    return re.sub(
        r"^[\U0001F000-\U0001FFFF\u2600-\u27BF\u2B00-\u2BFF"
        r"\U0001F900-\U0001F9FF\u26A0\u2705\u274C\U0001F534"
        r"\U0001F7E1\U0001F7E2🔴🟡✅⚠️]+\s*",
        "", text,
    ).strip()


def _collect_signals(audit_data: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Pull top strengths and top improvement areas from all auditor sections.
    Returns (strengths[:3], issues[:3]) with emoji stripped.
    """
    all_strengths: List[str] = []
    all_issues:    List[str] = []

    SECTIONS = ["seo", "website", "brand", "icp", "funnel",
                "freshness", "geo", "gbp", "analytics"]

    for key in SECTIONS:
        section = audit_data.get(key, {})
        if not section or section.get("skipped"):
            continue
        for s in section.get("strengths", []):
            if s and "✅" in s:
                all_strengths.append(_strip_emoji(s))
        for i in section.get("issues", []):
            if i and "🔴" in i:
                all_issues.insert(0, _strip_emoji(i))   # critical first
            elif i and "🟡" in i:
                all_issues.append(_strip_emoji(i))

    # Funnel stage issues/strengths
    for stage in audit_data.get("funnel", {}).get("stages", {}).values():
        for s in stage.get("strengths", []):
            if "✅" in s:
                all_strengths.append(_strip_emoji(s))
        for i in stage.get("issues", []):
            if "🔴" in i:
                all_issues.insert(0, _strip_emoji(i))

    # Deduplicate while preserving order
    seen_s, seen_i = set(), set()
    unique_s, unique_i = [], []
    for s in all_strengths:
        key = s[:60]
        if key not in seen_s:
            seen_s.add(key)
            unique_s.append(s)
    for i in all_issues:
        key = i[:60]
        if key not in seen_i:
            seen_i.add(key)
            unique_i.append(i)

    return unique_s[:3], unique_i[:3]


def _wrap_text(c: rl_canvas.Canvas, text: str, x: float, y: float,
               max_width: float, font: str, size: float,
               line_height: float, color=DARK_TEXT) -> float:
    """
    Draw wrapped text. Returns the y position after the last line drawn.
    Handles explicit \n newlines.
    """
    c.setFont(font, size)
    c.setFillColor(color)
    paragraphs = text.split("\n")
    for para in paragraphs:
        if not para.strip():
            y -= line_height * 0.6
            continue
        words = para.split()
        line  = ""
        for word in words:
            test = (line + " " + word).strip()
            if c.stringWidth(test, font, size) <= max_width:
                line = test
            else:
                if line:
                    c.drawString(x, y, line)
                    y -= line_height
                line = word
        if line:
            c.drawString(x, y, line)
            y -= line_height
    return y


def generate_teaser(
    config: ClientConfig,
    audit_data: Dict[str, Any],
    output_dir: str = "reports",
) -> str:
    """
    Generate the 1-page teaser PDF.
    Returns the absolute path to the saved file.
    """
    os.makedirs(output_dir, exist_ok=True)

    slug      = _slug(config.client_name or "client")
    filename  = f"{slug}_cash_teaser.pdf"
    out_path  = os.path.join(output_dir, filename)

    ai        = audit_data.get("ai_insights", {})
    score     = ai.get("overall_score", 0)
    grade     = ai.get("overall_grade", "—")
    cash_c    = ai.get("cash_c_score",  0)
    cash_a    = ai.get("cash_a_score",  0)
    cash_s    = ai.get("cash_s_score",  0)
    cash_h    = ai.get("cash_h_score",  0)
    date_str  = datetime.now().strftime("%B %d, %Y")

    strengths, issues = _collect_signals(audit_data)

    # ── Pad to exactly 3 items ─────────────────────────────────
    while len(strengths) < 3:
        strengths.append("Strong foundational presence in your category.")
    while len(issues) < 3:
        issues.append("Additional optimizations available — see full report.")

    c = rl_canvas.Canvas(out_path, pagesize=letter)

    # ══════════════════════════════════════════════════════════
    #  0. FULL PAGE DARK BACKGROUND
    # ══════════════════════════════════════════════════════════
    c.setFillColor(NAVY)
    c.rect(0, 0, W, H, fill=1, stroke=0)

    # ══════════════════════════════════════════════════════════
    #  1. HEADER BANNER  (card-tone strip, 90pt tall)
    # ══════════════════════════════════════════════════════════
    HEADER_H = 90
    c.setFillColor(NAVY_LIGHT)
    c.rect(0, H - HEADER_H, W, HEADER_H, fill=1, stroke=0)

    # Gold left accent bar
    c.setFillColor(GOLD)
    c.rect(0, H - HEADER_H, 6, HEADER_H, fill=1, stroke=0)

    # "C.A.S.H. REPORT" in gold
    c.setFillColor(GOLD)
    c.setFont("Helvetica-Bold", 22)
    c.drawString(20, H - 36, "C.A.S.H. REPORT")

    # "BY GMG" sub-label
    c.setFont("Helvetica", 10)
    c.setFillColor(colors.HexColor("#D4C49A"))
    c.drawString(22, H - 54, "BY GUERRILLA MARKETING GROUP")

    # Client name right-aligned
    c.setFont("Helvetica-Bold", 13)
    c.setFillColor(WHITE)
    client_label = (config.client_name or "Client").upper()
    c.drawRightString(W - 20, H - 38, client_label)

    # Date below client name
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.HexColor("#B0BED0"))
    c.drawRightString(W - 20, H - 54, date_str)

    # Electric blue bottom accent line on header
    c.setFillColor(ELEC_BLUE)
    c.rect(0, H - HEADER_H - 3, W, 3, fill=1, stroke=0)

    # ══════════════════════════════════════════════════════════
    #  2. SCORE HERO  (centred, ~180pt block)
    # ══════════════════════════════════════════════════════════
    HERO_TOP = H - HEADER_H - 3
    HERO_H   = 210

    c.setFillColor(NAVY_LIGHT)
    c.rect(0, HERO_TOP - HERO_H, W, HERO_H, fill=1, stroke=0)

    # ── Score circle (navy bg, gold score) ─────────────────────
    CX, CY = W / 2, HERO_TOP - 98
    CR      = 52

    c.setFillColor(NAVY)
    c.circle(CX, CY, CR, fill=1, stroke=0)

    # Gold ring
    c.setStrokeColor(GOLD)
    c.setLineWidth(3)
    c.circle(CX, CY, CR + 4, fill=0, stroke=1)

    # Score number
    score_str = str(int(score)) if isinstance(score, (int, float)) else "—"
    c.setFillColor(GOLD)
    c.setFont("Helvetica-Bold", 34)
    sw = c.stringWidth(score_str, "Helvetica-Bold", 34)
    c.drawString(CX - sw / 2, CY + 5, score_str)

    # "/100" below score
    c.setFont("Helvetica", 11)
    c.setFillColor(MID_GRAY)
    sw2 = c.stringWidth("/100", "Helvetica", 11)
    c.drawString(CX - sw2 / 2, CY - 14, "/100")

    # Grade badge (left of circle)
    grade_col = _grade_color(grade)
    BX, BY = CX - CR - 60, CY - 18
    BW, BH = 42, 36
    c.setFillColor(grade_col)
    c.roundRect(BX, BY, BW, BH, 6, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 22)
    gw = c.stringWidth(str(grade), "Helvetica-Bold", 22)
    c.drawString(BX + BW / 2 - gw / 2, BY + 8, str(grade))

    # "GRADE" label above badge
    c.setFont("Helvetica", 8)
    c.setFillColor(ELEC_BLUE)
    lw = c.stringWidth("GRADE", "Helvetica", 8)
    c.drawString(BX + BW / 2 - lw / 2, BY + BH + 4, "GRADE")

    # "OVERALL SCORE" label right of circle
    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(ELEC_BLUE)
    c.drawString(CX + CR + 14, CY + 10, "OVERALL")
    c.drawString(CX + CR + 14, CY - 2, "C.A.S.H.")
    c.drawString(CX + CR + 14, CY - 14, "SCORE")

    # ── 4 CASH component mini-scores ──────────────────────────
    COMP_Y  = HERO_TOP - HERO_H + 34
    labels  = ["C — Content", "A — Audience", "S — Sales", "H — Hold"]
    vals    = [cash_c, cash_a, cash_s, cash_h]
    col_w   = W / 4

    for i, (lbl, val) in enumerate(zip(labels, vals)):
        cx = col_w * i + col_w / 2
        vstr = str(int(val)) if isinstance(val, (int, float)) else "—"
        # Mini score circle
        mc_r = 18
        mc_y = COMP_Y + mc_r
        c.setFillColor(NAVY)
        c.circle(cx, mc_y, mc_r, fill=1, stroke=0)
        c.setFillColor(GOLD)
        c.setFont("Helvetica-Bold", 12)
        vsw = c.stringWidth(vstr, "Helvetica-Bold", 12)
        c.drawString(cx - vsw / 2, mc_y - 5, vstr)
        # Label below — 14pt gap below circle bottom (COMP_Y)
        c.setFont("Helvetica", 7.5)
        c.setFillColor(DARK_TEXT)
        lw2 = c.stringWidth(lbl, "Helvetica", 7.5)
        c.drawString(cx - lw2 / 2, COMP_Y - 20, lbl)

    # Divider below hero
    c.setStrokeColor(GOLD)
    c.setLineWidth(1.5)
    c.line(30, HERO_TOP - HERO_H - 2, W - 30, HERO_TOP - HERO_H - 2)

    # ══════════════════════════════════════════════════════════
    #  3. TWO-COLUMN BODY  (strengths left | improvements right)
    # ══════════════════════════════════════════════════════════
    BODY_TOP  = HERO_TOP - HERO_H - 22
    BODY_H    = 196
    GUTTER    = 18
    COL_W     = (W - GUTTER * 3) / 2    # ~279 pts each
    LEFT_X    = GUTTER
    RIGHT_X   = GUTTER * 2 + COL_W

    # Column backgrounds
    c.setFillColor(GREEN_BG)
    c.roundRect(LEFT_X, BODY_TOP - BODY_H, COL_W, BODY_H, 6, fill=1, stroke=0)
    c.setFillColor(RED_BG)
    c.roundRect(RIGHT_X, BODY_TOP - BODY_H, COL_W, BODY_H, 6, fill=1, stroke=0)

    # Column headers
    c.setFillColor(GREEN)
    c.roundRect(LEFT_X, BODY_TOP - 28, COL_W, 28, 6, fill=1, stroke=0)
    c.setFillColor(RED)
    c.roundRect(RIGHT_X, BODY_TOP - 28, COL_W, 28, 6, fill=1, stroke=0)

    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(WHITE)
    c.drawString(LEFT_X + 10,  BODY_TOP - 19, "✓  TOP 3 STRENGTHS")
    c.drawString(RIGHT_X + 10, BODY_TOP - 19, "▲  TOP 3 IMPROVEMENTS")

    # Strength items
    item_y = BODY_TOP - 52
    for i, text in enumerate(strengths, 1):
        # Bullet circle
        c.setFillColor(GREEN)
        c.circle(LEFT_X + 14, item_y + 4, 7, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(LEFT_X + 14, item_y + 1, str(i))
        # Text (wrapped)
        truncated = text[:110] + ("…" if len(text) > 110 else "")
        item_y = _wrap_text(
            c, truncated,
            x=LEFT_X + 26, y=item_y + 3,
            max_width=COL_W - 36,
            font="Helvetica", size=8.5,
            line_height=11.5, color=DARK_TEXT,
        )
        item_y -= 7

    # Improvement items
    imp_y = BODY_TOP - 52
    for i, text in enumerate(issues, 1):
        # Bullet circle
        c.setFillColor(RED)
        c.circle(RIGHT_X + 14, imp_y + 4, 7, fill=1, stroke=0)
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(RIGHT_X + 14, imp_y + 1, str(i))
        # Text (wrapped)
        truncated = text[:110] + ("…" if len(text) > 110 else "")
        imp_y = _wrap_text(
            c, truncated,
            x=RIGHT_X + 26, y=imp_y + 3,
            max_width=COL_W - 36,
            font="Helvetica", size=8.5,
            line_height=11.5, color=DARK_TEXT,
        )
        imp_y -= 7

    # ══════════════════════════════════════════════════════════
    #  4. CLOSING CALLOUT  (navy box with gold accent)
    # ══════════════════════════════════════════════════════════
    FOOTER_H   = 22
    CLOSE_TOP  = BODY_TOP - BODY_H - 14
    CLOSE_H    = CLOSE_TOP - FOOTER_H - 6   # fills page to footer
    CLOSE_X    = GUTTER
    CLOSE_W    = W - GUTTER * 2

    c.setFillColor(NAVY)
    c.roundRect(CLOSE_X, CLOSE_TOP - CLOSE_H, CLOSE_W, CLOSE_H, 8, fill=1, stroke=0)

    # Gold top accent bar on callout
    c.setFillColor(GOLD)
    c.roundRect(CLOSE_X, CLOSE_TOP - 5, CLOSE_W, 5, 3, fill=1, stroke=0)

    # Closing text
    msg_x  = CLOSE_X + 18
    msg_y  = CLOSE_TOP - 22
    msg_w  = CLOSE_W - 36

    _wrap_text(
        c, CLOSING_MESSAGE,
        x=msg_x, y=msg_y,
        max_width=msg_w,
        font="Helvetica", size=8.8,
        line_height=13, color=WHITE,
    )

    # ══════════════════════════════════════════════════════════
    #  5. FOOTER  (slim navy strip)
    # ══════════════════════════════════════════════════════════
    c.setFillColor(NAVY_LIGHT)
    c.rect(0, 0, W, FOOTER_H, fill=1, stroke=0)

    c.setFillColor(GOLD)
    c.setFont("Helvetica-Bold", 7.5)
    c.drawString(20, 7, "C.A.S.H. REPORT  ·  BY GMG")

    c.setFont("Helvetica", 7.5)
    c.setFillColor(colors.HexColor("#B0BED0"))
    c.drawRightString(W - 20, 7, f"gmg@goguerrilla.xyz  ·  {date_str}  ·  CONFIDENTIAL")

    c.save()
    return os.path.abspath(out_path)
