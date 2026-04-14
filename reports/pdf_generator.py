"""
C.A.S.H. Report by GMG — Full PDF Generator (ReportLab)
Primary report output. Navy / gold branding matches the Word backup.

Sections (page order)
---------------------
  1. Cover / title page
  2. GMG introduction
  3. C.A.S.H. score overview
  4. Executive summary
  5. C — Content  (website, SEO, brand, freshness)
  6. A — Audience (ICP, platform fit, social)
  7. S — Sales    (funnel, GBP, conversion)
  8. H — Hold     (retention, email, referral)
  9. GEO — Generative Engine Optimisation
 10. Competitive analysis
 11. 90-day action plan
 12. Next steps / CTA
 13. Appendix
"""
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Optional

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether, Image,
)

from config import ClientConfig

# ── Dark tech palette — Bloomberg terminal / cybersecurity theme ─
NAVY      = colors.HexColor("#0A0A0A")   # near-black page background
NAVY_MID  = colors.HexColor("#111827")   # dark charcoal card background
GOLD      = colors.HexColor("#C9A84C")   # GMG gold (unchanged)
GOLD_LITE = colors.HexColor("#D4A843")   # brighter gold for text accents
WHITE     = colors.white                 # pure white text / accents
OFF_WHITE = colors.HexColor("#111827")   # alias for card background
LGRAY     = colors.HexColor("#1A2234")   # alternate row dark
DGRAY     = colors.HexColor("#FFFFFF")   # body text = white on dark bg
MGRAY     = colors.HexColor("#94A3B8")   # cool gray secondary text
ELEC_BLUE = colors.HexColor("#00AEEF")   # electric blue for section headers
GREEN     = colors.HexColor("#00FF88")   # neon green for strengths
GREEN_BG  = colors.HexColor("#051A0F")   # dark green card background
AMBER     = colors.HexColor("#F59E0B")   # amber for mid-range score badges
AMBER_BG  = colors.HexColor("#0A1520")   # dark blue-teal card background
RED       = colors.HexColor("#FF4444")   # neon red for issues
RED_BG    = colors.HexColor("#1E0505")   # dark red card background
BORDER    = colors.HexColor("#2A3450")   # subtle dark table border

W, H = letter   # 612 × 792 pts


# ── Tiny helpers ──────────────────────────────────────────────

def _grade(score) -> str:
    s = int(score) if isinstance(score, (int, float)) else 0
    if s >= 80: return "A"
    if s >= 65: return "B"
    if s >= 50: return "C"
    if s >= 35: return "D"
    return "F"


def _score_color(score) -> colors.Color:
    s = int(score) if isinstance(score, (int, float)) else 0
    if s >= 65: return GREEN
    if s >= 35: return AMBER
    return RED


def _grade_color(grade: str) -> colors.Color:
    return {"A": colors.HexColor("#1A8C4E"),
            "B": colors.HexColor("#2E86C1"),
            "C": colors.HexColor("#D4AC0D"),
            "D": colors.HexColor("#CA6F1E"),
            "F": colors.HexColor("#C0392B")}.get(grade, MGRAY)


def _strip_emoji(text: str) -> str:
    return re.sub(
        r"^[\U0001F000-\U0001FFFF\u2600-\u27BF\u2B00-\u2BFF"
        r"\U0001F900-\U0001F9FF\u26A0\u2705\u274C\U0001F534"
        r"\U0001F7E1\U0001F7E2🔴🟡✅⚠️]+\s*", "", text,
    ).strip()


# ── Style factory ─────────────────────────────────────────────

def _ps(name, **kw) -> ParagraphStyle:
    return ParagraphStyle(name, **kw)


def _build_styles() -> dict:
    return {
        "h1":      _ps("h1",  fontName="Helvetica-Bold", fontSize=22, textColor=ELEC_BLUE,  spaceAfter=6),
        "h2":      _ps("h2",  fontName="Helvetica-Bold", fontSize=15, textColor=ELEC_BLUE, spaceBefore=4, spaceAfter=4),
        "h3":      _ps("h3",  fontName="Helvetica-Bold", fontSize=11, textColor=ELEC_BLUE, spaceBefore=8, spaceAfter=4),
        "body":    _ps("body", fontName="Helvetica",     fontSize=9.5, textColor=WHITE,    leading=14, spaceAfter=4),
        "body_sm": _ps("bsm",  fontName="Helvetica",     fontSize=8.5, textColor=MGRAY,    leading=12),
        "label":   _ps("lbl",  fontName="Helvetica-Bold", fontSize=8,  textColor=ELEC_BLUE),
        "center":  _ps("ctr",  fontName="Helvetica",     fontSize=9,   textColor=WHITE,    alignment=TA_CENTER),
        "tb":      _ps("tb",   fontName="Helvetica",     fontSize=8.5, textColor=WHITE,    leading=11),
        "tb_bold": _ps("tbb",  fontName="Helvetica-Bold",fontSize=8.5, textColor=WHITE,    leading=11),
        "tb_wh":   _ps("tbw",  fontName="Helvetica-Bold",fontSize=8.5, textColor=WHITE,    leading=11),
        "tb_ctr":  _ps("tbc",  fontName="Helvetica",     fontSize=8.5, textColor=WHITE,    alignment=TA_CENTER),
        "tb_ctr_w":_ps("tbcw", fontName="Helvetica-Bold",fontSize=9,   textColor=WHITE,    alignment=TA_CENTER),
        "small":   _ps("sm",   fontName="Helvetica",     fontSize=7.5, textColor=MGRAY),
        "gold":    _ps("gld",  fontName="Helvetica-Bold",fontSize=9,   textColor=GOLD),
        "tagline": _ps("tgl",  fontName="Helvetica",     fontSize=9,   textColor=MGRAY,    leading=13),
        "cover_name": _ps("cn", fontName="Helvetica-Bold", fontSize=24, textColor=ELEC_BLUE, leading=30),
        "cover_sub":  _ps("cs", fontName="Helvetica",    fontSize=14,  textColor=MGRAY),
        "cover_grade":_ps("cg", fontName="Helvetica-Bold",fontSize=42, textColor=ELEC_BLUE, alignment=TA_CENTER),
        "cover_score":_ps("csc",fontName="Helvetica-Bold",fontSize=18, textColor=WHITE,    alignment=TA_CENTER),
        "intro_head": _ps("ih", fontName="Helvetica-Bold",fontSize=18, textColor=WHITE,    alignment=TA_CENTER),
        "intro_sub":  _ps("is_",fontName="Helvetica",    fontSize=10,  textColor=MGRAY,    alignment=TA_CENTER),
        "intro_body": _ps("ib", fontName="Helvetica",    fontSize=9.5, textColor=WHITE,    leading=14),
    }


# ── Shared table styles ───────────────────────────────────────

def _hdr_style() -> TableStyle:
    """Dark-header table — electric blue header bar."""
    return TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY_MID),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  ELEC_BLUE),
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0),  8.5),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [NAVY_MID, LGRAY]),
        ("TEXTCOLOR",     (0, 1), (-1, -1), WHITE),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 7),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("BOX",           (0, 0), (-1, -1), 0.5, BORDER),
        ("INNERGRID",     (0, 0), (-1, -1), 0.5, BORDER),
    ])


def _gold_hdr_style() -> TableStyle:
    """Cyan-header table — electric blue header bar on dark rows."""
    return TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  ELEC_BLUE),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  NAVY),
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0),  8.5),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [NAVY_MID, LGRAY]),
        ("TEXTCOLOR",     (0, 1), (-1, -1), WHITE),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 7),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("BOX",           (0, 0), (-1, -1), 0.5, BORDER),
        ("INNERGRID",     (0, 0), (-1, -1), 0.5, BORDER),
    ])


class PDFReportGenerator:
    def __init__(self, config: ClientConfig, audit_data: Dict[str, Any]):
        self.config   = config
        self.data     = audit_data
        self.ai       = audit_data.get("ai_insights", {})
        self.date_str = datetime.now().strftime("%B %d, %Y")
        self.st       = _build_styles()
        self.cash     = self._resolve_cash()

    # ── Public entry point ─────────────────────────────────────

    def generate(self, output_path: str):
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        doc = SimpleDocTemplate(
            output_path, pagesize=letter,
            leftMargin=0.65*inch, rightMargin=0.65*inch,
            topMargin=0.55*inch,  bottomMargin=0.6*inch,
        )
        story = []

        story += self._cover()
        story.append(PageBreak())
        story += self._intro_page()
        story.append(PageBreak())
        story += self._scorecard()
        story.append(PageBreak())
        story += self._executive_summary()
        story.append(PageBreak())
        story += self._section_c()
        story.append(PageBreak())
        story += self._section_a()
        story.append(PageBreak())
        story += self._section_s()
        story.append(PageBreak())
        story += self._section_h()
        story.append(PageBreak())
        story += self._section_geo()
        story.append(PageBreak())
        story += self._section_competitive()
        story.append(PageBreak())
        story += self._action_plan()
        story.append(PageBreak())
        story += self._cta_section()
        story.append(PageBreak())
        story += self._appendix()

        doc.build(story,
                  onFirstPage=self._page_chrome,
                  onLaterPages=self._page_chrome)

    # ── Page chrome (header / footer on every page) ────────────

    def _page_chrome(self, canv, doc):
        from reportlab.pdfgen import canvas as rl_canvas
        canv.saveState()
        # Full page dark background
        canv.setFillColor(NAVY)
        canv.rect(0, 0, W, H, fill=1, stroke=0)
        # Header strip (slightly lighter card tone)
        canv.setFillColor(NAVY_MID)
        canv.rect(0, H - 28, W, 28, fill=1, stroke=0)
        canv.setFillColor(ELEC_BLUE)
        canv.setFont("Helvetica-Bold", 8)
        canv.drawString(0.65*inch, H - 18, "C.A.S.H. REPORT  ·  BY GMG")
        canv.setFillColor(MGRAY)
        canv.setFont("Helvetica", 8)
        canv.drawRightString(W - 0.65*inch, H - 18,
                             f"{self.config.client_name}  ·  {self.date_str}")
        # Electric blue accent under header
        canv.setFillColor(ELEC_BLUE)
        canv.rect(0, H - 31, W, 3, fill=1, stroke=0)
        # Footer strip
        canv.setFillColor(NAVY_MID)
        canv.rect(0, 0, W, 20, fill=1, stroke=0)
        canv.setFillColor(ELEC_BLUE)
        canv.setFont("Helvetica", 7)
        canv.drawCentredString(W / 2, 6,
                               f"Page {doc.page}  ·  Confidential  ·  gmg@goguerrilla.xyz")
        canv.restoreState()

    # ══════════════════════════════════════════════════════════
    #  SECTION HELPERS
    # ══════════════════════════════════════════════════════════

    def _sec_hdr(self, title: str) -> List:
        """Dark card section header with electric blue text and gold underline."""
        return [
            Spacer(1, 0.1*inch),
            KeepTogether([
                Table([[Paragraph(title, self.st["h2"])]],
                      colWidths=[W - 1.3*inch],
                      style=TableStyle([
                          ("BACKGROUND",   (0, 0), (-1, -1), NAVY_MID),
                          ("TOPPADDING",   (0, 0), (-1, -1), 8),
                          ("BOTTOMPADDING",(0, 0), (-1, -1), 8),
                          ("LEFTPADDING",  (0, 0), (-1, -1), 12),
                          ("TEXTCOLOR",    (0, 0), (-1, -1), ELEC_BLUE),
                          ("BOX",          (0, 0), (-1, -1), 1, ELEC_BLUE),
                      ])),
                HRFlowable(width="100%", thickness=2, color=ELEC_BLUE, spaceAfter=6),
            ]),
        ]

    def _sub_hdr(self, title: str) -> List:
        return [
            Spacer(1, 0.08*inch),
            Paragraph(title, self.st["h3"]),
            HRFlowable(width="100%", thickness=1, color=ELEC_BLUE, spaceAfter=4),
        ]

    def _cash_banner(self, letter: str, title: str, score, tagline: str) -> List:
        """Full-width CASH section banner: gold letter | navy title+tagline | score badge."""
        grade     = _grade(score)
        sc_color  = _score_color(score)
        score_str = f"{score}/100  ({grade})"
        # Auto-size letter column: single chars get large font, multi-char (GEO) get smaller
        ltr_size = 28 if len(letter) == 1 else (16 if len(letter) <= 3 else 12)
        ltr_col  = (0.65 if len(letter) == 1 else 0.9) * inch
        body_col = (W - 1.3*inch) - ltr_col - 1.2*inch
        data = [[
            Paragraph(letter,
                      _ps(f"ltr_{letter}", fontName="Helvetica-Bold",
                          fontSize=ltr_size, leading=ltr_size * 1.25,
                          textColor=ELEC_BLUE, alignment=TA_CENTER,
                          spaceBefore=0, spaceAfter=0)),
            Paragraph(f'<b><font color="white">{title}</font></b><br/>'
                      f'<font size="8" color="#B0BED0">{tagline}</font>',
                      _ps("cb", fontName="Helvetica-Bold", fontSize=13,
                          textColor=WHITE, leading=18)),
            Paragraph(score_str,
                      _ps("cs2", fontName="Helvetica-Bold", fontSize=12,
                          textColor=WHITE, alignment=TA_CENTER)),
        ]]
        t = Table(data, colWidths=[ltr_col, body_col, 1.2*inch])
        t.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), NAVY_MID),
            ("BACKGROUND",   (2, 0), (2, 0),   sc_color),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",        (0, 0), (0, 0),   "CENTER"),
            ("TOPPADDING",   (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
            ("LEFTPADDING",  (0, 0), (-1, -1), 8),
            ("BOX",          (0, 0), (-1, -1), 1, ELEC_BLUE),
        ]))
        return [Spacer(1, 0.05*inch), t, HRFlowable(width="100%", thickness=2, color=ELEC_BLUE, spaceAfter=8)]

    def _detail_table(self, headers: List[str], rows: List[tuple],
                      col_widths: List = None, gold_hdr: bool = False) -> Table:
        st    = self.st
        hrow  = [Paragraph(h, st["tb_wh"]) for h in headers]
        drows = []
        for row in rows:
            drows.append([Paragraph(str(c), st["tb"]) if not isinstance(c, Paragraph) else c
                          for c in row])
        t = Table([hrow] + drows, colWidths=col_widths)
        t.setStyle(_gold_hdr_style() if gold_hdr else _hdr_style())
        return t

    def _issues_strengths(self, issues: List[str], strengths: List[str]) -> List:
        if not issues and not strengths:
            return []
        max_r = max(len(issues), len(strengths), 1)
        st    = self.st
        rows  = [[
            Paragraph("⚠  ISSUES TO ADDRESS", st["tb_wh"]),
            Paragraph("✓  STRENGTHS", st["tb_wh"]),
        ]]
        for i in range(max_r):
            left  = Paragraph(issues[i]    if i < len(issues)    else "", st["tb"])
            right = Paragraph(strengths[i] if i < len(strengths) else "", st["tb"])
            rows.append([left, right])
        col = (W - 1.3*inch) / 2
        t   = Table(rows, colWidths=[col, col])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, 0),   RED),
            ("BACKGROUND",    (1, 0), (1, 0),   GREEN),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  NAVY),
            ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, 0),  8.5),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [NAVY_MID, LGRAY]),
            ("TEXTCOLOR",     (0, 1), (-1, -1), WHITE),
            ("VALIGN",        (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 7),
            ("BOX",           (0, 0), (-1, -1), 0.5, BORDER),
            ("INNERGRID",     (0, 0), (-1, -1), 0.5, BORDER),
        ]))
        return [Spacer(1, 0.05*inch), t, Spacer(1, 0.1*inch)]

    def _callout(self, text: str, bg: colors.Color = NAVY_MID) -> List:
        p  = Paragraph(text, _ps("callout", fontName="Helvetica", fontSize=9,
                                  textColor=WHITE, leading=14))
        t  = Table([[p]], colWidths=[W - 1.3*inch])
        t.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), bg),
            ("TOPPADDING",   (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
            ("LEFTPADDING",  (0, 0), (-1, -1), 14),
            ("RIGHTPADDING", (0, 0), (-1, -1), 14),
            ("BOX",          (0, 0), (-1, -1), 1, ELEC_BLUE),
        ]))
        return [t, Spacer(1, 0.08*inch)]

    # ══════════════════════════════════════════════════════════
    #  1. COVER PAGE
    # ══════════════════════════════════════════════════════════

    def _cover(self) -> List:
        st    = self.st
        score = self.ai.get("overall_score", self.cash.get("overall", 50))
        grade = self.ai.get("overall_grade", _grade(score))
        story = [Spacer(1, 0.15*inch)]

        # Logo — centered above banner, max 140pt wide, skip if file missing
        _LOGO_SEARCH = [
            os.path.join(os.path.dirname(__file__), "..", "gmg_cash_logo.png"),
            os.path.join(os.path.dirname(__file__), "gmg_cash_logo.png"),
            "gmg_cash_logo.png",
        ]
        for _logo_path in _LOGO_SEARCH:
            _logo_path = os.path.abspath(_logo_path)
            if os.path.isfile(_logo_path):
                try:
                    _MAX_W = 140          # pts (≈ 140px at 72 dpi)
                    _img   = Image(_logo_path)
                    _scale = min(_MAX_W / _img.imageWidth, 1.0)
                    _img.drawWidth  = _img.imageWidth  * _scale
                    _img.drawHeight = _img.imageHeight * _scale
                    _logo_tbl = Table([[_img]], colWidths=[W - 1.3*inch])
                    _logo_tbl.setStyle(TableStyle([
                        ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
                        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
                        ("TOPPADDING",   (0, 0), (-1, -1), 0),
                        ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
                    ]))
                    story.append(_logo_tbl)
                except Exception:
                    pass
                break

        # Dark top banner — big cyan title
        banner = Table(
            [[Paragraph("C.A.S.H. REPORT",
                        _ps("ct", fontName="Helvetica-Bold", fontSize=26,
                            textColor=ELEC_BLUE, alignment=TA_CENTER))]],
            colWidths=[W - 1.3*inch],
        )
        banner.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), NAVY_MID),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
            ("TOPPADDING",   (0, 0), (-1, -1), 18),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 18),
            ("LEFTPADDING",  (0, 0), (-1, -1), 14),
            ("RIGHTPADDING", (0, 0), (-1, -1), 14),
            ("BOX",          (0, 0), (-1, -1), 1, ELEC_BLUE),
        ]))
        story.append(banner)
        story.append(HRFlowable(width="100%", thickness=3, color=ELEC_BLUE, spaceAfter=16))

        # Client name
        story.append(Paragraph(self.config.client_name.upper(), st["cover_name"]))
        story.append(Paragraph("MARKETING AUDIT REPORT", st["cover_sub"]))
        story.append(Spacer(1, 0.12*inch))
        story.append(HRFlowable(width="100%", thickness=1.5, color=ELEC_BLUE, spaceAfter=16))

        # Grade + score hero
        # Fixed row height ensures the grade letter is truly centred in its cell
        _ROW_H = 80
        gc = _grade_color(grade)
        score_tbl = Table(
            [[Paragraph(grade, _ps("grd", fontName="Helvetica-Bold", fontSize=52,
                                   leading=44, textColor=WHITE, alignment=TA_CENTER,
                                   spaceBefore=0, spaceAfter=0)),
              Paragraph(f"OVERALL C.A.S.H. SCORE<br/><font size='30'>{score}</font>/100",
                        _ps("sc", fontName="Helvetica-Bold", fontSize=11,
                            textColor=WHITE, alignment=TA_CENTER, leading=34))]],
            colWidths=[1.4*inch, 3.2*inch],
            rowHeights=[_ROW_H],
        )
        score_tbl.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (0, 0), gc),
            ("BACKGROUND",   (1, 0), (1, 0), NAVY_MID),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
            ("TOPPADDING",   (0, 0), (0, 0),  18),
            ("BOTTOMPADDING",(0, 0), (0, 0),  18),
            ("TOPPADDING",   (1, 0), (1, 0),   8),
            ("BOTTOMPADDING",(1, 0), (1, 0),   8),
            ("LEFTPADDING",  (0, 0), (0, 0),   4),
            ("RIGHTPADDING", (0, 0), (0, 0),   4),
            ("LEFTPADDING",  (1, 0), (1, 0),  12),
            ("BOX",          (0, 0), (-1, -1), 2, ELEC_BLUE),
        ]))
        story.append(score_tbl)
        story.append(Spacer(1, 0.15*inch))

        # CASH component mini-row
        cash_row  = [("C", "Content",  self.cash.get("C", 50)),
                     ("A", "Audience", self.cash.get("A", 50)),
                     ("S", "Sales",    self.cash.get("S", 50)),
                     ("H", "Hold",     self.cash.get("H", 50))]
        cw        = (W - 1.3*inch) / 4
        comp_data = [[
            Paragraph(f'<font color="#00AEEF"><b>{let}</b></font> — {lbl}<br/>'
                      f'<font size="14"><b>{sc}</b></font>/100  ({_grade(sc)})',
                      _ps(f"comp{let}", fontName="Helvetica", fontSize=8.5,
                          textColor=WHITE, alignment=TA_CENTER, leading=14))
            for let, lbl, sc in cash_row
        ]]
        comp_t = Table(comp_data, colWidths=[cw, cw, cw, cw])
        comp_t.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), NAVY_MID),
            ("TOPPADDING",   (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
            ("INNERGRID",    (0, 0), (-1, -1), 0.5, ELEC_BLUE),
            ("BOX",          (0, 0), (-1, -1), 1.5, ELEC_BLUE),
        ]))
        story.append(comp_t)
        story.append(Spacer(1, 0.2*inch))

        # Cover footer metadata (no executive summary — that lives on its own page)
        story.append(HRFlowable(width="100%", thickness=1, color=ELEC_BLUE, spaceAfter=8))
        for line in [
            f"Prepared by: {self.config.agency_name}",
            f"Date: {self.date_str}",
            f"Website: {self.config.website_url or '—'}",
            f"Industry: {self.config.industry_category or self.config.client_industry or '—'}",
        ]:
            story.append(Paragraph(line, st["small"]))
        return story

    # ══════════════════════════════════════════════════════════
    #  2. GMG INTRODUCTION PAGE
    # ══════════════════════════════════════════════════════════

    def _intro_page(self) -> List:
        st    = self.st
        story = []

        # Top navy banner
        top = Table(
            [[Paragraph("Know Exactly Where Your Marketing Is Winning — and Losing",
                        _ps("ih", fontName="Helvetica-Bold", fontSize=13,
                            leading=16, textColor=WHITE, alignment=TA_CENTER))],
             [Paragraph(
                "A data-driven audit of your entire online presence — scored, benchmarked, "
                "and delivered with a clear 90-day growth plan.",
                st["intro_sub"])]],
            colWidths=[W - 1.3*inch],
        )
        top.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), NAVY),
            ("TOPPADDING",   (0, 0), (0, 0),   14),
            ("BOTTOMPADDING",(0, 0), (0, 0),   6),
            ("TOPPADDING",   (0, 1), (0, 1),   4),
            ("BOTTOMPADDING",(0, 1), (0, 1),   14),
            ("LEFTPADDING",  (0, 0), (-1, -1), 20),
            ("RIGHTPADDING", (0, 0), (-1, -1), 20),
        ]))
        story.append(top)
        story.append(HRFlowable(width="100%", thickness=4, color=ELEC_BLUE, spaceAfter=12))

        # Body copy
        story.append(Spacer(1, 0.12*inch))
        story.append(Paragraph(
            "The C.A.S.H. Report is GMG\u2019s proprietary marketing intelligence system, designed "
            "to give businesses and professionals a clear, strategic view of how their digital "
            "presence is truly performing. It evaluates your brand across four core pillars\u2014"
            "Content, Audience, Sales, and Hold (Retention)\u2014while identifying gaps in your "
            "funnel and conversion flow, then delivers a focused, prioritized 90-day action plan. "
            "Every benchmark is calibrated to your industry, ensuring you\u2019re measured against "
            "what actually drives results\u2014not generic metrics. Each insight connects directly "
            "to real business outcomes, including SEO, GEO visibility, direct contact pathways, "
            "community engagement, conversion efficiency, and long-term retention. This is more "
            "than a report\u2014it\u2019s a strategic roadmap. Review your results, understand the "
            "opportunities, and take the next step toward smarter, more effective growth.",
            st["intro_body"]))
        story.append(Spacer(1, 0.15*inch))

        # CASH pillars table
        story.append(Paragraph("THE C.A.S.H. FRAMEWORK", st["h3"]))
        story.append(HRFlowable(width="100%", thickness=1, color=ELEC_BLUE, spaceAfter=6))
        pillars = [
            ["C", "CONTENT",    "Website, SEO, brand consistency, content freshness — how strong is your foundation?"],
            ["A", "AUDIENCE",   "ICP alignment, platform fit, social presence — are you reaching the right people?"],
            ["S", "SALES",      "Lead capture, funnel quality, GBP, conversion — how effectively do you convert attention?"],
            ["H", "HOLD",       "Retention, referrals, email nurture, trust signals — how well do you keep and grow clients?"],
        ]
        cw  = W - 1.3*inch
        bgs = [NAVY, NAVY_MID, NAVY, NAVY_MID]
        rows = []
        for (let, lbl, desc), bg in zip(pillars, bgs):
            rows.append([
                Paragraph(f'<font size="18" color="#00AEEF"><b>{let}</b></font>',
                          _ps(f"pi{let}", fontName="Helvetica-Bold", fontSize=18,
                              textColor=ELEC_BLUE, alignment=TA_CENTER)),
                Paragraph(f'<b><font color="white">{lbl}</font></b><br/>'
                          f'<font size="8.5" color="#B0BED0">{desc}</font>',
                          _ps(f"pd{let}", fontName="Helvetica", fontSize=9,
                              textColor=WHITE, leading=13)),
            ])
        pt = Table(rows, colWidths=[0.55*inch, cw - 0.55*inch])
        cmds = [
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING",   (0, 0), (-1, -1), 9),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 9),
            ("LEFTPADDING",  (0, 0), (-1, -1), 10),
            ("INNERGRID",    (0, 0), (-1, -1), 0.5, BORDER),
            ("BOX",          (0, 0), (-1, -1), 1, ELEC_BLUE),
        ]
        for i, bg in enumerate(bgs):
            cmds.append(("BACKGROUND", (0, i), (-1, i), bg))
        pt.setStyle(TableStyle(cmds))
        story.append(pt)
        story.append(Spacer(1, 0.14*inch))

        # What you get
        story.append(Paragraph("WHAT THIS REPORT DELIVERS", st["h3"]))
        story.append(HRFlowable(width="100%", thickness=1, color=ELEC_BLUE, spaceAfter=6))
        deliverables = [
            "C.A.S.H. score across 4 strategic pillars with component breakdown",
            "15+ pages of detailed analysis across every marketing channel",
            "Industry-calibrated benchmarks — scored for your specific business type",
            "Competitor side-by-side comparison (SEO, social, website, GBP)",
            "GEO visibility score — how you rank in AI-generated answers",
            "Personalised 90-day action plan with prioritised quick wins",
            "Google Business Profile audit with NAP consistency check",
            "Content freshness and posting frequency analysis by platform",
        ]
        cw2 = (W - 1.3*inch) / 2
        check_rows = []
        for i in range(0, len(deliverables), 2):
            left  = f"✓  {deliverables[i]}"
            right = f"✓  {deliverables[i+1]}" if i + 1 < len(deliverables) else ""
            check_rows.append([
                Paragraph(left,  _ps("dl", fontName="Helvetica", fontSize=8.5,
                                      textColor=DGRAY, leading=12)),
                Paragraph(right, _ps("dr", fontName="Helvetica", fontSize=8.5,
                                      textColor=DGRAY, leading=12)),
            ])
        dt = Table(check_rows, colWidths=[cw2, cw2])
        dt.setStyle(TableStyle([
            ("ROWBACKGROUNDS",(0, 0), (-1, -1), [LGRAY, NAVY_MID]),
            ("TEXTCOLOR",    (0, 0), (-1, -1), WHITE),
            ("TOPPADDING",   (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
            ("LEFTPADDING",  (0, 0), (-1, -1), 8),
            ("BOX",          (0, 0), (-1, -1), 0.5, BORDER),
            ("INNERGRID",    (0, 0), (-1, -1), 0.5, BORDER),
        ]))
        story.append(dt)

        # Closing strip
        story.append(Spacer(1, 0.12*inch))
        cls = Table(
            [[Paragraph("Guerrilla Marketing Group  ·  gmg@goguerrilla.xyz  ·  Fractional CMO Services",
                        _ps("cls", fontName="Helvetica-Bold", fontSize=9,
                            textColor=ELEC_BLUE, alignment=TA_CENTER))]],
            colWidths=[W - 1.3*inch],
        )
        cls.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), NAVY_MID),
            ("TOPPADDING",   (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
            ("BOX",          (0, 0), (-1, -1), 1, ELEC_BLUE),
        ]))
        story.append(cls)
        return story

    # ══════════════════════════════════════════════════════════
    #  3. C.A.S.H. SCORECARD
    # ══════════════════════════════════════════════════════════

    def _scorecard(self) -> List:
        st    = self.st
        story = self._sec_hdr("C.A.S.H. SCORE OVERVIEW")

        overall = self.cash.get("overall", self.ai.get("overall_score", 50))
        grade   = self.ai.get("overall_grade", _grade(overall))

        # 4-box CASH scores
        cw4  = (W - 1.3*inch) / 4
        row1 = []
        row2 = []
        for let, lbl, sc in [("C","Content",self.cash.get("C",50)),
                              ("A","Audience",self.cash.get("A",50)),
                              ("S","Sales",self.cash.get("S",50)),
                              ("H","Hold / Retention",self.cash.get("H",50))]:
            row1.append(Paragraph(f"{let} — {lbl}", st["tb_wh"]))
            row2.append(Paragraph(f"{sc}/100  ({_grade(sc)})",
                                  _ps(f"sv{let}", fontName="Helvetica-Bold",
                                      fontSize=13, textColor=WHITE, alignment=TA_CENTER)))
        t4 = Table([row1, row2], colWidths=[cw4]*4)
        cmds4 = [
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
            ("TOPPADDING",   (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 8),
            ("INNERGRID",    (0, 0), (-1, -1), 0.5, ELEC_BLUE),
            ("BOX",          (0, 0), (-1, -1), 1, ELEC_BLUE),
        ]
        for ci, sc in enumerate([self.cash.get("C",50),self.cash.get("A",50),
                                  self.cash.get("S",50),self.cash.get("H",50)]):
            cmds4.append(("BACKGROUND", (ci,0), (ci,0), NAVY))
            cmds4.append(("BACKGROUND", (ci,1), (ci,1), _score_color(sc)))
        t4.setStyle(TableStyle(cmds4))
        story.append(t4)
        story.append(Spacer(1, 0.1*inch))

        # Overall score row
        cw2  = (W - 1.3*inch) / 2
        ot   = Table(
            [[Paragraph("OVERALL C.A.S.H. SCORE", st["tb_wh"]),
              Paragraph(f"{overall}/100  ({grade})",
                        _ps("ovs", fontName="Helvetica-Bold", fontSize=14,
                            textColor=WHITE, alignment=TA_CENTER))]],
            colWidths=[cw2, cw2],
        )
        ot.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (0, 0), NAVY),
            ("BACKGROUND",   (1, 0), (1, 0), _score_color(overall)),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
            ("TOPPADDING",   (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
            ("BOX",          (0, 0), (-1, -1), 1.5, ELEC_BLUE),
        ]))
        story.append(ot)
        story.append(Spacer(1, 0.12*inch))

        # Component breakdown detail table
        story += self._sub_hdr("Component Breakdown")
        web  = self.data.get("website", {}).get("scores", {})
        rows = [
            ("ICP / Audience Alignment",  self.data.get("icp",{}).get("score",50),      "A"),
            ("Brand Consistency",         self.data.get("brand",{}).get("score",50),     "A"),
            ("Content Freshness",         self.data.get("freshness",{}).get("score",50), "C"),
            ("SEO Health",                self.data.get("seo",{}).get("score",50),        "C"),
            ("Website Technical",         web.get("technical",50),                        "C"),
            ("Website Conversion",        web.get("conversion",50),                       "S"),
            ("Lead Capture / Funnel",     self.cash.get("S",50),                          "S"),
            ("Retention / Hold Systems",  self.cash.get("H",50),                          "H"),
            ("GEO Visibility",            self.data.get("geo",{}).get("score",50),        "GEO"),
            ("Google Business Profile",   self.data.get("gbp",{}).get("score",50),        "GEO"),
        ]
        tbl_rows = [
            (label, f"{sc}/100", pillar, _grade(sc),
             "Excellent" if sc >= 80 else "Good" if sc >= 65 else
             "Needs Work" if sc >= 50 else "Critical")
            for label, sc, pillar in rows
        ]
        cws = [2.5*inch, 0.8*inch, 0.65*inch, 0.65*inch, 1.3*inch]
        story.append(self._detail_table(
            ["COMPONENT", "SCORE", "PILLAR", "GRADE", "STATUS"],
            tbl_rows, col_widths=cws))
        return story

    # ══════════════════════════════════════════════════════════
    #  4. EXECUTIVE SUMMARY
    # ══════════════════════════════════════════════════════════

    def _executive_summary(self) -> List:
        story = self._sec_hdr("EXECUTIVE SUMMARY")
        ai    = self.ai

        summary = ai.get("executive_summary", "")
        if summary:
            story += self._callout(summary, LGRAY)

        opp   = ai.get("biggest_opportunity", "")
        waste = ai.get("biggest_waste", "")
        if opp or waste:
            cw2 = (W - 1.3*inch) / 2
            t   = Table(
                [[Paragraph(f"<b>BIGGEST OPPORTUNITY</b><br/>{opp or '—'}",
                             _ps("opp", fontName="Helvetica", fontSize=8.5,
                                 textColor=DGRAY, leading=13)),
                  Paragraph(f"<b>BIGGEST WASTE</b><br/>{waste or '—'}",
                             _ps("wst", fontName="Helvetica", fontSize=8.5,
                                 textColor=DGRAY, leading=13))]],
                colWidths=[cw2, cw2],
            )
            t.setStyle(TableStyle([
                ("BACKGROUND",   (0, 0), (0, 0), GREEN_BG),
                ("BACKGROUND",   (1, 0), (1, 0), AMBER_BG),
                ("TEXTCOLOR",    (0, 0), (-1, -1), WHITE),
                ("TOPPADDING",   (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING",(0, 0), (-1, -1), 10),
                ("LEFTPADDING",  (0, 0), (-1, -1), 10),
                ("BOX",          (0, 0), (-1, -1), 0.5, BORDER),
                ("INNERGRID",    (0, 0), (-1, -1), 1, BORDER),
            ]))
            story.append(t)
            story.append(Spacer(1, 0.1*inch))

        priorities = ai.get("top_3_priorities", [])
        if priorities:
            story += self._sub_hdr("Top 3 Priorities")
            story.append(self._detail_table(
                ["#", "ACTION", "IMPACT", "TIMELINE"],
                [(str(p.get("priority", i+1)), p.get("action",""),
                  p.get("impact",""), p.get("timeline",""))
                 for i, p in enumerate(priorities[:3])],
                col_widths=[0.3*inch, 2.2*inch, 2.1*inch, 1.2*inch]))

        verdict = ai.get("icp_alignment_verdict","") or self.data.get("icp",{}).get("icp_verdict","")
        if verdict:
            story += self._sub_hdr("ICP Alignment Verdict")
            story += self._callout(verdict, AMBER_BG)

        cr = ai.get("channel_recommendation","")
        if cr:
            story += self._sub_hdr("Channel Strategy")
            story += self._callout(cr, LGRAY)

        return story

    # ══════════════════════════════════════════════════════════
    #  5. SECTION C — CONTENT
    # ══════════════════════════════════════════════════════════

    def _section_c(self) -> List:
        story = self._cash_banner("C", "CONTENT", self.cash.get("C",50),
            "How fresh, consistent, and strategically distributed is the content?")

        # SEO — Issues & Strengths first (KeepTogether with header), then detail checks
        seo = self.data.get("seo", {})
        seo_hdr = self._sub_hdr(f"SEO Health  —  Score: {seo.get('score',50)}/100  ({_grade(seo.get('score',50))})")
        seo_is  = self._issues_strengths(seo.get("issues",[]), seo.get("strengths",[]))
        story  += [KeepTogether(seo_hdr + seo_is)]
        checks = [
            ("robots.txt present",    seo.get("robots_txt",{}).get("exists",False)),
            ("XML sitemap found",     seo.get("sitemap",{}).get("found",False)),
            ("Canonical tags",        seo.get("canonical",{}).get("present",False)),
            ("Open Graph tags",       seo.get("open_graph",{}).get("present",False)),
            ("OG Image present",      seo.get("open_graph",{}).get("has_og_image",False)),
        ]
        seo_rows = [(c, "✅ Pass" if v else "❌ Fail") for c, v in checks]
        story.append(self._detail_table(["SEO CHECK","STATUS"], seo_rows,
                                        col_widths=[3.5*inch, 1.2*inch]))

        # Website
        web = self.data.get("website",{})
        story += self._sub_hdr(f"Website Audit  —  Technical: {web.get('scores',{}).get('technical',50)}/100")
        meta = [
            ("URL",          web.get("url","—")),
            ("HTTPS",        "✅ Yes" if web.get("https_enabled") else "❌ No"),
            ("Load Time",    f"{web.get('load_time_seconds','?')}s"),
            ("Pages Crawled",str(web.get("pages_crawled",0))),
        ]
        story.append(self._detail_table(["FIELD","VALUE"], meta,
                                        col_widths=[1.8*inch, 3.5*inch]))
        story += self._issues_strengths(web.get("issues",[]), web.get("strengths",[]))

        # Brand — keep header + content together on same page
        brand = self.data.get("brand",{})
        brand_block = (
            self._sub_hdr(f"Brand Consistency  —  Score: {brand.get('score',50)}/100") +
            self._issues_strengths(brand.get("issues",[]), brand.get("strengths",[]))
        )
        story += [KeepTogether(brand_block)]

        # Freshness — overlay Meta live data source labels
        fresh     = self.data.get("freshness",{})
        meta      = self.data.get("meta", {})
        fb_live   = meta.get("facebook", {}).get("data_source") == "meta_graph_api"
        ig_live   = meta.get("instagram", {}).get("data_source") == "meta_graph_api"
        story += self._sub_hdr(f"Content Freshness  —  Score: {fresh.get('score',50)}/100")
        channels = fresh.get("channels",{})
        if channels:
            status_map = {"fresh":        "✅ Fresh",
                          "recent":       "✅ Recent",
                          "stale":        "🟡 Stale",
                          "dead":         "🔴 Inactive",
                          "unknown":      "❓ Unknown",
                          "unknown_inactive": "🟡 Unverified",
                          "api_blocked":  "📡 Live (Meta API)"}
            fr = []
            for p, d in channels.items():
                status_raw = d.get("status", "unknown")
                # Upgrade label for platforms now covered by Meta Graph API
                if status_raw == "api_blocked":
                    if (p == "Facebook" and fb_live) or (p == "Instagram" and ig_live):
                        display_status = "📡 Live (Meta API)"
                    else:
                        display_status = "⚠️ API Required"
                else:
                    display_status = status_map.get(status_raw, "❓")
                fr.append((p,
                           display_status,
                           str(d.get("posts_per_week") or "?"),
                           str(d.get("days_since_last_post") or "?")))
            story.append(self._detail_table(
                ["PLATFORM","STATUS","POSTS/WEEK","DAYS SINCE POST"], fr,
                col_widths=[1.5*inch, 1.5*inch, 1.1*inch, 1.6*inch]))
        story += self._issues_strengths(fresh.get("issues",[]), fresh.get("strengths",[]))

        cs = self.ai.get("content_strategy","")
        if cs:
            story += self._sub_hdr("Content Strategy")
            story += self._callout(cs, LGRAY)

        return story

    # ══════════════════════════════════════════════════════════
    #  6. SECTION A — AUDIENCE
    # ══════════════════════════════════════════════════════════

    def _section_a(self) -> List:
        story = self._cash_banner("A", "AUDIENCE", self.cash.get("A",50),
            "Are you reaching the right people on the right platforms?")

        icp   = self.data.get("icp",{})
        brand = self.data.get("brand",{})

        verdict = icp.get("icp_verdict","")
        if verdict:
            story += self._sub_hdr("ICP Alignment Verdict")
            story += self._callout(verdict, _score_color(icp.get("score",50)))

        pf = brand.get("platform_fit",{})
        ps = pf.get("platform_scores",{})
        if ps:
            story += self._sub_hdr("Platform Fit for Target Market")
            high = pf.get("high_fit",[])
            med  = pf.get("medium_fit",[])
            story.append(self._detail_table(
                ["PLATFORM","FIT SCORE","RECOMMENDATION"],
                [(p, f"{sc}/100",
                  "✅ Prioritize" if p in high else ("⚠️ Use selectively" if p in med else "🔴 Deprioritize"))
                 for p,sc in sorted(ps.items(), key=lambda x:-x[1])],
                col_widths=[1.5*inch, 1.1*inch, 3.1*inch]))

        story += self._issues_strengths(
            icp.get("issues",[]) + brand.get("issues",[]),
            icp.get("strengths",[]) + brand.get("strengths",[]))

        # ── Meta (Facebook + Instagram) live metrics ───────────
        meta = self.data.get("meta", {})
        fb   = meta.get("facebook", {})
        ig   = meta.get("instagram", {})
        fb_live = fb.get("data_source") == "meta_graph_api"
        ig_live = ig.get("data_source") == "meta_graph_api"

        if fb_live or ig_live:
            story += self._sub_hdr("Meta Platform Metrics  —  Live via Graph API")

            meta_rows = []
            if fb_live:
                meta_rows += [
                    ("Facebook Followers",     f"{fb.get('followers') or fb.get('fan_count') or 0:,}"),
                    ("Facebook Posts/Week",    f"{fb.get('posts_per_week') or '—'}"),
                    ("Facebook Last Post",     f"{fb.get('days_since_last_post') or '—'} days ago"
                                               if fb.get('days_since_last_post') is not None else "—"),
                    ("Facebook Engagement Rate", f"{fb.get('engagement_rate') or '—'}%"
                                                  if fb.get('engagement_rate') is not None else "—"),
                    ("Facebook Reach (28d)",   f"{fb.get('reach_28d') or '—':,}"
                                               if isinstance(fb.get('reach_28d'), int) else "—"),
                    ("Facebook Engagements (28d)", f"{fb.get('engagements_28d') or '—':,}"
                                                    if isinstance(fb.get('engagements_28d'), int) else "—"),
                ]
            if ig_live:
                meta_rows += [
                    ("Instagram Followers",    f"{ig.get('followers') or 0:,}"),
                    ("Instagram Total Posts",  f"{ig.get('total_posts') or '—'}"),
                    ("Instagram Posts/Week",   f"{ig.get('posts_per_week') or '—'}"),
                    ("Instagram Last Post",    f"{ig.get('days_since_last_post') or '—'} days ago"
                                               if ig.get('days_since_last_post') is not None else "—"),
                    ("Instagram Engagement Rate", f"{ig.get('engagement_rate') or '—'}%"
                                                   if ig.get('engagement_rate') is not None else "—"),
                    ("Avg Likes / Post",       f"{ig.get('avg_likes_per_post') or '—'}"),
                    ("Avg Comments / Post",    f"{ig.get('avg_comments_per_post') or '—'}"),
                ]

            story.append(self._detail_table(
                ["METRIC", "VALUE"], meta_rows,
                col_widths=[3.0*inch, 2.7*inch]))

            # Meta-specific issues/strengths
            meta_issues    = fb.get("issues", []) + ig.get("issues", [])
            meta_strengths = fb.get("strengths", []) + ig.get("strengths", [])
            if meta_issues or meta_strengths:
                story += self._issues_strengths(meta_issues, meta_strengths)

            # Meta recommendations
            meta_recs = meta.get("recommendations", [])
            if meta_recs:
                story += self._sub_hdr("Meta Recommendations")
                story.append(self._detail_table(
                    ["PLATFORM", "PRIORITY", "ACTION", "DETAIL", "TIMELINE"],
                    [(r.get("platform",""), r.get("priority",""),
                      r.get("action",""), r.get("detail",""),
                      r.get("timeline","")) for r in meta_recs],
                    col_widths=[0.8*inch, 0.9*inch, 1.45*inch, 1.85*inch, 1.0*inch]))

        elif meta:
            # Tier 1 only — FB public data available, no Page token for insights/IG
            fb_t1 = fb.get("data_source") == "meta_graph_api"
            story += self._sub_hdr("Meta Platform Metrics  —  Tier 1 (Public Data)")

            if fb_t1:
                t1_rows = [
                    ("Facebook Followers",  f"{fb.get('followers') or fb.get('fan_count') or 0:,}"),
                    ("Facebook Posts/Week", f"{fb.get('posts_per_week') or '—'}"),
                    ("Facebook Last Post",  f"{fb.get('days_since_last_post')} days ago"
                                            if fb.get('days_since_last_post') is not None else "—"),
                    ("Facebook Engagement Rate",  "—  (Enhanced Meta insights available after Page Access Token setup)"),
                    ("Facebook Reach (28d)",      "—  (Enhanced Meta insights available after Page Access Token setup)"),
                    ("Instagram",                 "—  (Enhanced Meta insights available after Page Access Token setup)"),
                ]
                story.append(self._detail_table(
                    ["METRIC", "VALUE"], t1_rows,
                    col_widths=[3.0*inch, 2.7*inch]))
                # Still show any Tier 1 strengths/issues (post frequency, followers)
                t1_issues    = fb.get("issues", [])
                t1_strengths = fb.get("strengths", [])
                if t1_issues or t1_strengths:
                    story += self._issues_strengths(t1_issues, t1_strengths)
            else:
                story += self._callout(
                    "Facebook and Instagram metrics are not yet available via the Meta Graph API.\n"
                    "Enhanced Meta insights available after Page Access Token setup.\n"
                    "Scopes needed: pages_show_list, pages_read_engagement, "
                    "instagram_basic, read_insights.",
                    NAVY_MID)

            # Show the Page Token setup recommendation
            meta_recs = [r for r in meta.get("recommendations", [])
                         if r.get("platform") == "Meta"]
            if meta_recs:
                story += self._sub_hdr("Meta Recommendations")
                story.append(self._detail_table(
                    ["PLATFORM", "PRIORITY", "ACTION", "DETAIL", "TIMELINE"],
                    [(r.get("platform",""), r.get("priority",""),
                      r.get("action",""), r.get("detail",""),
                      r.get("timeline","")) for r in meta_recs],
                    col_widths=[0.8*inch, 0.9*inch, 1.45*inch, 1.85*inch, 1.0*inch]))

        recs = icp.get("recommendations",[])
        if recs:
            story += self._sub_hdr("Audience & ICP Recommendations")
            story.append(self._detail_table(
                ["PRIORITY","ACTION","DETAIL","TIMELINE"],
                [(r.get("priority",""), r.get("action",""),
                  r.get("detail",""), r.get("timeline","")) for r in recs],
                col_widths=[0.9*inch, 1.8*inch, 2.3*inch, 1.0*inch]))

        cr = self.ai.get("channel_recommendation","")
        if cr:
            story += self._sub_hdr("Channel Strategy")
            story += self._callout(cr, NAVY)

        return story

    # ══════════════════════════════════════════════════════════
    #  7. SECTION S — SALES
    # ══════════════════════════════════════════════════════════

    def _section_s(self) -> List:
        story  = self._cash_banner("S", "SALES", self.cash.get("S",50),
            "How effectively does the brand convert attention into qualified leads?")
        funnel = self.data.get("funnel",{})
        stages = funnel.get("stages",{})

        if stages:
            story += self._sub_hdr("Funnel Stage Analysis")
            stage_labels = [("awareness","Awareness"),("capture","Lead Capture"),
                            ("conversion","Conversion")]
            fr = []
            for key, label in stage_labels:
                st2    = stages.get(key,{})
                n_crit = len([i for i in st2.get("issues",[]) if "🔴" in i])
                n_str  = len(st2.get("strengths",[]))
                status = "🔴 Critical" if n_crit >= 2 else ("⚠️ Needs Work" if n_crit == 1 else "✅ OK")
                fr.append((label, str(n_crit), str(n_str), status))
            story.append(self._detail_table(
                ["STAGE","CRITICAL ISSUES","STRENGTHS","STATUS"], fr,
                col_widths=[1.8*inch, 1.3*inch, 1.3*inch, 1.3*inch]))

        cap = stages.get("capture",{})
        con = stages.get("conversion",{})
        web = self.data.get("website",{})
        story += self._issues_strengths(
            cap.get("issues",[]) + con.get("issues",[]) + web.get("issues",[]),
            cap.get("strengths",[]) + con.get("strengths",[]) + web.get("strengths",[]))

        recs = funnel.get("recommendations",[])
        if recs:
            story += self._sub_hdr("Sales Funnel Recommendations")
            story.append(self._detail_table(
                ["PRIORITY","ACTION","DETAIL","TIMELINE"],
                [(r.get("priority",""), r.get("action",""),
                  r.get("example", r.get("detail","")), r.get("timeline","")) for r in recs],
                col_widths=[0.9*inch, 1.85*inch, 2.25*inch, 1.0*inch]))

        br = self.ai.get("budget_recommendation","")
        if br:
            story += self._sub_hdr("Budget Recommendation")
            story += self._callout(br, AMBER_BG)

        return story

    # ══════════════════════════════════════════════════════════
    #  8. SECTION H — HOLD (RETENTION)
    # ══════════════════════════════════════════════════════════

    def _section_h(self) -> List:
        story  = self._cash_banner("H", "HOLD", self.cash.get("H",50),
            "Are systems in place to retain clients and generate referrals?")
        funnel = self.data.get("funnel",{})
        stages = funnel.get("stages",{})
        nurture = stages.get("nurture",{})
        trust   = stages.get("trust",{})

        story += self._sub_hdr("Retention System Status")
        items = [
            ("Email Newsletter",
             "✅ Active" if self.config.has_active_newsletter else "🔴 Not found"),
            ("Email List Size",
             f"{self.config.email_list_size:,} contacts"
             if self.config.email_list_size > 0 else "🔴 None reported"),
            ("Referral System",
             f"✅ {self.config.referral_system_description or 'Yes'}"
             if self.config.has_referral_system else "🔴 None detected"),
            ("Send Frequency",
             self.config.email_send_frequency or "Not reported"),
        ]
        story.append(self._detail_table(
            ["RETENTION ELEMENT","STATUS"], items,
            col_widths=[2.3*inch, 3.4*inch]))

        # Analytics data if available
        analytics = self.data.get("analytics",{})
        if analytics and not analytics.get("note"):
            story += self._sub_hdr("Website Traffic (GA4)")
            ga_rows = [
                ("Monthly Visitors",     str(analytics.get("monthly_visitors","—"))),
                ("Traffic Trend",        analytics.get("traffic_trend_label","—")),
                ("Bounce Rate",          f"{analytics.get('bounce_rate_pct','—')}%"
                                         if analytics.get("bounce_rate_pct") else "—"),
                ("Avg Session Duration", analytics.get("avg_session_duration","—")),
            ]
            story.append(self._detail_table(
                ["METRIC","VALUE"], ga_rows,
                col_widths=[2.3*inch, 3.4*inch]))
            sources = analytics.get("top_traffic_sources",[])
            if sources:
                story += self._sub_hdr("Top Traffic Sources")
                story.append(self._detail_table(
                    ["CHANNEL","SESSIONS"],
                    [(s.get("channel",""), str(s.get("sessions",""))) for s in sources[:5]],
                    col_widths=[3.5*inch, 2.2*inch]))

        story += self._issues_strengths(
            nurture.get("issues",[]) + trust.get("issues",[]),
            nurture.get("strengths",[]) + trust.get("strengths",[]))

        story += self._sub_hdr("Retention Recommendations")
        hold_recs = [
            ("HIGH",   "Build a 5-email welcome sequence",
             "Introduce agency, share case study, book discovery call.", "2 wks"),
            ("HIGH",   "Launch a client referral program",
             "Offer 1 free month for every referred client who signs.", "1 month"),
            ("MEDIUM", "Biweekly email newsletter",
             "Compliance-safe tips, case studies, industry news.", "2–4 wks"),
            ("MEDIUM", "Collect testimonials from every client",
             "2–3 sentence quote + permission for website and LinkedIn.", "2 wks"),
        ]
        story.append(self._detail_table(
            ["PRIORITY","ACTION","DETAIL","TIMELINE"],
            hold_recs, col_widths=[0.9*inch, 1.9*inch, 2.2*inch, 1.0*inch]))
        return story

    # ══════════════════════════════════════════════════════════
    #  9. GEO — GENERATIVE ENGINE OPTIMISATION
    # ══════════════════════════════════════════════════════════

    def _section_geo(self) -> List:
        geo   = self.data.get("geo", {})
        score = geo.get("score", 50)
        story = self._cash_banner("GEO", "GENERATIVE ENGINE OPTIMISATION", score,
            "SERP rankings · on-page keyword optimisation · AI visibility scoring")

        # ── Component score table ──────────────────────────────
        comps = geo.get("components", {})
        if comps:
            story += self._sub_hdr("GEO Component Scores")
            weights = {
                "SERP Visibility":    "20%",
                "On-page SEO":        "15%",
                "Schema Markup":      "15%",
                "FAQ / Q&A Content":  "15%",
                "E-E-A-T Signals":    "15%",
                "Brand Authority":    "15%",
                "AI Citation Score":  "5%",
            }
            story.append(self._detail_table(
                ["COMPONENT", "WEIGHT", "SCORE", "GRADE"],
                [(n, weights.get(n, "—"), f"{sc}/100", _grade(sc))
                 for n, sc in comps.items()],
                col_widths=[2.4*inch, 0.7*inch, 0.8*inch, 0.75*inch]))

        # ── SERP / Search Console ──────────────────────────────
        serp_kws = geo.get("serp_keywords", [])
        serp_sum = geo.get("serp_summary", {})
        story += self._sub_hdr("SERP Visibility — Google Search Console")

        if serp_kws:
            # Summary metrics
            meta_rows = []
            if serp_sum.get("total_clicks") is not None:
                meta_rows += [
                    ("Total Clicks (90 days)",      f"{serp_sum['total_clicks']:,}"),
                    ("Total Impressions (90 days)",  f"{serp_sum['total_impressions']:,}"),
                    ("Average Position",
                     str(serp_sum["avg_position"]) if serp_sum.get("avg_position") else "—"),
                    ("Keywords in Top 3",            str(serp_sum.get("top_3_count", 0))),
                    ("Keywords on Page 1 (Top 10)",  str(serp_sum.get("top_10_count", 0))),
                    ("Non-branded Keywords",          str(serp_sum.get("non_branded_keywords", 0))),
                ]
            if meta_rows:
                story.append(self._detail_table(
                    ["METRIC", "VALUE"], meta_rows,
                    col_widths=[2.8*inch, 2.9*inch]))
                story.append(Spacer(1, 0.08*inch))

            # Keyword ranking table
            story.append(self._detail_table(
                ["KEYWORD", "CLICKS", "IMPR.", "CTR %", "POSITION"],
                [(r["query"][:55], str(r["clicks"]), str(r["impressions"]),
                  str(r["ctr"]), str(r["position"]))
                 for r in serp_kws[:12]],
                col_widths=[3.0*inch, 0.6*inch, 0.6*inch, 0.6*inch, 0.8*inch]))
        else:
            story += self._callout(
                "Search Console data unavailable. To unlock SERP keyword rankings:\n"
                "1. Go to Google Search Console → Settings → Users & permissions\n"
                "2. Add the service account email as a Full user\n"
                "3. Set GSC_SITE_URL in .env to match your verified property URL\n"
                "   (e.g. GSC_SITE_URL=https://goguerrilla.xyz/)",
                AMBER_BG)

        # ── On-page SEO ────────────────────────────────────────
        op = geo.get("onpage_detail", {})
        story += self._sub_hdr("On-page SEO Analysis")

        onpage_rows = []
        if op.get("title"):
            onpage_rows.append(("Title Tag", op["title"][:80] or "—"))
            onpage_rows.append(("Title Length", f"{len(op['title'])} chars"))
        else:
            onpage_rows.append(("Title Tag", "🔴 MISSING"))

        if op.get("meta_description"):
            onpage_rows.append(("Meta Description", op["meta_description"][:80] + ("…" if len(op["meta_description"]) > 80 else "")))
            onpage_rows.append(("Meta Desc Length", f"{len(op['meta_description'])} chars"))
        else:
            onpage_rows.append(("Meta Description", "🔴 MISSING"))

        h1s = op.get("h1s", [])
        onpage_rows.append(("H1 Tag(s)", h1s[0][:60] if h1s else "🔴 MISSING"))
        if len(h1s) > 1:
            onpage_rows.append(("H1 Count", f"⚠️ {len(h1s)} H1 tags (should be 1)"))

        h2s = op.get("h2s", [])
        onpage_rows.append(("H2 Tags",
            f"{len(h2s)} found: {', '.join(h[:30] for h in h2s[:3])}{'…' if len(h2s) > 3 else ''}"
            if h2s else "🔴 None found"))

        schema_types = op.get("schema_types", [])
        onpage_rows.append(("Schema Types",
            ", ".join(schema_types[:5]) if schema_types else "None detected"))
        onpage_rows.append(("FAQPage Schema",
            "✅ Present" if op.get("has_faq_schema") else "🟡 Missing — add FAQPage schema"))

        if op.get("word_count"):
            onpage_rows.append(("Homepage Word Count", f"{op['word_count']:,} words"))

        if onpage_rows:
            story.append(self._detail_table(
                ["ELEMENT", "FINDING"], onpage_rows,
                col_widths=[1.8*inch, 3.9*inch]))

        story += self._issues_strengths(geo.get("issues", []), geo.get("strengths", []))

        # ── AI Platform Visibility Notes ───────────────────────
        notes = geo.get("platform_notes", {})
        if notes:
            story += self._sub_hdr("AI Platform Visibility Forecast")
            note_rows = [(platform, note) for platform, note in notes.items()]
            story.append(self._detail_table(
                ["PLATFORM", "ASSESSMENT"],
                note_rows,
                col_widths=[1.4*inch, 4.3*inch]))

        # ── GEO Recommendations ────────────────────────────────
        recs = geo.get("recommendations", [])
        if recs:
            story += self._sub_hdr("GEO Recommendations")
            story.append(self._detail_table(
                ["PRIORITY", "ACTION", "IMPACT", "TIMELINE"],
                [(r.get("priority",""), r.get("action",""),
                  r.get("impact",""), r.get("timeline","")) for r in recs],
                col_widths=[0.9*inch, 2.0*inch, 2.1*inch, 1.0*inch]))

        # ── Google Business Profile ────────────────────────────
        gbp = self.data.get("gbp", {})
        story += self._sub_hdr(f"Google Business Profile  —  Score: {gbp.get('score',50)}/100")
        if gbp.get("found"):
            gbp_rows = [
                ("Business Name",    gbp.get("business_name","—")),
                ("Address",          gbp.get("address","—") or "—"),
                ("Phone",            gbp.get("phone","—") or "—"),
                ("Rating",           f"{gbp['rating']}/5" if gbp.get("rating") else "—"),
                ("Reviews",          str(gbp.get("review_count", 0))),
                ("Hours Visible",    "✅ Yes" if gbp.get("hours_listed") else "❌ No"),
                ("Appears Verified", "✅ Yes" if gbp.get("is_likely_verified") else "⚠️ Uncertain"),
                ("NAP Consistent",   "✅ Yes" if gbp.get("nap_consistent") else "⚠️ Check"),
                ("Profile Complete", f"{gbp.get('completeness_pct',0)}%"),
                ("GBP Score",        f"{gbp.get('score',50)}/100  ({gbp.get('grade','C')})"),
            ]
            story.append(self._detail_table(
                ["FIELD", "VALUE"], gbp_rows,
                col_widths=[2.0*inch, 3.7*inch]))
        elif gbp.get("note"):
            story += self._callout(gbp["note"], AMBER_BG)

        story += self._issues_strengths(gbp.get("issues", []), gbp.get("strengths", []))
        return story

    # ══════════════════════════════════════════════════════════
    #  10. COMPETITIVE ANALYSIS
    # ══════════════════════════════════════════════════════════

    def _section_competitive(self) -> List:
        comp_data = self.data.get("competitor",{})
        story     = self._sec_hdr("COMPETITIVE POSITIONING ANALYSIS")

        if comp_data.get("skipped") or not comp_data.get("competitors"):
            story += self._callout(
                comp_data.get("note","No competitor URLs were provided. Add them in the intake questionnaire."),
                LGRAY)

            # Still show competitive positioning narrative if available
            cp = self.ai.get("competitive_positioning","")
            if cp:
                story += self._sub_hdr("Competitive Positioning")
                story += self._callout(cp, LGRAY)
            return story

        challenge = comp_data.get("biggest_challenge","")
        if challenge:
            story += self._sub_hdr("Biggest Marketing Challenge")
            story += self._callout(challenge, AMBER_BG)

        # Competitor URL list
        competitors = comp_data.get("competitors",[])
        story += self._sub_hdr("Competitors Audited")
        story.append(self._detail_table(
            ["#","DOMAIN","SEO","PERF","TECH","SOCIAL"],
            [(str(i+1), c.get("domain",""),
              str(c.get("seo_score","—")), str(c.get("performance_score","—")),
              str(c.get("technical_score","—")), str(c.get("social_channel_count","—")))
             for i,c in enumerate(competitors)],
            col_widths=[0.3*inch, 2.2*inch, 0.65*inch, 0.65*inch, 0.65*inch, 0.65*inch]))
        story.append(Spacer(1, 0.1*inch))

        # Side-by-side comparison
        comparison = comp_data.get("comparison",{})
        rows       = comparison.get("rows",[])
        client     = comparison.get("client",{})
        if rows:
            story += self._sub_hdr("Side-by-Side Comparison")
            n_comps = len(competitors)
            cw_m  = 1.7*inch
            cw_cl = 0.85*inch
            cw_co = max(0.5*inch, (W - 1.3*inch - cw_m - cw_cl) / max(n_comps, 1))
            hdr   = (["METRIC", client.get("label", self.config.client_name)] +
                     [f"Comp {i+1}" for i in range(n_comps)])
            tbl_rows = []
            for row in rows:
                cv   = row.get("client_val","—")
                vals = row.get("comp_vals",[])
                tbl_rows.append(
                    [Paragraph(row.get("metric",""), self.st["tb_bold"])] +
                    [Paragraph(str(cv), _ps("cv", fontName="Helvetica-Bold", fontSize=8.5,
                                            textColor=ELEC_BLUE, alignment=TA_CENTER))] +
                    [Paragraph(str(v), self.st["tb_ctr"]) for v in vals]
                )
            cws_comp = [cw_m, cw_cl] + [cw_co] * n_comps
            ct = Table(
                [[Paragraph(h, self.st["tb_wh"]) for h in hdr]] + tbl_rows,
                colWidths=cws_comp,
            )
            ct.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0), NAVY_MID),
                ("BACKGROUND",    (1, 0), (1, 0),  ELEC_BLUE),
                ("TEXTCOLOR",     (0, 0), (-1, 0), ELEC_BLUE),
                ("TEXTCOLOR",     (1, 0), (1, 0),  NAVY),
                ("ROWBACKGROUNDS",(0, 1), (-1, -1),[NAVY_MID, LGRAY]),
                ("TEXTCOLOR",     (0, 1), (-1, -1), WHITE),
                ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
                ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
                ("TOPPADDING",    (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING",   (0, 0), (-1, -1), 6),
                ("BOX",           (0, 0), (-1, -1), 0.5, BORDER),
                ("INNERGRID",     (0, 0), (-1, -1), 0.5, BORDER),
            ]))
            story.append(ct)

        # Key insights
        insights = comp_data.get("insights",[])
        if insights:
            story += self._sub_hdr("Key Insights")
            for insight in insights:
                story.append(Paragraph(f"• {insight}", self.st["body"]))

        cp = self.ai.get("competitive_positioning","")
        if cp:
            story += self._sub_hdr("Competitive Positioning Strategy")
            story += self._callout(cp, LGRAY)

        return story

    # ══════════════════════════════════════════════════════════
    #  11. 90-DAY ACTION PLAN
    # ══════════════════════════════════════════════════════════

    def _action_plan(self) -> List:
        story = self._sec_hdr("90-DAY ACTION PLAN")
        plan  = self.ai.get("90_day_action_plan", [])

        if plan:
            rows = []
            for item in plan:
                if isinstance(item, dict):
                    rows.append((
                        item.get("phase", item.get("priority", "—")),
                        item.get("action", ""),
                        item.get("outcome", item.get("impact", "")),
                        item.get("timeline", ""),
                    ))
                else:
                    rows.append(("—", str(item), "", ""))
            story.append(self._detail_table(
                ["PHASE", "ACTION", "EXPECTED OUTCOME", "TIMELINE"],
                rows, col_widths=[0.7*inch, 2.2*inch, 2.1*inch, 1.0*inch]))
        else:
            priorities = self.ai.get("top_3_priorities",[])
            if priorities:
                story.append(self._detail_table(
                    ["PRIORITY","ACTION","IMPACT","TIMELINE"],
                    [(str(p.get("priority",i+1)), p.get("action",""),
                      p.get("impact",""), p.get("timeline",""))
                     for i,p in enumerate(priorities)],
                    col_widths=[0.9*inch, 2.2*inch, 1.9*inch, 1.0*inch]))

        return story

    # ══════════════════════════════════════════════════════════
    #  12. CTA / NEXT STEPS
    # ══════════════════════════════════════════════════════════

    def _cta_section(self) -> List:
        story = self._sec_hdr("NEXT STEPS WITH GMG")

        CTA_TEXT = (
            "Your report is just the starting point. Optimization begins now.\n\n"
            "A GMG strategist is already reviewing your results and will be reaching out "
            "with key insights and opportunities tailored to your business.\n\n"
            "If you'd prefer to get ahead and start the conversation sooner, you can "
            "schedule your strategy session here:\n\n"
            "www.gogmg.net/meeting"
        )

        # Prominent navy box — headline in cyan, body in white, URL in gold
        cta_content = [
            Paragraph(
                "Your report is just the starting point. Optimization begins now.",
                _ps("cta_h", fontName="Helvetica-Bold", fontSize=13,
                    textColor=ELEC_BLUE, leading=18, spaceAfter=10),
            ),
            Spacer(1, 8),
            Paragraph(
                "A GMG strategist is already reviewing your results and will be reaching "
                "out with key insights and opportunities tailored to your business.",
                _ps("cta_b", fontName="Helvetica", fontSize=10,
                    textColor=WHITE, leading=15),
            ),
            Spacer(1, 10),
            Paragraph(
                "If you'd prefer to get ahead and start the conversation sooner, "
                "schedule your strategy session at "
                "<font color='#00AEEF'><b>www.gogmg.net/meeting</b></font>",
                _ps("cta_b2", fontName="Helvetica", fontSize=10,
                    textColor=WHITE, leading=15),
            ),
        ]

        cta_box = Table(
            [[cta_content]],
            colWidths=[W - 1.3*inch],
        )
        cta_box.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), NAVY_MID),
            ("TOPPADDING",    (0, 0), (-1, -1), 22),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 22),
            ("LEFTPADDING",   (0, 0), (-1, -1), 24),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 24),
            ("BOX",           (0, 0), (-1, -1), 2, ELEC_BLUE),
        ]))

        story.append(Spacer(1, 0.1*inch))
        story.append(cta_box)
        return story

    # ══════════════════════════════════════════════════════════
    #  13. APPENDIX
    # ══════════════════════════════════════════════════════════

    def _appendix(self) -> List:
        story = self._sec_hdr("APPENDIX — METHODOLOGY & DATA SOURCES")

        story.append(Paragraph(
            "The C.A.S.H. Report is generated by a multi-layer auditing system that combines "
            "live data from public APIs, website scraping, and rule-based scoring calibrated "
            "by industry and business type. All scores are normalised to a 0–100 scale with "
            "letter grades (A–F).",
            self.st["body"]))
        story.append(Spacer(1, 0.08*inch))

        sources = [
            ("PageSpeed Insights API",  "Website performance, Core Web Vitals, mobile SEO"),
            ("Website scraping",        "Technical checks, content analysis, conversion elements"),
            ("Google Maps HTML",        "GBP listing confirmation, NAP detection"),
            ("YouTube Data API v3",     "Channel subscribers, upload recency"),
            ("Google Analytics 4",      "Traffic, bounce rate, session duration (if configured)"),
            ("OpenStreetMap Nominatim", "Business address verification for local businesses"),
            ("Rule-based AI analyzer",  "CASH scoring, issue weighting, action plan generation"),
        ]
        story.append(self._detail_table(
            ["DATA SOURCE","WHAT IT MEASURES"], sources,
            col_widths=[2.0*inch, 3.7*inch]))

        story.append(Spacer(1, 0.1*inch))
        story += self._sub_hdr("Scoring Methodology")
        story.append(Paragraph(
            "Each auditor applies a rubric specific to the business's industry subcategory. "
            "Deductions are capped to prevent extreme scores for new or niche businesses: "
            "confirmed-missing elements floor at 25–35, partially present elements score 50–65, "
            "and well-optimised elements score 75–100. The overall C.A.S.H. score is a "
            "weighted average of the four pillars.",
            self.st["body"]))

        story.append(Spacer(1, 0.15*inch))
        story.append(HRFlowable(width="100%", thickness=1, color=ELEC_BLUE))
        for line in [
            f"Audit date: {self.date_str}",
            f"Client: {self.config.client_name}",
            f"Industry: {self.config.industry_category or self.config.client_industry or '—'}",
            f"AI analysis: {'Claude AI' if getattr(self.config,'anthropic_api_key','') else 'Rule-based'}",
            "Schedule a follow-up audit in 90 days to measure progress.",
            f"Report by {self.config.agency_name}  ·  Confidential",
        ]:
            story.append(Paragraph(line, self.st["small"]))
        return story

    # ══════════════════════════════════════════════════════════
    #  CASH SCORE RESOLUTION
    # ══════════════════════════════════════════════════════════

    def _resolve_cash(self) -> Dict[str, Any]:
        ai = self.ai
        if ai.get("cash_c_score"):
            overall = ai.get("overall_score", 50)
            return {
                "C": ai.get("cash_c_score", 50),
                "A": ai.get("cash_a_score", 50),
                "S": ai.get("cash_s_score", 50),
                "H": ai.get("cash_h_score", 50),
                "overall": overall,
            }
        cs = ai.get("component_scores", {})
        overall = ai.get("overall_score",
                         round(sum([cs.get("C",50), cs.get("A",50),
                                    cs.get("S",50), cs.get("H",50)]) / 4))
        return {
            "C": cs.get("C", 50), "A": cs.get("A", 50),
            "S": cs.get("S", 50), "H": cs.get("H", 50),
            "overall": overall,
        }
