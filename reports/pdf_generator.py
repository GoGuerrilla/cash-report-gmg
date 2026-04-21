"""
C.A.S.H. Report by GMG — PDF Generator (Playwright / HTML)
-----------------------------------------------------------
Drop-in replacement for the ReportLab version.
Same public interface:  PDFReportGenerator(config, audit_data).generate(output_path)

Uses the enhanced dark-theme HTML/CSS template (navy background, cyan accents,
color-coded priority borders, red/green split tables, phase headers) rendered to
PDF via Playwright Chromium — identical visual design to the standalone
generate_report.py template on the Desktop.
"""

import base64
import html as _html_module
import os
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

from config import ClientConfig

# ── Logo ──────────────────────────────────────────────────────────────────────
_LOGO_SEARCH = [
    "/Users/davidsuppnick/Desktop/CASH GMG Audit/gmg_logo.png",
    os.path.join(os.path.dirname(__file__), "..", "gmg_cash_logo.png"),
    os.path.join(os.path.dirname(__file__), "..", "gmg_logo.png"),
    "gmg_logo.png",
]

_LOGO_SRC = ""
for _lp in _LOGO_SEARCH:
    _lp = os.path.abspath(_lp)
    if os.path.isfile(_lp):
        try:
            with open(_lp, "rb") as _f:
                _LOGO_SRC = "data:image/png;base64," + base64.b64encode(_f.read()).decode()
        except Exception:
            pass
        break


# ── CSS (dark-theme, cyan accents — identical to enhanced template) ────────────
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Barlow:wght@300;400;500;600;700&family=Barlow+Condensed:wght@700;800&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
:root{--navy:#1B2A4A;--cyan:#00AEEF;--red:#E74C3C;--green:#27AE60;--orange:#E67E22;--dark:#0D1B30;--border:rgba(0,174,239,.18)}
body{font-family:'Barlow',sans-serif;background:var(--dark);color:#fff;margin:0}
.page{width:794px;min-height:1123px;background:var(--dark);position:relative;overflow:visible;page-break-after:always;padding-bottom:60px}
.page:last-child{page-break-after:avoid}
.bg-grid{position:absolute;inset:0;pointer-events:none;background-image:linear-gradient(rgba(0,174,239,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(0,174,239,.025) 1px,transparent 1px);background-size:50px 50px}
.top-stripe{height:4px;background:linear-gradient(90deg,var(--cyan),rgba(0,174,239,.15))}
.page-header{display:flex;justify-content:space-between;align-items:center;padding:10px 40px;border-bottom:1px solid var(--border)}
.header-logo{height:38px;width:auto;object-fit:contain;filter:drop-shadow(0 0 8px rgba(0,174,239,.35))}
.header-right{text-align:right}
.header-brand{color:var(--cyan);font-weight:700;font-size:11px;letter-spacing:2px;text-transform:uppercase}
.header-meta{color:rgba(255,255,255,.4);font-size:11px}
.page-footer{position:absolute;bottom:0;left:0;right:0;height:44px;border-top:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 40px}
.page-footer span{font-size:11px;color:rgba(255,255,255,.25)}
.pf-email{color:rgba(0,174,239,.45)!important}
.body{padding:26px 40px 0}
.text-body{font-size:12.5px;color:rgba(255,255,255,.72);line-height:1.68;margin-bottom:14px}
.page-title{font-family:'Barlow Condensed',sans-serif;font-size:30px;font-weight:800;color:#fff;text-transform:uppercase;border-bottom:1px solid var(--border);padding-bottom:12px;margin-bottom:20px}
.page-title span{color:var(--cyan)}
.section-header{display:flex;align-items:center;gap:14px;background:rgba(0,174,239,.06);border:1px solid rgba(0,174,239,.2);border-left:4px solid var(--cyan);padding:14px 18px;margin-bottom:22px;break-inside:avoid}
.sh-letter{font-family:'Barlow Condensed',sans-serif;font-size:48px;font-weight:800;color:var(--cyan);line-height:1}
.sh-info{flex:1}
.sh-title{font-family:'Barlow Condensed',sans-serif;font-size:24px;font-weight:800;color:#fff;text-transform:uppercase}
.sh-sub{font-size:12px;color:rgba(255,255,255,.5);margin-top:2px}
.sh-badge{padding:10px 14px;text-align:center;min-width:85px}
.grade-a{background:rgba(39,174,96,.15);border:1px solid rgba(39,174,96,.3)}
.grade-b{background:rgba(0,174,239,.1);border:1px solid rgba(0,174,239,.25)}
.grade-c{background:rgba(0,174,239,.08);border:1px solid rgba(0,174,239,.2)}
.grade-d{background:rgba(231,76,60,.1);border:1px solid rgba(231,76,60,.25)}
.grade-f{background:rgba(231,76,60,.15);border:1px solid rgba(231,76,60,.35)}
.sh-badge-score{font-family:'Barlow Condensed',sans-serif;font-size:24px;font-weight:800;color:#fff;line-height:1}
.sh-badge-grade{font-size:10px;font-weight:700;letter-spacing:2px;color:rgba(255,255,255,.5);margin-top:2px}
.sub-title{font-family:'Barlow Condensed',sans-serif;font-size:13px;font-weight:700;color:var(--cyan);text-transform:uppercase;letter-spacing:2px;margin-bottom:9px;margin-top:18px;display:flex;align-items:center;gap:10px}
.sub-title::after{content:'';flex:1;height:1px;background:rgba(0,174,239,.2)}
.ptag{display:inline-block;font-size:9px;font-weight:700;padding:2px 8px;border-radius:2px;letter-spacing:2px;text-transform:uppercase;vertical-align:middle}
.ptag-critical{background:rgba(231,76,60,.18);color:var(--red);border:1px solid rgba(231,76,60,.3)}
.ptag-high{background:rgba(230,126,34,.18);color:var(--orange);border:1px solid rgba(230,126,34,.3)}
.ptag-medium{background:rgba(0,174,239,.12);color:var(--cyan);border:1px solid rgba(0,174,239,.25)}
.ptag-low{background:rgba(255,255,255,.06);color:rgba(255,255,255,.5);border:1px solid rgba(255,255,255,.1)}
.sdot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px;vertical-align:middle}
.sdot-g{background:var(--green)}.sdot-r{background:var(--red)}.sdot-y{background:#F39C12}.sdot-b{background:var(--cyan)}.sdot-gray{background:rgba(255,255,255,.3)}
.sbadge{display:inline-block;font-size:10px;font-weight:700;padding:2px 8px;border-radius:2px;letter-spacing:1px}
.sbadge-ok{background:rgba(39,174,96,.15);color:var(--green)}
.sbadge-good{background:rgba(0,174,239,.12);color:var(--cyan)}
.sbadge-warn{background:rgba(230,126,34,.15);color:var(--orange)}
.sbadge-critical{background:rgba(231,76,60,.15);color:var(--red)}
.sbadge-gray{background:rgba(255,255,255,.06);color:rgba(255,255,255,.5)}
.data-table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:16px}
.data-table thead tr{background:rgba(0,174,239,.08);border-bottom:1px solid rgba(0,174,239,.2)}
.data-table thead th{padding:7px 11px;text-align:left;font-size:10px;font-weight:700;color:var(--cyan);letter-spacing:2px;text-transform:uppercase}
.data-table tbody tr{border-bottom:1px solid rgba(255,255,255,.04);break-inside:avoid}
.data-table tbody tr:nth-child(odd){background:rgba(255,255,255,.02)}
.data-table tbody td{padding:8px 11px;color:rgba(255,255,255,.75);vertical-align:top;line-height:1.5}
.td-name{color:#fff!important;font-weight:600!important}
.td-good{color:var(--green)!important;font-weight:600!important}
.td-warn{color:#F39C12!important;font-weight:600!important}
.td-bad{color:var(--red)!important;font-weight:600!important}
.split-table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:16px}
.split-table thead tr{border-bottom:1px solid rgba(255,255,255,.08)}
.split-table thead th{padding:7px 13px;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase}
.th-issues{color:var(--red);text-align:left}
.th-strengths{color:var(--green);text-align:left}
.split-table tbody tr{break-inside:avoid}
.split-table tbody td{padding:6px 13px;font-size:12px;color:rgba(255,255,255,.72);vertical-align:top;line-height:1.55;border-bottom:1px solid rgba(255,255,255,.04)}
.col-issues{border-left:3px solid rgba(231,76,60,.5);background:rgba(231,76,60,.04)}
.col-strengths{border-left:3px solid rgba(39,174,96,.5);background:rgba(39,174,96,.04)}
.col-issues .bullet{color:var(--red);margin-right:4px}
.col-strengths .bullet{color:var(--green);margin-right:4px}
.rec-table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:16px}
.rec-table thead tr{background:rgba(0,174,239,.08);border-bottom:1px solid rgba(0,174,239,.2)}
.rec-table thead th{padding:7px 11px;font-size:10px;font-weight:700;color:var(--cyan);letter-spacing:2px;text-transform:uppercase}
.rec-table tbody tr{border-bottom:1px solid rgba(255,255,255,.05);break-inside:avoid}
.rec-table tbody tr:nth-child(odd){background:rgba(255,255,255,.02)}
.rec-table tbody td{padding:9px 11px;color:rgba(255,255,255,.75);vertical-align:top;line-height:1.5}
.action-col{color:#fff!important;font-weight:600!important}
.rec-row-critical td:first-child{border-left:3px solid var(--red)}
.rec-row-high td:first-child{border-left:3px solid var(--orange)}
.rec-row-medium td:first-child{border-left:3px solid var(--cyan)}
.rec-row-low td:first-child{border-left:3px solid rgba(255,255,255,.2)}
.score-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:2px;margin-bottom:18px}
.score-card{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);padding:13px 10px;text-align:center}
.sc-letter{font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:800;line-height:1;margin-bottom:3px}
.sc-label{font-size:10px;color:rgba(255,255,255,.35);text-transform:uppercase;letter-spacing:1.5px;margin-bottom:7px}
.sc-score{font-family:'Barlow Condensed',sans-serif;font-size:20px;font-weight:800;color:#fff;line-height:1}
.sc-grade{font-size:10px;font-weight:700;padding:2px 8px;border-radius:2px;display:inline-block;margin-top:5px}
.ga{background:rgba(39,174,96,.15);color:#27AE60}
.gb{background:rgba(0,174,239,.12);color:#00AEEF}
.gc{background:rgba(0,174,239,.1);color:#00AEEF}
.gd{background:rgba(231,76,60,.15);color:#E74C3C}
.gf{background:rgba(231,76,60,.2);color:#E74C3C}
.overall-score-banner{display:flex;overflow:hidden;margin-bottom:16px;border:1px solid rgba(0,174,239,.3)}
.osb-grade{background:var(--cyan);width:95px;flex-shrink:0;display:flex;flex-direction:column;align-items:center;justify-content:center}
.osb-letter{font-family:'Barlow Condensed',sans-serif;font-size:60px;font-weight:800;color:var(--navy);line-height:1}
.osb-label{font-size:10px;font-weight:700;color:rgba(27,42,74,.7);letter-spacing:2px}
.osb-info{flex:1;padding:16px 22px;background:linear-gradient(135deg,rgba(0,174,239,.1),rgba(0,174,239,.03))}
.osb-stitle{font-size:11px;font-weight:700;color:var(--cyan);letter-spacing:3px;text-transform:uppercase;margin-bottom:4px}
.osb-score{font-family:'Barlow Condensed',sans-serif;font-size:48px;font-weight:800;color:#fff;line-height:1}
.osb-score span{font-size:18px;font-weight:400;color:rgba(255,255,255,.35)}
.osb-desc{font-size:12px;color:rgba(255,255,255,.5);margin-top:4px}
.callout-cyan{background:linear-gradient(135deg,rgba(0,174,239,.12),rgba(0,174,239,.04));border:1px solid rgba(0,174,239,.28);border-left:4px solid var(--cyan);padding:16px 20px;border-radius:2px;margin-bottom:14px;break-inside:avoid}
.callout-red{background:linear-gradient(135deg,rgba(231,76,60,.1),rgba(231,76,60,.03));border:1px solid rgba(231,76,60,.25);border-left:4px solid var(--red);padding:16px 20px;border-radius:2px;margin-bottom:14px;break-inside:avoid}
.callout-green{background:linear-gradient(135deg,rgba(39,174,96,.1),rgba(39,174,96,.03));border:1px solid rgba(39,174,96,.25);border-left:4px solid var(--green);padding:16px 20px;border-radius:2px;margin-bottom:14px;break-inside:avoid}
.callout-label{font-size:10px;font-weight:700;letter-spacing:3px;text-transform:uppercase;margin-bottom:6px}
.callout-cyan .callout-label{color:var(--cyan)}
.callout-red .callout-label{color:var(--red)}
.callout-green .callout-label{color:var(--green)}
.callout-title{font-family:'Barlow Condensed',sans-serif;font-size:17px;font-weight:800;color:#fff;text-transform:uppercase;margin-bottom:7px}
.callout-body{font-size:12.5px;color:rgba(255,255,255,.72);line-height:1.65}
.callout-tags{display:flex;gap:8px;margin-top:11px;flex-wrap:wrap}
.ctag-green{background:rgba(39,174,96,.15);border:1px solid rgba(39,174,96,.3);color:var(--green);font-size:10px;font-weight:700;padding:3px 10px;letter-spacing:1px}
.ctag-orange{background:rgba(230,126,34,.15);border:1px solid rgba(230,126,34,.3);color:var(--orange);font-size:10px;font-weight:700;padding:3px 10px;letter-spacing:1px}
.ctag-cyan{background:rgba(0,174,239,.12);border:1px solid rgba(0,174,239,.28);color:var(--cyan);font-size:10px;font-weight:700;padding:3px 10px;letter-spacing:1px}
.priority-rows{display:flex;flex-direction:column;gap:9px;margin-bottom:18px}
.priority-row{display:flex;align-items:flex-start;gap:12px;border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.02);padding:13px 15px;position:relative;overflow:hidden;break-inside:avoid}
.priority-row::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px}
.pr-critical::before{background:var(--red)}
.pr-high::before{background:var(--orange)}
.pr-medium::before{background:var(--cyan)}
.pr-num{font-family:'Barlow Condensed',sans-serif;font-size:32px;font-weight:800;color:rgba(255,255,255,.07);line-height:1;flex-shrink:0;width:30px}
.pr-content{flex:1}
.pr-tag-line{margin-bottom:3px}
.pr-heading{font-family:'Barlow Condensed',sans-serif;font-size:15px;font-weight:800;color:#fff;text-transform:uppercase;margin-bottom:4px}
.pr-desc{font-size:12px;color:rgba(255,255,255,.62);line-height:1.55}
.pr-meta{display:flex;gap:7px;margin-top:7px;flex-wrap:wrap}
.pr-meta-tag{font-size:10px;font-weight:600;padding:2px 9px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);color:rgba(255,255,255,.45)}
.field-table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px}
.field-table tr{border-bottom:1px solid rgba(255,255,255,.05);break-inside:avoid}
.field-table tr:nth-child(odd){background:rgba(255,255,255,.02)}
.field-table td{padding:7px 11px;color:rgba(255,255,255,.72)}
.ft-label{color:rgba(255,255,255,.4);font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;width:180px}
.ft-val{color:#fff;font-weight:500}
.info-box{background:rgba(0,174,239,.06);border:1px solid rgba(0,174,239,.18);padding:14px 16px;margin-bottom:14px;font-size:12.5px;color:rgba(255,255,255,.72);line-height:1.65;break-inside:avoid}
.strategy-box{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);padding:14px 16px;margin-bottom:14px;font-size:12.5px;color:rgba(255,255,255,.72);line-height:1.65;break-inside:avoid}
.framework-items{display:flex;flex-direction:column;gap:10px;margin-bottom:20px}
.fw-item{display:flex;align-items:flex-start;gap:12px;padding:12px 16px;border:1px solid rgba(255,255,255,.07);background:rgba(255,255,255,.02);break-inside:avoid}
.fw-letter{font-family:'Barlow Condensed',sans-serif;font-size:38px;font-weight:800;color:var(--cyan);line-height:1;width:36px;flex-shrink:0}
.fw-name{font-family:'Barlow Condensed',sans-serif;font-size:15px;font-weight:800;color:#fff;text-transform:uppercase;margin-bottom:2px}
.fw-desc{font-size:12px;color:rgba(255,255,255,.6);line-height:1.5}
.deliverables{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-bottom:18px}
.deliv-item{display:flex;align-items:flex-start;gap:7px;padding:9px 11px;background:rgba(39,174,96,.06);border:1px solid rgba(39,174,96,.18);font-size:12px;color:rgba(255,255,255,.72);line-height:1.5}
.deliv-check{color:var(--green);font-weight:700;flex-shrink:0;margin-top:1px}
.phase-header{display:flex;align-items:center;gap:11px;padding:10px 14px;border-radius:2px 2px 0 0}
.ph-red{background:linear-gradient(90deg,rgba(231,76,60,.18),rgba(231,76,60,.04));border:1px solid rgba(231,76,60,.28);border-bottom:none}
.ph-orange{background:linear-gradient(90deg,rgba(230,126,34,.15),rgba(230,126,34,.04));border:1px solid rgba(230,126,34,.25);border-bottom:none}
.ph-cyan{background:linear-gradient(90deg,rgba(0,174,239,.12),rgba(0,174,239,.03));border:1px solid rgba(0,174,239,.22);border-bottom:none}
.ph-label{font-size:10px;font-weight:700;letter-spacing:3px;text-transform:uppercase}
.ph-red .ph-label{color:var(--red)}.ph-orange .ph-label{color:var(--orange)}.ph-cyan .ph-label{color:var(--cyan)}
.ph-title{font-family:'Barlow Condensed',sans-serif;font-size:15px;font-weight:800;color:#fff;text-transform:uppercase;flex:1}
.ph-days{font-size:11px;color:rgba(255,255,255,.4);background:rgba(255,255,255,.05);padding:2px 9px}
.phase-table-wrap{margin-bottom:16px;break-inside:avoid}
.pt-bordered-red{border:1px solid rgba(231,76,60,.2);border-top:none}
.pt-bordered-orange{border:1px solid rgba(230,126,34,.18);border-top:none}
.pt-bordered-cyan{border:1px solid rgba(0,174,239,.18);border-top:none}
.gbp-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:18px}
.gbp-field{background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.07);padding:11px 13px}
.gbp-field-label{font-size:10px;font-weight:700;color:rgba(255,255,255,.35);letter-spacing:2px;text-transform:uppercase;margin-bottom:4px}
.gbp-field-value{font-size:14px;font-weight:600;color:#fff}
.ai-table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:16px}
.ai-table tr{border-bottom:1px solid rgba(255,255,255,.05);break-inside:avoid}
.ai-table tr:nth-child(odd){background:rgba(255,255,255,.02)}
.ai-table td{padding:9px 13px;vertical-align:top;line-height:1.55}
.ai-platform{font-weight:700;color:var(--cyan);width:140px}
.cover-logo-wrap{text-align:center;padding:32px 0 24px}
.cover-logo-img{width:110px;height:110px;object-fit:contain;filter:drop-shadow(0 0 18px rgba(0,174,239,.4))}
.cover-title-box{background:rgba(0,174,239,.08);border:1px solid rgba(0,174,239,.25);text-align:center;padding:18px;margin-bottom:18px}
.cover-title-box h1{font-family:'Barlow Condensed',sans-serif;font-size:42px;font-weight:800;color:var(--cyan);text-transform:uppercase;letter-spacing:1px}
.cover-company{font-family:'Barlow Condensed',sans-serif;font-size:30px;font-weight:800;color:#fff;text-transform:uppercase;margin-bottom:3px}
.cover-sub{font-size:13px;color:rgba(255,255,255,.4);letter-spacing:3px;text-transform:uppercase;margin-bottom:20px}
.cover-divider{height:1px;background:rgba(0,174,239,.2);margin-bottom:20px}
.cover-meta{font-size:12px;color:rgba(255,255,255,.4);line-height:2;margin-top:20px}
.cover-meta strong{color:rgba(255,255,255,.65)}
.cta-hero{text-align:center;padding:32px 20px 22px;border-bottom:1px solid var(--border);margin-bottom:22px}
.cta-hero h2{font-family:'Barlow Condensed',sans-serif;font-size:40px;font-weight:800;color:#fff;text-transform:uppercase;line-height:1;margin-bottom:10px}
.cta-hero h2 span{color:var(--cyan)}
.cta-hero p{font-size:13px;color:rgba(255,255,255,.55);margin-bottom:20px;max-width:500px;margin-left:auto;margin-right:auto;line-height:1.65}
.cta-btn{display:inline-block;background:var(--cyan);color:var(--navy);font-family:'Barlow Condensed',sans-serif;font-size:16px;font-weight:800;letter-spacing:3px;text-transform:uppercase;padding:14px 42px;box-shadow:0 0 36px rgba(0,174,239,.3)}
.cta-url{font-size:12px;color:rgba(255,255,255,.3);margin-top:9px;letter-spacing:1px}
.cta-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:2px;margin-bottom:20px}
.cta-card{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);padding:16px 13px;text-align:center}
.cta-card-icon{font-size:22px;margin-bottom:7px}
.cta-card-name{font-family:'Barlow Condensed',sans-serif;font-size:14px;font-weight:800;color:#fff;text-transform:uppercase;margin-bottom:6px}
.cta-card-desc{font-size:12px;color:rgba(255,255,255,.5);line-height:1.55}
.contact-strip{display:grid;grid-template-columns:repeat(3,1fr);gap:2px}
.cs-item{background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.06);padding:13px;text-align:center}
.cs-label{font-size:10px;font-weight:700;color:rgba(255,255,255,.3);letter-spacing:2px;text-transform:uppercase;margin-bottom:5px}
.cs-value{font-size:13px;color:rgba(255,255,255,.8);font-weight:500}
.highlight{color:var(--cyan)!important}
.app-table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:14px}
.app-table thead tr{background:rgba(0,174,239,.08);border-bottom:1px solid rgba(0,174,239,.2)}
.app-table thead th{padding:7px 11px;font-size:10px;font-weight:700;color:var(--cyan);letter-spacing:2px;text-transform:uppercase}
.app-table tbody tr{border-bottom:1px solid rgba(255,255,255,.05);break-inside:avoid}
.app-table tbody tr:nth-child(odd){background:rgba(255,255,255,.02)}
.app-table tbody td{padding:7px 11px;color:rgba(255,255,255,.72);vertical-align:top;line-height:1.5}
.app-source{font-weight:600;color:#fff}
"""


# ── Tiny HTML helpers ──────────────────────────────────────────────────────────

def _h(text) -> str:
    """HTML-escape a value; return empty string for None."""
    if text is None:
        return ""
    return _html_module.escape(str(text))


def _grade(score) -> str:
    s = int(score) if score is not None else 0
    if s >= 80: return "A"
    if s >= 65: return "B"
    if s >= 50: return "C"
    if s >= 35: return "D"
    return "F"


def _gc(g: str) -> str:
    """CSS class for a grade letter."""
    return {"A": "ga", "B": "gb", "C": "gc", "D": "gd", "F": "gf"}.get(g, "gc")


def _sdot(color: str) -> str:
    cls = {"g": "sdot-g", "r": "sdot-r", "y": "sdot-y", "b": "sdot-b",
           "gray": "sdot-gray"}.get(color, "sdot-gray")
    return f'<span class="sdot {cls}"></span>'


def _sbadge(level: str, text: str) -> str:
    cls = {"ok": "sbadge-ok", "good": "sbadge-good", "warn": "sbadge-warn",
           "critical": "sbadge-critical", "gray": "sbadge-gray"}.get(level, "sbadge-gray")
    return f'<span class="sbadge {cls}">{_h(text)}</span>'


def _ptag(level: str) -> str:
    cls = {"CRITICAL": "ptag-critical", "HIGH": "ptag-high",
           "MEDIUM": "ptag-medium", "LOW": "ptag-low"}.get(level.upper(), "ptag-low")
    return f'<span class="ptag {cls}">{_h(level)}</span>'


def _sub(title: str) -> str:
    return f'<div class="sub-title">{_h(title)}</div>'


def _field(label: str, val: str) -> str:
    return f'<tr><td class="ft-label">{_h(label)}</td><td class="ft-val">{val}</td></tr>'


def _clean(text: str) -> str:
    """Strip leading emoji/status prefixes from audit issue/strength strings."""
    import re
    return re.sub(
        r'^[\U0001F000-\U0001FFFF\u2600-\u27BF🔴🟡✅⚠️📊]+\s*', "", str(text)
    ).strip()


def _split_row(issue: str, strength: str) -> str:
    i_td = (f'<td class="col-issues"><span class="bullet">■</span>{_h(_clean(issue))}</td>'
            if issue else '<td class="col-issues"></td>')
    s_td = (f'<td class="col-strengths"><span class="bullet">✓</span>{_h(_clean(strength))}</td>'
            if strength else '<td class="col-strengths"></td>')
    return f"<tr>{i_td}{s_td}</tr>"


def _split_table(issues: List[str], strengths: List[str]) -> str:
    if not issues and not strengths:
        return ""
    max_r = max(len(issues), len(strengths))
    thead = ('<thead><tr>'
             '<th class="th-issues" style="width:50%">■ Issues to Address</th>'
             '<th class="th-strengths" style="width:50%">✓ Strengths</th>'
             '</tr></thead>')
    rows = "".join(
        _split_row(issues[i] if i < len(issues) else "",
                   strengths[i] if i < len(strengths) else "")
        for i in range(max_r)
    )
    return f'<table class="split-table">{thead}<tbody>{rows}</tbody></table>'


def _rec_row(priority: str, action: str, detail: str, timeline: str) -> str:
    p = priority.upper()
    cls = {"CRITICAL": "rec-row-critical", "HIGH": "rec-row-high",
           "MEDIUM": "rec-row-medium", "LOW": "rec-row-low"}.get(p, "rec-row-low")
    return (f'<tr class="{cls}">'
            f'<td>{_ptag(p)}</td>'
            f'<td class="action-col">{_h(action)}</td>'
            f'<td>{_h(detail)}</td>'
            f'<td>{_h(timeline)}</td>'
            f'</tr>')


def _rec_table(rows_html: str) -> str:
    thead = ('<thead><tr>'
             '<th style="width:100px">Priority</th>'
             '<th>Action</th><th>Detail</th>'
             '<th style="width:80px">Timeline</th>'
             '</tr></thead>')
    return f'<table class="rec-table">{thead}<tbody>{rows_html}</tbody></table>'


def _phase_block(color: str, num: int, title: str, days: str, rows_html: str) -> str:
    ph_cls = {"red": "ph-red", "orange": "ph-orange", "cyan": "ph-cyan"}.get(color, "ph-cyan")
    tb_cls = {"red": "pt-bordered-red", "orange": "pt-bordered-orange",
              "cyan": "pt-bordered-cyan"}.get(color, "pt-bordered-cyan")
    header = (f'<div class="phase-header {ph_cls}">'
              f'<div class="ph-label">Phase {num}</div>'
              f'<div class="ph-title">{_h(title)}</div>'
              f'<div class="ph-days">{_h(days)}</div>'
              f'</div>')
    thead = ('<thead><tr>'
             '<th style="width:100px">Priority</th>'
             '<th>Action</th><th>Expected Outcome</th>'
             '<th style="width:80px">Timeline</th>'
             '</tr></thead>')
    return (f'<div class="phase-table-wrap">{header}'
            f'<table class="rec-table {tb_cls}">{thead}<tbody>{rows_html}</tbody></table>'
            f'</div>')


def _section_hdr(letter: str, title: str, subtitle: str, score, grade: str) -> str:
    gclass = f"grade-{grade.lower()}"
    ltr_size = "font-size:36px" if len(letter) > 1 else ""
    return (f'<div class="section-header">'
            f'<div class="sh-letter" style="{ltr_size}">{_h(letter)}</div>'
            f'<div class="sh-info">'
            f'<div class="sh-title">{_h(title)}</div>'
            f'<div class="sh-sub">{_h(subtitle)}</div>'
            f'</div>'
            f'<div class="sh-badge {gclass}">'
            f'<div class="sh-badge-score">{score}/100</div>'
            f'<div class="sh-badge-grade">GRADE {_h(grade)}</div>'
            f'</div>'
            f'</div>')


def _hdr(n: int, date_str: str, logo_src: str) -> str:
    return f"""
  <div class="top-stripe"></div>
  <div class="page-header">
    <img class="header-logo" src="{logo_src}" alt="GMG"/>
    <div class="header-right">
      <div class="header-brand">C.A.S.H. REPORT · BY GMG</div>
      <div class="header-meta">Guerrilla Marketing Group · {_h(date_str)}</div>
    </div>
  </div>"""


def _ftr(n: int) -> str:
    return f"""
  <div class="page-footer">
    <span>C.A.S.H. REPORT BY GMG</span>
    <span>Page {n} · Confidential</span>
    <span class="pf-email">gmg@goguerrilla.xyz</span>
  </div>"""


def _pg(n: int, body: str, date_str: str, logo_src: str) -> str:
    return (f'<div class="page"><div class="bg-grid"></div>'
            f'{_hdr(n, date_str, logo_src)}'
            f'<div class="body">{body}</div>'
            f'{_ftr(n)}</div>')


def _score_dot(score) -> str:
    s = int(score) if score is not None else 0
    if s >= 65: return _sdot("g")
    if s >= 50: return _sdot("b")
    if s >= 35: return _sdot("y")
    return _sdot("r")


def _score_badge(score) -> str:
    s = int(score) if score is not None else 0
    if s >= 80: return _sbadge("ok", "Excellent")
    if s >= 65: return _sbadge("good", "Good")
    if s >= 50: return _sbadge("warn", "Needs Work")
    return _sbadge("critical", "Critical")


def _letter_color(letter: str) -> str:
    return {"C": "#27AE60", "A": "#00AEEF", "S": "#E74C3C", "H": "#E74C3C"}.get(letter, "#00AEEF")


# ── Main Generator ────────────────────────────────────────────────────────────

class PDFReportGenerator:
    def __init__(self, config: ClientConfig, audit_data: Dict[str, Any]):
        self.config    = config
        self.data      = audit_data
        self.ai        = audit_data.get("ai_insights", {})
        self.date_str  = datetime.now().strftime("%B %d, %Y")
        self.cash      = self._resolve_cash()
        self.logo_src  = _LOGO_SRC

    # ── Public entry point ─────────────────────────────────────────────────────

    def generate(self, output_path: str):
        import logging as _logging
        _log = _logging.getLogger("webhook")
        os.makedirs(os.path.dirname(os.path.abspath(output_path)) or ".", exist_ok=True)

        # Point Playwright at the browser installed to the app directory during build.
        # On Railway, /root/.cache is NOT persisted — browsers must live under /app/.
        if not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/app/ms-playwright"
        _log.info("PDF: PLAYWRIGHT_BROWSERS_PATH=%s", os.environ.get("PLAYWRIGHT_BROWSERS_PATH"))

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright is required for the enhanced PDF template. "
                "Install with:  pip install playwright && playwright install chromium"
            )

        html_content = self._build_html()
        tmp = tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w", encoding="utf-8")
        tmp.write(html_content)
        tmp.close()
        _log.info("PDF: HTML written to %s (%d bytes)", tmp.name, os.path.getsize(tmp.name))
        try:
            with sync_playwright() as pw:
                # --no-sandbox is required when running as root in Docker/Railway containers
                browser = pw.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
                _log.info("PDF: Chromium launched OK")
                page = browser.new_page()
                page.goto(f"file://{tmp.name}", wait_until="networkidle", timeout=60_000)
                page.wait_for_timeout(500)
                page.pdf(path=output_path, width="794px", height="1123px",
                         print_background=True,
                         margin={"top": "0", "right": "0", "bottom": "0", "left": "0"})
                browser.close()
                _log.info("PDF: rendered and saved to %s", output_path)
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    # ── CASH score resolution ──────────────────────────────────────────────────

    def _resolve_cash(self) -> Dict[str, Any]:
        ai = self.ai
        if ai.get("cash_c_score"):
            return {
                "C":       ai.get("cash_c_score", 50),
                "A":       ai.get("cash_a_score", 50),
                "S":       ai.get("cash_s_score", 50),
                "H":       ai.get("cash_h_score", 50),
                "overall": ai.get("overall_score", 50),
            }
        cs = ai.get("component_scores", {})
        overall = ai.get("overall_score",
                         round(sum([cs.get("C", 50), cs.get("A", 50),
                                    cs.get("S", 50), cs.get("H", 50)]) / 4))
        return {"C": cs.get("C", 50), "A": cs.get("A", 50),
                "S": cs.get("S", 50), "H": cs.get("H", 50), "overall": overall}

    # ── HTML builder ──────────────────────────────────────────────────────────

    def _build_html(self) -> str:
        pages = "".join([
            self._page_cover(),
            self._page_framework(),
            self._page_scorecard(),
            self._page_executive(),
            self._page_content(),
            self._page_audience(),
            self._page_sales(),
            self._page_hold(),
            self._page_geo(),
            self._page_gbp_competitive(),
            self._page_action_plan(),
            self._page_cta(),
        ])
        return (f'<!DOCTYPE html><html><head><meta charset="UTF-8">'
                f'<style>{CSS}</style></head><body>{pages}</body></html>')

    # ── Reusable fragments ─────────────────────────────────────────────────────

    def _score_strip(self) -> str:
        items = [("C", "Content",  self.cash["C"]),
                 ("A", "Audience", self.cash["A"]),
                 ("S", "Sales",    self.cash["S"]),
                 ("H", "Hold",     self.cash["H"])]
        cards = ""
        for letter, label, sc in items:
            g    = _grade(sc)
            gcls = _gc(g)
            cards += (f'<div class="score-card">'
                      f'<div class="sc-letter" style="color:{_letter_color(letter)}">{letter}</div>'
                      f'<div class="sc-label">{label}</div>'
                      f'<div class="sc-score">{sc}/100</div>'
                      f'<div class="sc-grade {gcls}">{g}</div>'
                      f'</div>')
        return f'<div class="score-grid">{cards}</div>'

    def _overall_banner(self) -> str:
        score = self.cash["overall"]
        g     = self.ai.get("overall_grade", _grade(score))
        desc  = (self.ai.get("executive_summary", "") or "")[:120].rstrip(".")
        return (f'<div class="overall-score-banner">'
                f'<div class="osb-grade">'
                f'<div class="osb-letter">{_h(g)}</div>'
                f'<div class="osb-label">OVERALL</div>'
                f'</div>'
                f'<div class="osb-info">'
                f'<div class="osb-stitle">OVERALL C.A.S.H. SCORE</div>'
                f'<div class="osb-score">{score}<span>/100</span></div>'
                f'<div class="osb-desc">{_h(desc)}</div>'
                f'</div>'
                f'</div>')

    # ── PAGE 1: Cover ──────────────────────────────────────────────────────────

    def _page_cover(self) -> str:
        cfg = self.config
        body = (
            f'<div class="cover-logo-wrap">'
            f'<img class="cover-logo-img" src="{self.logo_src}" alt="GMG"/></div>'
            f'<div class="cover-title-box"><h1>C.A.S.H. REPORT</h1></div>'
            f'<div class="cover-company">{_h(cfg.client_name)}</div>'
            f'<div class="cover-sub">Marketing Audit Report</div>'
            f'<div class="cover-divider"></div>'
            f'{self._overall_banner()}'
            f'{self._score_strip()}'
            f'<div class="cover-meta">'
            f'<div><strong>Prepared by:</strong> {_h(cfg.agency_name)}</div>'
            f'<div><strong>Date:</strong> {_h(self.date_str)}</div>'
            f'<div><strong>Website:</strong> {_h(cfg.website_url or "—")}</div>'
            f'<div><strong>Industry:</strong> {_h(cfg.industry_category or cfg.client_industry or "—")}</div>'
            f'</div>'
        )
        return _pg(1, body, self.date_str, self.logo_src)

    # ── PAGE 2: Framework ─────────────────────────────────────────────────────

    def _page_framework(self) -> str:
        body = (
            f'<div class="page-title">Know Exactly Where Your Marketing Is '
            f'<span>Winning</span> — and Losing</div>'
            f'<div class="text-body">A data-driven audit of your entire online presence — '
            f'scored, benchmarked, and delivered with a clear 90-day growth plan.</div>'
            f'<div class="text-body">The C.A.S.H. Report is GMG\'s proprietary marketing '
            f'intelligence system, evaluating your brand across four core pillars — Content, '
            f'Audience, Sales, and Hold — while identifying funnel gaps and delivering a '
            f'focused, prioritized 90-day action plan.</div>'
            f'{_sub("The C.A.S.H. Framework")}'
            f'<div class="framework-items">'
            f'<div class="fw-item"><div class="fw-letter">C</div><div>'
            f'<div class="fw-name">Content</div>'
            f'<div class="fw-desc">Website, SEO, brand consistency, content freshness — how strong is your foundation?</div>'
            f'</div></div>'
            f'<div class="fw-item"><div class="fw-letter">A</div><div>'
            f'<div class="fw-name">Audience</div>'
            f'<div class="fw-desc">ICP alignment, platform fit, social presence — are you reaching the right people?</div>'
            f'</div></div>'
            f'<div class="fw-item"><div class="fw-letter">S</div><div>'
            f'<div class="fw-name">Sales</div>'
            f'<div class="fw-desc">Lead capture, funnel quality, GBP, conversion — how effectively do you convert attention?</div>'
            f'</div></div>'
            f'<div class="fw-item"><div class="fw-letter">H</div><div>'
            f'<div class="fw-name">Hold</div>'
            f'<div class="fw-desc">Retention, referrals, email nurture, trust signals — how well do you keep and grow clients?</div>'
            f'</div></div>'
            f'</div>'
            f'{_sub("What This Report Delivers")}'
            f'<div class="deliverables">'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>C.A.S.H. score across 4 pillars with component breakdown</div>'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>15+ pages of detailed analysis across every channel</div>'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>Industry-calibrated benchmarks for your business type</div>'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>Competitor side-by-side comparison (SEO, social, website)</div>'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>GEO visibility score — how you rank in AI-generated answers</div>'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>Personalised 90-day action plan with prioritised quick wins</div>'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>Google Business Profile audit with NAP consistency check</div>'
            f'<div class="deliv-item"><span class="deliv-check">✓</span>Content freshness and posting frequency by platform</div>'
            f'</div>'
            f'<div class="info-box" style="text-align:center;font-size:12px">'
            f'Guerrilla Marketing Group · gmg@goguerrilla.xyz · Fractional CMO Services</div>'
        )
        return _pg(2, body, self.date_str, self.logo_src)

    # ── PAGE 3: Scorecard ─────────────────────────────────────────────────────

    def _page_scorecard(self) -> str:
        web   = self.data.get("website", {}).get("scores", {})
        seo   = self.data.get("seo", {})
        geo   = self.data.get("geo", {})
        gbp   = self.data.get("gbp", {})
        icp   = self.data.get("icp", {})
        brand = self.data.get("brand", {})
        fresh = self.data.get("freshness", {})
        funnel = self.data.get("funnel", {})

        rows = [
            ("ICP / Audience Alignment",  icp.get("score", 50),      "A"),
            ("Brand Consistency",         brand.get("score", 50),     "A"),
            ("Content Freshness",         fresh.get("score", 50),     "C"),
            ("SEO Health",                seo.get("score", 50),        "C"),
            ("Website Technical",         web.get("technical", 50),    "C"),
            ("Website Conversion",        web.get("conversion", 50),   "S"),
            ("Lead Capture / Funnel",     self.cash["S"],               "S"),
            ("Retention / Hold Systems",  self.cash["H"],               "H"),
            ("GEO Visibility",            geo.get("score", 50),        "GEO"),
            ("Google Business Profile",   gbp.get("score", 50),        "GEO"),
        ]

        tbody = ""
        for label, sc, pillar in rows:
            g    = _grade(sc)
            gcls = _gc(g)
            tbody += (f'<tr>'
                      f'<td class="td-name">{_h(label)}</td>'
                      f'<td>{sc}/100</td>'
                      f'<td>{_h(pillar)}</td>'
                      f'<td><span class="sc-grade {gcls}">{g}</span></td>'
                      f'<td>{_score_dot(sc)}{_score_badge(sc)}</td>'
                      f'</tr>')

        comp_table = (
            f'<table class="data-table">'
            f'<thead><tr><th>Component</th><th>Score</th><th>Pillar</th>'
            f'<th>Grade</th><th>Status</th></tr></thead>'
            f'<tbody>{tbody}</tbody></table>'
        )

        body = (
            f'<div class="page-title">C.A.S.H. <span>Score Overview</span></div>'
            f'{self._score_strip()}'
            f'{self._overall_banner()}'
            f'{_sub("Component Breakdown")}'
            f'{comp_table}'
        )
        return _pg(3, body, self.date_str, self.logo_src)

    # ── PAGE 4: Executive Summary ─────────────────────────────────────────────

    def _page_executive(self) -> str:
        ai    = self.ai
        score = self.cash["overall"]
        g     = ai.get("overall_grade", _grade(score))
        exec_sum  = ai.get("executive_summary", "")
        opp       = ai.get("biggest_opportunity", "")
        waste     = ai.get("biggest_waste", "")
        priorities = ai.get("top_3_priorities", [])
        strategy  = ai.get("channel_recommendation", "")

        body = (
            f'<div class="page-title"><span>Executive</span> Summary</div>'
            f'<div class="text-body">{_h(exec_sum)}</div>'
        )

        if opp:
            body += (
                f'<div class="callout-cyan">'
                f'<div class="callout-label">⚡ Biggest Opportunity</div>'
                f'<div class="callout-body">{_h(opp)}</div>'
                f'</div>'
            )

        if waste:
            body += (
                f'<div class="callout-red">'
                f'<div class="callout-label">⚠ Biggest Waste</div>'
                f'<div class="callout-body">{_h(waste)}</div>'
                f'</div>'
            )

        if priorities:
            body += _sub("Top 3 Priorities")
            body += '<div class="priority-rows">'
            pr_cls_map = {"CRITICAL": "pr-critical", "HIGH": "pr-high", "MEDIUM": "pr-medium"}
            for i, p in enumerate(priorities[:3]):
                action   = p.get("action", "")
                impact   = p.get("impact", "")
                timeline = p.get("timeline", "")
                prio     = str(p.get("priority", i + 1))
                lv       = ("CRITICAL" if i == 0 else "HIGH" if i == 1 else "MEDIUM")
                pr_cls   = pr_cls_map.get(lv, "pr-medium")
                body += (
                    f'<div class="priority-row {pr_cls}">'
                    f'<div class="pr-num">0{i+1}</div>'
                    f'<div class="pr-content">'
                    f'<div class="pr-tag-line">{_ptag(lv)}</div>'
                    f'<div class="pr-heading">{_h(action)}</div>'
                    f'<div class="pr-desc">{_h(impact)}</div>'
                    f'<div class="pr-meta">'
                    f'<span class="pr-meta-tag">⏱ {_h(timeline)}</span>'
                    f'</div>'
                    f'</div></div>'
                )
            body += '</div>'

        if strategy:
            body += f'<div class="strategy-box">{_h(strategy)}</div>'

        return _pg(4, body, self.date_str, self.logo_src)

    # ── PAGE 5: Content (C) ───────────────────────────────────────────────────

    def _page_content(self) -> str:
        seo   = self.data.get("seo", {})
        web   = self.data.get("website", {})
        brand = self.data.get("brand", {})
        fresh = self.data.get("freshness", {})
        ai    = self.ai

        c_score = self.cash["C"]
        c_grade = _grade(c_score)

        seo_score = seo.get("score", 50)
        web_scores = web.get("scores", {})

        # SEO check rows
        def _tri(val):
            if val is True:  return f'{_sdot("g")}Pass'
            if val is False: return f'{_sdot("r")}Fail'
            return "— N/A"

        sig = seo.get("crawl_signals", {})
        seo_fields = "".join([
            _field("robots.txt",       _tri(seo.get("robots_txt", {}).get("exists"))),
            _field("XML sitemap",      _tri(seo.get("sitemap", {}).get("found"))),
            _field("Canonical tag",    _tri(seo.get("canonical", {}).get("present"))),
            _field("Page indexable",   _tri(sig.get("is_indexable", True) if sig else None)),
            _field("Open Graph tags",  _tri(seo.get("open_graph", {}).get("present"))),
            _field("Twitter Card tags", _tri(sig.get("has_twitter_card") if sig else None)),
        ])

        # Website meta
        dq = web.get("data_quality", {})
        rel_score = dq.get("reliability_score", "—")
        web_fields = "".join([
            _field("URL",      _h(web.get("url", self.config.website_url or "—"))),
            _field("HTTPS",    f'{_sdot("g")}Yes' if web.get("https_enabled") else f'{_sdot("r")}No'),
            _field("Load Time", _h(f'{web.get("load_time_seconds", "?")}s')),
            _field("Platform",  _h((web.get("platform") or "Unknown").title())),
            _field("Data Reliability", f'<span class="td-good">{rel_score}/100 — High reliability</span>'
                   if isinstance(rel_score, int) and rel_score >= 80 else _h(str(rel_score))),
        ])

        # Freshness channels table
        channels = fresh.get("channels", {})
        freshness_tbody = ""
        for platform, d in channels.items():
            status_raw = d.get("status", "unknown")
            ppw  = d.get("posts_per_week")
            days = d.get("days_since_last_post")
            if status_raw in ("fresh", "recent"):
                dot, badge = _sdot("g"), _sbadge("ok", "Fresh")
            elif status_raw == "stale":
                dot, badge = _sdot("y"), _sbadge("warn", "Stale")
            elif status_raw == "dead":
                dot, badge = _sdot("r"), _sbadge("critical", "Inactive")
            else:
                dot, badge = _sdot("gray"), _sbadge("gray", "API Required")
            ppw_str  = f'<span class="td-good">{ppw}</span>'  if ppw  else "—"
            days_str = f'<span class="td-good">{days}</span>' if days is not None else "—"
            freshness_tbody += (f'<tr>'
                                f'<td class="td-name">{_h(platform)}</td>'
                                f'<td>{dot}{badge}</td>'
                                f'<td>{ppw_str}</td>'
                                f'<td>{days_str}</td>'
                                f'</tr>')

        freshness_table = (
            f'<table class="data-table">'
            f'<thead><tr><th>Platform</th><th>Status</th>'
            f'<th>Posts/Week</th><th>Days Since Post</th></tr></thead>'
            f'<tbody>{freshness_tbody}</tbody></table>'
        ) if freshness_tbody else ""

        content_strategy = ai.get("content_strategy", "")

        body = (
            f'{_section_hdr("C", "Content", "How fresh, consistent, and strategically distributed is the content?", c_score, c_grade)}'
            f'{_sub(f"SEO Health — {seo_score}/100 ({_grade(seo_score)})")}'
            f'{_split_table(seo.get("issues", []), seo.get("strengths", []))}'
            f'{_sub("SEO Check Status")}'
            f'<table class="field-table"><tbody>{seo_fields}</tbody></table>'
            f'{_sub("Website Audit — Technical: " + str(web_scores.get("technical", 50)) + "/100")}'
            f'<table class="field-table"><tbody>{web_fields}</tbody></table>'
            f'{_split_table(web.get("issues", []), web.get("strengths", []))}'
            f'{_sub("Brand Consistency — Score: " + str(brand.get("score", 50)) + "/100")}'
            f'{_split_table(brand.get("issues", []), brand.get("strengths", []))}'
            f'{_sub("Content Freshness — Score: " + str(fresh.get("score", 50)) + "/100")}'
            f'{freshness_table}'
        )
        if content_strategy:
            body += (
                f'<div class="callout-cyan">'
                f'<div class="callout-label">Content Strategy</div>'
                f'<div class="callout-body">{_h(content_strategy)}</div>'
                f'</div>'
            )

        return _pg(5, body, self.date_str, self.logo_src)

    # ── PAGE 6: Audience (A) ──────────────────────────────────────────────────

    def _page_audience(self) -> str:
        icp   = self.data.get("icp", {})
        brand = self.data.get("brand", {})
        ai    = self.ai

        a_score = self.cash["A"]
        a_grade = _grade(a_score)

        # Platform fit table
        pf = brand.get("platform_fit", {})
        ps = pf.get("platform_scores", {})
        high_fit = pf.get("high_fit", [])
        med_fit  = pf.get("medium_fit", [])

        plat_tbody = ""
        for p, sc in sorted(ps.items(), key=lambda x: -x[1]):
            if p in high_fit:
                dot, rec = _sdot("g"), "Prioritize"
            elif p in med_fit:
                dot, rec = _sdot("y"), "Use selectively"
            else:
                dot, rec = _sdot("r"), "Deprioritize"
            plat_tbody += (f'<tr>'
                           f'<td class="td-name">{_h(p)}</td>'
                           f'<td class="{"td-good" if sc >= 65 else "td-warn" if sc >= 40 else "td-bad"}">{sc}/100</td>'
                           f'<td>{dot}{_h(rec)}</td>'
                           f'</tr>')

        plat_table = (
            f'<table class="data-table">'
            f'<thead><tr><th>Platform</th><th>Fit Score</th><th>Recommendation</th></tr></thead>'
            f'<tbody>{plat_tbody}</tbody></table>'
        ) if plat_tbody else ""

        # ICP recommendations
        recs = icp.get("recommendations", [])
        rec_rows = "".join(
            _rec_row(r.get("priority", "MEDIUM"),
                     r.get("action", ""),
                     r.get("detail", ""),
                     r.get("timeline", ""))
            for r in recs[:6]
        )

        strategy = ai.get("channel_recommendation", "")

        body = (
            f'{_section_hdr("A", "Audience", "Are you reaching the right people on the right platforms?", a_score, a_grade)}'
        )

        if plat_table:
            body += f'{_sub("Platform Fit for Target Market")}{plat_table}'

        all_issues    = icp.get("issues", []) + brand.get("issues", [])
        all_strengths = icp.get("strengths", []) + brand.get("strengths", [])
        body += _split_table(all_issues, all_strengths)

        if rec_rows:
            body += _sub("Audience & ICP Recommendations")
            body += _rec_table(rec_rows)

        # ── Social Media Snapshot ──────────────────────────────
        ch_data    = self.config.preloaded_channel_data
        yt_metrics = self.data.get("content", {}).get("youtube_metrics") or {}
        fresh_ch   = self.data.get("freshness", {}).get("channels", {})
        meta_token = bool(getattr(self.config, "meta_page_access_token", None))

        def _fmt_count(val):
            return f"{int(val):,}" if val is not None else "—"

        def _fmt_ppw(val):
            return str(val) if val is not None else "—"

        def _fmt_days(val):
            return str(val) if val is not None else "—"

        def _active_badge(ppw, days, pending=False):
            if pending:
                return _sbadge("gray", "Pending API")
            if ppw is not None and ppw >= 1:
                return f'{_sdot("g")}{_sbadge("ok", "Yes")}'
            if ppw is not None and ppw > 0:
                return f'{_sdot("y")}{_sbadge("warn", "Low")}'
            return f'{_sdot("r")}{_sbadge("critical", "Inactive")}'

        snap_rows = []

        # LinkedIn
        if self.config.linkedin_url:
            li = ch_data.get("linkedin", {})
            snap_rows.append((
                "LinkedIn",
                _fmt_count(li.get("followers")),
                _fmt_ppw(li.get("posts_per_week")),
                _fmt_days(li.get("days_since_last_post")
                          or fresh_ch.get("LinkedIn", {}).get("days_since_last_post")),
                _active_badge(li.get("posts_per_week"),
                              li.get("days_since_last_post")),
            ))

        # YouTube
        if self.config.youtube_channel_url:
            snap_rows.append((
                "YouTube",
                _fmt_count(yt_metrics.get("subscriber_count")),
                _fmt_ppw(yt_metrics.get("posts_per_week")),
                _fmt_days(yt_metrics.get("days_since_last_post")),
                _active_badge(yt_metrics.get("posts_per_week"),
                              yt_metrics.get("days_since_last_post")),
            ))

        # Facebook
        if self.config.facebook_page_url:
            if meta_token:
                fb = ch_data.get("facebook", {})
                snap_rows.append((
                    "Facebook",
                    _fmt_count(fb.get("followers")),
                    _fmt_ppw(fb.get("posts_per_week")),
                    _fmt_days(fb.get("days_since_last_post")
                              or fresh_ch.get("Facebook", {}).get("days_since_last_post")),
                    _active_badge(fb.get("posts_per_week"),
                                  fb.get("days_since_last_post")),
                ))
            else:
                snap_rows.append(("Facebook", "—", "—", "—",
                                  _sbadge("gray", "Pending API")))

        # Instagram
        if self.config.instagram_handle:
            if meta_token:
                ig = ch_data.get("instagram", {})
                snap_rows.append((
                    "Instagram",
                    _fmt_count(ig.get("followers")),
                    _fmt_ppw(ig.get("posts_per_week")),
                    _fmt_days(ig.get("days_since_last_post")
                              or fresh_ch.get("Instagram", {}).get("days_since_last_post")),
                    _active_badge(ig.get("posts_per_week"),
                                  ig.get("days_since_last_post")),
                ))
            else:
                snap_rows.append(("Instagram", "—", "—", "—",
                                  _sbadge("gray", "Pending API")))

        if snap_rows:
            snap_tbody = "".join(
                f'<tr>'
                f'<td class="td-name">{_h(r[0])}</td>'
                f'<td>{r[1]}</td>'
                f'<td>{r[2]}</td>'
                f'<td>{r[3]}</td>'
                f'<td>{r[4]}</td>'
                f'</tr>'
                for r in snap_rows
            )
            snap_table = (
                f'<table class="data-table">'
                f'<thead><tr>'
                f'<th>Platform</th><th>Followers / Subscribers</th>'
                f'<th>Posts/Week</th><th>Days Since Post</th><th>Active</th>'
                f'</tr></thead>'
                f'<tbody>{snap_tbody}</tbody></table>'
            )
            body += f'{_sub("Social Media Snapshot")}{snap_table}'

        if strategy:
            body += f'<div class="strategy-box">{_h(strategy)}</div>'

        return _pg(6, body, self.date_str, self.logo_src)

    # ── PAGE 7: Sales (S) ─────────────────────────────────────────────────────

    def _page_sales(self) -> str:
        funnel = self.data.get("funnel", {})
        web    = self.data.get("website", {})
        ai     = self.ai

        s_score = self.cash["S"]
        s_grade = _grade(s_score)

        stages = funnel.get("stages", {})

        # Funnel stage summary table
        stage_tbody = ""
        for key, label in [("awareness", "Awareness"), ("capture", "Lead Capture"), ("conversion", "Conversion")]:
            st = stages.get(key, {})
            n_crit = len([i for i in st.get("issues", []) if "🔴" in i])
            n_str  = len(st.get("strengths", []))
            if n_crit >= 2:
                dot, badge = _sdot("r"), _sbadge("critical", "Critical")
            elif n_crit == 1:
                dot, badge = _sdot("y"), _sbadge("warn", "Needs Work")
            else:
                dot, badge = _sdot("g"), _sbadge("ok", "OK")
            stage_tbody += (f'<tr><td class="td-name">{_h(label)}</td>'
                            f'<td>{n_crit}</td><td>{n_str}</td>'
                            f'<td>{dot}{badge}</td></tr>')

        stage_table = (
            f'<table class="data-table">'
            f'<thead><tr><th>Stage</th><th>Critical Issues</th>'
            f'<th>Strengths</th><th>Status</th></tr></thead>'
            f'<tbody>{stage_tbody}</tbody></table>'
        ) if stage_tbody else ""

        cap = stages.get("capture", {})
        con = stages.get("conversion", {})
        all_issues    = cap.get("issues", []) + con.get("issues", []) + web.get("issues", [])
        all_strengths = cap.get("strengths", []) + con.get("strengths", []) + web.get("strengths", [])

        # Funnel recommendations
        recs = funnel.get("recommendations", [])
        rec_rows = "".join(
            _rec_row(r.get("priority", "MEDIUM"),
                     r.get("action", ""),
                     r.get("example", r.get("detail", "")),
                     r.get("timeline", ""))
            for r in recs[:6]
        )

        budget_rec = ai.get("budget_recommendation", "")

        body = (
            f'{_section_hdr("S", "Sales", "How effectively does the brand convert attention into qualified leads?", s_score, s_grade)}'
            f'{_sub("Funnel Stage Analysis")}'
            f'{stage_table}'
            f'{_split_table(all_issues, all_strengths)}'
        )

        if rec_rows:
            body += f'{_sub("Sales Funnel Recommendations")}{_rec_table(rec_rows)}'

        if budget_rec:
            body += f'<div class="strategy-box">{_h(budget_rec)}</div>'

        return _pg(7, body, self.date_str, self.logo_src)

    # ── PAGE 8: Hold (H) ─────────────────────────────────────────────────────

    def _page_hold(self) -> str:
        funnel    = self.data.get("funnel", {})
        analytics = self.data.get("analytics", {})
        cfg       = self.config

        h_score = self.cash["H"]
        h_grade = _grade(h_score)

        stages  = funnel.get("stages", {})
        nurture = stages.get("nurture", {})
        trust   = stages.get("trust",   {})

        retention_fields = "".join([
            _field("Email Newsletter",
                   f'{_sdot("g")}Active' if cfg.has_active_newsletter
                   else f'<span class="td-bad">{_sdot("r")}Not found</span>'),
            _field("Email List Size",
                   f'{cfg.email_list_size:,} contacts' if cfg.email_list_size > 0
                   else f'<span class="td-bad">{_sdot("r")}None reported</span>'),
            _field("Referral System",
                   f'{_sdot("g")}{_h(cfg.referral_system_description or "Yes")}' if cfg.has_referral_system
                   else f'<span class="td-bad">{_sdot("r")}None detected</span>'),
        ])

        body = (
            f'{_section_hdr("H", "Hold", "Are systems in place to retain clients and generate referrals?", h_score, h_grade)}'
            f'{_sub("Retention System Status")}'
            f'<table class="field-table"><tbody>{retention_fields}</tbody></table>'
        )

        # Analytics
        if analytics and analytics.get("data_source") == "google_analytics_data_api_v4":
            visitors = analytics.get("monthly_visitors", 0)
            trend    = analytics.get("traffic_trend_label", "—")
            trend_pct = analytics.get("traffic_trend_pct")
            bounce   = analytics.get("bounce_rate_pct")
            dur      = analytics.get("avg_session_duration", "—")
            sources  = analytics.get("top_traffic_sources", [])

            # Traffic trend color
            trend_cls = "td-good" if (trend_pct or 0) > 0 else "td-warn"

            ga_tbody = (
                f'<tr><td class="td-name">Monthly Visitors</td><td>{visitors:,}</td>'
                f'<td class="td-name">Direct</td>'
                f'<td>{sources[0].get("sessions", "—") if sources else "—"}</td></tr>'
                f'<tr><td class="td-name">Traffic Trend</td>'
                f'<td class="{trend_cls}">{_h(trend)}</td>'
                f'<td class="td-name">Organic Search</td>'
                f'<td>{sources[1].get("sessions", "—") if len(sources) > 1 else "—"}</td></tr>'
                f'<tr><td class="td-name">Bounce Rate</td>'
                f'<td class="{"td-warn" if bounce and bounce > 50 else "td-good"}">'
                f'{bounce}%</td>'
                f'<td class="td-name">Referral</td>'
                f'<td>{sources[2].get("sessions", "—") if len(sources) > 2 else "—"}</td></tr>'
                f'<tr><td class="td-name">Avg Session Duration</td>'
                f'<td class="td-good">{_h(str(dur))}</td>'
                f'<td class="td-name">Organic Social</td>'
                f'<td>{sources[3].get("sessions", "—") if len(sources) > 3 else "—"}</td></tr>'
            ) if visitors else ""

            if ga_tbody:
                body += (
                    f'{_sub("Website Traffic (GA4)")}'
                    f'<table class="data-table">'
                    f'<thead><tr><th>Metric</th><th>Value</th>'
                    f'<th>Channel</th><th>Sessions</th></tr></thead>'
                    f'<tbody>{ga_tbody}</tbody></table>'
                )

        body += _split_table(
            nurture.get("issues", []) + trust.get("issues", []),
            nurture.get("strengths", []) + trust.get("strengths", []),
        )

        # Standard hold recommendations
        hold_rec_rows = "".join([
            _rec_row("HIGH",   "Build a 5-email welcome sequence",
                     "Introduce agency, share case study, book discovery call.", "2 weeks"),
            _rec_row("HIGH",   "Launch a client referral program",
                     "Offer 1 free month for every referred client who signs.", "1 month"),
            _rec_row("MEDIUM", "Biweekly email newsletter",
                     "Compliance-safe tips, case studies, industry news.", "2–4 weeks"),
            _rec_row("MEDIUM", "Collect testimonials from every client",
                     "2–3 sentence quote + permission for website and LinkedIn.", "2 weeks"),
        ])
        body += f'{_sub("Retention Recommendations")}{_rec_table(hold_rec_rows)}'

        return _pg(8, body, self.date_str, self.logo_src)

    # ── PAGE 9: GEO ───────────────────────────────────────────────────────────

    def _page_geo(self) -> str:
        geo = self.data.get("geo", {})
        seo = self.data.get("seo", {})

        geo_score = geo.get("score", 50)
        geo_grade = _grade(geo_score)

        # Component scores
        comps = geo.get("components", {})
        weights = {
            "SERP Visibility":   "20%",
            "On-page SEO":       "15%",
            "Schema Markup":     "15%",
            "FAQ / Q&A Content": "15%",
            "E-E-A-T Signals":   "15%",
            "Brand Authority":   "15%",
            "AI Citation Score": "5%",
        }
        comp_tbody = ""
        for name, sc in comps.items():
            g    = _grade(sc)
            gcls = _gc(g)
            sc_cls = ("td-good" if sc >= 65 else "td-warn" if sc >= 50 else "td-bad")
            comp_tbody += (f'<tr>'
                           f'<td class="td-name">{_h(name)}</td>'
                           f'<td>{weights.get(name, "—")}</td>'
                           f'<td class="{sc_cls}">{sc}/100</td>'
                           f'<td><span class="sc-grade {gcls}">{g}</span></td>'
                           f'</tr>')

        comp_table = (
            f'<table class="data-table">'
            f'<thead><tr><th>Component</th><th>Weight</th>'
            f'<th>Score</th><th>Grade</th></tr></thead>'
            f'<tbody>{comp_tbody}</tbody></table>'
        ) if comp_tbody else ""

        # On-page SEO details
        op = geo.get("onpage_detail", {})
        title_val = op.get("title", "") if op else ""
        meta_val  = op.get("meta_description", "") if op else ""
        h1s       = op.get("h1s", []) if op else []
        wc        = op.get("word_count", 0) if op else 0

        op_fields = "".join([
            _field("Title Tag",
                   f'<span class="td-bad">{_sdot("r")}MISSING</span>' if not title_val
                   else _h(title_val[:80])),
            _field("Meta Description",
                   f'<span class="td-bad">{_sdot("r")}MISSING</span>' if not meta_val
                   else _h(meta_val[:80])),
            _field("H1 Tag(s)",
                   f'<span class="td-bad">{_sdot("r")}MISSING</span>' if not h1s
                   else _h(h1s[0][:60])),
            _field("FAQPage Schema",
                   f'{_sdot("g")}Present' if op.get("has_faq_schema") else f'<span class="td-bad">{_sdot("r")}Missing</span>'),
            _field("Homepage Word Count",
                   f'<span class="td-good">{wc:,} words</span>' if wc else "—"),
            _field("Indexability",
                   f'{_sdot("g")}<span class="td-good">Indexable</span>'
                   if not seo.get("crawl_signals", {}).get("is_noindex") else f'{_sdot("r")}No-indexed'),
        ])

        # AI platform notes
        platform_notes = geo.get("platform_notes", {})
        ai_rows = ""
        for platform, note in [
            ("ChatGPT",           platform_notes.get("ChatGPT", "Likelihood depends on schema, FAQ, and E-E-A-T signals.")),
            ("Google AI Overview", platform_notes.get("Google AI Overview", "Eligibility requires Organization + FAQPage schema and E-E-A-T trust signals.")),
            ("Perplexity",        platform_notes.get("Perplexity", "Citation likelihood increases with case studies and guest posts.")),
            ("Search Console",    platform_notes.get("Search Console", "Connect Google Search Console to unlock query and ranking data.")),
        ]:
            dot_color = "r" if "unlikely" in note.lower() or "low" in note.lower() or "not yet" in note.lower() else "gray"
            ai_rows += (f'<tr>'
                        f'<td class="ai-platform">{_sdot(dot_color)}{_h(platform)}</td>'
                        f'<td>{_h(note)}</td>'
                        f'</tr>')

        # GEO recommendations
        geo_recs = geo.get("recommendations", [])
        geo_rec_rows = "".join(
            _rec_row(r.get("priority", "MEDIUM"),
                     r.get("action", ""),
                     r.get("impact", r.get("detail", "")),
                     r.get("timeline", ""))
            for r in geo_recs[:6]
        )

        body = (
            f'{_section_hdr("GEO", "Generative Engine Optimisation", "SERP rankings · on-page keyword optimisation · AI visibility scoring", geo_score, geo_grade)}'
            f'{_sub("GEO Component Scores")}'
            f'{comp_table}'
            f'{_sub("On-page SEO Analysis")}'
            f'<table class="field-table"><tbody>{op_fields}</tbody></table>'
            f'{_split_table(geo.get("issues", []), geo.get("strengths", []))}'
            f'{_sub("AI Platform Visibility Forecast")}'
            f'<table class="ai-table"><tbody>{ai_rows}</tbody></table>'
        )
        if geo_rec_rows:
            body += f'{_sub("GEO Recommendations")}{_rec_table(geo_rec_rows)}'

        return _pg(9, body, self.date_str, self.logo_src)

    # ── PAGE 10: GBP + Competitive ────────────────────────────────────────────

    def _page_gbp_competitive(self) -> str:
        gbp  = self.data.get("gbp", {})
        comp = self.data.get("competitor", {})

        gbp_score = gbp.get("score", 50)
        gbp_grade = _grade(gbp_score)

        # GBP grid
        gbp_name     = _h(gbp.get("business_name", self.config.client_name))
        gbp_address  = _h(gbp.get("address") or "—")
        gbp_phone    = _h(str(gbp.get("phone") or "—"))
        _gbp_rc      = gbp.get("review_count", 0) or 0
        gbp_reviews  = (f"~{_gbp_rc} (estimated)"
                        if _gbp_rc and not gbp.get("review_count_verified", True)
                        else str(_gbp_rc) if _gbp_rc else "0")
        gbp_hrs      = gbp.get("hours_listed", False)
        gbp_verified = gbp.get("is_likely_verified", gbp.get("found", False))
        gbp_nap      = gbp.get("nap_consistent", False)
        gbp_complete = gbp.get("completeness_pct", 0) or 0

        gbp_grid = (
            f'<div class="gbp-grid">'
            f'<div class="gbp-field"><div class="gbp-field-label">Business Name</div>'
            f'<div class="gbp-field-value">{gbp_name}</div></div>'
            f'<div class="gbp-field"><div class="gbp-field-label">Address</div>'
            f'<div class="gbp-field-value">{gbp_address}</div></div>'
            f'<div class="gbp-field"><div class="gbp-field-label">Phone</div>'
            f'<div class="gbp-field-value">{gbp_phone}</div></div>'
            f'<div class="gbp-field"><div class="gbp-field-label">Reviews</div>'
            f'<div class="gbp-field-value td-good">{gbp_reviews}</div></div>'
            f'<div class="gbp-field"><div class="gbp-field-label">Hours Visible</div>'
            f'<div class="gbp-field-value {"td-good" if gbp_hrs else "td-bad"}">'
            f'{_sdot("g") if gbp_hrs else _sdot("r")}{"Yes" if gbp_hrs else "No"}</div></div>'
            f'<div class="gbp-field"><div class="gbp-field-label">Verified</div>'
            f'<div class="gbp-field-value {"td-good" if gbp_verified else "td-warn"}">'
            f'{_sdot("g") if gbp_verified else _sdot("y")}{"Yes" if gbp_verified else "Unconfirmed"}</div></div>'
            f'<div class="gbp-field"><div class="gbp-field-label">NAP Consistent</div>'
            f'<div class="gbp-field-value {"td-good" if gbp_nap else "td-warn"}">'
            f'{_sdot("g") if gbp_nap else _sdot("y")}{"Yes" if gbp_nap else "Check needed"}</div></div>'
            f'<div class="gbp-field"><div class="gbp-field-label">Profile Complete</div>'
            f'<div class="gbp-field-value {"td-warn" if gbp_complete < 80 else "td-good"}">{gbp_complete}%</div></div>'
            f'</div>'
        )

        # Competitor table
        competitors = comp.get("competitors", [])
        def _score_or_na(val):
            return "N/A" if val is None else str(val)

        def _bool_or_na(val):
            if val is None: return "N/A"
            return "Yes" if val else "No"

        comp_tbody = ""
        for i, c in enumerate(competitors, 1):
            domain = _h(c.get("domain", c.get("url", f"Competitor {i}")))
            note   = c.get("note", "")
            note_td = f' <span style="color:#888;font-size:9px">({_h(note)})</span>' if note else ""
            comp_tbody += (f'<tr>'
                           f'<td>{i}</td>'
                           f'<td class="td-name">{domain}{note_td}</td>'
                           f'<td>{_score_or_na(c.get("seo_score"))}</td>'
                           f'<td>{_score_or_na(c.get("performance_score"))}</td>'
                           f'<td>{_score_or_na(c.get("technical_score"))}</td>'
                           f'<td>{c.get("social_channel_count", 0)}</td>'
                           f'</tr>')

        comp_table = (
            f'<table class="data-table">'
            f'<thead><tr><th>#</th><th>Domain</th><th>SEO</th>'
            f'<th>Perf</th><th>Tech</th><th>Social</th></tr></thead>'
            f'<tbody>{comp_tbody}</tbody></table>'
        ) if comp_tbody else '<div class="info-box">No competitor data collected.</div>'

        # Side-by-side comparison
        comparison = comp.get("comparison", {})
        client_data = comparison.get("client", {})
        comp_side_tbody = ""
        metrics = [
            ("SEO Score",          "seo_score",            "score"),
            ("Performance Score",  "performance_score",    "score"),
            ("Website Technical",  "technical_score",      "score"),
            ("Website Conversion", "conversion_score",     "score"),
            ("Social Channels",    "social_channel_count", "count"),
            ("Page Title",         "has_title",            "bool"),
            ("Meta Description",   "has_meta_desc",        "bool"),
            ("H1 Tag",             "has_h1",               "bool"),
            ("Open Graph Tags",    "has_og_tags",          "bool"),
            ("Structured Data",    "has_schema",           "bool"),
            ("Canonical Tag",      "has_canonical",        "bool"),
            ("robots.txt",         "has_robots_txt",       "bool"),
            ("XML Sitemap",        "has_sitemap",          "bool"),
        ]
        comp_objs = comparison.get("competitors", [])
        if client_data and comp_objs:
            for label, key, kind in metrics:
                raw_client = client_data.get(key)
                if kind == "bool":
                    client_disp = _bool_or_na(raw_client)
                elif kind == "score":
                    client_disp = _score_or_na(raw_client)
                else:
                    client_disp = str(raw_client) if raw_client is not None else "—"
                vals = f'<td class="td-good">{client_disp}</td>'
                for c in comp_objs[:3]:
                    raw = c.get(key)
                    if kind == "bool":
                        disp = _bool_or_na(raw)
                    elif kind == "score":
                        disp = _score_or_na(raw)
                    else:
                        disp = str(raw) if raw is not None else "—"
                    vals += f'<td>{disp}</td>'
                comp_side_tbody += f'<tr><td class="td-name">{_h(label)}</td>{vals}</tr>'

        comp_side_table = ""
        if comp_side_tbody:
            headers = '<th>Metric</th><th style="color:#00AEEF">You</th>'
            for c in comp_objs[:3]:
                headers += f'<th>{_h(c.get("domain", "Comp"))[:20]}</th>'
            comp_side_table = (
                f'<table class="data-table">'
                f'<thead><tr>{headers}</tr></thead>'
                f'<tbody>{comp_side_tbody}</tbody></table>'
            )

        body = (
            f'<div class="page-title">Google Business Profile <span>&amp; Competitive</span></div>'
            f'{_sub(f"Google Business Profile — Score: {gbp_score}/100 ({gbp_grade})")}'
            f'{gbp_grid}'
            f'{_split_table(gbp.get("issues", []), gbp.get("strengths", []))}'
            f'<div class="page-title" style="margin-top:18px">Competitive <span>Positioning Analysis</span></div>'
            f'{_sub("Competitors Audited")}'
            f'{comp_table}'
        )
        if comp_side_table:
            body += f'{_sub("Side-by-Side Comparison")}{comp_side_table}'

        insights = comp.get("insights", [])
        if insights:
            body += f'<div class="strategy-box">{_h(insights[0])}</div>'

        return _pg(10, body, self.date_str, self.logo_src)

    # ── PAGE 11: 90-Day Action Plan ───────────────────────────────────────────

    def _page_action_plan(self) -> str:
        ai    = self.ai
        plan  = ai.get("90_day_action_plan", [])
        prios = ai.get("top_3_priorities", [])

        # Try to use the 90-day plan; fall back to splitting top_3_priorities
        if plan:
            # Map week ranges to phases
            phase1_rows = phase2_rows = phase3_rows = ""
            for item in plan:
                week   = str(item.get("week", ""))
                action = item.get("action", "")
                outcome = item.get("outcome", "")
                tl     = f"Week {week}" if week else "—"
                # Assign to phase based on week number
                if any(x in week for x in ("1", "2", "1-2", "2-4", "3-4")):
                    phase1_rows += _rec_row("CRITICAL" if "1" in week else "HIGH",
                                            action, outcome, tl)
                elif any(x in week for x in ("5", "6", "7", "8", "5-8")):
                    phase2_rows += _rec_row("HIGH", action, outcome, tl)
                else:
                    phase3_rows += _rec_row("MEDIUM", action, outcome, tl)
        else:
            # Build from top_3_priorities and standard recs
            phase1_rows = phase2_rows = phase3_rows = ""
            for i, p in enumerate(prios[:3]):
                action   = p.get("action", "")
                impact   = p.get("impact", "")
                timeline = p.get("timeline", "")
                if i == 0:
                    phase1_rows += _rec_row("CRITICAL", action, impact, timeline)
                elif i == 1:
                    phase1_rows += _rec_row("HIGH",     action, impact, timeline)
                else:
                    phase2_rows += _rec_row("HIGH",     action, impact, timeline)
            phase3_rows = _rec_row("MEDIUM",
                                   "Review C.A.S.H. scores and set 90-day targets",
                                   "Continuous improvement loop established.", "Day 90")

        # Ensure phases have at least placeholder content
        if not phase1_rows:
            phase1_rows = _rec_row("CRITICAL", "Fix critical gaps identified in audit",
                                   "Address top issues from C.A.S.H. scorecard.", "Days 1–30")
        if not phase2_rows:
            phase2_rows = _rec_row("HIGH", "Build authority and pipeline",
                                   "Publish ICP-specific content and collect testimonials.", "Days 31–60")
        if not phase3_rows:
            phase3_rows = _rec_row("MEDIUM", "Scale systems and review progress",
                                   "Launch retention systems and measure results.", "Days 61–90")

        body = (
            f'<div class="page-title"><span>90-Day</span> Action Plan</div>'
            f'{_phase_block("red",    1, "Foundation — Fix Critical Gaps & Quick Wins", "Days 1–30",  phase1_rows)}'
            f'{_phase_block("orange", 2, "Authority — Build Proof & Pipeline",          "Days 31–60", phase2_rows)}'
            f'{_phase_block("cyan",   3, "Scale — Systems, Retention & Review",         "Days 61–90", phase3_rows)}'
        )
        return _pg(11, body, self.date_str, self.logo_src)

    # ── PAGE 12: CTA ──────────────────────────────────────────────────────────

    def _page_cta(self) -> str:
        body = (
            f'<div class="cta-hero">'
            f'<h2>Next Steps <span>With GMG</span></h2>'
            f'<p>Your report is just the starting point. A GMG strategist is already reviewing '
            f'your results and will be reaching out with key insights and opportunities tailored '
            f'to your business.</p>'
            f'<div class="cta-btn">📅 &nbsp; Schedule Your Strategy Session</div>'
            f'<div class="cta-url">www.gogmg.net/meeting &nbsp;·&nbsp; Free · 30 Minutes · No Obligation</div>'
            f'</div>'
            f'<div class="cta-cards">'
            f'<div class="cta-card"><div class="cta-card-icon">🎯</div>'
            f'<div class="cta-card-name">Score Walkthrough</div>'
            f'<div class="cta-card-desc">Review your C.A.S.H. scores and identify the 2–3 moves '
            f'that make the biggest difference in 90 days</div></div>'
            f'<div class="cta-card"><div class="cta-card-icon">🗺️</div>'
            f'<div class="cta-card-name">Custom Roadmap</div>'
            f'<div class="cta-card-desc">Leave with a prioritized action plan tailored to your '
            f'ICP, budget, and team capacity</div></div>'
            f'<div class="cta-card"><div class="cta-card-icon">💰</div>'
            f'<div class="cta-card-name">ROI Projection</div>'
            f'<div class="cta-card-desc">See what fixing your Sales and Hold scores means for '
            f'your pipeline in measurable outcomes</div></div>'
            f'</div>'
            f'<div class="contact-strip">'
            f'<div class="cs-item"><div class="cs-label">Email</div>'
            f'<div class="cs-value highlight">gmg@goguerrilla.xyz</div></div>'
            f'<div class="cs-item"><div class="cs-label">Website</div>'
            f'<div class="cs-value highlight">www.goguerrilla.xyz</div></div>'
            f'<div class="cs-item"><div class="cs-label">Schedule</div>'
            f'<div class="cs-value highlight">www.gogmg.net/meeting</div></div>'
            f'</div>'
            f'{_sub("Appendix — Methodology &amp; Data Sources")}'
            f'<table class="app-table">'
            f'<thead><tr><th>Data Source</th><th>What It Measures</th></tr></thead>'
            f'<tbody>'
            f'<tr><td class="app-source">PageSpeed Insights API</td>'
            f'<td>Website performance, Core Web Vitals, mobile SEO</td></tr>'
            f'<tr><td class="app-source">Website Scraping (Playwright)</td>'
            f'<td>Technical checks, content analysis, conversion elements</td></tr>'
            f'<tr><td class="app-source">Google Maps HTML</td>'
            f'<td>GBP listing confirmation, NAP detection</td></tr>'
            f'<tr><td class="app-source">YouTube Data API v3</td>'
            f'<td>Channel subscribers, upload recency</td></tr>'
            f'<tr><td class="app-source">Google Analytics 4</td>'
            f'<td>Traffic, bounce rate, session duration</td></tr>'
            f'<tr><td class="app-source">Google Search Console</td>'
            f'<td>Keyword rankings, clicks, impressions</td></tr>'
            f'<tr><td class="app-source">Rule-based / Claude AI Analyzer</td>'
            f'<td>CASH scoring, issue weighting, action plan generation</td></tr>'
            f'</tbody></table>'
            f'<div class="callout-cyan">'
            f'<div class="callout-label">Recommended Next Step</div>'
            f'<div class="callout-body">Schedule a follow-up audit in 90 days to measure progress. '
            f'Book at <strong>www.gogmg.net/meeting</strong> · Report by C.A.S.H. Report by GMG · Confidential</div>'
            f'</div>'
        )
        return _pg(12, body, self.date_str, self.logo_src)
