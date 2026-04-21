"""
C.A.S.H. Report — Email Delivery (SendGrid primary, Gmail SMTP fallback)

Required env variables
-----------------------
  SENDGRID_API_KEY       SendGrid API key (primary delivery method)
  SENDGRID_FROM_EMAIL    Verified sender address (e.g. cash-report@gogmg.net)
  GMG_TEAM_EMAIL         Team inbox that receives a copy of every report
  REPORT_EMAIL_TO        Default recipient address
  REPORT_RECIPIENT_EMAIL Alias for REPORT_EMAIL_TO

  REPORT_EMAIL_FROM is accepted as a fallback for SENDGRID_FROM_EMAIL.

Optional fallback (Gmail SMTP)
------------------------------
  REPORT_EMAIL_PASSWORD  Gmail App Password
  REPORT_EMAIL_SMTP      SMTP host (default: smtp.gmail.com)
  REPORT_EMAIL_PORT      SMTP port (default: 587)

Behaviour
---------
  - Tries SendGrid first if SENDGRID_API_KEY is set
  - Falls back to Gmail SMTP if SendGrid key is absent
  - If neither is configured → logs clearly and returns False
  - Never raises — always returns True/False
"""
import base64
import logging
import os
import smtplib
import ssl
from datetime import date
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

log = logging.getLogger("webhook")


def _mime_type(path: str) -> str:
    """Return the correct MIME type for a report attachment based on file extension."""
    if path.lower().endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return "application/pdf"


def _body_no_attachment(client_name: str, overall_score, overall_grade) -> str:
    return (
        f"Hi,\n\n"
        f"Your C.A.S.H. Report for {client_name} has been generated and is being processed.\n\n"
        f"Overall C.A.S.H. Score: {overall_score}/100 ({overall_grade})\n\n"
        f"Our team will follow up with your full report shortly. "
        f"If you have any questions in the meantime, please contact gmg@goguerrilla.xyz\n\n"
        f"A GMG strategist is already reviewing your results and will be reaching out "
        f"with key insights and opportunities tailored to your business."
    )


def _body_text(client_name: str, overall_score, overall_grade,
               attachment_label: str = "PDF") -> str:
    return (
        f"Hi,\n\n"
        f"Your C.A.S.H. Report for {client_name} is ready.\n\n"
        f"Overall C.A.S.H. Score: {overall_score}/100 ({overall_grade})\n\n"
        f"The full report is attached as a {attachment_label}.\n\n"
        f"C.A.S.H. stands for:\n"
        f"C — Content\n"
        f"A — Audience\n"
        f"S — Sales\n"
        f"H — Hold (Retention)\n\n"
        f"Your report is just the starting point. Optimization begins now.\n\n"
        f"A GMG strategist is already reviewing your results and will be reaching out "
        f"with key insights and opportunities tailored to your business.\n\n"
        f"If you'd prefer to get ahead and start the conversation sooner, you can "
        f"schedule your strategy session here:\n"
        f"www.gogmg.net/meeting"
    )


def _send_sendgrid(
    api_key: str, from_addr: str, to_addr: str,
    subject: str, body: str,
    report_path: Optional[str] = None,
    teaser_path: Optional[str] = None,
) -> bool:
    """Send via SendGrid Web API v3."""
    import urllib.request
    import urllib.error
    import json

    log.info("SendGrid: preparing email from=%s to=%s subject=%r", from_addr, to_addr, subject)

    content = [{"type": "text/plain", "value": body}]
    payload: dict = {
        "personalizations": [{"to": [{"email": to_addr}]}],
        "from": {"email": from_addr},
        "subject": subject,
        "content": content,
    }

    attachments = []
    for path in [report_path, teaser_path]:
        if path:
            if os.path.isfile(path):
                size = os.path.getsize(path)
                log.info("SendGrid: attaching %s (%d bytes)", path, size)
                with open(path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode()
                attachments.append({
                    "content":     encoded,
                    "filename":    os.path.basename(path),
                    "type":        _mime_type(path),
                    "disposition": "attachment",
                })
            else:
                log.warning("SendGrid: attachment not found — skipping: %s", path)

    if attachments:
        payload["attachments"] = attachments
        log.info("SendGrid: %d attachment(s) included", len(attachments))
    else:
        log.warning("SendGrid: sending with NO attachments — no PDF files found")

    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            log.info("SendGrid: API response status=%d", status)
            if status in (200, 202):
                log.info("SendGrid: email delivered successfully → %s", to_addr)
                return True
            log.warning("SendGrid: unexpected status %d", status)
            return False
    except urllib.error.HTTPError as e:
        body_err = e.read().decode("utf-8", errors="replace")
        log.error("SendGrid: HTTP %d error: %s", e.code, body_err[:500])
        return False
    except Exception as exc:
        log.error("SendGrid: unexpected error: %s", exc)
        return False


def _send_smtp(
    from_addr: str, password: str, to_addr: str,
    subject: str, body: str,
    smtp_host: str, smtp_port: int,
    report_path: Optional[str] = None,
    teaser_path: Optional[str] = None,
) -> bool:
    """Send via Gmail SMTP (STARTTLS on 587, SSL on 465)."""
    log.info("SMTP: preparing email from=%s to=%s host=%s:%d",
             from_addr, to_addr, smtp_host, smtp_port)

    msg = MIMEMultipart()
    msg["From"]    = from_addr
    msg["To"]      = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for path in [report_path, teaser_path]:
        if path:
            if os.path.isfile(path):
                log.info("SMTP: attaching %s (%d bytes)", path, os.path.getsize(path))
                with open(path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition",
                                f'attachment; filename="{os.path.basename(path)}"')
                msg.attach(part)
            else:
                log.warning("SMTP: attachment not found — skipping: %s", path)

    ctx = ssl.create_default_context()
    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx) as server:
                server.login(from_addr, password)
                server.sendmail(from_addr, to_addr, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.ehlo()
                server.starttls(context=ctx)
                server.ehlo()
                server.login(from_addr, password)
                server.sendmail(from_addr, to_addr, msg.as_string())
        log.info("SMTP: email delivered successfully → %s", to_addr)
        return True
    except smtplib.SMTPAuthenticationError:
        log.error(
            "SMTP: authentication failed for %s. "
            "Ensure REPORT_EMAIL_PASSWORD is a Gmail App Password "
            "(myaccount.google.com/apppasswords)", from_addr)
        return False
    except Exception as exc:
        log.error("SMTP: send failed: %s", exc)
        return False


def send_report(
    report_path: str,
    client_name: str,
    overall_score: Optional[int] = None,
    overall_grade: Optional[str] = None,
    to_addr: Optional[str] = None,
    from_addr: Optional[str] = None,
    password: Optional[str] = None,
    teaser_path: Optional[str] = None,
    attachment_label: Optional[str] = None,
) -> bool:
    """
    Email the full report to the recipient (PDF or DOCX depending on report_path).
    Uses SendGrid if SENDGRID_API_KEY is set, otherwise falls back to Gmail SMTP.
    Returns True on success, False on any failure. Never raises.
    """
    # ── Resolve all config from args or env ───────────────────────
    to_addr   = (to_addr   or os.environ.get("REPORT_EMAIL_TO")
                           or os.environ.get("REPORT_RECIPIENT_EMAIL", "")).strip()
    from_addr = (from_addr
                 or os.environ.get("SENDGRID_FROM_EMAIL")
                 or os.environ.get("REPORT_EMAIL_FROM", "")).strip()
    sg_key    = os.environ.get("SENDGRID_API_KEY", "").strip()
    password  = (password  or os.environ.get("REPORT_EMAIL_PASSWORD", "")).strip()
    smtp_host = os.environ.get("REPORT_EMAIL_SMTP", "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("REPORT_EMAIL_PORT", 587))

    # Auto-detect label from extension if not provided
    if not attachment_label:
        attachment_label = "DOCX" if (report_path or "").lower().endswith(".docx") else "PDF"

    # ── Diagnostic env var dump ────────────────────────────────────
    log.info("Email config: to=%r  from=%r  sg_key=%s  smtp_pw=%s",
             to_addr, from_addr,
             f"set ({len(sg_key)} chars)" if sg_key else "NOT SET",
             "set" if password else "NOT SET")
    log.info("Report attachment: %s  format=%s  exists=%s  size=%s",
             report_path, attachment_label,
             os.path.isfile(report_path) if report_path else False,
             f"{os.path.getsize(report_path)} bytes"
             if report_path and os.path.isfile(report_path) else "N/A")

    # ── Guard: missing recipients / sender ────────────────────────
    if not to_addr:
        log.error("Email skipped — no recipient address. "
                  "Set REPORT_EMAIL_TO env var or pass contact_email.")
        return False
    if not from_addr:
        log.error("Email skipped — no sender address. "
                  "Set SENDGRID_FROM_EMAIL (or REPORT_EMAIL_FROM) env var on Railway.")
        return False

    # ── Guard: report file missing ────────────────────────────────
    if report_path and not os.path.isfile(report_path):
        log.error("Email skipped — report file not found at: %s", report_path)
        return False

    # ── Choose body based on whether a report was generated ───────
    subject = f"Your C.A.S.H. Report is Ready — {client_name}"
    if report_path is None:
        log.error("PDF generation failed — sending fallback email to client")
        body = _body_no_attachment(client_name, overall_score, overall_grade)
        admin_addr = os.environ.get("ADMIN_NOTIFY_EMAIL", "gmg@goguerrilla.xyz").strip()
        if sg_key and from_addr and admin_addr:
            _send_sendgrid(
                sg_key, from_addr, admin_addr,
                f"⚠️ PDF Generation Failed — {client_name}",
                f"PDF generation failed for client: {client_name}\n"
                f"Recipient: {to_addr}\n\n"
                f"The fallback 'report being processed' email has been sent to the client.\n"
                f"Please follow up manually with the report.",
            )
    else:
        body = _body_text(client_name, overall_score, overall_grade, attachment_label)

    # ── Send ──────────────────────────────────────────────────────
    def _send_team_copy(ok: bool) -> bool:
        """After a successful client send, fire a beta copy to GMG_TEAM_EMAIL."""
        if not ok:
            return False
        team_addr = os.environ.get("GMG_TEAM_EMAIL", "gmg@goguerrilla.xyz").strip()
        if team_addr and team_addr != to_addr:
            team_subject = f"[BETA COPY] {subject}"
            log.info("Team copy: sending to %s", team_addr)
            if sg_key:
                _send_sendgrid(sg_key, from_addr, team_addr, team_subject, body,
                               report_path, teaser_path)
            elif password:
                _send_smtp(from_addr, password, team_addr, team_subject, body,
                           smtp_host, smtp_port, report_path, teaser_path)
        return True

    if sg_key:
        log.info("Email: using SendGrid")
        ok = _send_sendgrid(sg_key, from_addr, to_addr, subject, body,
                            report_path, teaser_path)
        if ok:
            return _send_team_copy(ok)
        # SendGrid failed — attempt SMTP fallback if configured
        if password:
            log.warning("SendGrid failed — falling back to SMTP (%s:%d)", smtp_host, smtp_port)
            smtp_ok = _send_smtp(from_addr, password, to_addr, subject, body,
                                 smtp_host, smtp_port, report_path, teaser_path)
            if smtp_ok:
                return _send_team_copy(smtp_ok)
        log.error("EMAIL DELIVERY FAILED for %s — SendGrid returned error AND no SMTP fallback succeeded. "
                  "Check SENDGRID_API_KEY validity and REPORT_EMAIL_PASSWORD.", to_addr)
        return False

    if password:
        log.info("Email: using SMTP (%s:%d)", smtp_host, smtp_port)
        ok = _send_smtp(from_addr, password, to_addr, subject, body,
                        smtp_host, smtp_port, report_path, teaser_path)
        if not ok:
            log.error("EMAIL DELIVERY FAILED for %s — SMTP authentication or send error. "
                      "Verify REPORT_EMAIL_PASSWORD is a valid Gmail App Password.", to_addr)
        return _send_team_copy(ok) if ok else False

    log.error("EMAIL DELIVERY FAILED — no delivery method configured. "
              "Set SENDGRID_API_KEY (recommended) or REPORT_EMAIL_PASSWORD.")
    return False
