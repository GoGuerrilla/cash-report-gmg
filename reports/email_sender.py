"""
C.A.S.H. Report — Email Delivery (SendGrid primary, Gmail SMTP fallback)

Required .env variables
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
  - If neither is configured → skips silently, returns False
  - Never raises — always returns True/False
"""
import base64
import os
import smtplib
import ssl
from datetime import date
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional


def _body_text(client_name: str, overall_score, overall_grade) -> str:
    score_line = ""
    if overall_score is not None and overall_grade:
        score_line = f"Overall C.A.S.H. Score: {overall_score}/100 ({overall_grade})\n\n"
    return (
        f"Hi,\n\n"
        f"Your C.A.S.H. Report for {client_name} is ready.\n\n"
        f"{score_line}"
        f"The full report is attached as a PDF.\n\n"
        f"C.A.S.H. stands for:\n"
        f"  C — Content\n"
        f"  A — Audience\n"
        f"  S — Sales\n"
        f"  H — Hold (Retention)\n\n"
        f"────────────────────────────────────────\n\n"
        f"Your report is just the starting point. Optimization begins now.\n\n"
        f"A GMG strategist is already reviewing your results and will be reaching out "
        f"with key insights and opportunities tailored to your business.\n\n"
        f"If you'd prefer to get ahead and start the conversation sooner, you can "
        f"schedule your strategy session here:\n\n"
        f"www.gogmg.net/meeting\n\n"
        f"────────────────────────────────────────\n\n"
        f"Report generated: {date.today().strftime('%B %d, %Y')}\n\n"
        f"—\n"
        f"C.A.S.H. Report by GMG · goguerrilla.xyz"
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

    content = [{"type": "text/plain", "value": body}]
    payload: dict = {
        "personalizations": [{"to": [{"email": to_addr}]}],
        "from": {"email": from_addr},
        "subject": subject,
        "content": content,
    }

    attachments = []
    for path in [report_path, teaser_path]:
        if path and os.path.isfile(path):
            with open(path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode()
            attachments.append({
                "content":     encoded,
                "filename":    os.path.basename(path),
                "type":        "application/pdf",
                "disposition": "attachment",
            })
    if attachments:
        payload["attachments"] = attachments

    data    = json.dumps(payload).encode("utf-8")
    req     = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status in (200, 202):
                print(f"  ✅  Report emailed via SendGrid → {to_addr}")
                return True
            print(f"  ⚠️  SendGrid returned status {resp.status}")
            return False
    except urllib.error.HTTPError as e:
        body_err = e.read().decode("utf-8", errors="replace")
        print(f"  ⚠️  SendGrid HTTP {e.code}: {body_err[:200]}")
        return False
    except Exception as exc:
        print(f"  ⚠️  SendGrid failed: {exc}")
        return False


def _send_smtp(
    from_addr: str, password: str, to_addr: str,
    subject: str, body: str,
    smtp_host: str, smtp_port: int,
    report_path: Optional[str] = None,
    teaser_path: Optional[str] = None,
) -> bool:
    """Send via Gmail SMTP (STARTTLS on 587, SSL on 465)."""
    msg = MIMEMultipart()
    msg["From"]    = from_addr
    msg["To"]      = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for path in [report_path, teaser_path]:
        if path and os.path.isfile(path):
            with open(path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition",
                            f'attachment; filename="{os.path.basename(path)}"')
            msg.attach(part)

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
        print(f"  ✅  Report emailed via SMTP → {to_addr}")
        return True
    except smtplib.SMTPAuthenticationError:
        print(
            f"  ⚠️  SMTP auth failed. Ensure REPORT_EMAIL_PASSWORD is a Gmail App Password.\n"
            f"      Generate one at: myaccount.google.com/apppasswords"
        )
        return False
    except Exception as exc:
        print(f"  ⚠️  SMTP failed: {exc}")
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
) -> bool:
    """
    Email the full report and teaser PDF (if present) to the configured recipient.
    Uses SendGrid if SENDGRID_API_KEY is set, otherwise falls back to Gmail SMTP.
    Returns True on success, False on any failure.
    """
    to_addr   = (to_addr   or os.environ.get("REPORT_EMAIL_TO")
                           or os.environ.get("REPORT_RECIPIENT_EMAIL", "")).strip()
    from_addr = (from_addr
                 or os.environ.get("SENDGRID_FROM_EMAIL")
                 or os.environ.get("REPORT_EMAIL_FROM", "")).strip()
    sg_key    = os.environ.get("SENDGRID_API_KEY", "").strip()
    password  = (password  or os.environ.get("REPORT_EMAIL_PASSWORD", "")).strip()
    smtp_host = os.environ.get("REPORT_EMAIL_SMTP", "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("REPORT_EMAIL_PORT", 587))

    if not to_addr or not from_addr:
        return False

    if report_path and not os.path.isfile(report_path):
        print(f"  ⚠️  Email skipped — report file not found: {report_path}")
        return False

    subject = f"C.A.S.H. Report Ready — {client_name}"
    body    = _body_text(client_name, overall_score, overall_grade)

    if sg_key:
        return _send_sendgrid(sg_key, from_addr, to_addr, subject, body,
                              report_path, teaser_path)

    if password:
        return _send_smtp(from_addr, password, to_addr, subject, body,
                          smtp_host, smtp_port, report_path, teaser_path)

    return False
