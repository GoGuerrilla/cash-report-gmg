"""
Shared HOLD notification utility.
Used by webhook_server.py and (later) website_auditor.py to fire the
internal hold-warning email when an audit cannot ship a client report.
Pure function, no globals, no per-instance state.
"""
import json
import logging
import os
import urllib.request

from config import ClientConfig

log = logging.getLogger(__name__)


def send_hold_warning_email(config: ClientConfig, contact_email: str,
                            reason: str) -> None:
    """Send admin warning when report is held due to insufficient data."""
    sg_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    from_addr = (os.environ.get("SENDGRID_FROM_EMAIL")
                 or os.environ.get("REPORT_EMAIL_FROM", "")).strip()
    admin_addr = os.environ.get("ADMIN_NOTIFY_EMAIL",
                                "gmg@goguerrilla.xyz").strip()
    if not sg_key or not from_addr or not admin_addr:
        log.warning("Hold warning email skipped — missing SendGrid config")
        return
    body = (
        f"⚠️ REPORT HELD — INSUFFICIENT DATA\n"
        f"{'─' * 40}\n"
        f"Client  : {config.client_name}\n"
        f"Website : {config.website_url or '—'}\n"
        f"Email   : {contact_email or '—'}\n\n"
        f"Reason: {reason}\n\n"
        f"Action required: manually verify the client's website and "
        f"social data, then re-trigger the audit from the admin panel."
    )
    try:
        payload = json.dumps({
            "personalizations": [{"to": [{"email": admin_addr}]}],
            "from": {"email": from_addr},
            "subject": (f"⚠️ Report Held — Insufficient Data: "
                        f"{config.client_name}"),
            "content": [{"type": "text/plain", "value": body}],
        }).encode()
        req = urllib.request.Request(
            "https://api.sendgrid.com/v3/mail/send",
            data=payload,
            headers={"Authorization": f"Bearer {sg_key}",
                     "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            log.info("Hold warning email sent → %s (status %s)",
                     admin_addr, r.status)
    except Exception as e:
        log.warning("Hold warning email failed: %s", e)
