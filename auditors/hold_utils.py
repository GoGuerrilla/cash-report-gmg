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


def _sendgrid_send(sg_key: str, payload: dict, label: str, to_addr: str) -> None:
    """Internal helper — fire one SendGrid send and log success/failure."""
    try:
        req = urllib.request.Request(
            "https://api.sendgrid.com/v3/mail/send",
            data=json.dumps(payload).encode(),
            headers={"Authorization": f"Bearer {sg_key}",
                     "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            log.info("Hold %s email sent → %s (status %s)",
                     label, to_addr, r.status)
    except Exception as e:
        log.warning("Hold %s email failed → %s: %s", label, to_addr, e)


def send_hold_warning_email(config: ClientConfig, contact_email: str,
                            reason: str) -> None:
    """Send admin + client notifications when a report is held.

    Two emails go out per HOLD event:
      - GMG admin gets the diagnostic warning (existing behaviour) with
        the technical reason and the action required.
      - The submitter gets a soft, non-technical "we're reviewing
        your report" notice so they don't feel ghosted while GMG
        investigates. The client message NEVER reveals Stage 4
        directive details — just acknowledges receipt and sets the
        right expectation (human follow-up).

    Per Dave 2026-05-19 (Awake Tech / Bullish on Business HOLD):
    previously only the admin got notified, so a client whose Wix
    submission was held would receive nothing after the initial
    "thanks, your report is on its way" confirmation. That's fine
    for beta where Dave manually re-triggers, but not for GTM where
    an unknown prospect needs a clear "we got your submission, we'll
    follow up shortly" signal.
    """
    sg_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    from_addr = (os.environ.get("SENDGRID_FROM_EMAIL")
                 or os.environ.get("REPORT_EMAIL_FROM", "")).strip()
    admin_addr = os.environ.get("ADMIN_NOTIFY_EMAIL",
                                "gmg@goguerrilla.xyz").strip()
    if not sg_key or not from_addr or not admin_addr:
        log.warning("Hold warning email skipped — missing SendGrid config")
        return

    # ── 1. Admin warning (existing, diagnostic) ──────────────────
    admin_body = (
        f"⚠️ REPORT HELD — INSUFFICIENT DATA\n"
        f"{'─' * 40}\n"
        f"Client  : {config.client_name}\n"
        f"Website : {config.website_url or '—'}\n"
        f"Email   : {contact_email or '—'}\n\n"
        f"Reason: {reason}\n\n"
        f"Action required: manually verify the client's website and "
        f"social data, then re-trigger the audit from the admin panel."
    )
    _sendgrid_send(
        sg_key,
        {
            "personalizations": [{"to": [{"email": admin_addr}]}],
            "from": {"email": from_addr},
            "subject": (f"⚠️ Report Held — Insufficient Data: "
                        f"{config.client_name}"),
            "content": [{"type": "text/plain", "value": admin_body}],
        },
        label="admin",
        to_addr=admin_addr,
    )

    # ── 2. Client soft notification (new) ────────────────────────
    # Only sends when the client's email differs from the admin
    # address (no self-send loop), and when an email is actually on
    # file. Keep the copy non-technical — the submitter doesn't need
    # to know which Stage 4 directive fired; they need to know their
    # submission was received and a human is reviewing it.
    if not contact_email:
        return
    if contact_email.strip().lower() == admin_addr.strip().lower():
        return
    client_name = (config.client_name or "there").strip() or "there"
    client_body = (
        f"Hi {client_name},\n\n"
        f"Thanks for requesting a C.A.S.H. Report from GMG. Your "
        f"submission for {config.website_url or 'your website'} came "
        f"through — we're just double-checking a couple of signals "
        f"before sending the full report your way.\n\n"
        f"A GMG team member is reviewing the audit now and will "
        f"reach out shortly with your finished report (typically "
        f"within one business day).\n\n"
        f"If you have any questions in the meantime, reply to this "
        f"email or write us at gmg@goguerrilla.xyz.\n\n"
        f"— The GMG Team\n"
        f"www.goguerrilla.xyz"
    )
    _sendgrid_send(
        sg_key,
        {
            "personalizations": [{"to": [{"email": contact_email}]}],
            "from": {"email": from_addr},
            "subject": "Your C.A.S.H. Report is being reviewed",
            "content": [{"type": "text/plain", "value": client_body}],
        },
        label="client",
        to_addr=contact_email,
    )
