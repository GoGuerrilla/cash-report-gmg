"""
Google Drive uploader — saves rendered reports to a shared Drive folder so they
land on the user's Mac via Drive Desktop sync (no SCP/tunnel required).

Activated by setting these Railway env vars:

  GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON  — full service-account JSON (raw, not
                                       base64). Service account must have Drive
                                       API enabled and Editor share access on
                                       the destination folder.
  GOOGLE_DRIVE_FOLDER_ID             — destination folder ID (the long token in
                                       the Drive folder URL after /folders/).

Both env vars must be set for upload to fire. Missing either var = silent skip
(non-fatal — does not block report email delivery).

Failures (auth, scope, network, quota) are logged at WARNING level and the
audit pipeline continues. Drive saving is supplementary, not load-bearing.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def _build_service():
    """Construct an authenticated Drive v3 service client. Returns None on any
    failure (missing key, missing libs, malformed JSON)."""
    raw = os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        return None
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("drive_uploader: GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON is not "
                    "valid JSON — %s", exc)
        return None
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=_SCOPES,
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except ImportError:
        log.warning("drive_uploader: google-api-python-client not installed — "
                    "Drive upload skipped")
        return None
    except Exception as exc:
        log.warning("drive_uploader: failed to build Drive client — %s", exc)
        return None


def upload_file(file_path: str, folder_id: Optional[str] = None,
                display_name: Optional[str] = None) -> Optional[str]:
    """
    Upload a file to the configured Drive folder.

    Args:
        file_path:    Local path to the file to upload.
        folder_id:    Override destination folder. Defaults to the
                      GOOGLE_DRIVE_FOLDER_ID env var.
        display_name: Override the filename shown in Drive. Defaults to the
                      basename of file_path.

    Returns:
        Google Drive file ID on success, None on any failure.
    """
    if not file_path or not os.path.isfile(file_path):
        log.warning("drive_uploader: file not found — %s", file_path)
        return None

    folder_id = folder_id or os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        # Silent skip — Drive integration not configured. Common during local
        # development; not an error condition.
        return None

    service = _build_service()
    if service is None:
        return None

    name = display_name or os.path.basename(file_path)
    try:
        from googleapiclient.http import MediaFileUpload
        media = MediaFileUpload(file_path, resumable=False)
        file = service.files().create(
            body={"name": name, "parents": [folder_id]},
            media_body=media,
            fields="id, name, webViewLink",
            supportsAllDrives=True,
        ).execute()
        file_id = file.get("id")
        log.info(
            "drive_uploader: uploaded %s → Drive id=%s view=%s",
            name, file_id, file.get("webViewLink", ""),
        )
        return file_id
    except Exception as exc:
        log.warning(
            "drive_uploader: upload failed for %s — %s",
            name, exc,
        )
        return None
