#!/usr/bin/env python3
"""
Download PDF attachments from Gmail label bank/sinopac.
Skips files already present in sinopac_pdfs/.
Discovers new messages automatically via Gmail search.

Requires: google-auth-oauthlib google-auth-httplib2 google-api-python-client
Setup: Enable Gmail API, place credentials.json in this directory.
"""

import base64
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "sinopac_pdfs"
TOKEN_FILE = ROOT / "token.json"
CREDS_FILE = ROOT / "credentials.json"
GMAIL_QUERY = "label:bank/sinopac has:attachment filename:pdf"


def get_credentials() -> Credentials:
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())
    return creds


def fetch_all_message_ids(service) -> list[str]:
    """Return all message IDs matching the Gmail query, handling pagination."""
    ids = []
    page_token = None
    while True:
        kwargs = {"userId": "me", "q": GMAIL_QUERY, "maxResults": 500}
        if page_token:
            kwargs["pageToken"] = page_token
        resp = service.users().messages().list(**kwargs).execute()
        ids.extend(m["id"] for m in resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return ids


def collect_pdf_parts(part: dict, results: list) -> None:
    mime = part.get("mimeType", "")
    filename = part.get("filename", "")
    if mime == "application/pdf" or (
        mime == "application/octet-stream" and filename.lower().endswith(".pdf")
    ):
        results.append(part)
    for child in part.get("parts", []):
        collect_pdf_parts(child, results)


def download_new_pdfs(service, msg_id: str, existing: set[str]) -> list[str]:
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    payload = msg.get("payload", {})

    pdf_parts: list[dict] = []
    collect_pdf_parts(payload, pdf_parts)

    saved = []
    for i, part in enumerate(pdf_parts):
        filename = part.get("filename") or f"{msg_id}_{i}.pdf"
        out_path = OUTPUT_DIR / filename

        if out_path.name in existing:
            saved.append(f"skip:{out_path.name}")
            continue

        att_id = part["body"].get("attachmentId")
        if not att_id:
            continue

        att = service.users().messages().attachments().get(
            userId="me", messageId=msg_id, id=att_id
        ).execute()

        data = base64.urlsafe_b64decode(att["data"])
        out_path.write_bytes(data)
        saved.append(out_path.name)

    return saved


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    existing = {p.name for p in OUTPUT_DIR.glob("*.pdf")}

    creds = get_credentials()
    service = build("gmail", "v1", credentials=creds)

    print(f"Searching Gmail: {GMAIL_QUERY!r}")
    message_ids = fetch_all_message_ids(service)
    print(f"Found {len(message_ids)} message(s), {len(existing)} already downloaded.\n")

    downloaded = skipped = 0
    for msg_id in message_ids:
        results = download_new_pdfs(service, msg_id, existing)
        for name in results:
            if name.startswith("skip:"):
                print(f"  skip  {name[5:]}")
                skipped += 1
            else:
                print(f"  saved {name}")
                downloaded += 1
                existing.add(name)

    print(f"\nDone. {downloaded} new PDF(s) downloaded, {skipped} skipped.")


if __name__ == "__main__":
    main()
