#!/usr/bin/env python3
"""Decrypt Sinopac PDF statements using passwords from environment.

Env vars:
  SINOPAC_PDF_PASSWORDS   Comma-separated list of candidate passwords.
                          The script tries each per file; first hit wins.
                          Example: export SINOPAC_PDF_PASSWORDS="<national-id>,<birth-date-yyyymmdd>"

Outputs decrypted copies into sinopac_pdfs/decrypted/ preserving filenames.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pikepdf

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "sinopac_pdfs"
DST = SRC / "decrypted"


def load_passwords() -> list[str]:
    raw = os.environ.get("SINOPAC_PDF_PASSWORDS", "").strip()
    if not raw:
        sys.exit(
            "ERROR: set SINOPAC_PDF_PASSWORDS=pw1,pw2,...\n"
            "       (comma-separated; tried in order per file)"
        )
    return [p for p in (s.strip() for s in raw.split(",")) if p]


def try_open(path: Path, passwords: list[str]) -> tuple[pikepdf.Pdf, str] | None:
    for pw in passwords:
        try:
            return pikepdf.open(path, password=pw), pw
        except pikepdf.PasswordError:
            continue
    return None


def main() -> int:
    passwords = load_passwords()
    DST.mkdir(exist_ok=True)
    pdfs = sorted(SRC.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {SRC}")
        return 1

    ok = fail = skipped = 0
    for pdf in pdfs:
        out = DST / pdf.name
        if out.exists():
            skipped += 1
            continue
        result = try_open(pdf, passwords)
        if result is None:
            print(f"FAIL  {pdf.name}  (no password matched)")
            fail += 1
            continue
        doc, _ = result
        try:
            doc.save(out)  # save without Encryption -> unencrypted copy
            ok += 1
            print(f"OK    {pdf.name}")
        finally:
            doc.close()

    print(f"\nDone. ok={ok} fail={fail} skipped(existing)={skipped}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
