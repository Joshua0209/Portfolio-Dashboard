"""PDF decryption boundary.

Sinopac PDFs are password-protected with one of several candidate
passwords (national ID for brokerage statements, birth date for
bank statements). The operator supplies a comma-separated list of
candidates via SINOPAC_PDF_PASSWORDS; we try each and short-circuit
on the first that opens the file.
"""
from __future__ import annotations

from pathlib import Path


def decrypt_pdf(src: Path, dst: Path, passwords: list[str]) -> str | None:
    """Try each password in order; save decrypted copy on first hit.

    Returns the matching password (for logging — the caller can
    record which password worked for which file). Returns None
    when no candidate matches; in that case dst is NOT created so
    no half-decrypted artifact lingers.

    An empty password list returns None without raising — the
    caller decides whether 'no candidates' is a config error.
    """
    import pikepdf

    for pw in passwords:
        try:
            with pikepdf.open(src, password=pw) as pdf:
                pdf.save(dst)
                return pw
        except pikepdf.PasswordError:
            continue
    return None
