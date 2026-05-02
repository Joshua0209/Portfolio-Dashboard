"""Reproducer for invest.ingestion.statement_parser.

The dispatcher layer: given a PDF path, decide which of the three
text-blob parsers (securities/foreign/bank) handles it, extract text
via pdfplumber, and return the typed parsed result.

  detect_statement_type(filename)  -> StatementType | None
      Filename keyword → statement type. Pure; no I/O.

  extract_pdf_text(path)           -> str
      pdfplumber wrapper. The only PDF-I/O boundary in the
      ingestion package.

  parse_statement(path)            -> ParsedXStatement
      Composes detect_statement_type + extract_pdf_text + the right
      text-blob parser. Top-level entry point used by trade_seeder
      and trade_verifier.

The dispatcher tests stub extract_pdf_text via monkeypatch to avoid
fixture PDF setup; the round-trip 'pdfplumber actually extracts text'
property is the responsibility of pdfplumber itself.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from invest.ingestion.foreign_parser import ParsedForeignStatement
from invest.ingestion.bank_parser import ParsedBankStatement
from invest.ingestion.statement_parser import (
    StatementType,
    detect_statement_type,
    parse_statement,
)
from invest.ingestion.tw_parser import ParsedSecuritiesStatement


# --- detect_statement_type ----------------------------------------------


class TestDetectStatementType:
    def test_tw_securities_filename(self):
        assert (
            detect_statement_type("202403_證券月對帳單.pdf")
            == StatementType.TW_SECURITIES
        )

    def test_foreign_filename(self):
        assert (
            detect_statement_type("202403_複委託對帳單.pdf")
            == StatementType.FOREIGN
        )

    def test_bank_filename(self):
        assert (
            detect_statement_type("202403_銀行綜合對帳單.pdf")
            == StatementType.BANK
        )

    def test_unknown_filename_returns_none(self):
        """Defensive: unknown PDFs in sinopac_pdfs/ shouldn't crash
        the pipeline. Caller chooses to skip or raise."""
        assert detect_statement_type("random_document.pdf") is None
        assert detect_statement_type("") is None

    def test_path_with_directory_works(self):
        """detect_statement_type should accept either a bare filename
        or a relative path with directories — orchestrators sometimes
        pass either."""
        assert (
            detect_statement_type("sinopac_pdfs/decrypted/202403_證券月對帳單.pdf")
            == StatementType.TW_SECURITIES
        )


# --- parse_statement (dispatch + I/O composition) -----------------------


_TW_TEXT = "成交年月：202403\n"
_FOREIGN_TEXT = "對帳單日期：2024/03/31\n"
_BANK_TEXT = "對帳單期間：2024/03/01-2024/03/31\n"


class TestParseStatement:
    def test_dispatches_to_securities_parser(self, monkeypatch, tmp_path):
        path = tmp_path / "202403_證券月對帳單.pdf"
        path.write_bytes(b"placeholder")  # extract_pdf_text is stubbed
        monkeypatch.setattr(
            "invest.ingestion.statement_parser.extract_pdf_text",
            lambda p: _TW_TEXT,
        )
        out = parse_statement(path)
        assert isinstance(out, ParsedSecuritiesStatement)
        assert out.month == "2024-03"

    def test_dispatches_to_foreign_parser(self, monkeypatch, tmp_path):
        path = tmp_path / "202403_複委託對帳單.pdf"
        path.write_bytes(b"placeholder")
        monkeypatch.setattr(
            "invest.ingestion.statement_parser.extract_pdf_text",
            lambda p: _FOREIGN_TEXT,
        )
        out = parse_statement(path)
        assert isinstance(out, ParsedForeignStatement)

    def test_dispatches_to_bank_parser(self, monkeypatch, tmp_path):
        path = tmp_path / "202403_銀行綜合對帳單.pdf"
        path.write_bytes(b"placeholder")
        monkeypatch.setattr(
            "invest.ingestion.statement_parser.extract_pdf_text",
            lambda p: _BANK_TEXT,
        )
        out = parse_statement(path)
        assert isinstance(out, ParsedBankStatement)

    def test_unknown_filename_raises(self, tmp_path):
        """INVARIANT: an unrecognized filename must raise loudly. A
        silent skip would let stale/misnamed PDFs disappear from the
        verifier's view."""
        path = tmp_path / "random.pdf"
        path.write_bytes(b"placeholder")
        with pytest.raises(ValueError):
            parse_statement(path)


# --- pdf_decryptor ------------------------------------------------------


class TestDecryptPdf:
    """Build a tiny encrypted PDF with pikepdf, then exercise the
    multi-password decrypt loop."""

    def _make_encrypted_pdf(self, path: Path, password: str) -> None:
        import pikepdf
        with pikepdf.new() as pdf:
            pdf.add_blank_page()
            pdf.save(path, encryption=pikepdf.Encryption(owner=password, user=password))

    def test_correct_password_decrypts(self, tmp_path):
        from invest.ingestion.pdf_decryptor import decrypt_pdf

        src = tmp_path / "encrypted.pdf"
        dst = tmp_path / "decrypted.pdf"
        self._make_encrypted_pdf(src, "secret123")

        result = decrypt_pdf(src, dst, ["wrong", "secret123", "other"])
        assert result == "secret123"
        assert dst.exists()

    def test_no_password_matches_returns_none(self, tmp_path):
        from invest.ingestion.pdf_decryptor import decrypt_pdf

        src = tmp_path / "encrypted.pdf"
        dst = tmp_path / "decrypted.pdf"
        self._make_encrypted_pdf(src, "secret123")

        result = decrypt_pdf(src, dst, ["wrong1", "wrong2"])
        assert result is None
        # On failure, dst must NOT be created (no half-decrypted artifact).
        assert not dst.exists()

    def test_first_matching_password_wins(self, tmp_path):
        """INVARIANT: the loop short-circuits on first success.
        Ensures we don't pointlessly try every candidate."""
        from invest.ingestion.pdf_decryptor import decrypt_pdf

        src = tmp_path / "encrypted.pdf"
        dst = tmp_path / "decrypted.pdf"
        self._make_encrypted_pdf(src, "abc")

        result = decrypt_pdf(src, dst, ["abc", "def"])
        assert result == "abc"

    def test_empty_password_list_returns_none(self, tmp_path):
        """No candidates → return None without raising. Callers
        decide whether empty passwords is a config error."""
        from invest.ingestion.pdf_decryptor import decrypt_pdf

        src = tmp_path / "encrypted.pdf"
        dst = tmp_path / "decrypted.pdf"
        self._make_encrypted_pdf(src, "x")

        assert decrypt_pdf(src, dst, []) is None
