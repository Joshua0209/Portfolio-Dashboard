"""Filename → parser dispatcher + pdfplumber I/O wrapper.

This is the only PDF-aware module in the ingestion package. The
three text-blob parsers (tw_parser.parse_securities_text,
foreign_parser.parse_foreign_text, bank_parser.parse_bank_text)
take pre-extracted text; this layer wraps them with pdfplumber
extraction and a filename-keyword dispatcher.

Public surface:
  StatementType            enum (TW_SECURITIES / FOREIGN / BANK)
  detect_statement_type    pure dispatcher
  extract_pdf_text         pdfplumber boundary
  parse_statement          composes the above with the right parser
"""
from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Optional, Union

from invest.ingestion.bank_parser import (
    ParsedBankStatement,
    parse_bank_text,
)
from invest.ingestion.foreign_parser import (
    ParsedForeignStatement,
    parse_foreign_text,
)
from invest.ingestion.tw_parser import (
    ParsedSecuritiesStatement,
    parse_securities_text,
)


class StatementType(StrEnum):
    TW_SECURITIES = "tw_securities"
    FOREIGN = "foreign"
    BANK = "bank"


_FILENAME_KEYWORDS: tuple[tuple[str, StatementType], ...] = (
    ("證券月對帳單", StatementType.TW_SECURITIES),
    ("複委託", StatementType.FOREIGN),
    ("銀行綜合", StatementType.BANK),
)


def detect_statement_type(filename: str) -> Optional[StatementType]:
    """Filename keyword → statement type. None if unrecognized.

    Accepts either bare filename or a path with directories.
    """
    for keyword, stype in _FILENAME_KEYWORDS:
        if keyword in filename:
            return stype
    return None


def extract_pdf_text(path: Path) -> str:
    """pdfplumber wrapper. Concatenates per-page text with newlines.

    The empty-string fallback per page handles pages that pdfplumber
    can't extract (rare, e.g. pure-image pages); they contribute
    nothing rather than crashing the parse.
    """
    import pdfplumber

    with pdfplumber.open(path) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


def parse_statement(
    path: Path,
) -> Union[ParsedSecuritiesStatement, ParsedForeignStatement, ParsedBankStatement]:
    """Top-level entry point. Dispatch by filename, extract, parse.

    Raises ValueError if the filename doesn't match any known
    statement type. Loud failure is intentional: silent skips would
    let stale/misnamed PDFs disappear from downstream coverage.
    """
    stype = detect_statement_type(path.name)
    if stype is None:
        raise ValueError(f"Unknown statement type for {path.name}")
    text = extract_pdf_text(path)
    if stype == StatementType.TW_SECURITIES:
        return parse_securities_text(text)
    if stype == StatementType.FOREIGN:
        return parse_foreign_text(text)
    if stype == StatementType.BANK:
        return parse_bank_text(text)
    raise ValueError(f"Unhandled statement type: {stype}")
