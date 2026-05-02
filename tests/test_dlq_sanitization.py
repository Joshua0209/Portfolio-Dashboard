"""Tests for the DLQ error_message sanitizer.

`failed_tasks.error_message` is exposed via the unauthenticated
`/api/admin/failed-tasks` endpoint; full filesystem paths in exception
strings would leak host layout when the dashboard is reachable from a
tunnel or LAN. The sanitizer in app/backfill_runner.py is the single
chokepoint for that data — every persist site routes through it.
"""
from __future__ import annotations

from pathlib import Path

from app.backfill_runner import _sanitize_error_message


def test_strips_project_root_path():
    """A traceback that includes /Users/.../investment/app/foo.py has
    the project root replaced with <project>."""
    project = str(Path(__file__).resolve().parent.parent)
    msg = f"FileNotFoundError: [Errno 2] No such file: '{project}/data/missing.json'"
    sanitized = _sanitize_error_message(msg)

    assert project not in sanitized
    assert "<project>/data/missing.json" in sanitized


def test_strips_home_directory():
    home = str(Path.home())
    msg = f"PermissionError: [Errno 13] {home}/secrets.txt"
    sanitized = _sanitize_error_message(msg)

    assert home not in sanitized
    assert "~/secrets.txt" in sanitized


def test_truncates_long_messages():
    """Pathological tracebacks must not balloon the row size — cap at
    500 chars with an ellipsis."""
    msg = "Error: " + "X" * 1000
    sanitized = _sanitize_error_message(msg)

    assert len(sanitized) == 500
    assert sanitized.endswith("...")


def test_passes_through_short_clean_messages():
    """Already-clean messages without paths are returned verbatim."""
    msg = "ValueError: invalid symbol format"
    sanitized = _sanitize_error_message(msg)

    assert sanitized == msg


def test_handles_empty_string():
    assert _sanitize_error_message("") == ""
