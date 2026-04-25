"""Shared helpers for API blueprints."""
from __future__ import annotations

from flask import current_app

from ..data_store import DataStore


def store() -> DataStore:
    return current_app.extensions["store"]


def envelope(data, **meta):
    """Consistent JSON envelope: { ok, data, ...meta }."""
    body = {"ok": True, "data": data}
    if meta:
        body["meta"] = meta
    return body
