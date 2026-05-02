"""Job modules — backfill, snapshot, verify_month, retry_failed.

Each module is runnable as a CLI shim (scripts/*.py) and as a
programmatic call from the FastAPI startup hook or admin endpoints.

State transitions for the cold-start backfill flow through
invest.core.state. The DLQ wrapper (_dlq) is private to this package
— it's a shared seam between backfill (writes failures) and
retry_failed (drains them).
"""
