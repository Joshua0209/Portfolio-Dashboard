from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlmodel import Field, SQLModel
from sqlalchemy import Column, JSON


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ReconcileEvent(SQLModel, table=True):
    __tablename__ = "reconcile_events"

    id: Optional[int] = Field(default=None, primary_key=True)
    pdf_month: str = Field(max_length=7, index=True)
    event_type: str = Field(index=True)
    detail: Dict[str, Any] = Field(sa_column=Column(JSON))
    status: str = Field(default="open", index=True)
    detected_at: datetime = Field(default_factory=_utcnow)
    dismissed_at: Optional[datetime] = None
