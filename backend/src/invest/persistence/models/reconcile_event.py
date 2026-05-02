from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, Optional

from sqlalchemy import Column, JSON
from sqlmodel import Field, SQLModel

from invest.persistence._utils import utcnow


class ReconcileStatus(StrEnum):
    OPEN = "open"
    DISMISSED = "dismissed"


class ReconcileEvent(SQLModel, table=True):
    __tablename__ = "reconcile_events"

    id: Optional[int] = Field(default=None, primary_key=True)
    pdf_month: str = Field(max_length=7, index=True)
    event_type: str = Field(index=True)
    detail: Dict[str, Any] = Field(sa_column=Column(JSON))
    status: str = Field(default=ReconcileStatus.OPEN, index=True)
    detected_at: datetime = Field(default_factory=utcnow)
    dismissed_at: Optional[datetime] = None
