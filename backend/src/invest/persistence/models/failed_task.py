from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlmodel import Field, SQLModel
from sqlalchemy import Column, JSON


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FailedTask(SQLModel, table=True):
    __tablename__ = "failed_tasks"

    id: Optional[int] = Field(default=None, primary_key=True)
    task_type: str = Field(index=True)
    payload: Dict[str, Any] = Field(sa_column=Column(JSON))
    error: str
    attempts: int = Field(default=1)
    first_failed_at: datetime = Field(default_factory=_utcnow)
    last_failed_at: datetime = Field(default_factory=_utcnow)
    resolved_at: Optional[datetime] = Field(default=None, index=True)
