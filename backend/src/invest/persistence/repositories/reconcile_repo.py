from datetime import datetime, timezone
from typing import List, Optional
from sqlmodel import Session, select
from invest.persistence.models.reconcile_event import ReconcileEvent


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ReconcileRepo:
    def __init__(self, session: Session):
        self.session = session
    def insert(self, event: ReconcileEvent) -> ReconcileEvent:
        self.session.add(event)
        self.session.commit()
        self.session.refresh(event)
        return event
    def find_by_id(self, event_id: int) -> Optional[ReconcileEvent]:
        return self.session.get(ReconcileEvent, event_id)
    def find_open(self) -> List[ReconcileEvent]:
        stmt = (
            select(ReconcileEvent)
            .where(ReconcileEvent.status == "open")
            .order_by(ReconcileEvent.detected_at)
        )
        return list(self.session.exec(stmt).all())
    def find_open_for_month(self, pdf_month: str) -> List[ReconcileEvent]:
        stmt = (
            select(ReconcileEvent)
            .where(
                ReconcileEvent.status == "open",
                ReconcileEvent.pdf_month == pdf_month,
            )
            .order_by(ReconcileEvent.detected_at)
        )
        return list(self.session.exec(stmt).all())
    def dismiss(self, event_id: int) -> None:
        event = self.session.get(ReconcileEvent, event_id)
        if event is None:
            return
        event.status = "dismissed"
        event.dismissed_at = _utcnow()
        self.session.add(event)
        self.session.commit()
