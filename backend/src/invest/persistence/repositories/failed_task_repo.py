from typing import List, Optional

from sqlmodel import Session, func, select

from invest.persistence._utils import utcnow
from invest.persistence.models.failed_task import FailedTask


class FailedTaskRepo:
    def __init__(self, session: Session):
        self.session = session

    def insert(self, task: FailedTask) -> FailedTask:
        self.session.add(task)
        self.session.commit()
        self.session.refresh(task)
        return task

    def find_by_id(self, task_id: int) -> Optional[FailedTask]:
        return self.session.get(FailedTask, task_id)

    def find_open(self) -> List[FailedTask]:
        stmt = (
            select(FailedTask)
            .where(FailedTask.resolved_at.is_(None))
            .order_by(FailedTask.first_failed_at)
        )
        return list(self.session.exec(stmt).all())

    def find_by_type(self, task_type: str) -> List[FailedTask]:
        stmt = (
            select(FailedTask)
            .where(FailedTask.task_type == task_type)
            .order_by(FailedTask.first_failed_at)
        )
        return list(self.session.exec(stmt).all())

    def count_open(self) -> int:
        stmt = select(func.count()).select_from(FailedTask).where(
            FailedTask.resolved_at.is_(None)
        )
        return self.session.exec(stmt).one()

    def bump_attempt(self, task_id: int, error: str) -> None:
        task = self.session.get(FailedTask, task_id)
        if task is None:
            return
        task.attempts += 1
        task.error = error
        task.last_failed_at = utcnow()
        self.session.add(task)
        self.session.commit()

    def mark_resolved(self, task_id: int) -> None:
        task = self.session.get(FailedTask, task_id)
        if task is None:
            return
        task.resolved_at = utcnow()
        self.session.add(task)
        self.session.commit()
