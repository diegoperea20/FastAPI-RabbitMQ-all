from typing import List, Optional
from sqlalchemy.orm import Session
from database import Task, TaskStatus
from schemas import TaskCreate, TaskUpdate, TaskPattern
from rabbitmq import producer


class TaskService:
    def __init__(self, db: Session):
        self.db = db

    def create_task(self, task_data: TaskCreate) -> Task:
        task = Task(
            title=task_data.title,
            description=task_data.description,
            pattern=task_data.pattern.value,
            status=TaskStatus.PENDING,
        )
        self.db.add(task)
        self.db.commit()
        self.db.refresh(task)

        producer.publish_by_pattern(
            pattern=task_data.pattern.value,
            task_id=task.id,
            title=task.title,
            description=task.description or "",
        )

        return task

    def get_task(self, task_id: int) -> Optional[Task]:
        return self.db.query(Task).filter(Task.id == task_id).first()

    def get_tasks(
        self,
        skip: int = 0,
        limit: int = 100,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        query = self.db.query(Task)
        if status:
            query = query.filter(Task.status == status)
        return query.order_by(Task.created_at.desc()).offset(skip).limit(limit).all()

    def count_tasks(self, status: Optional[TaskStatus] = None) -> int:
        query = self.db.query(Task)
        if status:
            query = query.filter(Task.status == status)
        return query.count()

    def update_task(self, task_id: int, task_data: TaskUpdate) -> Optional[Task]:
        task = self.get_task(task_id)
        if not task:
            return None

        update_data = task_data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            if field == "pattern" and value:
                setattr(task, field, value.value)
            else:
                setattr(task, field, value)

        self.db.commit()
        self.db.refresh(task)
        return task

    def delete_task(self, task_id: int) -> bool:
        task = self.get_task(task_id)
        if not task:
            return False
        self.db.delete(task)
        self.db.commit()
        return True

    def get_task_status(self, task_id: int) -> Optional[dict]:
        task = self.get_task(task_id)
        if not task:
            return None
        return {
            "id": task.id,
            "status": task.status,
            "retry_count": task.retry_count,
            "error_message": task.error_message,
            "updated_at": task.updated_at,
            "completed_at": task.completed_at,
        }