import logging
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete
from database import Task, TaskStatus
from schemas import TaskCreate, TaskUpdate, TaskPattern
from rabbitmq import producer

logger = logging.getLogger(__name__)


class TaskService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_task(self, task_data: TaskCreate) -> Task:
        task = Task(
            title=task_data.title,
            description=task_data.description,
            pattern=task_data.pattern.value,
            status=TaskStatus.PENDING,
        )
        self.db.add(task)
        await self.db.commit()
        await self.db.refresh(task)

        try:
            await producer.publish_by_pattern(
                pattern=task_data.pattern.value,
                task_id=task.id,
                title=task.title,
                description=task.description or "",
            )
        except Exception as e:
            logger.warning(f"Failed to publish task {task.id} to RabbitMQ: {e}")

        return task

    async def get_task(self, task_id: int) -> Optional[Task]:
        result = await self.db.execute(select(Task).where(Task.id == task_id))
        return result.scalar_one_or_none()

    async def get_tasks(
        self,
        skip: int = 0,
        limit: int = 100,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        query = select(Task)
        if status:
            query = query.where(Task.status == status)
        query = query.order_by(Task.created_at.desc()).offset(skip).limit(limit)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def count_tasks(self, status: Optional[TaskStatus] = None) -> int:
        query = select(func.count(Task.id))
        if status:
            query = query.where(Task.status == status)
        result = await self.db.execute(query)
        return result.scalar_one()

    async def update_task(self, task_id: int, task_data: TaskUpdate) -> Optional[Task]:
        task = await self.get_task(task_id)
        if not task:
            return None

        update_data = task_data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            if field == "pattern" and value:
                setattr(task, field, value.value)
            else:
                setattr(task, field, value)

        await self.db.commit()
        await self.db.refresh(task)
        return task

    async def delete_task(self, task_id: int) -> bool:
        task = await self.get_task(task_id)
        if not task:
            return False
        await self.db.delete(task)
        await self.db.commit()
        return True

    async def get_task_status(self, task_id: int) -> Optional[dict]:
        task = await self.get_task(task_id)
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