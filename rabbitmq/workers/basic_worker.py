import logging
import asyncio
from typing import Dict, Any
from sqlalchemy import select
from database import AsyncSessionLocal
from database.models import Task, TaskStatus
from rabbitmq.consumer import BaseConsumer
from datetime import datetime

logger = logging.getLogger(__name__)


class BasicWorker(BaseConsumer):
    def __init__(self):
        super().__init__(
            queue_name="tasks.basic",
            prefetch_count=1,
            max_retries=3,
            retry_delay_base=2.0,
        )

    async def handle_message(self, message: Dict[str, Any]) -> None:
        task_id = message.get("task_id")
        title = message.get("title", "")

        logger.info(f"[BASIC WORKER] Processing task {task_id}: {title}")

        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if not task:
                logger.error(f"Task {task_id} not found in database")
                return

            task.status = TaskStatus.PROCESSING
            task.updated_at = datetime.utcnow()
            await db.commit()

            await asyncio.sleep(1)

            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            await db.commit()

            logger.info(f"[BASIC WORKER] Task {task_id} completed successfully")


basic_worker = BasicWorker()