import logging
import asyncio
import os
from typing import Dict, Any
from sqlalchemy import select
from database import AsyncSessionLocal
from database.models import Task, TaskStatus
from rabbitmq.consumer import BaseConsumer
from datetime import datetime

logger = logging.getLogger(__name__)


class PubSubWorker(BaseConsumer):
    def __init__(self, subscriber_id: int = None):
        self.subscriber_id = subscriber_id or int(os.environ.get("SUBSCRIBER_ID", "1"))
        super().__init__(
            queue_name="tasks.pubsub",
            prefetch_count=1,
            max_retries=3,
            retry_delay_base=2.0,
        )

    async def handle_message(self, message: Dict[str, Any]) -> None:
        task_id = message.get("task_id")
        title = message.get("title", "")

        logger.info(f"[PUBSUB SUBSCRIBER #{self.subscriber_id}] Received task {task_id}: {title}")

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

            logger.info(f"[PUBSUB SUBSCRIBER #{self.subscriber_id}] Task {task_id} completed successfully")


def create_subscriber(subscriber_id: int) -> PubSubWorker:
    return PubSubWorker(subscriber_id=subscriber_id)