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


class RoutingWorker(BaseConsumer):
    def __init__(self, worker_id: int = None, binding_key: str = "task.*"):
        self.worker_id = worker_id or int(os.environ.get("WORKER_ID", "1"))
        self.binding_key = binding_key or os.environ.get("BINDING_KEY", "task.*")
        super().__init__(
            queue_name="tasks.routing",
            prefetch_count=1,
            max_retries=3,
            retry_delay_base=2.0,
        )

    async def handle_message(self, message: Dict[str, Any]) -> None:
        task_id = message.get("task_id")
        title = message.get("title", "")
        routing_key = message.get("routing_key", "unknown")

        logger.info(f"[ROUTING WORKER #{self.worker_id}] Processing task {task_id} (routing_key: {routing_key}): {title}")

        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
            if not task:
                logger.error(f"Task {task_id} not found in database")
                return

            task.status = TaskStatus.PROCESSING
            task.updated_at = datetime.utcnow()
            await db.commit()

            await asyncio.sleep(1.5)

            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            await db.commit()

            logger.info(f"[ROUTING WORKER #{self.worker_id}] Task {task_id} completed successfully")


def create_routing_worker(worker_id: int, binding_key: str = "task.*") -> RoutingWorker:
    return RoutingWorker(worker_id=worker_id, binding_key=binding_key)