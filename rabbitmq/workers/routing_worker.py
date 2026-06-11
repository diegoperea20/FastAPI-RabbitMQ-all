import logging
import os
from typing import Dict, Any
from database import get_db
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

    def handle_message(self, message: Dict[str, Any]) -> None:
        task_id = message.get("task_id")
        title = message.get("title", "")
        description = message.get("description", "")
        routing_key = message.get("routing_key", "unknown")

        logger.info(f"[ROUTING WORKER #{self.worker_id}] Processing task {task_id} (routing_key: {routing_key}): {title}")

        db = next(get_db())
        try:
            task = db.query(Task).filter(Task.id == task_id).first()
            if not task:
                logger.error(f"Task {task_id} not found in database")
                return

            task.status = TaskStatus.PROCESSING
            task.updated_at = datetime.utcnow()
            db.commit()

            import time
            time.sleep(1.5)

            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            db.commit()

            logger.info(f"[ROUTING WORKER #{self.worker_id}] Task {task_id} completed successfully")

        except Exception as e:
            logger.error(f"[ROUTING WORKER #{self.worker_id}] Error processing task {task_id}: {e}", exc_info=True)
            if task:
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
                task.updated_at = datetime.utcnow()
                db.commit()
            raise
        finally:
            db.close()


def create_routing_worker(worker_id: int, binding_key: str = "task.*") -> RoutingWorker:
    return RoutingWorker(worker_id=worker_id, binding_key=binding_key)