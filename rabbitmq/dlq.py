import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from database import get_db
from database.models import Task, TaskStatus
from rabbitmq.consumer import BaseConsumer
from config import settings

logger = logging.getLogger(__name__)


class DLQConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            queue_name="tasks.dlq",
            prefetch_count=1,
            max_retries=0,
        )

    def handle_message(self, message: Dict[str, Any]) -> None:
        task_id = message.get("task_id")
        error = message.get("_last_error", "Unknown error")
        retry_count = message.get("_retry_count", 0)

        logger.error(f"Task {task_id} moved to DLQ after {retry_count} retries. Error: {error}")

        db = next(get_db())
        try:
            task = db.query(Task).filter(Task.id == task_id).first()
            if task:
                task.status = TaskStatus.FAILED
                task.error_message = f"DLQ: {error}"
                task.retry_count = retry_count
                task.completed_at = datetime.utcnow()
                db.commit()
                logger.info(f"Updated task {task_id} status to FAILED in database")
            else:
                logger.warning(f"Task {task_id} not found in database")
        except Exception as e:
            logger.error(f"Error updating task in DLQ handler: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()


dlq_consumer = DLQConsumer()