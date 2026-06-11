import logging
import os
from typing import Dict, Any
from database import get_db
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

    def handle_message(self, message: Dict[str, Any]) -> None:
        task_id = message.get("task_id")
        title = message.get("title", "")
        description = message.get("description", "")

        logger.info(f"[PUBSUB SUBSCRIBER #{self.subscriber_id}] Received task {task_id}: {title}")

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
            time.sleep(1)

            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            db.commit()

            logger.info(f"[PUBSUB SUBSCRIBER #{self.subscriber_id}] Task {task_id} completed successfully")

        except Exception as e:
            logger.error(f"[PUBSUB SUBSCRIBER #{self.subscriber_id}] Error processing task {task_id}: {e}", exc_info=True)
            if task:
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
                task.updated_at = datetime.utcnow()
                db.commit()
            raise
        finally:
            db.close()


def create_subscriber(subscriber_id: int) -> PubSubWorker:
    return PubSubWorker(subscriber_id=subscriber_id)