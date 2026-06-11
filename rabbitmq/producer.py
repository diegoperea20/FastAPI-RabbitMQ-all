import json
import time
import uuid
from typing import Any, Dict
import pika
from .connection import create_connection
import logging

logger = logging.getLogger(__name__)


class TaskProducer:
    def _publish(
        self,
        exchange: str,
        routing_key: str,
        message: Dict[str, Any],
        persistent: bool = True,
    ) -> None:
        conn = create_connection()
        try:
            with conn.channel() as channel:
                channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=json.dumps(message, default=str),
                    properties=pika.BasicProperties(
                        delivery_mode=2 if persistent else 1,
                        content_type="application/json",
                        message_id=str(uuid.uuid4()),
                        timestamp=int(time.time()),
                    ),
                )
        finally:
            conn.close()

    def publish_basic(self, task_id: int, title: str, description: str = "") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "basic",
        }
        self._publish(
            exchange="tasks.direct",
            routing_key="task.basic",
            message=message,
        )
        logger.info(f"Published basic task {task_id} to tasks.direct/task.basic")

    def publish_work_queue(self, task_id: int, title: str, description: str = "") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "work_queue",
        }
        self._publish(
            exchange="tasks.direct",
            routing_key="task.work_queue",
            message=message,
        )
        logger.info(f"Published work_queue task {task_id} to tasks.direct/task.work_queue")

    def publish_fanout(self, task_id: int, title: str, description: str = "") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "fanout",
        }
        self._publish(
            exchange="tasks.fanout",
            routing_key="",
            message=message,
        )
        logger.info(f"Published fanout task {task_id} to tasks.fanout (broadcast)")

    def publish_routing(self, task_id: int, title: str, description: str = "", routing_key: str = "task.general") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "routing",
            "routing_key": routing_key,
        }
        self._publish(
            exchange="tasks.topic",
            routing_key=routing_key,
            message=message,
        )
        logger.info(f"Published routing task {task_id} to tasks.topic/{routing_key}")

    def publish_by_pattern(self, pattern: str, task_id: int, title: str, description: str = "", routing_key: str = "task.general") -> None:
        if pattern == "basic":
            self.publish_basic(task_id, title, description)
        elif pattern == "work_queue":
            self.publish_work_queue(task_id, title, description)
        elif pattern == "fanout":
            self.publish_fanout(task_id, title, description)
        elif pattern == "routing":
            self.publish_routing(task_id, title, description, routing_key)
        else:
            raise ValueError(f"Unknown pattern: {pattern}")


producer = TaskProducer()