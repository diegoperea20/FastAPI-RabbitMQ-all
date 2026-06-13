import json
import time
import uuid
from typing import Any, Dict
import aio_pika
from aio_pika import Message, DeliveryMode
from .connection import connection_manager
import logging

logger = logging.getLogger(__name__)


class TaskProducer:
    async def _publish(
        self,
        exchange: str,
        routing_key: str,
        message: Dict[str, Any],
        persistent: bool = True,
    ) -> None:
        async with connection_manager.channel() as channel:
            rmq_exchange = await channel.get_exchange(exchange)
            await rmq_exchange.publish(
                Message(
                    body=json.dumps(message, default=str).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
                    content_type="application/json",
                    message_id=str(uuid.uuid4()),
                    timestamp=int(time.time()),
                ),
                routing_key=routing_key,
            )

    async def publish_basic(self, task_id: int, title: str, description: str = "") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "basic",
        }
        await self._publish(
            exchange="tasks.direct",
            routing_key="task.basic",
            message=message,
        )
        logger.info(f"Published basic task {task_id} to tasks.direct/task.basic")

    async def publish_work_queue(self, task_id: int, title: str, description: str = "") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "work_queue",
        }
        await self._publish(
            exchange="tasks.direct",
            routing_key="task.work_queue",
            message=message,
        )
        logger.info(f"Published work_queue task {task_id} to tasks.direct/task.work_queue")

    async def publish_fanout(self, task_id: int, title: str, description: str = "") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "fanout",
        }
        await self._publish(
            exchange="tasks.fanout",
            routing_key="",
            message=message,
        )
        logger.info(f"Published fanout task {task_id} to tasks.fanout (broadcast)")

    async def publish_routing(self, task_id: int, title: str, description: str = "", routing_key: str = "task.general") -> None:
        message = {
            "task_id": task_id,
            "title": title,
            "description": description,
            "pattern": "routing",
            "routing_key": routing_key,
        }
        await self._publish(
            exchange="tasks.topic",
            routing_key=routing_key,
            message=message,
        )
        logger.info(f"Published routing task {task_id} to tasks.topic/{routing_key}")

    async def publish_by_pattern(self, pattern: str, task_id: int, title: str, description: str = "", routing_key: str = "task.general") -> None:
        if pattern == "basic":
            await self.publish_basic(task_id, title, description)
        elif pattern == "work_queue":
            await self.publish_work_queue(task_id, title, description)
        elif pattern == "fanout":
            await self.publish_fanout(task_id, title, description)
        elif pattern == "routing":
            await self.publish_routing(task_id, title, description, routing_key)
        else:
            raise ValueError(f"Unknown pattern: {pattern}")


producer = TaskProducer()