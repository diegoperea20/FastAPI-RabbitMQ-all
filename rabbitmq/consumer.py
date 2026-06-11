import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional
import pika
from .connection import create_connection
from config import settings

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    def __init__(
        self,
        queue_name: str,
        prefetch_count: int = 1,
        max_retries: int = 3,
        retry_delay_base: float = 2.0,
    ):
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base
        self._conn = None
        self._running = False

    def _calculate_retry_delay(self, attempt: int) -> float:
        return self.retry_delay_base * (2 ** (attempt - 1))

    def _process_message(
        self,
        channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        try:
            message = json.loads(body.decode("utf-8"))
            task_id = message.get("task_id")
            retry_count = message.get("_retry_count", 0)

            logger.info(f"Processing task {task_id} from {self.queue_name} (attempt {retry_count + 1})")

            self.handle_message(message)

            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Successfully processed task {task_id}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            self._handle_failure(channel, method, properties, body, e)

    def _handle_failure(
        self,
        channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
        error: Exception,
    ) -> None:
        try:
            message = json.loads(body.decode("utf-8"))
            task_id = message.get("task_id")
            retry_count = message.get("_retry_count", 0)

            if retry_count >= self.max_retries:
                logger.error(f"Task {task_id} exceeded max retries ({self.max_retries}), sending to DLQ")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                retry_count += 1
                delay = self._calculate_retry_delay(retry_count)
                logger.warning(f"Task {task_id} failed, retry {retry_count}/{self.max_retries} after {delay}s")

                message["_retry_count"] = retry_count
                message["_last_error"] = str(error)

                time.sleep(delay)

                channel.basic_publish(
                    exchange=method.exchange,
                    routing_key=method.routing_key,
                    body=json.dumps(message, default=str),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                    ),
                )
                channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error in failure handler: {e}", exc_info=True)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    @abstractmethod
    def handle_message(self, message: Dict[str, Any]) -> None:
        pass

    def start(self) -> None:
        self._running = True
        self._conn = create_connection()
        with self._conn.channel() as channel:
            channel.basic_qos(prefetch_count=self.prefetch_count)
            channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._process_message,
                auto_ack=False,
            )
            logger.info(f"Starting consumer for queue: {self.queue_name}")
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                logger.info("Consumer interrupted")
                channel.stop_consuming()
            except Exception as e:
                logger.error(f"Consumer error: {e}", exc_info=True)
                raise
            finally:
                self._conn.close()

    def stop(self) -> None:
        self._running = False


class CallbackConsumer(BaseConsumer):
    def __init__(
        self,
        queue_name: str,
        callback: Callable[[Dict[str, Any]], None],
        prefetch_count: int = 1,
        max_retries: int = 3,
        retry_delay_base: float = 2.0,
    ):
        super().__init__(queue_name, prefetch_count, max_retries, retry_delay_base)
        self.callback = callback

    def handle_message(self, message: Dict[str, Any]) -> None:
        self.callback(message)