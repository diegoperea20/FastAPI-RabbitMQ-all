import json
import logging
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional
import aio_pika
from aio_pika import Message, DeliveryMode
from .connection import connection_manager

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
        self._running = False
        self._consumer_tag: Optional[str] = None
        self._channel: Optional[aio_pika.Channel] = None
        self._queue: Optional[aio_pika.Queue] = None

    def _calculate_retry_delay(self, attempt: int) -> float:
        return self.retry_delay_base * (2 ** (attempt - 1))

    async def _process_message(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
    ) -> None:
        try:
            body = message.body.decode("utf-8")
            msg_data = json.loads(body)
            task_id = msg_data.get("task_id")
            retry_count = msg_data.get("_retry_count", 0)

            logger.info(f"Processing task {task_id} from {self.queue_name} (attempt {retry_count + 1})")

            await self.handle_message(msg_data)

            await message.ack()
            logger.info(f"Successfully processed task {task_id}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            await self._handle_failure(message, e)

    async def _handle_failure(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        error: Exception,
    ) -> None:
        try:
            body = message.body.decode("utf-8")
            msg_data = json.loads(body)
            task_id = msg_data.get("task_id")
            retry_count = msg_data.get("_retry_count", 0)

            if retry_count >= self.max_retries:
                logger.error(f"Task {task_id} exceeded max retries ({self.max_retries}), sending to DLQ")
                await message.nack(requeue=False)
            else:
                retry_count += 1
                delay = self._calculate_retry_delay(retry_count)
                logger.warning(f"Task {task_id} failed, retry {retry_count}/{self.max_retries} after {delay}s")

                msg_data["_retry_count"] = retry_count
                msg_data["_last_error"] = str(error)

                await asyncio.sleep(delay)

                await self._republish_with_retry(message, msg_data)
                await message.ack()

        except Exception as e:
            logger.error(f"Error in failure handler: {e}", exc_info=True)
            await message.nack(requeue=False)

    async def _republish_with_retry(
        self,
        original_message: aio_pika.abc.AbstractIncomingMessage,
        msg_data: Dict[str, Any],
    ) -> None:
        async with connection_manager.channel() as channel:
            exchange = await channel.get_exchange(original_message.exchange)
            await exchange.publish(
                Message(
                    body=json.dumps(msg_data, default=str).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    content_type="application/json",
                ),
                routing_key=original_message.routing_key,
            )

    @abstractmethod
    async def handle_message(self, message: Dict[str, Any]) -> None:
        pass

    async def start(self) -> None:
        self._running = True
        async with connection_manager.channel() as channel:
            self._channel = channel
            await channel.set_qos(prefetch_count=self.prefetch_count)
            self._queue = await channel.get_queue(self.queue_name)
            self._consumer_tag = await self._queue.consume(self._process_message)
            logger.info(f"Starting consumer for queue: {self.queue_name}")

            try:
                while self._running:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.info(f"Consumer for {self.queue_name} cancelled")
            except Exception as e:
                logger.error(f"Consumer error: {e}", exc_info=True)
                raise
            finally:
                await self.stop()

    async def stop(self) -> None:
        self._running = False
        if self._queue and self._consumer_tag:
            try:
                await self._queue.cancel(self._consumer_tag)
                logger.info(f"Stopped consumer for queue: {self.queue_name}")
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")


class CallbackConsumer(BaseConsumer):
    def __init__(
        self,
        queue_name: str,
        callback: Callable[[Dict[str, Any]], Any],
        prefetch_count: int = 1,
        max_retries: int = 3,
        retry_delay_base: float = 2.0,
    ):
        super().__init__(queue_name, prefetch_count, max_retries, retry_delay_base)
        self.callback = callback

    async def handle_message(self, message: Dict[str, Any]) -> None:
        if asyncio.iscoroutinefunction(self.callback):
            await self.callback(message)
        else:
            self.callback(message)