import logging
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager
import aio_pika
from config import settings

logger = logging.getLogger(__name__)


class RabbitMQConnectionManager:
    def __init__(self):
        self._connection: Optional[aio_pika.RobustConnection] = None
        self._closed = False

    async def connect(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            return

        self._connection = await aio_pika.connect_robust(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            virtualhost=settings.RABBITMQ_VHOST,
            login=settings.RABBITMQ_USER,
            password=settings.RABBITMQ_PASSWORD,
            heartbeat=settings.RABBITMQ_HEARTBEAT,
            blocked_connection_timeout=settings.RABBITMQ_BLOCKED_CONNECTION_TIMEOUT,
            timeout=settings.RABBITMQ_CONNECTION_TIMEOUT,
        )

        logger.info("RabbitMQ robust connection established")

    @asynccontextmanager
    async def channel(self) -> AsyncGenerator[aio_pika.Channel, None]:
        if self._connection is None or self._connection.is_closed:
            await self.connect()

        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=settings.WORKER_PREFETCH_COUNT)
        try:
            yield channel
        finally:
            if not channel.is_closed:
                await channel.close()

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[aio_pika.Connection, None]:
        if self._connection is None or self._connection.is_closed:
            await self.connect()
        yield self._connection

    async def is_connected(self) -> bool:
        if self._connection is None:
            return False
        try:
            return not self._connection.is_closed
        except Exception:
            return False

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            self._connection = None

        logger.info("RabbitMQ connection closed")


connection_manager = RabbitMQConnectionManager()


async def get_connection_manager() -> RabbitMQConnectionManager:
    return connection_manager