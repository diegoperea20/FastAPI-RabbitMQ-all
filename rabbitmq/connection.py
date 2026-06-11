import pika
import logging
import time
from typing import Optional
from contextlib import contextmanager
from config import settings

logger = logging.getLogger(__name__)


def create_connection() -> "RabbitMQConnection":
    return RabbitMQConnection()


class RabbitMQConnection:
    def __init__(self):
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None
        self.connect()

    def connect(self) -> None:
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(
                    settings.RABBITMQ_USER,
                    settings.RABBITMQ_PASSWORD
                )
                parameters = pika.ConnectionParameters(
                    host=settings.RABBITMQ_HOST,
                    port=settings.RABBITMQ_PORT,
                    virtual_host=settings.RABBITMQ_VHOST,
                    credentials=credentials,
                    heartbeat=settings.RABBITMQ_HEARTBEAT,
                    blocked_connection_timeout=settings.RABBITMQ_BLOCKED_CONNECTION_TIMEOUT,
                    connection_attempts=3,
                    retry_delay=retry_delay,
                )
                self._connection = pika.BlockingConnection(parameters)
                self._channel = self._connection.channel()
                logger.info("Connected to RabbitMQ successfully")
                return
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"RabbitMQ connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    logger.error("Failed to connect to RabbitMQ after all retries")
                    raise

    def get_channel(self) -> pika.channel.Channel:
        if self._channel is None or self._channel.is_closed:
            self.connect()
        return self._channel

    def get_connection(self) -> pika.BlockingConnection:
        if self._connection is None or self._connection.is_closed:
            self.connect()
        return self._connection

    def is_connected(self) -> bool:
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
        )

    def close(self) -> None:
        if self._channel and not self._channel.is_closed:
            self._channel.close()
        if self._connection and not self._connection.is_closed:
            self._connection.close()
        logger.info("RabbitMQ connection closed")

    @contextmanager
    def channel(self):
        ch = self.get_channel()
        try:
            yield ch
        except pika.exceptions.AMQPError as e:
            logger.error(f"Channel error: {e}")
            self.connect()
            raise


connection_manager = RabbitMQConnection()


def get_connection() -> RabbitMQConnection:
    return connection_manager