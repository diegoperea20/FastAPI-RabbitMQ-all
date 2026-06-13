import aio_pika
from aio_pika import ExchangeType
from .connection import connection_manager

EXCHANGES = {
    "tasks.direct": {"type": ExchangeType.DIRECT, "durable": True},
    "tasks.fanout": {"type": ExchangeType.FANOUT, "durable": True},
    "tasks.topic": {"type": ExchangeType.TOPIC, "durable": True},
    "tasks.dlx": {"type": ExchangeType.DIRECT, "durable": True},
}

QUEUES = {
    "tasks.basic": {"durable": True},
    "tasks.work_queue": {"durable": True},
    "tasks.pubsub": {"durable": True},
    "tasks.routing": {"durable": True},
    "tasks.dlq": {"durable": True},
}

BINDINGS = [
    {"queue": "tasks.basic", "exchange": "tasks.direct", "routing_key": "task.basic"},
    {"queue": "tasks.work_queue", "exchange": "tasks.direct", "routing_key": "task.work_queue"},
    {"queue": "tasks.pubsub", "exchange": "tasks.fanout", "routing_key": ""},
    {"queue": "tasks.routing", "exchange": "tasks.topic", "routing_key": "task.*"},
    {"queue": "tasks.dlq", "exchange": "tasks.dlx", "routing_key": "dlq"},
]

DLX_ARGUMENTS = {
    "x-dead-letter-exchange": "tasks.dlx",
    "x-dead-letter-routing-key": "dlq",
}


async def declare_exchanges(channel: aio_pika.Channel) -> None:
    for name, config in EXCHANGES.items():
        await channel.declare_exchange(
            name=name,
            type=config["type"],
            durable=config["durable"],
        )


async def declare_queues(channel: aio_pika.Channel) -> None:
    for name, config in QUEUES.items():
        args = {}
        if name != "tasks.dlq":
            args = DLX_ARGUMENTS
        await channel.declare_queue(
            name=name,
            durable=config["durable"],
            arguments=args,
        )


async def declare_bindings(channel: aio_pika.Channel) -> None:
    for binding in BINDINGS:
        exchange = await channel.get_exchange(binding["exchange"])
        queue = await channel.get_queue(binding["queue"])
        await queue.bind(exchange, routing_key=binding["routing_key"])


async def setup_infrastructure() -> None:
    async with connection_manager.channel() as channel:
        await declare_exchanges(channel)
        await declare_queues(channel)
        await declare_bindings(channel)