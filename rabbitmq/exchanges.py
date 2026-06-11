from .connection import create_connection

EXCHANGES = {
    "tasks.direct": {"type": "direct", "durable": True},
    "tasks.fanout": {"type": "fanout", "durable": True},
    "tasks.topic": {"type": "topic", "durable": True},
    "tasks.dlx": {"type": "direct", "durable": True},
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


def declare_exchanges(channel) -> None:
    for name, config in EXCHANGES.items():
        channel.exchange_declare(
            exchange=name,
            exchange_type=config["type"],
            durable=config["durable"],
        )


def declare_queues(channel) -> None:
    for name, config in QUEUES.items():
        args = {}
        if name != "tasks.dlq":
            args = DLX_ARGUMENTS
        channel.queue_declare(
            queue=name,
            durable=config["durable"],
            arguments=args,
        )


def declare_bindings(channel) -> None:
    for binding in BINDINGS:
        channel.queue_bind(
            queue=binding["queue"],
            exchange=binding["exchange"],
            routing_key=binding["routing_key"],
        )


def setup_infrastructure() -> None:
    conn = create_connection()
    with conn.channel() as channel:
        declare_exchanges(channel)
        declare_queues(channel)
        declare_bindings(channel)
    conn.close()