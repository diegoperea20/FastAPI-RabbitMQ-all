import logging
import sys
import threading
import signal
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rabbitmq import (
    basic_worker,
    create_worker,
    create_subscriber,
    create_routing_worker,
    dlq_consumer,
    setup_infrastructure,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


workers = []
threads = []
shutdown_event = threading.Event()


def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}, shutting down workers...")
    shutdown_event.set()
    for worker in workers:
        worker.stop()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def run_worker(worker, name):
    try:
        logger.info(f"Starting {name}")
        worker.start()
    except Exception as e:
        logger.error(f"Error in {name}: {e}", exc_info=True)


def main():
    logger.info("Setting up RabbitMQ infrastructure...")
    setup_infrastructure()

    worker_configs = [
        ("basic_worker", basic_worker),
        ("work_queue_worker_1", create_worker(1)),
        ("work_queue_worker_2", create_worker(2)),
        ("pubsub_subscriber_1", create_subscriber(1)),
        ("pubsub_subscriber_2", create_subscriber(2)),
        ("routing_worker_1", create_routing_worker(1, "task.*")),
        ("routing_worker_2", create_routing_worker(2, "task.high.*")),
        ("dlq_consumer", dlq_consumer),
    ]

    for name, worker in worker_configs:
        workers.append(worker)
        thread = threading.Thread(target=run_worker, args=(worker, name), daemon=True)
        threads.append(thread)
        thread.start()

    logger.info(f"Started {len(workers)} workers")
    logger.info("Workers running. Press Ctrl+C to stop.")

    shutdown_event.wait()

    logger.info("Waiting for threads to finish...")
    for thread in threads:
        thread.join(timeout=5)

    logger.info("All workers stopped")


if __name__ == "__main__":
    main()