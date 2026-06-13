import asyncio
import logging
import signal
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rabbitmq import (
    basic_worker,
    create_worker,
    create_subscriber,
    create_routing_worker,
    dlq_consumer,
    setup_infrastructure,
    connection_manager,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def run_worker(worker, name):
    try:
        logger.info(f"Starting {name}")
        await worker.start()
    except asyncio.CancelledError:
        logger.info(f"{name} cancelled")
    except Exception as e:
        logger.error(f"Error in {name}: {e}", exc_info=True)


async def main():
    logger.info("Setting up RabbitMQ infrastructure...")
    await connection_manager.connect()
    await setup_infrastructure()

    workers = [
        ("basic_worker", basic_worker),
        ("work_queue_worker_1", create_worker(1)),
        ("work_queue_worker_2", create_worker(2)),
        ("pubsub_subscriber_1", create_subscriber(1)),
        ("pubsub_subscriber_2", create_subscriber(2)),
        ("routing_worker_1", create_routing_worker(1, "task.*")),
        ("routing_worker_2", create_routing_worker(2, "task.high.*")),
        ("dlq_consumer", dlq_consumer),
    ]

    tasks = [
        asyncio.create_task(run_worker(worker, name))
        for name, worker in workers
    ]

    logger.info(f"Started {len(tasks)} workers")
    logger.info("Workers running. Press Ctrl+C to stop.")

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Shutdown signal received")
    finally:
        logger.info("Stopping workers...")
        for _, worker in workers:
            await worker.stop()
        await connection_manager.close()
        logger.info("All workers stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting...")