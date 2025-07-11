import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from config.temporal import TemporalConfiguration

from models.etl.flow import ETLFlow
from models.logger import logger


async def start_worker():
    client = await Client.connect(
        target_host=TemporalConfiguration.host,
        namespace=TemporalConfiguration.namespace,
    )
    worker = Worker(
        client=client,
        task_queue=TemporalConfiguration.queue,
        workflows=[ETLFlow],
        activities=ETLFlow.get_activities(),
    )

    logger.info("Starting Temporal worker...")
    logger.info("Listening on task queue: %s", TemporalConfiguration.queue)

    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(start_worker())
    except KeyboardInterrupt:
        print("Worker stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
