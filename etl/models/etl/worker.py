from temporalio.client import Client
from temporalio.worker import Worker

from config.temporal import TemporalConfiguration

from models.etl.flow import ETLFlow
from models.etl.streaming_flow import StreamingETLFlow
from models.logger import logger


class ETLWorker:
    """Manager class for Temporal worker lifecycle in ETL operations."""

    def __init__(self, client: Client) -> None:
        self._worker = Worker(
            client=client,
            task_queue=TemporalConfiguration.queue,
            workflows=[ETLFlow],
            activities=ETLFlow.get_activities(),
        )

    async def run(self) -> None:
        """Start the worker to process ETL workflows from the task queue."""
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self._worker.task_queue)
        await self._worker.run()


class StreamingETLWorker:
    """Manager class for Temporal worker lifecycle in streaming ETL operations."""

    def __init__(self, client: Client) -> None:
        self._worker = Worker(
            client=client,
            task_queue=TemporalConfiguration.queue,
            workflows=[StreamingETLFlow],
            activities=StreamingETLFlow.get_activities(),
        )

    async def run(self):
        """Start the worker to process streaming ETL workflows from the task queue."""
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self._worker.task_queue)
        await self._worker.run()
