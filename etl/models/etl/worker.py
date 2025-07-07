import logging

from temporalio.client import Client
from temporalio.worker import Worker

from config.worker import WorkerConfig

from models.etl.flow import ETLFlow
from models.etl.streaming_flow import StreamingETLFlow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLWorker:
    """Manager class for Temporal worker lifecycle in ETL operations."""

    async def __init__(self) -> None:
        """Initialize the ETL worker manager.

        Args:
            client: Connected Temporal client for server communication
        """
        config = WorkerConfig()
        client = await Client.connect(config.host)
        self._worker = Worker(
            client,
            task_queue=config.queue,
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

    async def __init__(self) -> None:
        """Initialize the streaming ETL worker manager.

        Args:
            client: Connected Temporal client for server communication
        """
        config = WorkerConfig()
        client = await Client.connect(config.host)
        self._worker = Worker(
            client,
            task_queue=config.queue,
            workflows=[StreamingETLFlow],
            activities=StreamingETLFlow.get_activities(),
        )

    async def run(self):
        """Start the worker to process streaming ETL workflows from the task queue."""
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self._worker.task_queue)
        await self._worker.run()
