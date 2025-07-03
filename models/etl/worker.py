import logging
from typing import Type

from temporalio.worker import Worker
from temporalio.client import Client

from models.etl.flow import ETLFlow
from models.etl.streaming_etl_flow import StreamingETLFlow


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLWorker:
    """
    Manager class for Temporal worker lifecycle in ETL operations.
    """
    
    def __init__(self, client: Client) -> None:
        """
        Initialize the ETL worker manager.
        
        Args:
            client: Connected Temporal client for server communication
        """
        self.client = client
        self._worker = Worker(
            self.client,
            task_queue=ETLFlow.queue_name,
            workflows=[ETLFlow],
            activities=ETLFlow.get_activities(),
        )

    async def run(self) -> None:
        """
        Start the worker to process ETL workflows from the task queue.
        """
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self._worker.task_queue)
        await self._worker.run()


class StreamingETLWorker:
    """
    Manager class for Temporal worker lifecycle in streaming ETL operations.
    """
    
    def __init__(self, client: Client) -> None:
        """
        Initialize the streaming ETL worker manager.
        
        Args:
            client: Connected Temporal client for server communication
        """
        self.client = client
        self._worker = Worker(
            self.client,
            task_queue=StreamingETLFlow.queue_name,
            workflows=[StreamingETLFlow],
            activities=StreamingETLFlow.get_activities(),
        )
    
    async def run(self):
        """
        Start the worker to process streaming ETL workflows from the task queue.
        """
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self._worker.task_queue)
        await self._worker.run()