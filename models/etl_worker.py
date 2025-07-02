import logging
from typing import Type

from temporalio.worker import Worker
from temporalio.client import Client

from models.etl_flow import ETLFlow
from models.streaming_etl_flow import StreamingETLFlow


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
        self._worker = None

    def _get_worker(self) -> Worker:
        """
        Create and configure a Temporal worker for ETL workflows.
        
        Returns:
            Configured Temporal worker ready to process workflows and activities
        """
        logger.info("Creating Temporal worker for task queue: %s", ETLFlow.queue_name)        
        return Worker(
            self.client,
            task_queue=ETLFlow.queue_name,
            workflows=[ETLFlow],
            activities=ETLFlow.get_activities(),
        )

    async def run(self) -> None:
        """
        Start the worker to process ETL workflows from the task queue.
        
        The method runs indefinitely until interrupted or an error occurs.
        It provides comprehensive logging of worker status and task queue information.
        """
        self._worker = self._worker if self._worker else self._get_worker()
        
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self._worker.task_queue)
        
        await self._worker.run()


class StreamingETLWorker:
    """
    Specialized worker for streaming ETL operations.
    
    Inherits from ETLWorker and can be extended with additional streaming-specific functionality.
    """
    
    def __init__(self, client: Client) -> None:
        """
        Initialize the streaming ETL worker manager.
        
        Args:
            client: Connected Temporal client for server communication
        """
        self.client = client
        self._worker = None
    
    def _get_worker(self) -> Worker:
        """
        Create and configure a Temporal worker for streaming ETL workflows.
        
        Returns:
            Configured Temporal worker ready to process streaming workflows and activities
        """
        logger.info("Creating Temporal worker for streaming task queue: %s", ETLFlow.queue_name)
        return Worker(
            self.client,
            task_queue=StreamingETLFlow.queue_name,
            workflows=[StreamingETLFlow],
            activities=StreamingETLFlow.get_activities(),
        )
    
    async def run(self):
        """
        Start the worker to process ETL workflows from the task queue.
        
        The method runs indefinitely until interrupted or an error occurs.
        It provides comprehensive logging of worker status and task queue information.
        """
        self._worker = self._worker if self._worker else self._get_worker()
        
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self._worker.task_queue)
        
        await self._worker.run()