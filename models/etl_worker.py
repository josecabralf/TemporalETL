import logging
from typing import Type

from temporalio.worker import Worker
from temporalio.client import Client

from models.etl_flow import ETLFlow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLWorker:
    """
    Manager class for Temporal worker lifecycle in ETL operations.
    """
    
    def __init__(self, client: Client, task_queue: str, etl_workflow_type: Type[ETLFlow]) -> None:
        """
        Initialize the ETL worker manager.
        
        Args:
            client: Connected Temporal client for server communication
            task_queue: Name of the task queue for this worker to poll
            etl_workflow_type: Flow class that defines the workflow and activities
        """
        self.client = client
        self.task_queue = task_queue
        self.etl_workflow_type = etl_workflow_type
        self.worker = None

    def get_worker(self) -> Worker:
        """
        Create and configure a Temporal worker for ETL workflows.
        
        Returns:
            Configured Temporal worker ready to process workflows and activities
        """
        logger.info("Creating Temporal worker for task queue: %s", self.task_queue)        
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
        self.worker = self.worker if self.worker else self.get_worker()
        
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self.worker.task_queue)
        await self.worker.run()