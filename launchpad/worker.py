import logging
from typing import Type

from temporalio.worker import Worker
from temporalio.client import Client

from models.activities import IActivities

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LaunchpadWorker:
    """
    A class to manage the Launchpad worker lifecycle.
    This class is used to start and stop the worker for processing workflows.
    """
    
    def __init__(self, client: Client, task_queue: str, lp_workflow_type: Type[IActivities]):
        self.client = client
        self.task_queue = task_queue
        self.lp_workflow_type = lp_workflow_type
        self.worker = None

    def get_worker(self) -> Worker:
        """
        Create and configure a Temporal worker for the Launchpad workflow.
        """
        # Import the workflow class here to avoid event loop issues
        from models.etl_workflow import ETLWorkflow

        logger.info("Creating Temporal worker for task queue: %s", self.task_queue)        
        return Worker(
            self.client,
            task_queue=self.task_queue,
            workflows=[ETLWorkflow],
            activities=self.lp_workflow_type.get_activities(),
        )

    async def run(self):
        """
        Start the worker to process workflows.
        """
        if self.worker is None:
            self.worker = self.get_worker()
            
        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", self.worker.task_queue)

        await self.worker.run()