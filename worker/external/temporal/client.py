from typing import Sequence
from temporalio.client import Client
from temporalio.worker import Worker

from external.temporal.config import TemporalConfig

from models.logger import logger


class TemporalClient:
    """API class for interacting with the Temporal server."""

    @classmethod
    async def _create_client(cls):
        return await Client.connect(
            target_host=TemporalConfig.host,
            namespace=TemporalConfig.namespace,
        )

    @classmethod
    async def create_worker(cls, workflows: Sequence, activities: Sequence):
        client = await cls._create_client()

        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", TemporalConfig.queue)

        return Worker(
            client=client,
            task_queue=TemporalConfig.queue,
            workflows=workflows,
            activities=activities,
        )
