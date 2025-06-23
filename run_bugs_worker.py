import os
import asyncio
from temporalio.client import Client
from launchpad.worker import LaunchpadWorker
from launchpad.flows.bugs import BugsFlow


async def main():
    # Connect to local Temporal service
    client = await Client.connect(TEMPORAL_HOST)
    
    # Create worker - this needs to be done in async context
    worker = LaunchpadWorker(client, BugsFlow.queue_name, BugsFlow)
    
    # Run the worker
    await worker.run()


if __name__ == "__main__":
    TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")

    asyncio.run(main())