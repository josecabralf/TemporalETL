import os
import asyncio
from temporalio.client import Client
from models.etl.worker import ETLWorker


async def main():
    # Connect to local Temporal service
    client = await Client.connect(TEMPORAL_HOST)

    # Create worker - this needs to be done in async context
    worker = ETLWorker(client)

    # Run the worker
    await worker.run()


if __name__ == "__main__":
    TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
