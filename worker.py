import os
import sys
import asyncio
from temporalio.client import Client
from models.etl.worker import ETLWorker, StreamingETLWorker


async def start_worker():
    client = await Client.connect(TEMPORAL_HOST)
    await ETLWorker(client).run()


async def start_streaming_worker():
    client = await Client.connect(TEMPORAL_HOST)
    await StreamingETLWorker(client).run()


if __name__ == "__main__":
    TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")

    # read args and check if streaming worker
    streaming = len(sys.argv) > 1 and (
        sys.argv[1] == "-s" or sys.argv[1] == "--streaming"
    )

    try:
        if streaming:
            asyncio.run(start_streaming_worker())
        else:
            asyncio.run(start_worker())
    except KeyboardInterrupt:
        print("Worker stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
