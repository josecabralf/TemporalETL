import asyncio
import sys

from temporalio.client import Client

from config.temporal import TemporalConfiguration

from models.etl.worker import ETLWorker, StreamingETLWorker


async def start_worker():
    client = await Client.connect(
        TemporalConfiguration.host, namespace=TemporalConfiguration.namespace
    )
    await ETLWorker(client).run()


async def start_streaming_worker():
    client = await Client.connect(
        TemporalConfiguration.host, namespace=TemporalConfiguration.namespace
    )
    await StreamingETLWorker(client).run()


if __name__ == "__main__":
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
