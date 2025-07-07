import asyncio
import sys

from models.etl.worker import ETLWorker, StreamingETLWorker


async def start_worker():
    await ETLWorker().run()


async def start_streaming_worker():
    await StreamingETLWorker().run()


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
