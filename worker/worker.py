import asyncio

from external.temporal.client import TemporalClient

from models.etl.flow import ETLFlow


async def start_worker():
    worker = await TemporalClient.create_worker(
        workflows=[ETLFlow],
        activities=ETLFlow.get_activities(),
    )
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(start_worker())
    except KeyboardInterrupt:
        print("Worker stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
