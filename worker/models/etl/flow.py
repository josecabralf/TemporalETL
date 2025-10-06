import asyncio
from datetime import timedelta
from typing import Any, Dict, List, Tuple

from temporalio import activity, workflow

from external.wpe_db.client import WorkplaceDBClient

from models.etl.extract_strategy import ExtractStrategy
from models.etl.transform_strategy import TransformStrategy
from models.etl.input import ETLInput
from models.etl.query import QueryFactory
from models.event import Event
from models.logger import logger


@workflow.defn
class ETLFlow:
    """Temporal workflow for processing Launchpad data through an ETL pipeline."""

    BATCH_SIZE: int = 500
    MAX_CONCURRENT_CHUNKS: int = 3

    @staticmethod
    def get_activities() -> List[Any]:
        """Return a list of activity functions to register with the Temporal worker."""
        return [get_metadata, extract_data, transform_data, load_data]

    @workflow.run
    async def run(self, input: ETLInput) -> Dict[str, Any]:
        """Execute the ETL workflow pipeline."""
        metadata = await workflow.execute_activity(
            get_metadata,
            input,
            start_to_close_timeout=timedelta(minutes=1),
        )
        logger.info(
            f"ETL flow {workflow.info().workflow_id} metadata: {
                '\n  ' + '\n  '.join(f'{k}: {v}' for k, v in metadata.items())
            }"
        )
        summary = {
            **metadata,
            "workflow_id": workflow.info().workflow_id,
            "items_processed": 0,
            "items_inserted": 0,
            "chunks_processed": 0,
            "batch_size": self.BATCH_SIZE,
        }

        # Track processing state
        total_processed = 0
        total_inserted = 0
        batch_count = 0

        extracted = await workflow.execute_activity(
            extract_data,
            input,
            start_to_close_timeout=timedelta(hours=1),
        )
        summary["items_extracted"] = len(extracted)

        if not extracted:
            logger.warning("No data extracted. Exiting ETL workflow.")
            return summary
        logger.info(f"Extracted {len(extracted)} items.")

        # Semaphore to limit concurrent chunk processing
        concurrent_chunks = asyncio.Semaphore(self.MAX_CONCURRENT_CHUNKS)

        async def process_chunk(
            chunk_id: int, chunk_data: List[Dict[str, Any]]
        ) -> Tuple[int, int]:
            """Process a single chunk of data through transform and load stages"""
            async with concurrent_chunks:
                logger.info(f"Processing chunk {chunk_id} with {len(chunk_data)} items")

                transformed = await workflow.execute_activity(
                    transform_data,
                    args=(
                        chunk_data,
                        input.args["source_kind_id"],
                    ),
                    start_to_close_timeout=timedelta(minutes=10),
                )
                logger.info(f"Chunk {chunk_id}: transformed {len(transformed)} events")

                inserted = await workflow.execute_activity(
                    load_data,
                    transformed,
                    start_to_close_timeout=timedelta(minutes=10),
                )
                logger.info(f"Chunk {chunk_id}: inserted {inserted} records")

                return len(transformed), inserted

        # Process chunks
        chunk_tasks = []
        for i in range(0, len(extracted), self.BATCH_SIZE):
            batch = extracted[i : i + self.BATCH_SIZE]
            batch_count += 1
            task = asyncio.create_task(process_chunk(i, batch))
            chunk_tasks.append(task)

            if len(chunk_tasks) >= self.MAX_CONCURRENT_CHUNKS:
                done, pending = await asyncio.wait(
                    chunk_tasks, return_when=asyncio.FIRST_COMPLETED
                )

                for completed_task in done:
                    processed, inserted = await completed_task
                    total_processed += processed
                    total_inserted += inserted

                chunk_tasks = list(pending)

        if chunk_tasks:
            results = await asyncio.gather(*chunk_tasks)
            for processed, inserted in results:
                total_processed += processed
                total_inserted += inserted

        summary = {
            **summary,
            "items_processed": total_processed,
            "items_inserted": total_inserted,
            "chunks_processed": batch_count,
        }

        logger.info(
            f"ETL completed: {'\n  ' + '\n  '.join(f'{k}: {v}' for k, v in summary.items())}"
        )

        return summary


@activity.defn
async def get_metadata(input: ETLInput) -> Dict[str, Any]:
    """Get metadata about the extraction to help inform the processing results."""
    return QueryFactory.create(input.query_type, args=input.args).to_summary_base()


@activity.defn
async def extract_data(input: ETLInput) -> List[Dict[str, Any]]:
    query = QueryFactory.create(input.query_type, args=input.args)
    strategy_key = f"{query.source_kind_id}-{query.event_type}"
    extract_data = ExtractStrategy.create(strategy_key)
    logger.info(
        f"Extracting data using: {query.source_kind_id}.{query.event_type}.{extract_data.__name__} for query: {type(query).__name__}"
    )
    return await extract_data(query)


@activity.defn
async def transform_data(events: List[dict], source_kind_id: str) -> List[Event]:
    transform_data = TransformStrategy.create(source_kind_id)
    logger.info(f"Transforming {source_kind_id} data ({len(events)} events)")
    transformed = transform_data(events)
    logger.info(f"Successfully transformed {len(transformed)}/{len(events)} events")
    return transformed


@activity.defn
async def load_data(events: List[Event]) -> int:
    events_table = f"{events[0].source_kind_id}_events" if events else "events"
    logger.info(f"Inserting batch of {len(events)} events into {events_table}")

    return WorkplaceDBClient().insert_events_batch(events, events_table)
