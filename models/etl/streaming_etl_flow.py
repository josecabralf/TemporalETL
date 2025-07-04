import logging
import asyncio
from datetime import timedelta
from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass

from temporalio import activity, workflow

from db.db import Database
from models.event import Event
from models.etl.extract_cmd import ExtractStrategy
from models.etl.flow_input import ETLFlowInput
from models.etl.query import QueryFactory


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class StreamingConfig:
    """Configuration for streaming ETL pipeline"""

    extract_chunk_size: int = 100  # Number of items to extract per chunk
    transform_batch_size: int = 1000  # Number of events to transform per batch
    load_batch_size: int = 500  # Number of events to load per batch
    max_concurrent_chunks: int = 3  # Maximum concurrent processing chunks
    memory_threshold_mb: int = 500  # Memory threshold to trigger backpressure


@workflow.defn
class StreamingETLFlow:
    """
    Streaming Temporal workflow for processing data through an ETL pipeline.
    Processes data in chunks to optimize memory usage and enable processing of large datasets.
    """

    queue_name: str = "streaming-etl-task-queue"

    @staticmethod
    def get_activities() -> List[Any]:
        """Return list of activity functions for registration with Temporal worker."""
        return [
            get_etl_metadata,
            streaming_extract_data,
            transform_data_batch,
            load_data_batch,
        ]

    @workflow.run
    async def run(
        self, input: ETLFlowInput, config: Optional[StreamingConfig] = None
    ) -> Dict[str, Any]:
        """
        Execute the streaming ETL workflow pipeline.

        Args:
            input: FlowInput containing workflow parameters
            config: Optional streaming configuration, uses defaults if not provided

        Returns:
            Dictionary containing workflow execution summary
        """
        if config is None:
            config = StreamingConfig()

        summary: Dict[str, Any] = {
            "workflow_id": workflow.info().workflow_id,
            "items_processed": 0,
            "items_inserted": 0,
            "chunks_processed": 0,
            "config": {
                "extract_chunk_size": config.extract_chunk_size,
                "transform_batch_size": config.transform_batch_size,
                "load_batch_size": config.load_batch_size,
            },
        }

        # Get metadata about the extraction to plan processing
        metadata = await workflow.execute_activity(
            "get_extraction_metadata",
            input,
            start_to_close_timeout=timedelta(minutes=2),
        )
        summary.update(metadata)

        # Track processing state
        total_processed = 0
        total_inserted = 0
        chunk_count = 0

        # Semaphore to limit concurrent chunk processing
        concurrent_chunks = asyncio.Semaphore(config.max_concurrent_chunks)

        async def process_chunk(
            chunk_id: int, chunk_data: List[Dict[str, Any]]
        ) -> Tuple[int, int]:
            """Process a single chunk of data through transform and load stages"""
            async with concurrent_chunks:
                logger.info(f"Processing chunk {chunk_id} with {len(chunk_data)} items")

                transformed = await workflow.execute_activity(
                    "transform_data_batch",
                    args=(
                        chunk_data,
                        input.args["source_kind_id"],
                        input.args["event_type"],
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                )
                inserted = await workflow.execute_activity(
                    "load_data_batch",
                    args=(transformed, config.load_batch_size),
                    start_to_close_timeout=timedelta(minutes=10),
                )

                logger.info(
                    f"Chunk {chunk_id}: transformed {len(transformed)} events, inserted {inserted} records"
                )
                return len(transformed), inserted

        # Start streaming extraction - get all chunks first, then process
        chunks = await workflow.execute_activity(
            "streaming_extract_data",
            args=(input, config.extract_chunk_size),
            start_to_close_timeout=timedelta(hours=1),
        )

        # Process each chunk
        chunk_tasks = []
        for chunk_id, chunk_data in chunks:
            chunk_count += 1

            task = asyncio.create_task(process_chunk(chunk_id, chunk_data))
            chunk_tasks.append(task)

            if len(chunk_tasks) >= config.max_concurrent_chunks:
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

        summary.update(
            {
                "items_processed": total_processed,
                "items_inserted": total_inserted,
                "chunks_processed": chunk_count,
            }
        )

        logger.info(
            f"Streaming ETL completed: {total_processed} items processed, {total_inserted} items inserted across {chunk_count} chunks"
        )
        return summary


@activity.defn
async def get_etl_metadata(input: ETLFlowInput) -> Dict[str, Any]:
    """
    Get metadata about the extraction to help inform the processing results.
    """
    return QueryFactory.create(input.query_type, args=input.args).to_summary_base()


@activity.defn
async def streaming_extract_data(
    input: ETLFlowInput, chunk_size: int
) -> List[Tuple[int, List[Dict[str, Any]]]]:
    query = QueryFactory.create(input.query_type, args=input.args)
    extract_method = ExtractStrategy.create(input.extract_strategy)

    if not getattr(extract_method, "is_streaming", False):
        logger.error(
            "Extract method %s does not support streaming extraction. Returning empty list.",
            input.extract_strategy,
        )
        return []

    chunks = []
    chunk_id = 0
    async for chunk_data in extract_method(query, chunk_size):
        logger.info(f"Received streaming chunk {chunk_id} with {len(chunk_data)} items")
        chunks.append((chunk_id, chunk_data))
        chunk_id += 1
        activity.heartbeat(f"Processed chunk {chunk_id} with {len(chunk_data)} items")

    return chunks


@activity.defn
async def transform_data_batch(
    events: List[dict], source_kind_id: str, event_type: str
) -> List[Event]:
    logger.info(f"Transforming batch of {len(events)} events")

    transformed_events = []
    for e in events:
        try:
            event = Event(
                id=None,  # ID will be assigned by the database
                source_kind_id=source_kind_id,
                parent_item_id=e["parent_item_id"],
                event_id=e["event_id"],
                event_type=event_type,
                relation_type=e["relation_type"],
                employee_id=e["employee_id"],
                event_time_utc=e["event_time_utc"],
                week=None,  # Calculated in __post_init__
                timezone=e.get("time_zone", "UTC"),
                event_time=None,  # Calculated in __post_init__
                event_properties=e.get("event_properties", {}),
                relation_properties=e.get("relation_properties", {}),
                metrics=e.get("metrics", {}),
            )
            transformed_events.append(event)
        except Exception as ex:
            logger.error(
                f"Error transforming event {e.get('event_id', 'unknown')}: {ex}"
            )
            continue

    logger.info(
        f"Successfully transformed {len(transformed_events)} out of {len(events)} events"
    )
    return transformed_events


@activity.defn
async def load_data_batch(events: List[Event], batch_size: int) -> int:
    if not events:
        return 0

    db = Database()
    total_inserted = 0
    for i in range(0, len(events), batch_size):
        sub_batch = events[i : i + batch_size]
        total_inserted += db.insert_events_batch(sub_batch)
        loaded = i + batch_size
        activity.heartbeat(f"Loaded {loaded}/{len(events)} events")

    return total_inserted
