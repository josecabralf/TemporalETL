import logging
from datetime import timedelta
from typing import Any, Dict, List

from temporalio import activity, workflow

from db.db import Database
from models.event import Event
from models.etl.extract_cmd import ExtractStrategy
from models.etl.flow_input import ETLFlowInput
from models.etl.query import QueryFactory


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@workflow.defn
class ETLFlow:
    """
    Temporal workflow for processing Launchpad data through an ETL pipeline.
    """

    queue_name: str = "etl-task-queue"
    BATCH_SIZE: int = 1000

    @staticmethod
    def get_activities() -> List[Any]:
        """Return a list of activity functions to register with the Temporal worker."""
        return [get_etl_metadata, extract_data, transform_data, load_data]

    @workflow.run
    async def run(self, input: ETLFlowInput) -> Dict[str, Any]:
        """Execute the ETL workflow pipeline."""
        summary: Dict[str, Any] = {
            "workflow_id": workflow.info().workflow_id,
            "items_extracted": 0,
            "items_processed": 0,
            "items_inserted": 0,
        }

        metadata = await workflow.execute_activity(
            "get_etl_metadata",
            input,
            start_to_close_timeout=timedelta(minutes=1),
        )
        summary.update(metadata)

        extracted = await workflow.execute_activity(
            "extract_data",
            input,  # pass in input so that query can be reconstructed
            start_to_close_timeout=timedelta(minutes=10),
        )
        summary["items_extracted"] = len(extracted)
        logger.info(f"Extracted {len(extracted)} items from Launchpad.")

        transformed = await workflow.execute_activity(
            "transform_data",
            args=(extracted, input.args["source_kind_id"], input.args["event_type"]),
            start_to_close_timeout=timedelta(minutes=1),
        )
        summary["items_processed"] = len(transformed)
        logger.info(f"Transformed {len(transformed)} items.")

        inserted = await workflow.execute_activity(
            "load_data",
            transformed,
            start_to_close_timeout=timedelta(minutes=3),
        )
        summary["items_inserted"] = inserted
        logger.info(f"Inserted {inserted} items into the database.")

        return summary


@activity.defn
async def get_etl_metadata(input: ETLFlowInput) -> Dict[str, Any]:
    """Get metadata about the extraction to help inform the processing results."""
    return QueryFactory.create(input.query_type, args=input.args).to_summary_base()


@activity.defn
async def extract_data(input: ETLFlowInput) -> List[Dict[str, Any]]:
    query = QueryFactory.create(input.query_type, args=input.args)
    extract_data = ExtractStrategy.create(input.extract_strategy)
    logger.info(
        f"Extracting data using: {query.source_kind_id}.{query.event_type}.{extract_data.__name__} for query: {type(query).__name__}"
    )
    return await extract_data(query)


@activity.defn
async def transform_data(
    events: List[dict], source_kind_id: str, event_type: str
) -> List[Event]:
    return [
        Event(
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
        for e in events
    ]


@activity.defn
async def load_data(events: List[Event]) -> int:
    if not events:
        return 0

    db = Database()
    total_inserted = 0
    for i in range(0, len(events), ETLFlow.BATCH_SIZE):
        batch = events[i : i + ETLFlow.BATCH_SIZE]
        total_inserted += db.insert_events_batch(batch)
        loaded = i + len(batch)
        activity.heartbeat(f"Loaded {loaded}/{len(events)} events")

    return total_inserted
