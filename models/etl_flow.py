import logging
from datetime import timedelta
from typing import Dict, List, Any

from temporalio import activity, workflow

from models.event import Event
from models.query import QueryFactory
from models.extract_cmd import ExtractMethodFactory


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
        """
        Return a list of activity functions to register with the Temporal worker.
        
        The activities should be defined in the same file as the workflow class
        or imported from related modules.
        
        Returns:
            List of activity function references that will be registered
            with the Temporal worker.
            
        Raises:
            NotImplementedError: If not implemented by concrete class.
            
        Example:
            @staticmethod
            def get_activities() -> List[Any]:
                return [extract_data, transform_data, load_data]
        """
        return [extract_data, transform_data, load_data]

    
    @workflow.run
    async def run(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the ETL workflow pipeline.
        
        Args:
            input: Dictionary containing workflow parameters with structure:
                - type: Query type identifier (e.g., "LaunchpadQuery")
                - args: Dictionary of query-specific arguments
                
        Returns:
            Dictionary containing workflow execution summary
                
        Raises:
            ActivityError: If any activity fails after exhausting retries
            ValueError: If input parameters are invalid or missing
        """
        query = QueryFactory.create(input["type"], args=input["args"])

        summary = query.to_summary_base()
        summary["workflow_id"] = workflow.info().workflow_id

        extracted = await workflow.execute_activity(
            "extract_data", input, # pass in input so that query can be reconstructed
            start_to_close_timeout=timedelta(minutes=10),
        )
        logger.info(f"Extracted {len(extracted)} items from Launchpad.")

        transformed = await workflow.execute_activity(
            "transform_data", args=(extracted, query.source_kind_id, query.event_type),
            start_to_close_timeout=timedelta(minutes=1),
        )
        summary["items_processed"] = len(transformed)
        logger.info(f"Transformed {len(transformed)} items.")

        inserted = await workflow.execute_activity(
            "load_data", transformed,
            start_to_close_timeout=timedelta(minutes=3),
        )
        summary["items_inserted"] = inserted
        logger.info(f"Inserted {inserted} items into the database.")

        return summary


@activity.defn
async def extract_data(input: Dict[str, Any]) -> List[dict]:
    query = QueryFactory.create(input["type"], args=input["args"])
    extract_data = ExtractMethodFactory.create(f'{query.source_kind_id}-{query.event_type}')
    logger.info(f"Extracting data using method: {query.source_kind_id}.{query.event_type}.{extract_data.__name__} for query: {type(query).__name__}")
    return await extract_data(query)


@activity.defn
async def transform_data(events: List[dict], source_kind_id: str, event_type: str) -> List[Event]:
    return [Event(
        id=                     None,  # ID will be assigned by the database
        source_kind_id=         source_kind_id,
        parent_item_id=         e['parent_item_id'],
        event_id=               e['event_id'],

        event_type=             event_type,
        relation_type=          e['relation_type'],

        employee_id=            e['employee_id'],

        event_time_utc=         e['event_time_utc'],
        week=                   None, # Calculated in __post_init__
        timezone=               e.get('time_zone', 'UTC'),
        event_time=             None, # Calculated in __post_init__

        event_properties=       e.get('event_properties', {}),
        relation_properties=    e.get('relation_properties', {}),
        metrics=                e.get('metrics', {})
    ) for e in events]


@activity.defn
async def load_data(events: List[Event]) -> int:
    from db.db import Database
    db = Database()

    already_inserted: int = int(activity.info().heartbeat_details[0]) if activity.info().heartbeat_details else 0
    loaded = len(events)

    for i in range(already_inserted, loaded, ETLFlow.BATCH_SIZE):
        batch = events[i:i + ETLFlow.BATCH_SIZE]
        db.insert_events_batch(batch)

        inserted = i + len(batch)
        activity.heartbeat(inserted)

    return loaded