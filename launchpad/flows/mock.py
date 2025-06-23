from typing import List, Any, Dict
from launchpad.query import LaunchpadQuery
from models.etl_flow import ETLFlow
from models.event import Event
from temporalio import activity, workflow
from launchpadlib.launchpad import Launchpad


class MockFlow(ETLFlow):
    """
    Mock implementation of Launchpad ETL workflow for testing and development.
    """
    queue_name = "launchpad-mock-task-queue"

    @staticmethod
    def get_activities() -> List[Any]:
        return [extract_data, transform_data, load_data]


@activity.defn
async def extract_data(query: LaunchpadQuery) -> List[Dict[str, Any]]:
    # test creating launchpad instance
    lp = Launchpad.login_with(
        application_name=query.application_name,
        service_root=query.service_root,
        version=query.version,
    )

    return [
        {"id": 1, "employee_id": "petergriffin"}, 
        {"id": 2, "employee_id": "john-cook"}, 
        {"id": 3, "employee_id": "taskmaster"}]

@activity.defn
async def transform_data(data: List[Dict[str, Any]]) -> List[Event]:
    return [Event(
        id=None,
        source_kind_id="mock_source",
        parent_item_id=str(item["id"]),
        event_id=f"mock_event_{item['id']}",
        event_type="mock_event_type",
        relation_type="mock_relation_type",
        employee_id=item["employee_id"],
        event_time_utc="2023-10-01T12:00:00Z",
        week=None,  # This can be calculated based on event_time_utc
        timezone="UTC",
        event_time="2023-10-01T12:00:00Z",
        event_properties=None,
        relation_properties=None,
        metrics=None
    ) for item in data]

@activity.defn
async def load_data(events: List[Event]) -> int:
    print(f"Mock loading {len(events)} events into the database.")
    return len(events)