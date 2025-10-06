from typing import List, Dict

from external.salesforce.client import SalesforceClient

from models.event import Event
from models.hash import sha256
from models.logger import logger
from models.etl.transform_strategy import transform_method

from sources.launchpad.query import LaunchpadQuery


@transform_method("launchpad")
def transform_data(events: List[Dict]) -> List[Event]:
    """Transform the launchpad data as per the requirements."""
    employee_hrc_map = SalesforceClient.get_launchpad_employee_ids()

    transformed = []
    for event in events:
        launchpad_id = event.get("employee_id")
        if not launchpad_id:
            raise ValueError("Employee ID is required for transformation")

        try:
            employee_id = employee_hrc_map.get(launchpad_id, None)
            if not employee_id:  # Not a member
                employee_id = sha256(launchpad_id)  # Anonymize ID

            e = Event(
                id=None,  # Assigned by the database
                source_kind_id="launchpad",
                parent_item_id=event["parent_item_id"],
                event_id=event["event_id"],
                event_type=event["event_type"],
                relation_type=event["relation_type"],
                employee_id=employee_id,
                event_time_utc=event["event_time_utc"],
                week=None,  # Calculated in __post_init__
                timezone=event.get("time_zone", "UTC"),
                event_time=None,  # Calculated in __post_init__
                event_properties=event.get("event_properties", {}),
                relation_properties=event.get("relation_properties", {}),
                metrics=event.get("metrics", {}),
                version=LaunchpadQuery.version(),
                specific_version=LaunchpadQuery.specific_version(),
            )

            if e.event_type == "question_created":  # assignee may be an employee
                assignee = e.event_properties.get("assignee", "")
                id = employee_hrc_map.get(assignee, None)
                e.relation_properties["assignee"] = id if id else sha256(assignee)

            transformed.append(e)
        except Exception as ex:
            logger.error(
                f"Error transforming event {event.get('event_id', 'unknown')}: {ex}"
            )
            continue

    return transformed
