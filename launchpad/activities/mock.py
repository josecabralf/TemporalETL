from launchpad.query import LaunchpadQuery

from models.activities import IActivities
from models.event import Event

from temporalio import activity

from launchpadlib.launchpad import Launchpad


class MockActivities(IActivities):
    """
    Mock implementation of Launchpad activities for testing purposes.
    """
    queue_name = "launchpad-mock-task-queue"

    @staticmethod
    def get_activities() -> list:
        return [extract_data, transform_data, load_data]


@activity.defn
async def extract_data(query: LaunchpadQuery) -> list:
    """
    Mock extract data from Launchpad.
    :param launchpad: The Launchpad API client instance.
    :param query: information about the data to extract.
    """

    # Log the query details
    print(f"Extracting data with query: {query.to_summary_base()}")

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
async def transform_data(data) -> list:
    """
    Mock transform scraped data to event format.
    :param data: The scraped data.
    :return: A list of events in the required format.
    """
    return [Event.create_from_data(
        event_id=str(item["id"]),
        parent_id="parent-id",
        week="2023-10-01",
        employee_id=str(item["employee_id"]),
        event_type="bug:created",
        event_time_utc="2023-10-01T00:00:00Z"
    ) for item in data]

@activity.defn
async def load_data(events: list) -> int:
    """
    Mock load data into the database.
    :param events: Events to load into the database.
    """
    print(f"Mock loading {len(events)} events into the database.")
    return len(events)