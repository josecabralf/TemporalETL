from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from models.date_utils import change_timezone, get_week_start_date


@dataclass
class Event:
    """Represents a standardized event in the Worklytics format for database storage.

    Attributes:
        id (str): Unique event identifier, typically combining entity type and ID
        parent_id (str): Identifier of the parent entity (bug ID, merge proposal ID, etc.)
        week (str): ISO date of the Monday of the week when the event occurred (YYYY-MM-DD)
        employee_id (str): Unique identifier for the employee/developer
        type (str): Event type following the format "entity:action"
                   (e.g., "bug:created", "merge_proposal:approved", "question:answered")
        time_utc (str): ISO timestamp of when the event occurred (YYYY-MM-DDTHH:MM:SSZ)
        source_kind_id (str): Source system identifier, defaults to "launchpad"
        relation_properties (Optional[Dict[str, Any]]): Additional event-specific metadata
                                                       stored as key-value pairs
    """

    id: Optional[int]  # serial4
    source_kind_id: str  # varchar
    parent_item_id: str  # varchar
    event_id: str  # varchar, unique identifier

    event_type: str  # varchar
    relation_type: str  # varchar

    employee_id: str  # varchar

    event_time_utc: str  # timestamp
    week: Optional[str]  # date
    timezone: Optional[str]  # varchar
    event_time: Optional[str]  # timestamp in timezone

    event_properties: Dict[str, Any]
    relation_properties: Dict[str, Any]
    metrics: Dict[str, Any]

    version: str
    specific_version: str

    def __post_init__(self) -> None:
        """Validate and normalize event data after initialization.

        Raises:
            ValueError: If any required field is empty or invalid
        """
        # ID is assigned by the database, so we don't validate it here
        # if not self.id:           raise ValueError("Event ID cannot be empty")
        if not self.source_kind_id:
            raise ValueError("Source kind ID cannot be empty")
        if not self.event_id:
            raise ValueError("Event ID cannot be empty")

        if not self.event_type:
            raise ValueError("Event type cannot be empty")
        if not self.relation_type:
            raise ValueError("Relation type cannot be empty")

        if not self.employee_id:
            raise ValueError("Employee ID cannot be empty")

        if not self.event_time_utc:
            raise ValueError("Event time cannot be empty")
        if not self.week:
            self.week = get_week_start_date(datetime.fromisoformat(self.event_time_utc))
        if not self.timezone:
            self.timezone = "UTC"
        if not self.event_time:
            self.event_time = change_timezone(
                self.event_time_utc, from_tz="UTC", to_tz=self.timezone
            )

        # Initialize empty dicts if None
        if self.relation_properties is None:
            self.relation_properties = {}
        if self.event_properties is None:
            self.event_properties = {}
        if self.metrics is None:
            self.metrics = {}
