from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class Event:
    """
    Represents a Launchpad event to be stored in the database.
    
    Attributes:
        id: Unique event identifier
        week: Date of the first day of the week the event occurred (YYYY-MM-DD format)
        employee_id: Identifier for the employee
        source_kind_id: Source identifier (defaults to "launchpad")
        event_type: Type of event (e.g., "bug:created", "bug:commented", "merge_proposal:approved")
        event_time_utc: UTC timestamp of when the event occurred
        relation_properties: Dictionary containing additional event-specific information
    """
    
    id: str
    parent_id: str
    week: str
    employee_id: str
    type: str
    time_utc: str
    source_kind_id: str = "launchpad"
    relation_properties: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Validate and normalize event data after initialization."""
        if not self.id:
            raise ValueError("Event ID cannot be empty")
        
        if not self.parent_id:
            raise ValueError("Parent ID cannot be empty")
        
        if not self.employee_id:
            raise ValueError("Employee ID cannot be empty")
        
        if not self.type:
            raise ValueError("Event type cannot be empty")
        
        if not self.time_utc:
            raise ValueError("Event time cannot be empty")
        
        if not self.week:
            self.week = get_week_start_date(datetime.strptime(self.time_utc, '%Y-%m-%dT%H:%M:%SZ'))
        
        # Initialize relation_properties as empty dict if None
        if self.relation_properties is None:
            self.relation_properties = {}
    
    @classmethod
    def create_from_data(
        cls,
        event_id: str,
        parent_id: str,
        week: str,
        employee_id: str,
        event_type: str,
        event_time_utc: str,
        relation_properties: Optional[Dict[str, Any]] = None,
        source_kind_id: str = "launchpad"
    ) -> "Event":
        """
        Create an Event instance from individual data fields.
        
        Args:
            event_id: Unique event identifier
            parent_id: Unique identifier for the parent entity (e.g. bug, merge proposal, etc.)
            week: Week start date (YYYY-MM-DD)
            employee_id: Employee identifier
            event_type: Event type string
            event_time_utc: UTC timestamp string
            relation_properties: Additional event data
            source_kind_id: Source identifier
            
        Returns:
            Event instance
        """
        return cls(
            id=event_id,
            parent_id=parent_id,
            week=week,
            employee_id=employee_id,
            type=event_type,
            time_utc=event_time_utc,
            source_kind_id=source_kind_id,
            relation_properties=relation_properties or {}
        )
    
    def add_relation_property(self, key: str, value: Any) -> None:
        """
        Add a property to the relation_properties dictionary.
        
        Args:
            key: Property key
            value: Property value
        """
        if self.relation_properties is None:
            self.relation_properties = {}
        self.relation_properties[key] = value
    
    def add_relation_properties(self, properties: Dict[str, Any]) -> None:
        """
        Add multiple properties to the relation_properties dictionary.
        
        Args:
            properties: Dictionary of properties to add
        """
        if self.relation_properties is None:
            self.relation_properties = {}
        self.relation_properties.update(properties)

    def relation_properties_as_json(self) -> str | None:
        """
        Get relation properties as a JSON string.
        
        Returns:
            str: JSON string representation of relation properties
        """
        import json
        return json.dumps(self.relation_properties, ensure_ascii=False) if self.relation_properties else None


def get_week_start_date(date_obj: datetime) -> str:
    """
    Get the start date (Monday) of the week for a given date.
    
    Args:
        date_obj: datetime object
        
    Returns:
        str: Date in YYYY-MM-DD format representing the start of the week
    """
    days_since_monday = date_obj.weekday()
    week_start = date_obj - timedelta(days=days_since_monday)
    return week_start.strftime('%Y-%m-%d')
