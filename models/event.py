from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class Event:
    """
    Represents a standardized event in the Worklytics format for database storage.
    
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
    
    id: str
    parent_id: str
    week: str
    employee_id: str
    type: str
    time_utc: str
    source_kind_id: str = "launchpad"
    relation_properties: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        """
        Validate and normalize event data after initialization.
        
        Raises:
            ValueError: If any required field is empty or invalid
        """
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
    
    def add_relation_property(self, key: str, value: Any) -> None:
        """
        Add a single property to the relation_properties dictionary.
        
        Args:
            key: Property key/name for the metadata field
            value: Property value, can be any JSON-serializable type
        """
        if self.relation_properties is None:
            self.relation_properties = {}
        self.relation_properties[key] = value
    
    def add_relation_properties(self, properties: Dict[str, Any]) -> None:
        """
        Add multiple properties to the relation_properties dictionary.
        
        Args:
            properties: Dictionary of key-value pairs to add to event metadata
        """
        if self.relation_properties is None:
            self.relation_properties = {}
        self.relation_properties.update(properties)

    def relation_properties_as_json(self) -> Optional[str]:
        """
        Serialize relation properties to a JSON string for database storage.
                
        Returns:
            JSON string representation of relation properties, or None if empty
        """
        import json
        return json.dumps(self.relation_properties, ensure_ascii=False) if self.relation_properties else None


def get_week_start_date(date_obj: datetime) -> str:
    """
    Calculate the Monday date for the week containing the given date.
    
    Args:
        date_obj: datetime object for which to find the week start
        
    Returns:
        ISO date string (YYYY-MM-DD) representing the Monday of the week
    """
    days_since_monday = date_obj.weekday()
    week_start = date_obj - timedelta(days=days_since_monday)
    return week_start.strftime('%Y-%m-%d')
