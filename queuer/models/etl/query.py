from dataclasses import dataclass


@dataclass
class Query:
    """Abstract base class for all ETL query operations."""

    source_kind_id: str
    event_type: str

    def __init__(self, source_kind_id: str, event_type: str) -> None:
        """Initialize the query with source kind and event type."""
        self.source_kind_id = source_kind_id
        self.event_type = event_type

    def to_dict(self) -> dict:
        raise NotImplementedError(
            "Subclasses must implement the to_dict method to convert query parameters to a dictionary."
        )
