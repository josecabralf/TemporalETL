from dataclasses import dataclass

from models.etl.query import Query


@dataclass
class LaunchpadQuery(Query):
    """Query implementation for Launchpad API data extraction."""

    member: str
    date_start: str
    date_end: str

    def __init__(
        self,
        member: str,
        date_start: str,
        date_end: str,
        source_kind_id: str,
        event_type: str,
    ) -> None:
        """Initialize LaunchpadQuery with connection and scope parameters."""
        self.member = member
        self.date_start = date_start
        self.date_end = date_end
        super().__init__(source_kind_id, event_type)

    def to_dict(self) -> dict:
        """Convert the query parameters to a dictionary."""
        return {
            "member": self.member,
            "date_start": self.date_start,
            "date_end": self.date_end,
            "source_kind_id": self.source_kind_id,
            "event_type": self.event_type,
        }
