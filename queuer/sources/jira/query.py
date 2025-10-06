from dataclasses import dataclass

from models.etl.query import Query


@dataclass
class JiraQuery(Query):
    """Query implementation for Jira API data extraction."""

    issue_id: str
    date_start: str
    date_end: str

    def __init__(
        self,
        issue_id: str,
        date_start: str,
        date_end: str,
        source_kind_id: str,
        event_type: str,
    ) -> None:
        """Initialize JiraQuery with connection and scope parameters."""
        self.issue_id = issue_id
        self.date_start = date_start
        self.date_end = date_end
        super().__init__(source_kind_id, event_type)

    def to_dict(self) -> dict:
        """Convert the query parameters to a dictionary."""
        return {
            "issue_id": self.issue_id,
            "date_start": self.date_start,
            "date_end": self.date_end,
            "source_kind_id": self.source_kind_id,
            "event_type": self.event_type,
        }
