from dataclasses import dataclass
from typing import Any, Dict
from models.etl.query import Query, query_type


@dataclass
@query_type("jira")
class JiraQuery(Query):
    """Query implementation for Jira API data extraction."""

    issue_id: str
    date_start: str
    date_end: str

    _version = "1.1.0"

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

    @classmethod
    def version(cls) -> str:
        return cls._version.split(".")[0]

    @classmethod
    def specific_version(cls) -> str:
        return cls._version

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Query":
        """Create a query instance from a dictionary of parameters."""
        return JiraQuery(
            issue_id=data["issue_id"],
            date_start=data["date_start"],
            date_end=data["date_end"],
            source_kind_id=data["source_kind_id"],
            event_type=data["event_type"],
        )

    def to_summary_base(self) -> Dict[str, Any]:
        """Convert the query to a summary dictionary for reporting and logging."""
        return {
            "query_type": self.__class__.__name__,
            "version": self.specific_version(),
            "source_kind_id": self.source_kind_id,
            "event_type": self.event_type,
            "issue_id": self.issue_id,
            "date_start": self.date_start,
            "date_end": self.date_end,
        }
