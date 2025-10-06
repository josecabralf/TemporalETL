from dataclasses import dataclass
from typing import Any, Dict

from models.etl.query import Query, query_type
from sources.launchpad.config import LaunchpadConfiguration


@dataclass
@query_type("launchpad")
class LaunchpadQuery(Query):
    """Query implementation for Launchpad API data extraction."""

    member: str
    date_start: str
    date_end: str

    _version = "2.0.0"

    def __init__(
        self,
        member: str,
        date_start: str,
        date_end: str,
        source_kind_id: str,
        event_type: str,
    ) -> None:
        """Initialize LaunchpadQuery with connection and scope parameters."""
        self.member = member.lower()  # lp usernames are all lowercase
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
    def from_dict(data: Dict[str, Any]) -> "LaunchpadQuery":
        """Create a LaunchpadQuery instance from a dictionary of parameters.

        Args:
            data: Dictionary containing Launchpad query parameters
                 Expected keys: member, date_start, date_end, event_type

        Returns:
            Configured LaunchpadQuery instance ready for use
        """
        return LaunchpadQuery(
            member=data.get("member", ""),
            date_start=data.get("date_start", ""),
            date_end=data.get("date_end", ""),
            source_kind_id="launchpad",
            event_type=data.get("event_type", ""),
        )

    def to_summary_base(self) -> Dict[str, Any]:
        """Generate a summary dictionary for logging and monitoring purposes.

        Returns:
            Dictionary containing key query information for summary reporting
        """
        return {
            "query_type": self.__class__.__name__,
            "version": self.specific_version(),
            "source_kind_id": self.source_kind_id,
            "event_type": self.event_type,
            "launchpad": LaunchpadConfiguration.connection_details(),
            "member": self.member,
            "date_start": self.date_start,
            "date_end": self.date_end,
        }
