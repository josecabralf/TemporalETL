from dataclasses import dataclass
from typing import Dict, Any

from models.etl.query import Query, query_type


@dataclass
@query_type("launchpad")
class LaunchpadQuery(Query):
    """
    Query implementation for Launchpad API data extraction.
    """
    application_name: str
    service_root: str
    version: str
    member: str
    data_date_start: str
    data_date_end: str

    def __init__(self, 
                 application_name: str,
                 service_root: str,
                 version: str,
                 member: str,
                 data_date_start: str,
                 data_date_end: str,
                 source_kind_id: str,
                 event_type: str
                 ) -> None:
        """
        Initialize LaunchpadQuery with connection and scope parameters.
        
        Args:
            application_name: Application identifier for Launchpad API access
            service_root: Target Launchpad environment (typically "production")
            version: API version to use (typically "devel" for latest features)
            member: Launchpad member username for filtering data
            data_date_start: Start date for data extraction (YYYY-MM-DD)
            data_date_end: End date for data extraction (YYYY-MM-DD)
        """
        self.application_name = application_name
        self.service_root = service_root
        self.version = version
        self.member = member
        self.data_date_start = data_date_start
        self.data_date_end = data_date_end
        super().__init__(source_kind_id, event_type)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "LaunchpadQuery":
        """
        Create a LaunchpadQuery instance from a dictionary of parameters.
        
        Args:
            data: Dictionary containing Launchpad query parameters
                 Expected keys: application_name, service_root, version,
                                member, data_date_start, data_date_end, event_type
                 
        Returns:
            Configured LaunchpadQuery instance ready for use
        """
        return LaunchpadQuery(
            application_name=data.get("application_name", ""),
            service_root=data.get("service_root", ""),
            version=data.get("version", ""),
            member=data.get("member", ""),
            data_date_start=data.get("data_date_start", ""),
            data_date_end=data.get("data_date_end", ""),
            source_kind_id="launchpad",
            event_type=data.get("event_type", "")
        )

    def to_summary_base(self) -> Dict[str, Any]:
        """
        Generate a summary dictionary for logging and monitoring purposes.
        
        Returns:
            Dictionary containing key query information for summary reporting
        """
        return {
            "query_type": self.__class__.__name__,
            "source_kind_id": self.source_kind_id,
            "event_type": self.event_type,
            "launchpad": f"{self.application_name}@{self.service_root}:{self.version}",
            "member": self.member,
            "data_date_start": self.data_date_start,
            "data_date_end": self.data_date_end,
        }