from dataclasses import dataclass

from models.query import IQuery


@dataclass
class LaunchpadQuery(IQuery):
    """
    Class to hold parameters for Launchpad workflows.
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
                 data_date_end: str):
        self.application_name = application_name
        self.service_root = service_root
        self.version = version
        self.member = member
        self.data_date_start = data_date_start
        self.data_date_end = data_date_end

    @staticmethod
    def from_dict(data: dict):
        return LaunchpadQuery(
            application_name=data.get("application_name", ""),
            service_root=data.get("service_root", ""),
            version=data.get("version", ""),
            member=data.get("member", ""),
            data_date_start=data.get("data_date_start", ""),
            data_date_end=data.get("data_date_end", "")
        )

    def to_summary_base(self) -> dict:
        return {
            "launchpad": f"{self.application_name}@{self.service_root}:{self.version}",
            "member": self.member,
            "data_date_start": self.data_date_start,
            "data_date_end": self.data_date_end,
        }