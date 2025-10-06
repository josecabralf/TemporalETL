import requests
from typing import List
from simple_salesforce.api import Salesforce as SalesforceAPI

from external.salesforce.config import SalesforceConfiguration
from external.salesforce.soql import SalesforceQuery

from models.logger import logger


class SalesforceClient:
    """Class to interact with Salesforce API."""

    @staticmethod
    def _execute(query: str):
        access = SalesforceConfiguration()
        s = SalesforceAPI(
            username=access.username,
            password=access.password,
            security_token=access.token,
            session=requests.Session(),
        )
        return s.query_all(query)

    @classmethod
    def get_launchpad_ids(cls) -> List[str]:
        """Fetches all employees' launchpad IDs."""
        query = SalesforceQuery.get_all_launchpad_ids_query()
        response = cls._execute(query)
        records = []
        for record in response.get("records", []):
            if record.get("Launchpad_ID__c"):
                records.append(record["Launchpad_ID__c"])
            else:
                logger.warning(
                    "Employee %s does not have a Launchpad ID set.",
                    record.get("fHCM2__Unique_Id__c", "UnknownID"),
                )

        return records
