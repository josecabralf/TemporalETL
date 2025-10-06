import requests
from typing import Dict

from simple_salesforce.api import Salesforce

from external.salesforce.config import SalesforceConfig
from external.salesforce.soql import SalesforceQuery


class SalesforceClient:
    @staticmethod
    def _execute(query: str):
        access = SalesforceConfig()
        s = Salesforce(
            username=access.username,
            password=access.password,
            security_token=access.token,
            session=requests.Session(),
        )
        return s.query_all(query)

    @classmethod
    def get_launchpad_employee_ids(cls) -> Dict[str, str]:
        """
        Fetches the HRc IDs for the given launchpad IDs.
        This is a placeholder for the actual implementation that would interact with HRc.
        """
        query = SalesforceQuery.get_launchpad_employee_ids()
        response = cls._execute(query)
        return {
            record["Launchpad_ID"]: record["Unique_Id"]
            for record in response.get("records", [])
        }

    @classmethod
    def get_all_email_employee_ids(cls) -> Dict[str, str]:
        """
        Fetches the HRc IDs for the given email addresses.
        This is a placeholder for the actual implementation that would interact with HRc.
        """
        query = SalesforceQuery.get_all_employee_email_ids()
        response = cls._execute(query)
        return {
            record["Email"]: record["Unique_Id"]
            for record in response.get("records", [])
        }
