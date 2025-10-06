from typing import Any, Dict, List

from trino.dbapi import connect
from trino.auth import JWTAuthentication

from external.trino.gcp import GCP
from external.trino.soql import TrinoQuery
from external.trino.config import TrinoConfig


class TrinoClient:
    @staticmethod
    def _execute(query: str, is_mapping: bool = False) -> List[Dict[str, Any]]:
        credentials = GCP.get_credentials()
        trino_config = TrinoConfig()

        with connect(
            host=trino_config.host,
            port=trino_config.port,
            http_scheme=trino_config.http_scheme,
            auth=JWTAuthentication(credentials.token),
            verify=True,
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

            if is_mapping:
                result = [{row[0]: row[1] for row in rows}]
                return result

            if not rows or not cursor.description:
                return []

            columns = [x[0] for x in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]

        return result

    @classmethod
    def get_issue(cls, issue_id: str) -> Dict[str, Any]:
        """
        Gets issue details
        """
        try:
            return cls._execute(query=TrinoQuery.get_issue(issue_id))[0]
        except IndexError:
            return {}

    @classmethod
    def get_all_users(cls) -> Dict[str, Any]:
        """
        Gets mappings of user IDs to email addresses from the Jira dataset.
        """
        try:
            return cls._execute(query=TrinoQuery.get_all_users(), is_mapping=True)[0]
        except IndexError:
            return {}
