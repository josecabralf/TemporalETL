from typing import Any, Dict, List

from trino.dbapi import connect
from trino.auth import JWTAuthentication

from external.trino.gcp import GCPConfiguration
from external.trino.soql import TrinoQuery
from external.trino.config import TrinoConfiguration


class TrinoClient:
    @staticmethod
    def _execute(query: str) -> List[Dict[str, Any]]:
        credentials = GCPConfiguration.get_credentials()
        trino_config = TrinoConfiguration()

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

            if not rows or not cursor.description:
                return []

            columns = [x[0] for x in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]

        return result

    @classmethod
    def get_issues_ids_and_last_update(cls) -> List[Dict[str, Any]]:
        """Gets all issue keys from the Jira dataset."""
        return cls._execute(query=TrinoQuery.get_issues_ids_and_last_update())
