class TrinoQuery:
    @staticmethod
    def get_issues_ids_and_last_update() -> str:
        """
        Get all issue keys from Jira.
        """
        return """
            SELECT
                iss.id AS id,
                json_extract_scalar(iss.fields, '$.updated') AS last_updated
            FROM jira.issues iss
        """
