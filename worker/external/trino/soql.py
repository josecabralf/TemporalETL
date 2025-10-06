from simple_salesforce.format import format_soql


class TrinoQuery:
    @staticmethod
    def get_issue(issue_id: str) -> str:
        """
        Get issue details by issue ID.
        Details include all the information needed to extract all issue events.
        """
        return format_soql(
            """
            SELECT
                iss.id AS id,
                iss.self AS url,
                iss.fields AS fields,
                iss.changelog AS changelog
            FROM jira.issues iss
            WHERE iss.id = {issue_id} 
            """,
            issue_id=issue_id,
        )

    @staticmethod
    def get_all_users() -> str:
        """
        Get all users with email addresses from Jira.
        """
        return """
            SELECT 
                u.accountId AS id,
                u.emailAddress AS email
            FROM jira.users AS u
            WHERE u.emailAddress IS NOT NULL
        """
