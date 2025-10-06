class SalesforceQuery:
    """Class to hold SOQL queries."""

    @staticmethod
    def get_all_launchpad_ids_query():
        """Returns a SOQL query to fetch all launchpad IDs."""
        return """
            SELECT Unique_Id, Launchpad_ID
            FROM Team_Member
            WHERE Unique_Id != null
                AND Is_Test_User__c = FALSE
                AND Employment_Status = 'Active Employee'
            """
