class SalesforceQuery:
    @staticmethod
    def get_launchpad_employee_ids():
        return """
            SELECT Unique_Id, Launchpad_Id
            FROM Team_Member
            WHERE Unique_Id != null 
                AND Is_Test_User__c = FALSE 
                AND Employment_Status = 'Active Employee'
        """

    @staticmethod
    def get_all_employee_email_ids():
        return """
            SELECT Unique_Id, Email
            FROM Team_Member
            WHERE Unique_Id != null 
        """
