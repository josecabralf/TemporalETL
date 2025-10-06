import os


class SalesforceConfiguration:
    """
    Configuration for HRc.
    This class is used to manage the access credentials for HRc.
    """

    def __init__(self):
        self.username = os.getenv("SF_USERNAME")
        self.password = os.getenv("SF_PASSWORD")
        self.token = os.getenv("SF_TOKEN")
        if not all([self.username, self.password, self.token]):
            raise ValueError(
                "Salesforce credentials are not fully set in environment variables."
            )
