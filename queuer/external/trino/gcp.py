import os

from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request


class GCPConfiguration:
    """
    Configuration for GCP.
    This class is used to manage the access credentials for GCP.
    """

    @staticmethod
    def _service_account_info() -> dict:
        private_key = os.getenv("GCP_PRIVATE_KEY")
        if private_key:
            private_key = private_key.replace("\\n", "\n")
        return {
            "type": os.getenv("GCP_ACCOUNT_TYPE"),
            "project_id": os.getenv("GCP_PROJECT_ID"),
            "private_key_id": os.getenv("GCP_PRIVATE_KEY_ID"),
            "private_key": private_key,
            "client_id": os.getenv("GCP_CLIENT_ID"),
            "client_email": os.getenv("GCP_CLIENT_EMAIL"),
            "client_x509_cert_url": os.getenv("GCP_CLIENT_X509_CERT_URL"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "universe_domain": "googleapis.com",
        }

    @classmethod
    def get_credentials(cls) -> Credentials:
        credentials = Credentials.from_service_account_info(
            cls._service_account_info(), scopes=["openid", "email"]
        )
        credentials.refresh(Request())

        return credentials
