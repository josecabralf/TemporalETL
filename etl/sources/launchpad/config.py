import os

from launchpadlib.launchpad import Launchpad
from launchpadlib.credentials import AccessToken, Credentials

class LaunchpadConfiguration:
    """
    This class provides the parameters for connecting to Launchpad.
    """
    _web_root: str = "production"
    _version: str = "devel"
    _auth_engine: str = "oauth1"

    @classmethod
    def _get_credentials(cls) -> Credentials:
        return Credentials(
            consumer_name=os.getenv("LP_CONSUMER_KEY", ""),
            consumer_secret=os.getenv("LP_CONSUMER_SECRET", ""),
            access_token=AccessToken(
                key=os.getenv("LP_ACCESS_TOKEN_KEY", ""),
                secret=os.getenv("LP_ACCESS_TOKEN_SECRET", "")
            )
        )
    
    @classmethod
    def get_launchpad_instance(cls) -> Launchpad:
        """
        Returns a Launchpad instance configured with the credentials and settings.
        """
        credentials = cls._get_credentials()
        return Launchpad(
            credentials,
            authorization_engine=cls._auth_engine,
            credential_store=credentials,
            service_root=cls._web_root,
            version=cls._version
        )