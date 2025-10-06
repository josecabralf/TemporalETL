import os


class TrinoConfiguration:
    """
    Access Configuration for Trino.
    """

    def __init__(self) -> None:
        self.host = os.getenv("TRINO_HOST")
        if not self.host:
            raise ValueError("Trino host is not set in environment variables.")

        self.port = int(os.getenv("TRINO_PORT", 443))
        self.http_scheme = os.getenv("TRINO_HTTP_SCHEME", "https")
