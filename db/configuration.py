import os


class DatabaseConfiguration:
    """
    Configuration class for the Database singleton.

    This class is used to store database connection parameters and
    provides methods to validate and retrieve these parameters.
    """

    def __init__(self):
        self._host = os.getenv("DB_HOST", "localhost")
        self._port = os.getenv("DB_PORT", "5432")
        self._name = os.getenv("DB_NAME", "launchpad_events")
        self._user = os.getenv("DB_USER", "postgres")
        self._password = os.getenv("DB_PASSWORD")
        if not self._password:
            raise ValueError(
                "DB_PASSWORD environment variable is required for PostgreSQL connection"
            )

        self.events_table = os.getenv("DB_EVENTS_TABLE", "launchpad_events")

    @property
    def connection_string(self) -> str:
        """
        Generate a PostgreSQL connection string.

        Returns:
            str: Connection string for psycopg2.
        """
        return f"host={self._host} port={self._port} dbname={self._name} user={self._user} password={self._password}"
