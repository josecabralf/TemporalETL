import os


class DatabaseConfiguration:
    """Configuration class for the Database."""

    def __init__(self):
        self._password = os.getenv("DB_PASSWORD")
        self._host = os.getenv("DB_HOST")
        self._port = os.getenv("DB_PORT")
        self._name = os.getenv("DB_NAME")
        self._user = os.getenv("DB_USER")

        self.events_table = os.getenv("DB_EVENTS_TABLE", "events")

    @property
    def connection_string(self) -> str:
        """Generate a PostgreSQL connection string."""
        return f"host={self._host} port={self._port} dbname={self._name} user={self._user} password={self._password}"
