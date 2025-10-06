import os


class WorkplaceDBConfig:
    """Configuration class for the Database."""

    def __init__(self):
        self._password = os.getenv("WPE_DB_PASSWORD")
        self._host = os.getenv("WPE_DB_HOST")
        self._port = os.getenv("WPE_DB_PORT")
        self._name = os.getenv("WPE_DB_NAME")
        self._user = os.getenv("WPE_DB_USER")
        self._schema = os.getenv("WPE_DB_SCHEMA")

    @property
    def connection_string(self) -> str:
        """Generate a PostgreSQL connection string."""
        options = f"-c search_path={self._schema}" if self._schema else ""
        return (
            f"host={self._host} port={self._port} dbname={self._name} "
            f"user={self._user} password={self._password} options='{options}'"
        )
