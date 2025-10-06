from contextlib import contextmanager
import psycopg2
import psycopg2.extras
import threading
from typing import List

from external.wpe_db.config import WorkplaceDBConfig
from external.wpe_db.sql import SQLQuery

from models.event import Event
from models.logger import logger


class WorkplaceDBClient:
    """Simple database manager for event storage using PostgreSQL."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("New Database instance")
                    cls._instance = super(WorkplaceDBClient, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._config = WorkplaceDBConfig()
        self._initialized = True

    def _ensure_table_in_schema(self, table_name: str) -> None:
        """Ensure the database schema exists."""
        import re

        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        logger.info("Ensuring schema exists in the database")
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                query = SQLQuery.create_events_table(table_name)
                cursor.execute(query)
            conn.commit()

    @contextmanager
    def _get_connection(self):
        """Get a database connection."""
        conn = None
        try:
            conn = psycopg2.connect(
                self._config.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def insert_events_batch(
        self, events: List[Event], events_table: str = "events_table"
    ) -> int:
        """Insert multiple events in a single batch transaction.

        Args:
            events: List of Event objects to insert

        Returns:
            Number of events successfully inserted
        """
        if not events:
            return 0

        self._ensure_table_in_schema(events_table)
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Update existing events event_properties
                # The ON CONFLICT in insert prevents duplicates, but doesn't update props
                # So, if something changed during extraction period, we update it here
                psycopg2.extras.execute_values(
                    cursor,
                    SQLQuery.update_event_properties(events_table),
                    [
                        (
                            e.event_id,
                            psycopg2.extras.Json(e.event_properties),
                            psycopg2.extras.Json(e.metrics),
                            # Keep track of which script version updated this event
                            e.version,
                            e.specific_version,
                        )
                        for e in events
                    ],
                )
                logger.info(f"Updated properties for {cursor.rowcount} existing events")

                # Insert new events
                psycopg2.extras.execute_values(
                    cursor,
                    SQLQuery.insert_events(events_table),
                    [
                        (
                            e.source_kind_id,
                            e.parent_item_id,
                            e.event_id,
                            e.event_type,
                            e.relation_type,
                            e.employee_id,
                            e.event_time_utc,
                            e.week,
                            e.timezone,
                            e.event_time,
                            psycopg2.extras.Json(e.event_properties)
                            if e.event_properties
                            else None,
                            psycopg2.extras.Json(e.relation_properties)
                            if e.relation_properties
                            else None,
                            psycopg2.extras.Json(e.metrics) if e.metrics else None,
                            # Keep track of which script version inserted this event
                            e.version,
                            e.specific_version,
                        )
                        for e in events
                    ],
                )
                inserted_count = cursor.rowcount

                conn.commit()
                logger.info(f"Successfully inserted {inserted_count} events")
                return inserted_count
