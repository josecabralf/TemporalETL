import threading
from contextlib import contextmanager
from typing import List

import psycopg2
import psycopg2.extras

from db.sql import (
    get_create_events_table_query,
    get_insert_events_query,
    get_update_events_parents_query,
)
from config.database import DatabaseConfiguration
from models.event import Event
from models.logger import logger


class Database:
    """Simple database manager for event storage using PostgreSQL."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("New Database instance")
                    cls._instance = super(Database, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._config = DatabaseConfiguration()
        self._ensure_schema()
        self._initialized = True

    def _ensure_schema(self) -> None:
        """Ensure the database schema exists."""
        import re

        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", self._config.events_table):
            raise ValueError(f"Invalid table name: {self._config.events_table}")

        logger.info("Ensuring schema exists in the database")
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(get_create_events_table_query(self._config.events_table))
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

    def insert_events_batch(self, events: List[Event]) -> int:
        """Insert multiple events in a single batch transaction.

        Args:
            events: List of Event objects to insert

        Returns:
            Number of events successfully inserted
        """
        if not events:
            return 0

        logger.info(f"Inserting batch of {len(events)} events")

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Insert events
                values = [
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
                    )
                    for e in events
                ]

                psycopg2.extras.execute_values(
                    cursor, get_insert_events_query(self._config.events_table), values
                )
                inserted_count = cursor.rowcount

                # Update parent event properties
                parent_updates = {
                    e.parent_item_id: e.event_properties
                    for e in events
                    if e.parent_item_id
                }
                if parent_updates:
                    update_values = [
                        (parent_id, psycopg2.extras.Json(props))
                        for parent_id, props in parent_updates.items()
                    ]

                    psycopg2.extras.execute_values(
                        cursor,
                        get_update_events_parents_query(self._config.events_table),
                        update_values,
                    )

                conn.commit()
                logger.info(f"Successfully inserted {inserted_count} events")
                return inserted_count
