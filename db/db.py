import logging
import psycopg2
import psycopg2.extras
import threading
import os
from typing import List, Optional
from models.event import Event


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    """
    Thread-safe singleton database manager for event storage using PostgreSQL.
    
    This class provides a centralized, thread-safe interface for PostgreSQL database operations
    in the TemporalETL system. It implements the singleton pattern to ensure a single
    database connection is shared across the application while maintaining thread safety
    through proper locking mechanisms.
    
    The class is designed for insert-only operations to support the ETL use case where
    events are extracted, transformed, and loaded into PostgreSQL without modification.
    
    Environment Variables:
        DB_HOST: PostgreSQL host (default: localhost)
        DB_PORT: PostgreSQL port (default: 5432) 
        DB_NAME: Database name (default: launchpad_events)
        DB_USER: Database user (default: postgres)
        DB_PASSWORD: Database password (required)
    """
    
    _instance: Optional["Database"] = None
    _lock = threading.Lock()
    _initialized = False
    
    def __new__(cls) -> "Database":
        """
        Create singleton instance with double-checked locking pattern.
        
        Returns:
            The singleton Database instance
        """
        """Create singleton instance with double-checked locking."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new Database instance")
                    cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """
        Initialize database connection and schema (executed only once per singleton).
        """
        if not Database._initialized:
            with Database._lock:
                if not Database._initialized:
                    self._db_lock = threading.Lock()
                    self._init_database()
                    Database._initialized = True
    
    def _init_database(self) -> None:
        """
        Initialize database connection and create tables if they don't exist.
        
        Connection parameters can be configured via environment variables:
        - DB_HOST: Database host (default: localhost)
        - DB_PORT: Database port (default: 5432)
        - DB_NAME: Database name (default: db)
        - DB_USER: Database user (default: postgres)
        - DB_PASSWORD: Database password (required)
        """
        logger.info("Initializing database connection")
        # Get database connection parameters from environment
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'db')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD')
        
        if not db_password: raise ValueError("DB_PASSWORD environment variable is required for PostgreSQL connection")
        
        # Create connection string
        connection_string = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}"
        
        self.connection = psycopg2.connect(
            connection_string,
            cursor_factory=psycopg2.extras.RealDictCursor  # Return rows as dictionaries
        )
        logger.info("Connected to PostgreSQL database")
        
        # Set autocommit to False for transaction control
        self.connection.autocommit = False

        # Create launchpad_events table with PostgreSQL syntax
        with self.connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS launchpad_events (
                    id SERIAL PRIMARY KEY,
                    source_kind_id VARCHAR NOT NULL,
                    parent_item_id VARCHAR,
                    event_id VARCHAR NOT NULL UNIQUE,

                    event_type VARCHAR NOT NULL,
                    relation_type VARCHAR NOT NULL,

                    employee_id VARCHAR NOT NULL,

                    event_time_utc TIMESTAMP NOT NULL,
                    week DATE NOT NULL,
                    timezone VARCHAR,
                    event_time TIMESTAMP,

                    event_properties JSONB,
                    relation_properties JSONB,
                    metrics JSONB
                )
            """)
            logger.info("Ensured launchpad_events table exists")
        
        self.connection.commit()
    
    def insert_events_batch(self, events: List[Event]) -> int:
        """
        Insert multiple events in a single batch transaction (thread-safe).
        
        This method performs optimized batch insertion of Event objects into the
        PostgreSQL database using prepared statements and individual insert operations.
        
        The method is fully thread-safe and uses database-level locking to prevent
        concurrent modification issues. Individual event insertion failures are
        logged but don't abort the entire batch operation.
        
        Args:
            events: List of Event objects to insert into the database
            
        Returns:
            Number of events successfully inserted into the database
            
        Raises:
            Exception: Database-level errors are caught and logged, but the method
                    returns the count of successful insertions rather than raising exceptions
        """
        if not events:
            return 0
            
        successful_inserts = 0
        logger.info("Starting batch insert of %s events", len(events))
        with self._db_lock:
            try:
                with self.connection.cursor() as cursor:
                    # Prepare the insert statement
                    insert_query = """
                        INSERT INTO launchpad_events (
                            source_kind_id, parent_item_id, event_id,
                            event_type, relation_type, employee_id,
                            event_time_utc, week, timezone, event_time,
                            event_properties, relation_properties, metrics
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    
                    # Insert events one by one to handle individual failures
                    for i, event in enumerate(events):
                        try:
                            event_tuple = (
                                event.source_kind_id,
                                event.parent_item_id,
                                event.event_id,
                                event.event_type,
                                event.relation_type,
                                event.employee_id,
                                event.event_time_utc,
                                event.week,
                                event.timezone,
                                event.event_time,
                                psycopg2.extras.Json(event.event_properties) if event.event_properties else None,
                                psycopg2.extras.Json(event.relation_properties) if event.relation_properties else None,
                                psycopg2.extras.Json(event.metrics) if event.metrics else None
                            )
                            
                            cursor.execute(insert_query, event_tuple)
                            successful_inserts += 1
                            
                        except Exception as e: # Continue with the next event
                            logger.error("Error inserting event %s (event_id: %s): %s", i, getattr(event, 'event_id', 'unknown'), e)
                            continue
                    
                    # Commit all successful insertions
                    self.connection.commit()
                    logger.info("Batch insert completed: %s/%s events inserted successfully", successful_inserts, len(events))
                    
                    return successful_inserts
            except Exception as e:
                # Rollback on connection-level error
                self.connection.rollback()
                logger.error(f"Error during batch insert operation: %s", e)
                return successful_inserts
        
    def close(self) -> None:
        """
        Close the database connection in a thread-safe manner.
        
        This method properly closes the database connection and should be called
        when the database is no longer needed. It's automatically called when
        using the Database as a context manager.
        """
        with self._db_lock:
            if self.connection:
                self.connection.close()
                logger.info("Database connection closed")
    
    def __enter__(self) -> "Database":
        """
        Context manager entry point.
        
        Returns:
            The Database instance for use in with statements
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit point.
        
        Args:
            exc_type: Exception type (if any)
            exc_val: Exception value (if any)  
            exc_tb: Exception traceback (if any)
        """
        self.close()


def get_database() -> Database:
    """
    Convenience function to get the singleton Database instance.
    
    Returns:
        The singleton Database instance
    """
    return Database()