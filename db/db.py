import sqlite3
import json
import threading
import os
from typing import List, Optional
from models.event import Event


class Database:
    """
    Thread-safe singleton database manager for Launchpad event storage.
    
    This class provides a centralized, thread-safe interface for database operations
    in the TemporalETL system. It implements the singleton pattern to ensure a single
    database connection is shared across the application while maintaining thread safety
    through proper locking mechanisms.
    
    The class is designed for insert-only operations to support the ETL use case where
    events are extracted, transformed, and loaded into the database without modification.
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
        
        The connection string can be configured via the CONNECTION_STRING environment
        variable, defaulting to 'launchpad_events.db' for local development.
        """
        self.connection = sqlite3.connect(
            os.getenv('CONNECTION_STRING', 'launchpad_events.db'),
            check_same_thread=False  # Allow connection to be used across threads
        )

        self.connection.execute("PRAGMA foreign_keys = ON")
        
        # Create events table
        # TODO: define schema
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS launchpad_events (
            )
        """)
        
        self.connection.commit()
    
    def insert_events_batch(self, events: List[Event]) -> int:
        """
        Insert multiple events in a single batch transaction (thread-safe).
        
        This method performs optimized batch insertion of Event objects into the
        database.
        
        The method is fully thread-safe and uses database-level locking to prevent
        concurrent modification issues. Individual event insertion failures are
        logged but don't abort the entire batch operation.
        
        Args:
            events: List of Event objects to insert into the database
            
        Returns:
            Number of events successfully inserted into the database
            
        Raises:
            Exception: Database-level errors are caught and logged, but the method
                      returns 0 for complete failures rather than raising exceptions
        """
        # TODO: Implement actual database loading logic
        return 0
    
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