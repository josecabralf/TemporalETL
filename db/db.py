"""
Database connection and management module.

Following the spec, the data to include for each event is:
- id: unique event id
- week: date of the first day of the week the event occurred
- employee_id
- source_kind_id: will be "launchpad"
- event_type: could be bug:created, bug:commented, merge_proposal:approved, etc. 
- event_time_utc: timestamp
- relation_properties: this is a json string with additional information available about the event. 
    For example, it could include the IDs of bugs associated with a merge proposal, or the importance or status of a bug. 
    Anything that is extracted, that is not already in one of the other columns should be stored here. 
"""

import sqlite3
import json
import threading
from typing import List
from models.event import Event
import os


class Database:
    """Thread-safe singleton database manager for Launchpad events - insert only operations."""
    
    _instance = None
    _lock = threading.Lock()
    _initialized = False
    
    def __new__(cls):
        """Create singleton instance with double-checked locking."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize database connection (only once)."""
        if not Database._initialized:
            with Database._lock:
                if not Database._initialized:
                    self._db_lock = threading.Lock()
                    self._init_database()
                    Database._initialized = True
    
    def _init_database(self):
        """Initialize database and create tables if they don't exist."""
        self.connection = sqlite3.connect(
            os.getenv('CONNECTION_STRING', 'launchpad_events.db'),
            check_same_thread=False  # Allow connection to be used across threads
        )

        self.connection.execute("PRAGMA foreign_keys = ON")
        
        # Create events table
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                week DATE NOT NULL,
                employee_id TEXT NOT NULL,
                source_kind_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_time_utc TIMESTAMP NOT NULL,
                relation_properties TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.connection.commit()
    
    def insert_events_batch(self, events: List[Event]) -> int:
        """
        Insert multiple events in a batch operation (thread-safe).
        
        Args:
            events: List of Event objects
            
        Returns:
            int: Number of successfully inserted events
        """
        with self._db_lock:
            successful_inserts = 0
            try:
                cursor = self.connection.cursor()
                for event in events:
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO events 
                            (id, week, employee_id, source_kind_id, event_type, event_time_utc, relation_properties)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            event.id,
                            event.week,
                            event.employee_id,
                            event.source_kind_id,
                            event.type,
                            event.time_utc,
                            json.dumps(event.relation_properties) if event.relation_properties else None
                        ))
                        successful_inserts += 1
                    except Exception as e:
                        print(f"Error inserting event {event.type}-{event.id}: {e}")
                        continue
                
                self.connection.commit()
                
            except Exception as e:
                print(f"Error in batch insert: {e}")
                self.connection.rollback()
                successful_inserts = 0
            
            return successful_inserts
    
    def close(self):
        """Close database connection (thread-safe)."""
        with self._db_lock:
            if self.connection:
                self.connection.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience function to get the singleton instance
def get_database() -> Database:
    """Get the singleton database instance."""
    return Database()