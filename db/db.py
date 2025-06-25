import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool
import threading
import os
import time
from typing import List, Optional
from models.event import Event
from contextlib import contextmanager


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    """
    Thread-safe singleton database manager for event storage using PostgreSQL.
    Designed for high-concurrency temporal workflows with connection pooling
    and automatic reconnection handling.
    
    Environment Variables:
        DB_HOST: PostgreSQL host (default: localhost)
        DB_PORT: PostgreSQL port (default: 5432) 
        DB_NAME: Database name (default: launchpad_events)
        DB_USER: Database user (default: postgres)
        DB_PASSWORD: Database password (required)
        DB_MIN_CONN: Minimum connections in pool (default: 1)
        DB_MAX_CONN: Maximum connections in pool (default: 20)
    """
    
    _instance: Optional["Database"] = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls) -> "Database":
        """
        Create singleton instance with double-checked locking pattern.
        
        Returns: the singleton Database instance
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new Database instance")
                    cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """
        Initialize database connection pool and schema (executed only once per singleton).
        """
        if not Database._initialized:
            with Database._lock:
                if not Database._initialized:
                    self._pool_lock = threading.Lock()
                    self._init_database()
                    Database._initialized = True

    def _init_database(self) -> None:
        """
        Initialize database connection pool and create tables if they don't exist.
        
        Connection parameters can be configured via environment variables.
        Creates a threaded connection pool for better concurrency handling.
        """
        logger.info("Initializing database connection pool")
        
        # Get database connection parameters from environment
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'db')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD')
        min_conn = int(os.getenv('DB_MIN_CONN', '1'))
        max_conn = int(os.getenv('DB_MAX_CONN', '20'))
        
        if not db_password:
            raise ValueError("DB_PASSWORD environment variable is required for PostgreSQL connection")
        
        # Create connection string
        self.connection_string = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}"
        
        # Initialize connection pool
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn, max_conn,
                self.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            logger.info(f"Created PostgreSQL connection pool (min={min_conn}, max={max_conn})")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
        
        # Create schema using a connection from the pool
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """
        Ensure the database schema exists using a connection from the pool.
        """
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
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
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to create schema: {e}")
                raise

    @contextmanager
    def get_connection(self, max_retries: int = 3, retry_delay: float = 1.0):
        """
        Get a connection from the pool with automatic retry and health checking.
        
        Args:
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retry attempts in seconds
            
        Yields: A database connection from the pool
            
        Raises:
            Exception: If unable to get a healthy connection after retries
        """
        connection = None
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                with self._pool_lock:
                    if self.pool.closed:
                        logger.warning("Connection pool is closed, recreating...")
                        self._recreate_pool()
                    
                    connection = self.pool.getconn()
                
                # Check if connection is healthy
                if self._is_connection_healthy(connection):
                    try:
                        yield connection
                        return
                    finally:
                        # Always return connection to pool
                        with self._pool_lock:
                            if not self.pool.closed:
                                self.pool.putconn(connection)
                else:
                    # Connection is unhealthy, discard it
                    logger.warning("Discarding unhealthy connection")
                    with self._pool_lock:
                        if not self.pool.closed:
                            self.pool.putconn(connection, close=True)
                    
            except Exception as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                
                if connection:
                    try:
                        with self._pool_lock:
                            if not self.pool.closed:
                                self.pool.putconn(connection, close=True)
                    except:
                        pass  # Ignore errors when discarding bad connection
                
                if attempt < max_retries:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
        
        # All attempts failed
        raise Exception(f"Failed to get healthy database connection after {max_retries + 1} attempts. Last error: {last_exception}")

    def _is_connection_healthy(self, connection) -> bool:
        """
        Check if a database connection is healthy and ready for use.
        
        Args:
            connection: Database connection to check
            
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            # Check if connection is closed
            if connection.closed:
                return False
            
            # Check if connection is executing (shouldn't be for a fresh connection)
            if connection.isexecuting():
                logger.warning("Connection is still executing a query")
                return False
            
            # Try a simple query to verify connection works
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            return True
            
        except Exception as e:
            logger.warning(f"Connection health check failed: {e}")
            return False
    
    def _recreate_pool(self) -> None:
        """
        Recreate the connection pool if it's been closed or corrupted.
        Should be called with _pool_lock held.
        """
        try:
            if hasattr(self, 'pool') and not self.pool.closed:
                self.pool.closeall()
        except:
            pass  # Ignore errors when closing old pool
        
        min_conn = int(os.getenv('DB_MIN_CONN', '1'))
        max_conn = int(os.getenv('DB_MAX_CONN', '20'))
        
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            min_conn, max_conn,
            self.connection_string,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        logger.info("Recreated connection pool")

    def insert_events_batch(self, events: List[Event], max_retries: int = 3) -> int:
        """
        Insert multiple events in a single batch transaction with automatic retry.
        
        Args:
            events: List of Event objects to insert into the database
            max_retries: Maximum number of retry attempts for the entire operation
            
        Returns: number of events successfully inserted into the database
            
        Raises: database-level errors after all retries are exhausted
        """
        if not events:
            return 0
        
        logger.info("Starting batch insert of %s events", len(events))
        
        for attempt in range(max_retries + 1):
            try:
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        # Prepare the insert statement
                        insert_query = """
                            INSERT INTO launchpad_events (
                                source_kind_id, 
                                parent_item_id, 
                                event_id,
                                event_type, 
                                relation_type, 
                                employee_id,
                                event_time_utc, 
                                week, 
                                timezone, 
                                event_time,
                                event_properties, 
                                relation_properties, 
                                metrics
                            ) 
                            VALUES %s
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING id
                        """

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
                                psycopg2.extras.Json(e.event_properties) if e.event_properties else None,
                                psycopg2.extras.Json(e.relation_properties) if e.relation_properties else None,
                                psycopg2.extras.Json(e.metrics) if e.metrics else None
                            )
                            for e in events
                        ]

                        psycopg2.extras.execute_values(cursor, insert_query, values)
                        inserted_count = cursor.rowcount
                        
                        conn.commit()
                        logger.info(f"Successfully inserted {inserted_count} events")
                        return inserted_count
                        
            except Exception as e:
                logger.warning(f"Batch insert attempt {attempt + 1} failed: {e}")
                if attempt == max_retries:
                    logger.error(f"All {max_retries + 1} batch insert attempts failed. Last error: {e}")
                    raise
                else:
                    time.sleep(1.0 * (2 ** attempt))  # Exponential backoff
        
        return 0
    
    def health_check(self) -> bool:
        """
        Perform a health check on the database connection pool.
        
        Returns:
            True if the database is healthy and responsive, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version()")
                    result = cursor.fetchone()
                    logger.info(f"Database health check passed: {result['version']}")
                    return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    def get_pool_status(self) -> dict:
        """
        Get current connection pool status for monitoring.
        
        Returns:
            Dictionary with pool statistics
        """
        with self._pool_lock:
            if self.pool.closed:
                return {"status": "closed"}
            
            return {
                "status": "active",
                "min_connections": self.pool.minconn,
                "max_connections": self.pool.maxconn,
                "closed": self.pool.closed
            }
    
    def close(self) -> None:
        """
        Close all connections in the pool in a thread-safe manner.
        
        This method properly closes all database connections and should be called
        when the database is no longer needed.
        """
        with self._pool_lock:
            if hasattr(self, 'pool') and not self.pool.closed:
                self.pool.closeall()
                logger.info("All database connections closed")
    
    def __enter__(self) -> "Database":
        """
        Context manager entry point.
        
        Returns: the Database instance for use in with statements
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