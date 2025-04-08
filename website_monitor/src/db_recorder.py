import logging
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime

import psycopg
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from .http_health_checker import HealthCheckResult

# Import specific error classes from db_recorder_errors.py
from .errors import (
    DatabaseError, ConnectionError, ConnectionTimeoutError,
    TransactionError, QueryError, SchemaError,
    IntegrityError, ForeignKeyError, UniqueViolationError, 
    CheckConstraintError, PartitionError,
    HealthCheckError, StatusCodeError, ContentMatchError, TimeoutError
)

# Redefine InitialConnectionError as a subclass of ConnectionError
class InitialConnectionError(ConnectionError):
    """Exception raised when the initial connection to the database fails"""
    def __init__(self, message="Initial connection to database failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class HealthCheckDatabase:
    """
    A class to interact with the PostgreSQL database for health check operations.
    
    This class provides methods to:
    - Initialize the database schema
    - Add health check settings
    - Add health check results
    - Query health check settings
    """
    
    def __init__(
        self, 
        connection_string: str,
        min_pool_size: int = 1,
        max_pool_size: int = 2,
        schema_path: str = "pgschema/pgschema.sql",
        logger: Optional[logging.Logger] = None,
        connection_timeout: float = 30.0,
        retry_attempts: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the HealthCheckDatabase.
        
        Args:
            connection_string: PostgreSQL connection string (e.g., "postgresql://user:pass@localhost:5432/dbname")
            min_pool_size: Minimum connections in the pool
            max_pool_size: Maximum connections in the pool
            schema_path: Path to the SQL schema file
            logger: Optional logger to use
            connection_timeout: Timeout in seconds for connection attempts
            retry_attempts: Number of retry attempts for transient errors
            retry_delay: Delay between retry attempts in seconds
        """
        self.connection_string = connection_string
        self.schema_path = schema_path
        self.logger = logger or logging.getLogger(__name__)
        self.pool = None
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.connection_timeout = connection_timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self._initialized = False
        self.result_buffer = []
        self.buffer_lock = asyncio.Lock()
        self.flush_task = None
        self.shutdown_event = asyncio.Event()
    
    async def initialize(self):
        """
        Initialize the database connection pool and schema.
        
        This method must be called before using any other methods.
        Raises InitialConnectionError if the first connection attempt fails.
        """
        try:
            self.logger.info("Initializing database connection pool")
            
            # Create the pool without opening it immediately
            self.pool = AsyncConnectionPool(
                conninfo=self.connection_string,
                min_size=self.min_pool_size,
                max_size=self.max_pool_size,
                open=False,
                configure=self._configure_connection
            )
            
            # Open the pool manually with timeout
            try:
                await asyncio.wait_for(self.pool.open(), timeout=self.connection_timeout)
            except asyncio.TimeoutError:
                self.logger.error("Timed out waiting for database pool to open")
                if self.pool:
                    await self.pool.close()
                raise ConnectionTimeoutError(self.connection_timeout)
            
            # Validate the connection works by running a simple query
            try:
                async with self.pool.connection() as conn:
                    await conn.execute("SELECT 1")
            except Exception as e:
                self.logger.error(f"Failed to execute test query: {str(e)}")
                if self.pool:
                    await self.pool.close()
                raise InitialConnectionError(f"Database connection verification failed: {str(e)}")
            
            # Start the buffer flush task
            await self.start_buffer_flush_task()
            
            self._initialized = True
            self.logger.info("Database initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing database: {str(e)}", exc_info=True)
            # Clean up pool if needed
            if hasattr(self, 'pool') and self.pool:
                try:
                    await self.pool.close()
                except Exception:
                    pass
            
            # For the first connection, we want to raise a specific error
            if not self._initialized:
                if isinstance(e, (ConnectionError, InitialConnectionError)):
                    raise  # Re-raise if it's already our custom error
                else:
                    raise InitialConnectionError(f"Failed to initialize database: {str(e)}") from e
            else:
                # For subsequent connection issues in re-initialization
                if isinstance(e, DatabaseError):
                    raise  # Re-raise if it's already a DatabaseError subclass
                else:
                    raise DatabaseError(f"Failed to initialize database: {str(e)}") from e
    
    async def _configure_connection(self, conn: AsyncConnection):
        """Configure each connection in the pool."""
        try:
            # Set dictionary cursor as row factory
            conn.row_factory = psycopg.rows.dict_row
            
            # Set application name
            async with conn.transaction():
                await conn.execute("SET application_name = 'health_checker';")
            
            # Make sure connection is in a clean state
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                await conn.rollback()
                
        except Exception as e:
            self.logger.error(f"Error configuring connection: {str(e)}")
            # Make sure to rollback any pending transaction
            try:
                await conn.rollback()
            except Exception:
                pass  # Ignore if rollback fails too
            # Re-raise so the pool knows this connection failed
            if isinstance(e, DatabaseError):
                raise
            else:
                raise ConnectionError(f"Error configuring connection: {str(e)}") from e
    
    async def close(self):
        """Close the database connection pool."""
        # Stop the buffer flush task first
        try:
            await self.stop_buffer_flush_task()
        except Exception as e:
            self.logger.error(f"Error stopping buffer flush task: {str(e)}", exc_info=True)

        if self.pool:
            self.logger.info("Closing database connection pool")
            await self.pool.close()
            self.pool = None
            self._initialized = False
    
    async def is_healthy(self) -> bool:
        """
        Check if the database connection is healthy.
        
        Returns:
            True if database is connected and responding, False otherwise
        """
        if not self.pool:
            return False
            
        try:
            async with self.pool.connection() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            self.logger.warning(f"Database health check failed: {str(e)}")
            return False
    
    async def _check_tables_exist(self) -> bool:
        """Check if the required tables exist in the database."""
        try:
            async with self.pool.connection() as conn:
                result = await conn.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'healthcheck_settings'
                    ) AS table_exists;
                    """
                )
                exists_row = await result.fetchone()
                
                # With dict_row factory, we need to access by column name
                return exists_row['table_exists'] if exists_row else False
        except Exception as e:
            self.logger.error(f"Error checking if tables exist: {str(e)}")
            if isinstance(e, DatabaseError):
                raise
            else:
                raise SchemaError(f"Error checking if tables exist: {str(e)}") from e
    
    async def _execute_with_retry(self, operation_name: str, coro_func, *args, **kwargs):
        """
        Execute a database operation with retry logic for transient errors.
        
        Args:
            operation_name: Name of the operation for logging
            coro_func: Coroutine function to execute
            *args, **kwargs: Arguments to pass to the coroutine function
            
        Returns:
            Result of the coroutine function
            
        Raises:
            DatabaseError: If all retry attempts fail
        """
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return await coro_func(*args, **kwargs)
            except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                # These are typically transient errors that might resolve with a retry
                if attempt == self.retry_attempts:
                    self.logger.error(
                        f"{operation_name} failed after {self.retry_attempts} attempts: {str(e)}", 
                        exc_info=True
                    )
                    raise ConnectionError(f"{operation_name} failed: {str(e)}") from e
                
                self.logger.warning(
                    f"{operation_name} failed (attempt {attempt}/{self.retry_attempts}): {str(e)}. "
                    f"Retrying in {self.retry_delay} seconds..."
                )
                
                # Check if pool is still healthy
                if not await self.is_healthy():
                    self.logger.warning("Database connection unhealthy, attempting to reinitialize")
                    try:
                        await self.close()
                        await self.initialize()
                    except Exception as reinit_error:
                        self.logger.error(f"Failed to reinitialize database: {str(reinit_error)}")
                        if isinstance(reinit_error, DatabaseError):
                            raise
                        else:
                            raise ConnectionError(f"Failed to reinitialize database: {str(reinit_error)}") from reinit_error
                
                await asyncio.sleep(self.retry_delay)
            except psycopg.errors.UniqueViolation as e:
                # Handle integrity errors
                self.logger.error(f"{operation_name} failed due to unique constraint violation: {str(e)}", exc_info=True)
                raise UniqueViolationError(str(e)) from e
            except psycopg.errors.ForeignKeyViolation as e:
                self.logger.error(f"{operation_name} failed due to foreign key violation: {str(e)}", exc_info=True)
                raise ForeignKeyError(str(e)) from e
            except psycopg.errors.CheckViolation as e:
                self.logger.error(f"{operation_name} failed due to an integrity constraint violation: {str(e)}", exc_info=True)
                raise CheckConstraintError(str(e)) from e
            except psycopg.errors.InFailedSqlTransaction as e:
                self.logger.error(f"{operation_name} failed due to transaction failure: {str(e)}", exc_info=True)
                raise TransactionError(f"{operation_name} failed: {str(e)}") from e
            except psycopg.errors.IntegrityError as e:
                self.logger.error(f"{operation_name} failed due to integrity constraint violation: {str(e)}", exc_info=True)
                raise IntegrityError(str(e)) from e
            except DatabaseError as e:
                # For our custom database errors, don't retry and re-raise directly
                self.logger.error(f"{operation_name} failed with database error: {str(e)}", exc_info=True)
                raise
            except Exception as e:
                # For other exceptions, we don't retry
                self.logger.error(f"{operation_name} failed: {str(e)}", exc_info=True)
                raise QueryError(f"{operation_name} failed: {str(e)}") from e
    
    async def get_all_health_check_settings(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all health check settings.
        
        Args:
            active_only: If True, return only active health checks
            
        Returns:
            List of health check settings as dictionaries
        """
        async def _get_settings():
            async with self.pool.connection() as conn:
                if active_only:
                    result = await conn.execute(
                        """
                        SELECT * FROM healthcheck_settings
                        WHERE active = TRUE
                        ORDER BY id;
                        """
                    )
                else:
                    result = await conn.execute(
                        """
                        SELECT * FROM healthcheck_settings
                        ORDER BY id;
                        """
                    )
                
                rows = await result.fetchall()
                settings = [dict(row) for row in rows]
                
                self.logger.debug(f"Retrieved {len(settings)} health check settings")
                return settings
        
        return await self._execute_with_retry("Get health check settings", _get_settings)
    
    def _map_error_type(self, error: Optional[HealthCheckError]) -> Optional[str]:
        """Map error object to database enum type."""
        if error is None:
            return None
        elif isinstance(error, StatusCodeError):
            return 'StatusCodeError'
        elif isinstance(error, ContentMatchError):
            return 'ContentMatchError'
        elif isinstance(error, TimeoutError):
            return 'TimeoutError'
        elif isinstance(error, HealthCheckError) and "Connection error" in str(error):
            return 'ConnectionError'
        else:
            return 'UnexpectedError'
 
    async def add_health_check_result(
        self,
        target_id: int,
        result: HealthCheckResult
    ) -> int:
        """
        Add a health check result to the database.
        
        Args:
            target_id: ID of the health check target, matching with setting's id
            result: HealthCheckResult object
            
        Returns:
            The ID of the newly created result entry
        """
        async def _add_result():
            error_type = self._map_error_type(result.error)
            error_message = str(result.error) if result.error else None
            content_match_success = None
            
            # Determine content match success
            if error_type == 'ContentMatchError':
                content_match_success = False
            elif result.success and result.content is not None:
                content_match_success = True
            
            check_time = datetime.now()
            
            # Prepare the query and parameters outside the transaction
            query = """
            INSERT INTO healthcheck_results
            (target_id, check_time, response_time, status_code, success, 
            error_type, error_message, content_match_success)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """
            
            params = (
                target_id,
                check_time,
                result.response_time_ms,
                result.status_code,
                result.success,
                error_type,
                error_message,
                content_match_success
            )
            
            async with self.pool.connection() as conn:
                # First, try direct insert without partition check
                try:
                    # Use a simple transaction that will auto-rollback on exception
                    async with conn.transaction():
                        result_db = await conn.execute(query, params)
                        row = await result_db.fetchone()
                        new_id = row['id']
                        
                        self.logger.info(
                            f"Added health check result: id={new_id}, target_id={target_id}, " +
                            f"success={result.success}, status_code={result.status_code}"
                        )
                        return new_id
                        
                except psycopg.errors.CheckViolation as e:
                    # If we get here, the transaction has already been rolled back by the context manager
                    if "no partition of relation" in str(e):
                        self.logger.info("Partition not found, creating it now")
                        
                        # Create the partition in its own transaction
                        async with conn.transaction():
                            await self._ensure_partition_exists(conn, check_time)
                        
                        # Now try the insert again in a new transaction
                        async with conn.transaction():
                            result_db = await conn.execute(query, params)
                            row = await result_db.fetchone()
                            new_id = row['id']
                            
                            self.logger.info(
                                f"Added health check result after creating partition: id={new_id}, target_id={target_id}, " +
                                f"success={result.success}, status_code={result.status_code}"
                            )
                            return new_id
                    else:
                        # Re-raise as a CheckConstraintError if it's a different CheckViolation error
                        raise CheckConstraintError(str(e)) from e
        
        return await self._execute_with_retry("Add health check result", _add_result)
    
    async def _ensure_partition_exists(self, conn: AsyncConnection, check_time: datetime) -> None:
        """
        Ensure that a monthly partition exists for the given date.
        
        This method creates a new monthly partition if one doesn't exist for the 
        specified date's month.
        
        Args:
            conn: Database connection
            check_time: Timestamp to check/create partition for
        """
        try:
            # Format partition name and date ranges for month
            year = check_time.year
            month = check_time.month
            
            # Calculate next month for end date
            next_month_year = year
            next_month = month + 1
            if next_month > 12:
                next_month = 1
                next_month_year += 1
                
            partition_month = check_time.strftime('%Y_%m')
            start_date = f"{year}-{month:02d}-01 00:00:00"
            end_date = f"{next_month_year}-{next_month:02d}-01 00:00:00"
            
            partition_name = f"healthcheck_results_{partition_month}"
            
            # Check if partition exists
            check_query = """
            SELECT EXISTS (
                SELECT FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s
                AND c.relkind = 'r'
            ) AS table_exists;
            """
            
            result = await conn.execute(check_query, (partition_name,))
            exists_row = await result.fetchone()
            
            # Using dict_row, so access by column name, not index
            if not exists_row or not exists_row['table_exists']:
                # Create partition directly
                create_query = f"""
                CREATE TABLE {partition_name} PARTITION OF healthcheck_results
                FOR VALUES FROM ('{start_date}') TO ('{end_date}');
                """
                
                self.logger.info(f"Creating new monthly partition: {partition_name}")
                await conn.execute(create_query)
                self.logger.info(f"Created new monthly partition: {partition_name}")
                
                # Don't rely on stored procedure for tests, create next month partition explicitly too
                try:
                    # Calculate next-next month partition (for extra buffer)
                    next_next_month_year = next_month_year
                    next_next_month = next_month + 1
                    if next_next_month > 12:
                        next_next_month = 1
                        next_next_month_year += 1
                    
                    next_partition_month = f"{next_month_year}_{next_month:02d}"
                    next_start_date = end_date
                    next_end_date = f"{next_next_month_year}-{next_next_month:02d}-01 00:00:00"
                    
                    next_partition_name = f"healthcheck_results_{next_partition_month}"
                    
                    # Check if next month partition exists
                    result = await conn.execute(check_query, (next_partition_name,))
                    next_exists_row = await result.fetchone()
                    
                    # Using dict_row, access by column name
                    if not next_exists_row or not next_exists_row['table_exists']:
                        next_create_query = f"""
                        CREATE TABLE {next_partition_name} PARTITION OF healthcheck_results
                        FOR VALUES FROM ('{next_start_date}') TO ('{next_end_date}');
                        """
                        
                        self.logger.info(f"Creating next monthly partition: {next_partition_name}")
                        await conn.execute(next_create_query)
                        self.logger.info(f"Created next monthly partition: {next_partition_name}")
                except Exception as inner_e:
                    # Log but continue - this is just a preemptive creation
                    self.logger.warning(f"Could not create future partition: {str(inner_e)}")
                    # Use more specific error type if applicable
                    if isinstance(inner_e, DatabaseError):
                        raise
                    raise PartitionError(f"Could not create future partition: {str(inner_e)}") from inner_e
        except Exception as e:
            self.logger.error(f"Error ensuring partition exists: {str(e)}", exc_info=True)
            # For tests, we need to raise the error so we know what went wrong
            if isinstance(e, DatabaseError):
                raise
            raise PartitionError(f"Error ensuring partition exists: {str(e)}") from e
        
    async def start_buffer_flush_task(self):
        """Start the periodic buffer flush task."""
        self.shutdown_event.clear()
        self.flush_task = asyncio.create_task(self._flush_buffer_periodically())
        self.logger.info("Started result buffer with 3s flush interval")

    async def stop_buffer_flush_task(self):
        """Stop the buffer flush task and flush any remaining results."""
        if self.flush_task:
            self.shutdown_event.set()
            await self.flush_task
            # Flush any remaining results
            await self.flush_result_buffer()

    async def buffer_health_check_result(self, target_id: int, result: HealthCheckResult) -> None:
        """Add a health check result to the buffer for batch processing."""
        async with self.buffer_lock:
            self.result_buffer.append((target_id, result))

    async def flush_result_buffer(self) -> None:
        """Flush all buffered results to the database."""
        async with self.buffer_lock:
            if not self.result_buffer:
                return
            
            buffer_to_flush = self.result_buffer.copy()
            self.result_buffer.clear()
        
        if buffer_to_flush:
            self.logger.info(f"Flushing {len(buffer_to_flush)} health check results")
            await self._batch_insert_results(buffer_to_flush)

    async def _flush_buffer_periodically(self):
        """Periodically flush the result buffer every 3 seconds."""
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(3.0)  # 3 second interval
                if not self.shutdown_event.is_set():
                    await self.flush_result_buffer()
            except Exception as e:
                self.logger.error(f"Error in periodic flush task: {str(e)}", exc_info=True)

    async def _batch_insert_results(self, results: List[Tuple[int, HealthCheckResult]]) -> None:
        """
        Insert multiple health check results using batched INSERT statements.
        Limits each INSERT to a maximum of 100 rows for better performance.
        """
        if not results:
            return
        
        async def _do_batch_insert():
            check_time = datetime.now()
            total_inserted = 0
            
            # Define max batch size
            MAX_BATCH_SIZE = 100
            
            async with self.pool.connection() as conn:
                # Process all results in a single transaction, but with multiple INSERTs
                async with conn.transaction():
                    # Process results in chunks of MAX_BATCH_SIZE
                    for i in range(0, len(results), MAX_BATCH_SIZE):
                        chunk = results[i:i + MAX_BATCH_SIZE]
                        
                        # Prepare the INSERT statement and parameters for this chunk
                        placeholders = []
                        params = []
                        
                        for target_id, result in chunk:
                            error_type = self._map_error_type(result.error)
                            error_message = str(result.error) if result.error else None
                            content_match_success = None
                            
                            if error_type == 'ContentMatchError':
                                content_match_success = False
                            elif result.success and result.content is not None:
                                content_match_success = True
                            
                            # Add placeholders for this row
                            placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s)")
                            
                            # Add parameters for this row
                            params.extend([
                                target_id, check_time, result.response_time_ms, result.status_code,
                                result.success, error_type, error_message, content_match_success
                            ])
                        
                        # Build and execute the insert for this chunk
                        query = f"""
                        INSERT INTO healthcheck_results
                        (target_id, check_time, response_time, status_code, success, 
                        error_type, error_message, content_match_success)
                        VALUES {', '.join(placeholders)}
                        """

                        try:
                            async with self.pool.connection() as conn:
                                await conn.execute(query, params)
                        except psycopg.errors.CheckViolation as e:
                            # If we get here, the transaction has already been rolled back by the context manager
                            if "no partition of relation" in str(e):
                                self.logger.info("Partition not found, creating it now")
                                
                                # Create the partition in its own transaction
                                await self._ensure_partition_exists(conn, check_time)

                                # Rerun the query
                                async with self.pool.connection() as conn:
                                    await conn.execute(query, params)
                            else:
                                # Re-raise as a CheckConstraintError if it's a different CheckViolation error
                                raise CheckConstraintError(str(e)) from e
                        
                        total_inserted += len(chunk)
                        
                        self.logger.debug(f"Inserted chunk of {len(chunk)} results (total: {total_inserted})")
                    
                    self.logger.info(f"Inserted total of {total_inserted} health check results in {(len(results) + MAX_BATCH_SIZE - 1) // MAX_BATCH_SIZE} batches")
    
        await self._execute_with_retry("Batch insert health check results", _do_batch_insert)