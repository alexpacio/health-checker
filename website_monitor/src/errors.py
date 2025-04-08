# Custom exception classes for database recorder

from typing import Optional


class DatabaseError(Exception):
    """Base exception for all database-related errors"""
    def __init__(self, message="A database error occurred", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class ConnectionError(DatabaseError):
    """Error establishing or maintaining database connection"""
    def __init__(self, message="Failed to connect to the database", *args, **kwargs):
        super().__init__(message, *args, **kwargs)

class InitialConnectionError(ConnectionError):
    """Exception raised when the initial connection to the database fails"""
    def __init__(self, message="Initial connection to database failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)

class ConnectionTimeoutError(ConnectionError):
    """Connection attempt timed out"""
    def __init__(self, timeout_seconds=None, *args, **kwargs):
        message = "Database connection timed out"
        if timeout_seconds is not None:
            message += f" after {timeout_seconds} seconds"
        super().__init__(message, *args, **kwargs)

class TransactionError(DatabaseError):
    """Error during transaction execution"""
    def __init__(self, message="Transaction failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class QueryError(DatabaseError):
    """Error executing a database query"""
    def __init__(self, query=None, params=None, *args, **kwargs):
        message = "Database query failed"
        if query:
            # Truncate long queries for readability
            query_str = str(query)
            if len(query_str) > 200:
                query_str = query_str[:197] + "..."
            message += f": {query_str}"
        super().__init__(message, *args, **kwargs)
        self.query = query
        self.params = params


class SchemaError(DatabaseError):
    """Error related to database schema"""
    def __init__(self, message="Database schema error", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class IntegrityError(DatabaseError):
    """Database integrity constraint violation"""
    def __init__(self, message="Database integrity constraint violated", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class ForeignKeyError(IntegrityError):
    """Foreign key constraint violation"""
    def __init__(self, constraint=None, *args, **kwargs):
        message = "Foreign key constraint violated"
        if constraint:
            message += f": {constraint}"
        super().__init__(message, *args, **kwargs)


class UniqueViolationError(IntegrityError):
    """Unique constraint violation"""
    def __init__(self, constraint=None, *args, **kwargs):
        message = "Unique constraint violated"
        if constraint:
            message += f": {constraint}"
        super().__init__(message, *args, **kwargs)


class CheckConstraintError(IntegrityError):
    """Check constraint violation"""
    def __init__(self, constraint=None, *args, **kwargs):
        message = "Check constraint violated"
        if constraint:
            message += f": {constraint}"
        super().__init__(message, *args, **kwargs)


class PartitionError(DatabaseError):
    """Error related to table partitioning"""
    def __init__(self, message="Partition operation failed", *args, **kwargs):
        super().__init__(message, *args, **kwargs)




# Custom exception classes for health checks
class HealthCheckError(Exception):
    """Base exception for health check failures."""
    pass


class StatusCodeError(HealthCheckError):
    """Exception raised when status code doesn't match the expected one."""
    
    def __init__(self, expected: int, actual: int, message: Optional[str] = None):
        self.expected = expected
        self.actual = actual
        self.message = message or f"Expected status code {expected}, got {actual}"
        super().__init__(self.message)


class ContentMatchError(HealthCheckError):
    """Exception raised when response content doesn't match the regex pattern."""
    
    def __init__(self, pattern: str, content: str, message: Optional[str] = None):
        self.pattern = pattern
        # Truncate content to avoid extremely large error messages
        self.content = content[:100] + "..." if len(content) > 100 else content
        self.message = message or f"Response content did not match pattern '{pattern}'"
        super().__init__(self.message)


class TimeoutError(HealthCheckError):
    """Exception raised when the health check times out."""
    
    def __init__(self, timeout_ms: int, message: Optional[str] = None):
        self.timeout_ms = timeout_ms
        self.message = message or f"Health check timed out after {timeout_ms}ms"
        super().__init__(self.message)
