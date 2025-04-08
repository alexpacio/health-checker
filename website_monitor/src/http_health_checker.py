import asyncio
import re
import time
import uuid
from typing import Dict, Optional
import aiohttp
from aiohttp import ClientTimeout
import logging
from dataclasses import dataclass
from urllib.parse import urlparse

from .errors import ContentMatchError, HealthCheckError, StatusCodeError, TimeoutError

@dataclass
class HealthCheckResult:
    """Result of a health check operation."""
    url: str
    status_code: int
    response_time_ms: float
    success: bool
    content: Optional[str] = None
    error: Optional[HealthCheckError] = None


class HttpHealthChecker:
    """
    Asynchronous health checker for HTTP and HTTPS endpoints.
    
    This class provides functionality to perform health checks on web endpoints
    asynchronously, with validation of status codes and response content.
    """
    
    def __init__(
        self,
        url: str,
        expected_status_code: int = 200,
        content_regex: Optional[str] = None,
        timeout_ms: int = 5000,
        headers: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
        log_level: int = logging.INFO
    ):
        """
        Initialize the health checker with the target URL and validation parameters.
        
        Args:
            url: The URL to health check (HTTP or HTTPS)
            expected_status_code: The expected HTTP status code
            content_regex: Optional regex pattern to match against response content
            timeout_ms: Request timeout in milliseconds
            headers: Optional headers to include in the request
            logger: Optional logger instance for logging
            log_level: Logging level for standard operations (defaults to INFO)
        
        Raises:
            ValueError: If URL is invalid or parameters are out of acceptable ranges
        """
        # Set up logging
        self.logger = logger or logging.getLogger(__name__)
        self.log_level = log_level
        
        # Generate a unique ID for tracking this health checker instance
        self.instance_id = str(uuid.uuid4())[:8]
        
        self.logger.debug(f"[HealthChecker-{self.instance_id}] Initializing for URL: {url}")
        
        # Validate URL
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            error_msg = f"Invalid URL: {url}"
            self.logger.error(f"[HealthChecker-{self.instance_id}] {error_msg}")
            raise ValueError(error_msg)
            
        if parsed_url.scheme not in ("http", "https"):
            error_msg = f"URL scheme must be http or https, got: {parsed_url.scheme}"
            self.logger.error(f"[HealthChecker-{self.instance_id}] {error_msg}")
            raise ValueError(error_msg)
        
        # Validate other parameters
        if expected_status_code < 100 or expected_status_code > 599:
            error_msg = f"Invalid status code: {expected_status_code}"
            self.logger.error(f"[HealthChecker-{self.instance_id}] {error_msg}")
            raise ValueError(error_msg)
            
        if timeout_ms <= 0:
            error_msg = f"Timeout must be positive, got: {timeout_ms}"
            self.logger.error(f"[HealthChecker-{self.instance_id}] {error_msg}")
            raise ValueError(error_msg)
        
        self.url = url
        self.expected_status_code = expected_status_code
        self.timeout_ms = timeout_ms
        self.timeout_sec = timeout_ms / 1000.0
        self.headers = headers or {}
        
        # Precompile regex for better performance if provided
        self.content_regex = None
        if content_regex:
            try:
                self.content_regex = re.compile(content_regex)
                self.logger.debug(f"[HealthChecker-{self.instance_id}] Compiled regex pattern: {content_regex}")
            except re.error as e:
                error_msg = f"Invalid regex pattern: {content_regex}. Error: {str(e)}"
                self.logger.error(f"[HealthChecker-{self.instance_id}] {error_msg}")
                raise ValueError(error_msg)
                
        self.logger.log(self.log_level, f"[HealthChecker-{self.instance_id}] Initialized health checker for {url} " +
                      f"(Expected status: {expected_status_code}, Timeout: {timeout_ms}ms, " +
                      f"Content check: {'Yes' if content_regex else 'No'})")
        
        # We've moved this code to the initialization block above
    
    async def check(self) -> HealthCheckResult:
        """
        Perform the health check asynchronously.
        
        Returns:
            HealthCheckResult object containing the check results
            
        Raises:
            HealthCheckError: Base class for health check exceptions
            StatusCodeError: When status code doesn't match expected value
            ContentMatchError: When content doesn't match the regex pattern
            TimeoutError: When the request times out
            Exception: For any other unexpected errors
        """
        # Generate a unique check ID for tracing this specific health check run
        check_id = str(uuid.uuid4())[:6]
        log_prefix = f"[HealthChecker-{self.instance_id}][Check-{check_id}]"
        
        self.logger.log(self.log_level, f"{log_prefix} Starting health check for {self.url}")
        
        start_time = time.time()
        response_time_ms = 0
        
        try:
            # Create a timeout object for aiohttp
            timeout = ClientTimeout(total=self.timeout_sec)
            
            self.logger.debug(f"{log_prefix} Creating HTTP session with timeout {self.timeout_sec}s")
            if self.headers:
                self.logger.debug(f"{log_prefix} Using custom headers: {self.headers}")
                
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    self.logger.debug(f"{log_prefix} Sending GET request to {self.url}")
                    async with session.get(self.url, headers=self.headers) as response:
                        # Calculate response time
                        response_time_ms = (time.time() - start_time) * 1000
                        
                        self.logger.debug(
                            f"{log_prefix} Received response: status={response.status}, "
                            f"content-type={response.headers.get('content-type', 'unknown')}, "
                            f"time={response_time_ms:.2f}ms"
                        )
                        
                        # Check status code
                        if response.status != self.expected_status_code:
                            error = StatusCodeError(
                                expected=self.expected_status_code,
                                actual=response.status
                            )
                            self.logger.warning(f"{log_prefix} {str(error)}")
                            
                            # Log response headers for debugging
                            self.logger.debug(f"{log_prefix} Response headers: {dict(response.headers)}")
                            
                            return HealthCheckResult(
                                url=self.url,
                                status_code=response.status,
                                response_time_ms=response_time_ms,
                                success=False,
                                error=error
                            )
                        
                        # Check content if regex is provided
                        if self.content_regex:
                            self.logger.debug(f"{log_prefix} Checking content against regex: {self.content_regex.pattern}")
                            
                            # Try to get text content, handle different content types
                            try:
                                content = await response.text()
                                content_length = len(content)
                                self.logger.debug(f"{log_prefix} Received text content ({content_length} chars)")
                                
                                # Check if content matches regex
                                match = self.content_regex.search(content)
                                if not match:
                                    error = ContentMatchError(
                                        pattern=self.content_regex.pattern,
                                        content=content
                                    )
                                    self.logger.warning(f"{log_prefix} {str(error)}")
                                    
                                    # Log a snippet of the content for debugging
                                    content_preview = content[:200] + "..." if len(content) > 200 else content
                                    self.logger.debug(f"{log_prefix} Content snippet: {content_preview}")
                                    
                                    return HealthCheckResult(
                                        url=self.url,
                                        status_code=response.status,
                                        response_time_ms=response_time_ms,
                                        success=False,
                                        content=content[:100] + "..." if len(content) > 100 else content,
                                        error=error
                                    )
                                else:
                                    self.logger.debug(f"{log_prefix} Content matched regex pattern")
                                    # Log the match for debugging
                                    match_text = match.group(0)
                                    self.logger.debug(f"{log_prefix} Match: {match_text[:50]}{'...' if len(match_text) > 50 else ''}")
                            except UnicodeDecodeError as ude:
                                error = ContentMatchError(
                                    pattern=self.content_regex.pattern,
                                    content="[Binary content]",
                                    message="Cannot check regex pattern on binary content"
                                )
                                self.logger.warning(f"{log_prefix} {str(error)} (Decode error: {str(ude)})")
                                
                                # Try to log content type for debugging
                                content_type = response.headers.get('content-type', 'unknown')
                                self.logger.debug(f"{log_prefix} Problematic content type: {content_type}")
                                
                                return HealthCheckResult(
                                    url=self.url,
                                    status_code=response.status,
                                    response_time_ms=response_time_ms,
                                    success=False,
                                    content="[Binary content]",
                                    error=error
                                )
                        
                        # All checks passed
                        content = await response.text() if self.content_regex else None
                        self.logger.log(
                            self.log_level, 
                            f"{log_prefix} Health check successful: {self.url} " +
                            f"(status={response.status}, time={response_time_ms:.2f}ms)"
                        )
                        
                        return HealthCheckResult(
                            url=self.url,
                            status_code=response.status,
                            response_time_ms=response_time_ms,
                            success=True,
                            content=content
                        )
                
                except asyncio.TimeoutError:
                    response_time_ms = self.timeout_ms  # Set to max timeout
                    error = TimeoutError(self.timeout_ms)
                    self.logger.warning(f"{log_prefix} {str(error)}")
                    
                    return HealthCheckResult(
                        url=self.url,
                        status_code=0,
                        response_time_ms=response_time_ms,
                        success=False,
                        error=error
                    )
                
                except aiohttp.ClientError as e:
                    response_time_ms = (time.time() - start_time) * 1000
                    error = HealthCheckError(f"Connection error: {str(e)}")
                    self.logger.warning(f"{log_prefix} {str(error)}")
                    
                    # Log more details about the error
                    self.logger.debug(f"{log_prefix} Client error details: {type(e).__name__}: {str(e)}")
                    
                    return HealthCheckResult(
                        url=self.url,
                        status_code=0,
                        response_time_ms=response_time_ms,
                        success=False,
                        error=error
                    )
        
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            error = HealthCheckError(f"Unexpected error during health check: {str(e)}")
            self.logger.error(f"{log_prefix} {str(error)}", exc_info=True)
            
            return HealthCheckResult(
                url=self.url,
                status_code=0,
                response_time_ms=response_time_ms,
                success=False,
                error=error
            )


# Function to run a single health check
async def run_health_check(
    url: str,
    expected_status_code: int = 200,
    content_regex: Optional[str] = None,
    timeout_ms: int = 5000,
    headers: Optional[Dict[str, str]] = None,
    logger: Optional[logging.Logger] = None,
    log_level: int = logging.INFO
) -> HealthCheckResult:
    """
    Convenience function to run a single health check without creating a class instance.
    
    Args:
        url: The URL to health check
        expected_status_code: The expected HTTP status code
        content_regex: Optional regex pattern to match against response content
        timeout_ms: Request timeout in milliseconds
        headers: Optional headers to include in the request
        logger: Optional logger instance for logging
        log_level: Logging level for standard operations (defaults to INFO)
        
    Returns:
        HealthCheckResult object containing the check results
    """
    # Set up default logger if not provided
    if logger is None:
        logger = logging.getLogger(__name__)
        
    logger.log(log_level, f"Running http health check for {url}")
    
    checker = HttpHealthChecker(
        url=url,
        expected_status_code=expected_status_code,
        content_regex=content_regex,
        timeout_ms=timeout_ms,
        headers=headers,
        logger=logger,
        log_level=log_level
    )
    
    result = await checker.check()
    
    if result.success:
        logger.log(log_level, f"Health check successful: {url} (time={result.response_time_ms:.2f}ms)")
    else:
        logger.warning(f"Health check failed: {url} (error={result.error})")
        
    return result