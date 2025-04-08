import unittest
import asyncio
import logging
import re
import json
import time
import random
from aiohttp import web

# Import the module to test
from src.http_health_checker import HttpHealthChecker, run_health_check, HealthCheckResult
from src.errors import HealthCheckError, StatusCodeError, ContentMatchError, TimeoutError


# Test HTTP server for all tests
class TestHttpServer:
    def __init__(self, host='127.0.0.1', port=None, logger=None):
        self.host = host
        # Use a random port to avoid conflicts
        self.port = port or random.randint(8800, 9800)
        self.logger = logger or logging.getLogger(__name__)
        self.app = web.Application()
        self.setup_routes()
        self.runner = None
        self.site = None
    
    def setup_routes(self):
        # Healthy endpoint - returns 200 with expected content
        self.app.router.add_get('/health', self.health_handler)
        
        # Unhealthy endpoint - returns 500
        self.app.router.add_get('/health/error', self.error_handler)
        
        # Timeout endpoint - delays response
        self.app.router.add_get('/health/timeout', self.timeout_handler)
        
        # Content mismatch endpoint - returns 200 but with wrong content
        self.app.router.add_get('/health/content-mismatch', self.content_mismatch_handler)
        
        # Binary content endpoint - returns non-text content
        self.app.router.add_get('/health/binary', self.binary_handler)
        
        # Connection refused simulation (will return 404, but we won't configure a handler)
        # /health/connection-error 
    
    async def health_handler(self, request):
        """Returns healthy response."""
        return web.json_response({
            "status": "ok",
            "timestamp": time.time(),
            "services": {
                "database": "up",
                "cache": "up"
            }
        })
    
    async def error_handler(self, request):
        """Returns error status code."""
        return web.Response(
            status=500,
            body=json.dumps({
                "status": "error",
                "message": "Internal server error"
            }),
            content_type='application/json'
        )
    
    async def timeout_handler(self, request):
        """Simulates a timeout by waiting before response."""
        # Wait to simulate slow response (adjust based on test timeout setting)
        await asyncio.sleep(3)
        return web.json_response({
            "status": "ok",
            "timestamp": time.time()
        })
    
    async def content_mismatch_handler(self, request):
        """Returns 200 but with content that won't match expected regex."""
        return web.json_response({
            "status": "different",
            "timestamp": time.time()
        })
    
    async def binary_handler(self, request):
        """Returns binary data that should cause Unicode decode errors."""
        binary_data = bytes([random.randint(0, 255) for _ in range(100)])
        return web.Response(
            body=binary_data,
            content_type='application/octet-stream'
        )
    
    async def start(self):
        """Start the HTTP server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        # Try different ports if binding fails
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                self.site = web.TCPSite(self.runner, self.host, self.port)
                await self.site.start()
                self.logger.info(f"Test HTTP server started at http://{self.host}:{self.port}")
                return f"http://{self.host}:{self.port}"
            except OSError as e:
                if attempt < max_attempts - 1:
                    self.port = random.randint(8800, 9800)
                    self.logger.warning(f"Port binding failed, trying new port {self.port}")
                else:
                    raise
    
    async def stop(self):
        """Stop the HTTP server."""
        if self.site and self.runner:
            self.logger.info("Stopping test HTTP server")
            await self.runner.cleanup()
            self.logger.info("Test HTTP server stopped")


class TestHttpHealthCheckerInit(unittest.TestCase):
    """Test cases for HttpHealthChecker initialization."""
    
    def setUp(self):
        self.logger = logging.getLogger("test_logger")
        self.valid_url = "https://example.com/health"
    
    def test_init_valid_parameters(self):
        """Test initialization with valid parameters."""
        checker = HttpHealthChecker(
            url=self.valid_url,
            expected_status_code=200,
            content_regex=r'"status":\s*"ok"',
            timeout_ms=5000,
            headers={"User-Agent": "TestClient"},
            logger=self.logger
        )
        
        self.assertEqual(checker.url, self.valid_url)
        self.assertEqual(checker.expected_status_code, 200)
        self.assertIsNotNone(checker.content_regex)
        self.assertEqual(checker.timeout_ms, 5000)
        self.assertEqual(checker.timeout_sec, 5.0)
        self.assertEqual(checker.headers, {"User-Agent": "TestClient"})
    
    def test_init_default_parameters(self):
        """Test initialization with default parameters."""
        checker = HttpHealthChecker(url=self.valid_url)
        
        self.assertEqual(checker.url, self.valid_url)
        self.assertEqual(checker.expected_status_code, 200)
        self.assertIsNone(checker.content_regex)
        self.assertEqual(checker.timeout_ms, 5000)
        self.assertEqual(checker.timeout_sec, 5.0)
        self.assertEqual(checker.headers, {})
    
    def test_init_invalid_url(self):
        """Test initialization with invalid URL."""
        invalid_urls = [
            "not-a-url",
            "ftp://example.com",
            "//no-scheme.com",
            "http://"
        ]
        
        for url in invalid_urls:
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    HttpHealthChecker(url=url)
    
    def test_init_invalid_status_code(self):
        """Test initialization with invalid status code."""
        invalid_status_codes = [99, 600, -1]
        
        for status_code in invalid_status_codes:
            with self.subTest(status_code=status_code):
                with self.assertRaises(ValueError):
                    HttpHealthChecker(url=self.valid_url, expected_status_code=status_code)
    
    def test_init_invalid_timeout(self):
        """Test initialization with invalid timeout."""
        invalid_timeouts = [0, -100]
        
        for timeout in invalid_timeouts:
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    HttpHealthChecker(url=self.valid_url, timeout_ms=timeout)
    
    def test_init_invalid_regex(self):
        """Test initialization with invalid regex pattern."""
        invalid_patterns = [
            "(",  # unclosed parenthesis
            "[",  # unclosed bracket
            "*"   # invalid quantifier
        ]
        
        for pattern in invalid_patterns:
            with self.subTest(pattern=pattern):
                with self.assertRaises(ValueError):
                    HttpHealthChecker(url=self.valid_url, content_regex=pattern)


class TestHttpHealthChecker(unittest.IsolatedAsyncioTestCase):
    """Test cases for HttpHealthChecker with real HTTP requests."""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method."""
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('test_health_checker')
        
        # Start the test HTTP server
        self.http_server = TestHttpServer(logger=self.logger)
        self.server_base_url = await self.http_server.start()
        self.logger.info(f"Test server started at {self.server_base_url}")
    
    async def asyncTearDown(self):
        """Clean up test fixtures after each test method."""
        if hasattr(self, 'http_server'):
            await self.http_server.stop()
    
    async def test_check_success(self):
        """Test successful health check."""
        checker = HttpHealthChecker(
            url=f"{self.server_base_url}/health",
            expected_status_code=200,
            content_regex=r'"status":\s*"ok"',
            timeout_ms=5000,
            logger=self.logger
        )
        
        result = await checker.check()
        
        self.assertTrue(result.success)
        self.assertEqual(result.status_code, 200)
        self.assertIsNotNone(result.response_time_ms)
        self.assertIsNone(result.error)
    
    async def test_check_status_code_error(self):
        """Test health check with status code error."""
        checker = HttpHealthChecker(
            url=f"{self.server_base_url}/health/error",
            expected_status_code=200,  # Expecting 200 but will get 500
            timeout_ms=5000,
            logger=self.logger
        )
        
        result = await checker.check()
        
        self.assertFalse(result.success)
        self.assertEqual(result.status_code, 500)
        self.assertIsInstance(result.error, StatusCodeError)
    
    async def test_check_content_match_error(self):
        """Test health check with content match error."""
        checker = HttpHealthChecker(
            url=f"{self.server_base_url}/health/content-mismatch",
            expected_status_code=200,
            content_regex=r'"status":\s*"ok"',  # Expecting "ok" but will get "different"
            timeout_ms=5000,
            logger=self.logger
        )
        
        result = await checker.check()
        
        self.assertFalse(result.success)
        self.assertEqual(result.status_code, 200)
        self.assertIsInstance(result.error, ContentMatchError)
    
    async def test_check_timeout_error(self):
        """Test health check with timeout error."""
        checker = HttpHealthChecker(
            url=f"{self.server_base_url}/health/timeout",
            expected_status_code=200,
            timeout_ms=1000,  # 1 second timeout (endpoint takes 3 seconds)
            logger=self.logger
        )
        
        result = await checker.check()
        
        self.assertFalse(result.success)
        self.assertEqual(result.status_code, 0)
        self.assertIsInstance(result.error, TimeoutError)
    
    async def test_check_connection_error(self):
        """Test health check with connection error or timeout."""
        # A URL pointing to a non-existent port or endpoint to simulate a connection error
        # Note: Depending on the OS and network stack, this might result in either:
        # - An immediate connection refused error 
        # - A timeout after the system waits for a response
        # Both are valid failure scenarios we need to handle
        checker = HttpHealthChecker(
            url=f"http://{self.http_server.host}:{self.http_server.port + 10000}/health",
            expected_status_code=200,
            timeout_ms=2000,
            logger=self.logger
        )
        
        result = await checker.check()
        
        # Verify the basic failure case
        self.assertFalse(result.success)
        self.assertEqual(result.status_code, 0)
        
        # The error could be either a direct connection error or a timeout
        # Both are acceptable results for this test
        self.assertIsInstance(result.error, HealthCheckError)
        
        # Don't check the specific error message as it might be either
        # a timeout or connection error depending on the environment
    
    async def test_check_unicode_decode_error(self):
        """Test health check with Unicode decode error."""
        checker = HttpHealthChecker(
            url=f"{self.server_base_url}/health/binary",
            expected_status_code=200,
            content_regex=r'"status":\s*"ok"',  # This won't match binary data
            timeout_ms=5000,
            logger=self.logger
        )
        
        result = await checker.check()
        
        self.assertFalse(result.success)
        self.assertEqual(result.status_code, 200)
        self.assertIsInstance(result.error, ContentMatchError)
        self.assertEqual(result.content, "[Binary content]")


class TestRunHealthCheckFunction(unittest.IsolatedAsyncioTestCase):
    """Test the run_health_check convenience function."""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method."""
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('test_run_health_check')
        
        # Start the test HTTP server
        self.http_server = TestHttpServer(logger=self.logger)
        self.server_base_url = await self.http_server.start()
        self.logger.info(f"Test server started at {self.server_base_url}")
    
    async def asyncTearDown(self):
        """Clean up test fixtures after each test method."""
        if hasattr(self, 'http_server'):
            await self.http_server.stop()
    
    async def test_run_health_check_success(self):
        """Test run_health_check with a successful endpoint."""
        result = await run_health_check(
            url=f"{self.server_base_url}/health",
            expected_status_code=200,
            content_regex=r'"status":\s*"ok"',
            timeout_ms=5000,
            logger=self.logger
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.status_code, 200)
        self.assertIsNotNone(result.response_time_ms)
        self.assertIsNone(result.error)
    
    async def test_run_health_check_failure(self):
        """Test run_health_check with a failing endpoint."""
        result = await run_health_check(
            url=f"{self.server_base_url}/health/error",
            expected_status_code=200,  # Expecting 200 but will get 500
            timeout_ms=5000,
            logger=self.logger
        )
        
        self.assertFalse(result.success)
        self.assertEqual(result.status_code, 500)
        self.assertIsInstance(result.error, StatusCodeError)


if __name__ == '__main__':
    unittest.main()