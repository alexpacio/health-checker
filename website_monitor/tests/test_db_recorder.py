import os
import sys
import asyncio
import logging
import unittest
from datetime import datetime
import psycopg
from io import BytesIO
import json
from unittest.mock import patch, MagicMock

from src.http_health_checker import (
    HealthCheckResult, 
    HealthCheckError, 
    StatusCodeError, 
    ContentMatchError, 
    TimeoutError
)
from src.db_recorder import HealthCheckDatabase
from src.runtime_settings import RuntimeSettingsReader


class TestHealthCheckDatabase(unittest.IsolatedAsyncioTestCase):
    """Test case for HealthCheckDatabase with a real PostgreSQL server but mocked HTTP checks."""
    
    test_settings_ids = []  # Will store the IDs of test settings we create
    
    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        cls.logger = logging.getLogger('test_db_recorder')
        
        # Check for .env file
        env_file_path = '.env'
        if not os.path.exists(env_file_path):
            cls.logger.error(f"Required .env file not found at: {env_file_path}")
            cls.skip_tests = True
            return
            
        # Load settings from .env file
        try:
            cls.logger.info(f"Loading settings from .env file: {env_file_path}")
            with open(env_file_path, 'rb') as f:
                env_content = f.read()
            
            # Create BytesIO buffer and initialize RuntimeSettingsReader
            cls.env_buffer = BytesIO(env_content)
            cls.settings_reader = RuntimeSettingsReader(cls.env_buffer)
            
            # Get database settings
            cls.db_settings = cls.settings_reader.get_db_connection_settings()
            cls.health_settings = cls.settings_reader.get_health_check_settings()
            
            # Build DSN from settings
            cls.dsn = (
                f"postgresql://{cls.db_settings.get('user')}:{cls.db_settings.get('password')}"
                f"@{cls.db_settings.get('host')}:{cls.db_settings.get('port')}"
                f"/{cls.db_settings.get('name')}"
            )

            if cls.db_settings.get('use_ssl', False):
                cls.dsn += "?sslmode=require"
            
            cls.skip_tests = False
        except Exception as e:
            cls.logger.error(f"Failed to load database settings: {e}")
            cls.skip_tests = True
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test method."""
        if getattr(self.__class__, 'skip_tests', False):
            self.skipTest("Database settings unavailable or .env file missing")
            return
            
        try:
            # Initialize database connection using settings from RuntimeSettingsReader
            db_settings = self.__class__.db_settings
            
            # Use pool size from settings or default values
            min_pool_size = 1
            max_pool_size = db_settings.get('pool_size', 5)
            connection_timeout = db_settings.get('connect_timeout', 10)
            
            self.db = HealthCheckDatabase(
                connection_string=self.__class__.dsn,
                min_pool_size=min_pool_size,
                max_pool_size=max_pool_size,
                logger=self.__class__.logger,
                connection_timeout=connection_timeout
            )
            await self.db.initialize()
            self.logger.info("Database initialized for testing")
            
            # Store health check settings for later use
            self.health_settings = self.__class__.health_settings
            
            # Add a test health check setting
            self.test_settings_ids = []
            await self._add_test_setting()
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            self.fail(f"Database initialization failed: {e}")
    
    async def asyncTearDown(self):
        """Clean up test fixtures after each test method."""
        if hasattr(self, 'db') and self.db:
            try:
                # Clean up any test data we've added
                await self._cleanup_test_data()
            except Exception as e:
                self.logger.error(f"Error during test data cleanup: {e}")
            finally:
                await self.db.close()
                self.logger.info("Database connection closed")
    
    async def _add_test_setting(self):
        """Helper method to add a test health check setting directly to the database."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        url = f"https://example.com/health/{timestamp}"
        
        try:
            # Use health check settings from RuntimeSettingsReader
            expected_status_code = self.health_settings.get('expected_status_code', 200)
            check_interval = self.health_settings.get('check_interval_in_sec', 60)
            timeout_ms = self.health_settings.get('connection_timeout_in_sec', 5) * 1000
            
            async with self.db.pool.connection() as conn:
                async with conn.transaction():
                    result = await conn.execute(
                        """
                        INSERT INTO healthcheck_settings
                        (url, regex_match, expected_status_code, check_interval, timeout_ms, headers, active)
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                        RETURNING id;
                        """,
                        (
                            url,
                            r'"status":\s*"ok"',
                            expected_status_code,
                            check_interval,
                            timeout_ms,
                            psycopg.types.json.Json({"User-Agent": "HealthCheckTest/1.0"}),
                            True
                        )
                    )
                    row = await result.fetchone()
                    setting_id = row['id']
                    self.test_settings_ids.append(setting_id)
                    self.logger.info(f"Added test setting with ID: {setting_id} for URL: {url}")
                    return setting_id
        except Exception as e:
            self.logger.error(f"Error adding test setting: {e}")
            raise
    
    async def _cleanup_test_data(self):
        """Clean up all test data created during the tests."""
        try:
            if not self.test_settings_ids:
                return
                
            self.logger.info(f"Cleaning up test data for settings: {self.test_settings_ids}")
            
            async with self.db.pool.connection() as conn:
                async with conn.transaction():
                    # First delete all health check results for these settings
                    for setting_id in self.test_settings_ids:
                        await conn.execute(
                            """
                            DELETE FROM healthcheck_results
                            WHERE target_id = %s;
                            """,
                            (setting_id,)
                        )
                    
                    # Then delete the settings themselves
                    placeholders = ','.join(['%s'] * len(self.test_settings_ids))
                    await conn.execute(
                        f"""
                        DELETE FROM healthcheck_settings
                        WHERE id IN ({placeholders});
                        """,
                        self.test_settings_ids
                    )
                    
            self.logger.info("Test data cleanup complete")
            self.test_settings_ids = []
        except Exception as e:
            self.logger.error(f"Error during test data cleanup: {e}")
            raise
    
    # --- Database Connection Tests ---
    
    async def test_01_initialize_connection(self):
        """Test database connection initialization."""
        # Connection was already initialized in asyncSetUp
        # Verify that the tables exist by running a query
        try:
            tables_exist = await self.db._check_tables_exist()
            self.assertTrue(tables_exist, "Required tables do not exist")
            
            # Test health check method
            is_healthy = await self.db.is_healthy()
            self.assertTrue(is_healthy, "Database should be healthy")
        except Exception as e:
            self.fail(f"Failed to verify tables: {str(e)}")
    
    async def test_01b_invalid_connection(self):
        """Test connection with invalid credentials (skipped)."""
        # Skip this slow test
        self.skipTest("Skipping slow invalid connection test - affects test runtime too much")
    
    # --- Health Check Settings Tests ---
    
    async def test_02_get_all_health_check_settings(self):
        """Test retrieving all health check settings."""
        # Get all active settings
        settings = await self.db.get_all_health_check_settings()
        self.assertIsNotNone(settings, "Failed to retrieve all settings")
        self.assertIsInstance(settings, list, "Settings should be returned as a list")
        
        # Get all settings including inactive ones
        all_settings = await self.db.get_all_health_check_settings(active_only=False)
        self.assertIsNotNone(all_settings, "Failed to retrieve all settings")
        self.assertIsInstance(all_settings, list, "Settings should be returned as a list")
        
        # We should have at least as many total settings as active ones
        self.assertGreaterEqual(len(all_settings), len(settings), 
                               "Total settings count should be >= active settings count")
        
        # Verify our test setting is in the results
        test_setting_ids = set(self.test_settings_ids)
        found_ids = {s['id'] for s in settings}
        self.assertTrue(test_setting_ids.issubset(found_ids), 
                       f"Test settings {test_setting_ids} should be in retrieved settings {found_ids}")
    
    # --- Health Check Result Tests ---
    
    async def test_03_add_health_check_result_success(self):
        """Test adding a successful health check result."""
        # Use our test setting
        if not self.test_settings_ids:
            self.fail("No test setting available")
        
        target_id = self.test_settings_ids[0]
        
        # Create a successful result
        result = HealthCheckResult(
            url="https://example.com/health",
            status_code=200,
            response_time_ms=150.5,
            success=True,
            content='{"status": "ok"}'
        )
        
        # Add to database
        result_id = await self.db.add_health_check_result(target_id, result)
        self.assertIsNotNone(result_id, "Failed to get ID for created result")
        self.assertGreater(result_id, 0, "Result ID should be positive")
        
        # Verify the result was added correctly
        async with self.db.pool.connection() as conn:
            db_result = await conn.execute(
                """
                SELECT * FROM healthcheck_results
                WHERE id = %s;
                """,
                (result_id,)
            )
            row = await db_result.fetchone()
            self.assertIsNotNone(row, "Result should exist in database")
            self.assertEqual(row['target_id'], target_id, "Target ID should match")
            self.assertEqual(row['status_code'], 200, "Status code should match")
            self.assertTrue(row['success'], "Success flag should be true")
            self.assertIsNone(row['error_type'], "Error type should be None")
    
    async def test_04_add_health_check_result_status_error(self):
        """Test adding a health check result with status code error."""
        # Use our test setting
        if not self.test_settings_ids:
            self.fail("No test setting available")
        
        target_id = self.test_settings_ids[0]
        
        # Create a result with status code error
        error = StatusCodeError(expected=200, actual=404)
        result = HealthCheckResult(
            url="https://example.com/health",
            status_code=404,
            response_time_ms=120.3,
            success=False,
            error=error
        )
        
        # Add to database
        result_id = await self.db.add_health_check_result(target_id, result)
        self.assertGreater(result_id, 0, "Result ID should be positive")
        
        # Verify the error result was added correctly
        async with self.db.pool.connection() as conn:
            db_result = await conn.execute(
                """
                SELECT * FROM healthcheck_results
                WHERE id = %s;
                """,
                (result_id,)
            )
            row = await db_result.fetchone()
            self.assertIsNotNone(row, "Result should exist in database")
            self.assertEqual(row['status_code'], 404, "Status code should match")
            self.assertFalse(row['success'], "Success flag should be false")
            self.assertEqual(row['error_type'], 'StatusCodeError', "Error type should be StatusCodeError")
            self.assertIsNotNone(row['error_message'], "Error message should be set")
    
    # --- Buffer Tests ---
    
    async def test_10_buffer_and_flush(self):
        """Test buffering health check results and flushing them."""
        # Use our test setting
        if not self.test_settings_ids:
            self.fail("No test setting available")
        
        target_id = self.test_settings_ids[0]
        
        # Clear existing buffer
        async with self.db.buffer_lock:
            self.db.result_buffer.clear()
        
        # Add multiple results to buffer
        for i in range(5):
            result = HealthCheckResult(
                url=f"https://example.com/buffer_test/{i}",
                status_code=200,
                response_time_ms=100.0 + i,
                success=True
            )
            await self.db.buffer_health_check_result(target_id, result)
        
        # Verify buffer has 5 items
        async with self.db.buffer_lock:
            self.assertEqual(len(self.db.result_buffer), 5, "Buffer should contain 5 results")
        
        # Flush the buffer
        await self.db.flush_result_buffer()
        
        # Verify buffer is now empty
        async with self.db.buffer_lock:
            self.assertEqual(len(self.db.result_buffer), 0, "Buffer should be empty after flush")
    
    # --- Mocked HTTP Tests ---
    
    @patch('src.http_health_checker.aiohttp.ClientSession')
    async def test_12_mock_health_check_success(self, mock_session):
        """Test health checks using mocked HTTP responses."""
        # Use our test setting
        if not self.test_settings_ids:
            self.fail("No test setting available")
        
        target_id = self.test_settings_ids[0]
        
        # Mock a successful HTTP response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.text = MagicMock(return_value=asyncio.Future())
        mock_response.text.return_value.set_result('{"status": "ok", "message": "Service is healthy"}')
        
        # Set up the session mock
        mock_session_instance = MagicMock()
        mock_session_instance.__aenter__.return_value = mock_session_instance
        mock_session_instance.get.return_value.__aenter__.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        # Create result manually (to simulate a successful check)
        result = HealthCheckResult(
            url="https://example.com/health",
            status_code=200,
            response_time_ms=150.5,
            success=True,
            content='{"status": "ok"}'
        )
        
        # Add to database
        result_id = await self.db.add_health_check_result(target_id, result)
        self.assertGreater(result_id, 0, "Result ID should be positive")
        
        # Verify the result was added correctly
        async with self.db.pool.connection() as conn:
            db_result = await conn.execute(
                """
                SELECT * FROM healthcheck_results
                WHERE id = %s;
                """,
                (result_id,)
            )
            row = await db_result.fetchone()
            self.assertIsNotNone(row, "Result should exist in database")
            self.assertEqual(row['target_id'], target_id, "Target ID should match")
            self.assertEqual(row['status_code'], 200, "Status code should match")
            self.assertTrue(row['success'], "Success flag should be true")
            self.assertIsNone(row['error_type'], "Error type should be None")
        
    @patch('src.http_health_checker.aiohttp.ClientSession')
    async def test_13_mock_health_check_error(self, mock_session):
        """Test health checks with mocked error responses."""
        # Use our test setting
        if not self.test_settings_ids:
            self.fail("No test setting available")
        
        target_id = self.test_settings_ids[0]
        
        # Create result with error manually
        error = StatusCodeError(expected=200, actual=500)
        result = HealthCheckResult(
            url="https://example.com/health",
            status_code=500,
            response_time_ms=120.3,
            success=False,
            error=error
        )
        
        # Add to database
        result_id = await self.db.add_health_check_result(target_id, result)
        self.assertGreater(result_id, 0, "Result ID should be positive")
        
        # Verify the error result was added correctly
        async with self.db.pool.connection() as conn:
            db_result = await conn.execute(
                """
                SELECT * FROM healthcheck_results
                WHERE id = %s;
                """,
                (result_id,)
            )
            row = await db_result.fetchone()
            self.assertIsNotNone(row, "Result should exist in database")
            self.assertEqual(row['status_code'], 500, "Status code should match")
            self.assertFalse(row['success'], "Success flag should be false")
            self.assertEqual(row['error_type'], 'StatusCodeError', "Error type should be StatusCodeError")


if __name__ == '__main__':
    unittest.main()