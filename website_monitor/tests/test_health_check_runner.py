import asyncio
import logging
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call
from io import BytesIO

# Import from the src package correctly
from src.health_check_runner import (
    HealthCheckRunner, 
    SchedulerError, 
    SettingsRefreshError, 
    RunnerStopError,
    HealthCheckRunError
)
from src.http_health_checker import HealthCheckResult, HttpHealthChecker
from src.runtime_settings import HealthCheckSettings
from src.db_recorder import HealthCheckDatabase
from src.errors import DatabaseError, HealthCheckError, StatusCodeError, TimeoutError, ContentMatchError

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_runner")

class TestHealthCheckRunner(unittest.IsolatedAsyncioTestCase):
    """Test cases for the HealthCheckRunner class."""
    
    async def asyncSetUp(self):
        """Set up test fixtures before each test."""
        # Create mock objects
        self.mock_db = AsyncMock(spec=HealthCheckDatabase)
        
        # Configure the mock database
        self.mock_db._initialized = True
        
        # Mock health settings
        self.health_settings = {
            'checker_loop_frequency_in_sec': 1,
            'check_interval_in_sec': 5,
            'expected_status_code': 200,
            'connection_timeout_in_sec': 5,
            'targets_settings_refresh_rate_in_sec': 60
        }
                
        # Create test instance
        self.runner = HealthCheckRunner(
            db=self.mock_db,
            logger=logger,
            health_settings=self.health_settings
        )
        
        # Sample health check settings for testing
        self.sample_settings = [
            {
                'id': 1,
                'url': 'https://example.com',
                'check_interval': 60,
                'expected_status_code': 200,
                'regex_match': None,
                'timeout_ms': 5000,
                'headers': None,
                'active': True
            },
            {
                'id': 2,
                'url': 'https://api.example.com/health',
                'check_interval': 30,
                'expected_status_code': 200,
                'regex_match': '{"status":"up"}',
                'timeout_ms': 3000,
                'headers': '{"Authorization": "Bearer test-token"}',
                'active': True
            }
        ]
        
        # Configure mock to return sample settings
        self.mock_db.get_all_health_check_settings.return_value = self.sample_settings
        
        # Sample health check result
        self.sample_result = HealthCheckResult(
            url='https://example.com',
            status_code=200,
            response_time_ms=150.5,
            success=True,
            content=None,
            error=None
        )
        
        # Sample failed health check result
        self.sample_failed_result = HealthCheckResult(
            url='https://api.example.com/health',
            status_code=500,
            response_time_ms=250.0,
            success=False,
            content=None,
            error=StatusCodeError(expected=200, actual=500)
        )
    
    async def asyncTearDown(self):
        """Clean up after each test."""
        # Ensure runner is stopped
        if self.runner.running:
            # Create a real awaitable for gather to use
            async def mock_gather(*args, **kwargs):
                return None
                
            # Patch asyncio.gather to use our mock function
            with patch('asyncio.gather', mock_gather):
                await self.runner.stop()
    
    async def test_init(self):
        """Test HealthCheckRunner initialization."""
        # Check initial state
        self.assertEqual(self.runner.db, self.mock_db)
        self.assertEqual(self.runner.logger, logger)
        self.assertEqual(self.runner.runtime_settings, self.health_settings)
        
        self.assertFalse(self.runner.running)
        self.assertIsNone(self.runner.scheduler_task)
        self.assertEqual(self.runner.settings, [])
        self.assertEqual(self.runner.last_runs, {})
        self.assertEqual(self.runner.active_tasks, set())
    
    async def test_init_invalid_db(self):
        """Test initialization with invalid db parameter."""
        with self.assertRaises(ValueError):
            HealthCheckRunner(
                db="not a HealthCheckDatabase",
                logger=logger,
                health_settings=self.health_settings
            )
    
    async def test_start(self):
        """Test starting the health check runner."""
        # Setup
        self.mock_db.initialize = AsyncMock()
        
        # Execute
        await self.runner.start()
        
        # Verify
        self.assertTrue(self.runner.running)
        self.assertIsNotNone(self.runner.scheduler_task)
        self.assertEqual(self.runner.settings, self.sample_settings)
        
        # Verify database was initialized if needed
        if not self.mock_db._initialized:
            self.mock_db.initialize.assert_called_once()
        
        # Verify settings were refreshed
        self.mock_db.get_all_health_check_settings.assert_called_once_with(active_only=True)
        
        # Verify last_runs was initialized with epoch time (0)
        self.assertEqual(list(self.runner.last_runs.keys()), [1, 2])
        for check_id in self.runner.last_runs:
            self.assertEqual(self.runner.last_runs[check_id], 0)
        
        # Clean up
        # Create a real awaitable for gather to use
        async def mock_gather(*args, **kwargs):
            return None
            
        # Patch asyncio.gather to use our mock function
        with patch('asyncio.gather', mock_gather):
            await self.runner.stop()
    
    async def test_start_database_error(self):
        """Test starting when database raises an error."""
        # Setup
        self.mock_db._initialized = False
        self.mock_db.initialize.side_effect = DatabaseError("Test database error")
        
        # Execute and verify
        with self.assertRaises(SettingsRefreshError):
            await self.runner.start()
        
        self.assertFalse(self.runner.running)
        self.assertIsNone(self.runner.scheduler_task)
    
    async def test_stop(self):
        """Test stopping the health check runner."""
        # Setup - Start the runner first
        # Patch _scheduler_loop to avoid actually running it
        with patch.object(self.runner, '_scheduler_loop', AsyncMock()):
            await self.runner.start()
            self.assertTrue(self.runner.running)
            
            # Create real futures for the active tasks
            future1 = asyncio.Future()
            future1.done = MagicMock(return_value=False)
            
            future2 = asyncio.Future()
            future2.done = MagicMock(return_value=True)
            
            # Add these to active_tasks
            self.runner.active_tasks = {future1, future2}
            
            # Create a real awaitable for gather to use
            async def mock_gather(*args, **kwargs):
                # Call cancel on any futures that are not done
                for arg in args:
                    if not arg.done():
                        arg.cancel()
                return None
                
            # Patch asyncio.gather to use our mock function
            with patch('asyncio.gather', mock_gather):
                # Execute
                await self.runner.stop()
                
                # Verify
                self.assertFalse(self.runner.running)
                self.assertIsNone(self.runner.scheduler_task)
                
                # Verify task was cancelled - check the status of the futures
                self.assertTrue(future1.cancelled())
                self.assertFalse(future2.cancelled())  # Was already done, so not cancelled
    
    async def test_stop_not_running(self):
        """Test stopping when not running."""
        # Setup
        self.assertFalse(self.runner.running)
        
        # Execute
        await self.runner.stop()
        
        # Verify - Should do nothing without errors
        self.assertFalse(self.runner.running)
    
    async def test_refresh_settings(self):
        """Test refreshing health check settings."""
        # Setup
        new_settings = [
            {
                'id': 1,
                'url': 'https://example.com',
                'check_interval': 120,  # Changed interval
                'expected_status_code': 200,
                'regex_match': None,
                'timeout_ms': 5000,
                'headers': None,
                'active': True
            },
            {
                'id': 3,  # New check
                'url': 'https://new.example.com',
                'check_interval': 30,
                'expected_status_code': 200,
                'regex_match': None,
                'timeout_ms': 5000,
                'headers': None,
                'active': True
            }
            # Note: ID 2 is missing (simulating removed check)
        ]
        
        # Setup last_runs with initial data
        self.runner.last_runs = {1: 100, 2: 200}
        
        # Configure mock to return new settings
        self.mock_db.get_all_health_check_settings.return_value = new_settings
        
        # Execute
        result = await self.runner.refresh_settings()
        
        # Verify
        self.assertEqual(result, new_settings)
        self.assertEqual(self.runner.settings, new_settings)
        
        # Verify last_runs was updated properly
        self.assertEqual(len(self.runner.last_runs), 2)  # ID 1 and 3
        self.assertIn(1, self.runner.last_runs)
        self.assertIn(3, self.runner.last_runs)
        self.assertNotIn(2, self.runner.last_runs)  # Should be removed
        self.assertEqual(self.runner.last_runs[1], 100)  # Should keep old time
        self.assertEqual(self.runner.last_runs[3], 0)  # New check should start at 0
    
    async def test_refresh_settings_database_error(self):
        """Test refresh settings when database raises an error."""
        # Setup
        self.mock_db.get_all_health_check_settings.side_effect = DatabaseError("Test database error")
        
        # Execute and verify
        with self.assertRaises(DatabaseError):
            await self.runner.refresh_settings()
    
    async def test_scheduler_loop(self):
        """Test the scheduler loop."""
        # This is a more complex test that requires patching asyncio.sleep
        # We'll set up the test, let the loop run for a bit, then stop it
        
        # Mock the _run_health_check method
        self.runner._run_health_check = AsyncMock(return_value=self.sample_result)
        
        # Use patch to replace asyncio.sleep and control flow
        with patch('asyncio.sleep') as mock_sleep:
            # Set up to allow 2 loop iterations
            mock_sleep.side_effect = [None, None, asyncio.CancelledError()]
            
            # Set up initial state
            self.runner.running = True
            self.runner.settings = self.sample_settings
            self.runner.last_runs = {1: 0, 2: 0}  # Set to epoch time to ensure they run
            
            # Execute scheduler loop in a task
            with self.assertRaises(asyncio.CancelledError):
                await self.runner._scheduler_loop()
            
            # Verify health checks were executed
            # Should run both checks in the first iteration
            self.assertEqual(self.runner._run_health_check.call_count, 2)
            
            # Verify refresh settings was called after refresh_counter reaches interval
            if mock_sleep.call_count >= self.health_settings['targets_settings_refresh_rate_in_sec']:
                self.mock_db.get_all_health_check_settings.call_count > 1
    
    @patch('time.time')
    async def test_scheduler_loop_respects_intervals(self, mock_time):
        """Test that scheduler respects check intervals."""
        # Setup to control time.time()
        current_time = 1000.0  # Start at a specific time
        
        def time_side_effect():
            nonlocal current_time
            return current_time
        
        mock_time.side_effect = time_side_effect
        
        # Mock the _run_health_check method
        self.runner._run_health_check = AsyncMock(return_value=self.sample_result)
        
        # Use patch to replace asyncio.sleep
        with patch('asyncio.sleep') as mock_sleep:
            # Set up to allow 3 loop iterations
            mock_sleep.side_effect = [None, None, None, asyncio.CancelledError()]
            
            # Set up initial state
            self.runner.running = True
            self.runner.settings = self.sample_settings
            
            # Set last runs to be at different states
            # ID 1 (60 sec interval) was run 10 seconds ago - shouldn't run yet
            # ID 2 (30 sec interval) was run 40 seconds ago - should run
            self.runner.last_runs = {
                1: current_time - 10,  # 10 seconds ago
                2: current_time - 40   # 40 seconds ago
            }
            
            # Run one iteration of the loop
            try:
                await self.runner._scheduler_loop()
            except asyncio.CancelledError:
                pass
            
            # Verify only check ID 2 was run (interval exceeded)
            self.assertEqual(self.runner._run_health_check.call_count, 1)
            self.runner._run_health_check.assert_called_with(self.sample_settings[1])
            
            # Now advance time enough for ID 1 to also need execution
            current_time += 55  # Now 65 seconds since ID 1 was last run
            
            # Reset the mock
            self.runner._run_health_check.reset_mock()
            
            # Run another iteration
            try:
                # Need to reset side effect for asyncio.sleep
                mock_sleep.side_effect = [None, None, None, asyncio.CancelledError()]
                await self.runner._scheduler_loop()
            except asyncio.CancelledError:
                pass
            
            # Verify both checks were run
            self.assertEqual(self.runner._run_health_check.call_count, 2)
    
    async def test_run_health_check(self):
        """Test running a single health check."""
        # Setup
        setting = self.sample_settings[0]
        
        # Patch HttpHealthChecker to return a predetermined result
        with patch('src.health_check_runner.HttpHealthChecker') as mock_checker_class:
            # Setup the mock checker
            mock_checker = AsyncMock()
            mock_checker.check.return_value = self.sample_result
            mock_checker_class.return_value = mock_checker
            
            # Execute
            result = await self.runner._run_health_check(setting)
            
            # Verify
            self.assertEqual(result, self.sample_result)
            
            # Verify HttpHealthChecker was initialized correctly
            mock_checker_class.assert_called_once_with(
                url=setting['url'],
                expected_status_code=setting['expected_status_code'],
                content_regex=setting['regex_match'],
                timeout_ms=setting['timeout_ms'],
                headers=setting['headers'],
                logger=self.runner.logger
            )
            
            # Verify check was called
            mock_checker.check.assert_called_once()
            
            # Verify result was stored in database
            self.mock_db.buffer_health_check_result.assert_called_once_with(
                setting['id'], 
                self.sample_result
            )
    
    async def test_run_health_check_with_default_values(self):
        """Test running a health check with default values from runtime settings."""
        # Setup - setting with minimal configuration
        minimal_setting = {
            'id': 3,
            'url': 'https://minimal.example.com',
            'active': True
        }
        
        # Patch HttpHealthChecker to return a predetermined result
        with patch('src.health_check_runner.HttpHealthChecker') as mock_checker_class:
            # Setup the mock checker
            mock_checker = AsyncMock()
            mock_checker.check.return_value = self.sample_result
            mock_checker_class.return_value = mock_checker
            
            # Execute
            result = await self.runner._run_health_check(minimal_setting)
            
            # Verify HttpHealthChecker was initialized with defaults from runtime settings
            mock_checker_class.assert_called_once_with(
                url=minimal_setting['url'],
                expected_status_code=self.health_settings['expected_status_code'],
                content_regex=None,
                timeout_ms=self.health_settings['connection_timeout_in_sec'] * 1000,
                headers=None,
                logger=self.runner.logger
            )
    
    async def test_run_health_check_error(self):
        """Test running a health check that encounters an error."""
        # Setup
        setting = self.sample_settings[1]
        
        # Patch HttpHealthChecker to raise an error
        with patch('src.health_check_runner.HttpHealthChecker') as mock_checker_class:
            # Setup the mock checker
            mock_checker = AsyncMock()
            mock_checker.check.side_effect = HealthCheckError("Test health check error")
            mock_checker_class.return_value = mock_checker
            
            # Execute - should re-raise the HealthCheckError
            with self.assertRaises(HealthCheckError):
                await self.runner._run_health_check(setting)
            
            # Verify database was not called (error was raised)
            self.mock_db.buffer_health_check_result.assert_not_called()
    
    async def test_run_health_check_database_error(self):
        """Test running a health check with database error when storing result."""
        # Setup
        setting = self.sample_settings[0]
        
        # Patch HttpHealthChecker to return a predetermined result
        with patch('src.health_check_runner.HttpHealthChecker') as mock_checker_class:
            # Setup the mock checker
            mock_checker = AsyncMock()
            mock_checker.check.return_value = self.sample_result
            mock_checker_class.return_value = mock_checker
            
            # Make database raise an error
            self.mock_db.buffer_health_check_result.side_effect = DatabaseError("Test database error")
            
            # Execute - should re-raise the DatabaseError
            with self.assertRaises(DatabaseError):
                await self.runner._run_health_check(setting)
    
    async def test_health_check_done_callback(self):
        """Test the callback for when a health check task completes."""
        # Setup
        check_id = 1
        url = "https://example.com"
        
        # Create a completed task with a result
        task = asyncio.Future()
        task.set_result(self.sample_result)
        
        # Add task to active tasks
        self.runner.active_tasks.add(task)
        
        # Execute
        self.runner._health_check_done(task, check_id, url)
        
        # Verify
        self.assertNotIn(task, self.runner.active_tasks)  # Should be removed
    
    async def test_health_check_done_callback_failure(self):
        """Test the callback for when a health check task fails."""
        # Setup
        check_id = 2
        url = "https://api.example.com/health"
        
        # Create a completed task with a failed result
        task = asyncio.Future()
        task.set_result(self.sample_failed_result)
        
        # Add task to active tasks
        self.runner.active_tasks.add(task)
        
        # Execute
        self.runner._health_check_done(task, check_id, url)
        
        # Verify
        self.assertNotIn(task, self.runner.active_tasks)  # Should be removed
    
    async def test_health_check_done_callback_exception(self):
        """Test the callback for when a health check task raises exception."""
        # Setup
        check_id = 2
        url = "https://api.example.com/health"
        
        # Create a completed task with an exception
        task = asyncio.Future()
        test_exception = HealthCheckError("Test exception")
        task.set_exception(test_exception)
        
        # Add task to active tasks
        self.runner.active_tasks.add(task)
        
        # Execute - should catch and log the exception
        self.runner._health_check_done(task, check_id, url)
        
        # Verify
        self.assertNotIn(task, self.runner.active_tasks)  # Should be removed
    
    async def test_integration_start_stop(self):
        """Integration test for starting and stopping the runner."""
        # Patch the _scheduler_loop to prevent it from actually running
        with patch.object(self.runner, '_scheduler_loop', AsyncMock()):
            # Start the runner
            await self.runner.start()
            
            # Verify it's running
            self.assertTrue(self.runner.running)
            self.assertIsNotNone(self.runner.scheduler_task)
            
            # Create a real awaitable for gather to use
            async def mock_gather(*args, **kwargs):
                return None
                
            # Patch asyncio.gather to use our mock function
            with patch('asyncio.gather', mock_gather):
                # Stop the runner
                await self.runner.stop()
                
                # Verify it's stopped
                self.assertFalse(self.runner.running)
                self.assertIsNone(self.runner.scheduler_task)


if __name__ == '__main__':
    unittest.main()