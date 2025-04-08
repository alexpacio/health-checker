import asyncio
import logging
import time
from typing import Dict, List, Optional, Set, Any, TypedDict, Union, cast

from .runtime_settings import HealthCheckSettings

from .db_recorder import HealthCheckDatabase
from .http_health_checker import HttpHealthChecker, HealthCheckResult
from .errors import DatabaseError, HealthCheckError

# Type definitions
class HealthCheckSetting(TypedDict, total=False):
    """Type definition for health check settings retrieved from database."""
    id: int
    url: str
    check_interval: int  # in seconds
    expected_status_code: int
    regex_match: Optional[str]
    timeout_ms: int
    headers: Optional[Union[str, Dict[str, str]]]
    active: bool

# Custom error classes
class HealthCheckRunnerError(Exception):
    """Base exception for health check runner errors."""
    pass


class SchedulerError(HealthCheckRunnerError):
    """Exception for scheduler-related errors."""
    def __init__(self, message: str = "Scheduler operation failed", *args: Any, **kwargs: Any) -> None:
        super().__init__(message, *args, **kwargs)


class SettingsRefreshError(HealthCheckRunnerError):
    """Exception for errors during settings refresh."""
    def __init__(self, message: str = "Failed to refresh health check settings", *args: Any, **kwargs: Any) -> None:
        super().__init__(message, *args, **kwargs)


class RunnerStopError(HealthCheckRunnerError):
    """Exception when stopping the runner fails."""
    def __init__(self, message: str = "Failed to stop health check runner", *args: Any, **kwargs: Any) -> None:
        super().__init__(message, *args, **kwargs)


class HealthCheckRunError(HealthCheckRunnerError):
    """Exception for errors during health check execution."""
    def __init__(
        self, 
        check_id: Optional[int] = None, 
        url: Optional[str] = None, 
        message: str = "Health check execution failed", 
        *args: Any, 
        **kwargs: Any
    ) -> None:
        if check_id is not None:
            message = f"{message} for check ID {check_id}"
        if url:
            message = f"{message} ({url})"
        super().__init__(message, *args, **kwargs)
        self.check_id: Optional[int] = check_id
        self.url: Optional[str] = url


class HealthCheckRunner:
    """
    A runner that schedules and executes health checks based on settings from the database.
    
    This class:
    1. Loads health check settings from a database
    2. Runs a scheduler to check every second if any health checks need to be executed
    3. Executes health checks based on their configured interval
    4. Stores the results back in the database
    """
    
    def __init__(self, db: HealthCheckDatabase, logger: logging.Logger, health_settings: HealthCheckSettings) -> None:
        """
        Initialize the health check runner.
        
        Args:
            db: An instance of HealthCheckDatabase
            logger: Optional logger for logging messages
            
        Raises:
            ValueError: If db is not an instance of HealthCheckDatabase
        """
        if not isinstance(db, HealthCheckDatabase):
            raise ValueError("db must be an instance of HealthCheckDatabase")
            
        self.db: HealthCheckDatabase = db
        self.logger: logging.Logger = logger
        self.runtime_settings: HealthCheckSettings = health_settings
        
        # Internal state
        self.running: bool = False
        self.scheduler_task: Optional[asyncio.Task[None]] = None
        self.settings: List[HealthCheckSetting] = []
        self.last_runs: Dict[int, float] = {}  # Dictionary to track when each check was last run
        self.active_tasks: Set[asyncio.Task[Any]] = set()  # Set to track active health check tasks
        
        self.logger.info("HealthCheckRunner initialized")
    
    async def start(self) -> None:
        """
        Start the health check runner.
        
        This will:
        1. Load health check settings from the database
        2. Start the scheduler loop
        
        Raises:
            SettingsRefreshError: If loading settings fails
            SchedulerError: If starting the scheduler fails
        """
        if self.running:
            self.logger.warning("Health check runner already running")
            return
            
        try:
            # Ensure database is initialized
            if not self.db._initialized:
                self.logger.info("Database not initialized, initializing now")
                await self.db.initialize()
                
            # Load initial settings
            await self.refresh_settings()
            
            # Set running state before starting scheduler
            self.running = True
            
            # Start scheduler loop as a background task
            self.scheduler_task = asyncio.create_task(self._scheduler_loop())
            self.scheduler_task.set_name("health_check_scheduler")
            self.logger.info("Health check runner started")
        except DatabaseError as e:
            self.running = False
            self.logger.error(f"Database error when starting health check runner: {str(e)}", exc_info=True)
            raise SettingsRefreshError(f"Database error: {str(e)}") from e
        except Exception as e:
            self.running = False
            self.logger.error(f"Failed to start health check runner: {str(e)}", exc_info=True)
            if isinstance(e, HealthCheckRunnerError):
                raise
            raise SchedulerError(f"Failed to start health check runner: {str(e)}") from e
    
    async def stop(self) -> None:
        """
        Stop the health check runner.
        
        This will:
        1. Stop the scheduler loop
        2. Cancel any active health check tasks
        
        Raises:
            RunnerStopError: If stopping the runner fails
        """
        if not self.running:
            self.logger.warning("Health check runner is not running")
            return
            
        self.logger.info("Stopping health check runner")
        self.running = False
        
        try:
            # Cancel scheduler task
            if self.scheduler_task:
                self.logger.debug("Cancelling scheduler task")
                self.scheduler_task.cancel()
                try:
                    await self.scheduler_task
                except asyncio.CancelledError:
                    pass
                self.scheduler_task = None
            
            # Cancel all active health check tasks
            active_tasks = list(self.active_tasks)
            if active_tasks:
                self.logger.info(f"Cancelling {len(active_tasks)} active health check tasks")
                for task in active_tasks:
                    if not task.done():
                        task.cancel()
                
                # Wait for all tasks to complete cancellation
                await asyncio.gather(*active_tasks, return_exceptions=True)
                
            self.active_tasks.clear()
            self.logger.info("Health check runner stopped")
        except Exception as e:
            self.logger.error(f"Error stopping health check runner: {str(e)}", exc_info=True)
            if isinstance(e, HealthCheckRunnerError):
                raise
            raise RunnerStopError(f"Failed to stop health check runner: {str(e)}") from e
    
    async def refresh_settings(self) -> List[HealthCheckSetting]:
        """
        Refresh health check settings from the database.
        
        Returns:
            List of health check settings
            
        Raises:
            SettingsRefreshError: If refreshing settings fails
            DatabaseError: If a database error occurs
        """
        try:
            self.logger.info("Refreshing health check settings")
            settings: List[HealthCheckSetting] = await self.db.get_all_health_check_settings(active_only=True)
            self.logger.info(f"Loaded {len(settings)} active health check settings")
            
            # Update settings
            self.settings = settings
            
            # Update last_runs to add new checks or remove old ones
            current_ids: Set[int] = {setting['id'] for setting in settings}
            
            # Remove old checks from last_runs
            removed_ids: Set[int] = set(self.last_runs.keys()) - current_ids
            for check_id in removed_ids:
                self.last_runs.pop(check_id, None)
                self.logger.debug(f"Removed health check ID {check_id} from runner")
                
            # Initialize new checks in last_runs
            for check_id in current_ids:
                if check_id not in self.last_runs:
                    # Set to 0 (epoch time) to run new checks immediately
                    self.last_runs[check_id] = 0
                    url = next((s['url'] for s in settings if s['id'] == check_id), "unknown")
                    self.logger.debug(f"Added new health check ID {check_id} for {url}")
                    
            return self.settings
        except DatabaseError as e:
            self.logger.error(f"Database error when refreshing settings: {str(e)}", exc_info=True)
            raise  # Re-raise DatabaseError directly
        except Exception as e:
            self.logger.error(f"Failed to refresh health check settings: {str(e)}", exc_info=True)
            if isinstance(e, HealthCheckRunnerError):
                raise
            raise SettingsRefreshError(f"Failed to refresh health check settings: {str(e)}") from e
    
    async def _scheduler_loop(self) -> None:
        """
        Main scheduler loop that runs every second.
        
        This checks if any health checks need to be executed based on their interval.
        
        Raises:
            SchedulerError: If the scheduler loop encounters an error
        """
        try:
            self.logger.info("Scheduler loop started")
            
            refresh_counter: int = 0
            settings_refresh_interval: int = self.runtime_settings.get('targets_settings_refresh_rate_in_sec')  # Refresh settings every 60 seconds
            checker_loop_frequency_in_sec: int = self.runtime_settings.get('checker_loop_frequency_in_sec')
            check_interval_in_sec: int = self.runtime_settings.get('check_interval_in_sec')
            
            while self.running:
                loop_start_time: float = time.time()
                
                # Refresh settings periodically
                refresh_counter += 1
                if refresh_counter >= settings_refresh_interval:
                    try:
                        await self.refresh_settings()
                        refresh_counter = 0
                    except Exception as e:
                        self.logger.error(f"Error refreshing settings in scheduler loop: {str(e)}", exc_info=True)
                        # Continue running with current settings
                
                # Check which health checks need to be run
                current_time: float = time.time()
                tasks_started: int = 0
                
                # Clean up completed tasks
                self.active_tasks = {task for task in self.active_tasks if not task.done()}
                
                for setting in self.settings:
                    check_id: int = setting['id']
                    last_run: float = self.last_runs.get(check_id, 0)
                    interval_ms: int = setting.get('check_interval', check_interval_in_sec) * 1000  # Default to 1 minute if not specified
                    
                    # Convert to seconds for comparison with time.time()
                    interval_seconds: float = interval_ms / 1000.0
                    
                    # Check if it's time to run this health check
                    if current_time - last_run >= interval_seconds:
                        # Create a new task for this health check
                        url: str = setting.get('url', 'unknown URL')
                        task: asyncio.Task[HealthCheckResult] = asyncio.create_task(self._run_health_check(setting))
                        task.set_name(f"health_check_{check_id}_{int(current_time)}")
                        
                        # Add cleanup callback
                        task.add_done_callback(lambda t, cid=check_id, u=url: 
                            self._health_check_done(t, cid, u))
                        
                        # Track the task
                        self.active_tasks.add(task)
                        
                        # Update last run time
                        self.last_runs[check_id] = current_time
                        
                        self.logger.debug(f"Scheduled health check for {url} (ID: {check_id})")
                        tasks_started += 1
                
                if tasks_started > 0:
                    self.logger.info(f"Started {tasks_started} health check(s), active tasks: {len(self.active_tasks)}")
                
                # Calculate sleep time to maintain 1 second interval
                elapsed: float = time.time() - loop_start_time

                # If elapsed time is greater than 1 second, set sleep time to 0
                sleep_time: float = max(0, float(checker_loop_frequency_in_sec) - elapsed)
                
                # Sleep until next check
                await asyncio.sleep(sleep_time)
                
            self.logger.info("Scheduler loop stopped")
        except asyncio.CancelledError:
            self.logger.info("Scheduler loop cancelled")
            raise
        except Exception as e:
            self.running = False
            self.logger.error(f"Error in scheduler loop: {str(e)}", exc_info=True)
            if isinstance(e, HealthCheckRunnerError):
                raise
            raise SchedulerError(f"Scheduler loop failed: {str(e)}") from e
    
    def _health_check_done(self, task: asyncio.Task[HealthCheckResult], check_id: int, url: str) -> None:
        """
        Callback for when a health check task completes.
        
        Args:
            task: The completed task
            check_id: The ID of the health check
            url: The URL of the health check
        """
        # Remove the task from active tasks
        self.active_tasks.discard(task)
        
        # Check for exceptions
        if not task.cancelled():
            try:
                # This will re-raise any exception from the task
                result: HealthCheckResult = task.result()
                if result and not result.success:
                    self.logger.warning(f"Health check failed for {url} (ID: {check_id}): {result.error}")
            except Exception as e:
                self.logger.error(f"Health check task for {url} (ID: {check_id}) failed: {str(e)}", exc_info=True)
    
    async def _run_health_check(self, setting: HealthCheckSetting) -> HealthCheckResult:
        """
        Run a single health check and store the result.
        
        Args:
            setting: The health check settings dictionary
            
        Returns:
            HealthCheckResult: The result of the health check
            
        Raises:
            HealthCheckError: If the health check encounters an error
            DatabaseError: If storing the result encounters a database error
            HealthCheckRunError: For other errors during health check execution
        """
        check_id: int = setting['id']
        url: str = setting.get('url')
        expected_status_code: int = setting.get('expected_status_code', self.runtime_settings.get('expected_status_code'))
        regex_match: Optional[str] = setting.get('regex_match')
        timeout_ms: int = setting.get('timeout_ms', self.runtime_settings.get('connection_timeout_in_sec') * 1000)
        headers_json: Optional[Union[str, Dict[str, str]]] = setting.get('headers')
        
        # Parse headers JSON if it exists
        headers: Optional[Dict[str, str]] = None
        if headers_json:
            try:
                if isinstance(headers_json, str):
                    import json
                    headers = json.loads(headers_json)
                else:
                    # Assume it's already a dict/object
                    headers = cast(Dict[str, str], headers_json)
            except Exception as e:
                self.logger.warning(f"Failed to parse headers for check ID {check_id}: {str(e)}")
        
        try:
            # Log the start of health check
            self.logger.info(f"Running health check for {url} (ID: {check_id})")
            
            # Create and run health checker
            checker = HttpHealthChecker(
                url=url,
                expected_status_code=expected_status_code,
                content_regex=regex_match,
                timeout_ms=timeout_ms,
                headers=headers,
                logger=self.logger
            )
            
            # Run the check
            result: HealthCheckResult = await checker.check()
            
            # Log result
            if result.success:
                self.logger.info(f"Health check successful for {url} (ID: {check_id}), " +
                                f"response time: {result.response_time_ms:.2f}ms")
            else:
                self.logger.warning(f"Health check failed for {url} (ID: {check_id}), " +
                                    f"error: {result.error}")
            
            # Store result in database
            try:
                result_id: int = await self.db.buffer_health_check_result(check_id, result)
                self.logger.debug(f"Stored health check result ID {result_id} for check ID {check_id}")
            except DatabaseError as db_error:
                # Log but also re-raise database errors
                self.logger.error(f"Failed to store health check result for {url} (ID: {check_id}): {str(db_error)}")
                raise
            
            return result
        except HealthCheckError as e:
            # HTTP health checker errors are expected and part of the result
            self.logger.warning(f"Health check error for {url} (ID: {check_id}): {str(e)}")
            raise
        except DatabaseError as e:
            # Re-raise database errors
            self.logger.error(f"Database error storing health check result for {url} (ID: {check_id}): {str(e)}")
            raise
        except Exception as e:
            # For other unexpected errors
            self.logger.error(f"Error running health check for {url} (ID: {check_id}): {str(e)}", exc_info=True)
            if isinstance(e, HealthCheckRunnerError):
                raise
            raise HealthCheckRunError(check_id=check_id, url=url, message=f"Error running health check: {str(e)}") from e