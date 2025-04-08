#!/usr/bin/env python3
"""
Health Checker Application Entry Point

This script serves as the main entry point for the health checker application.
It sets up signal handling, initializes the database, and manages the health check runner.

Usage:
    python run.py [--config=path/to/config.env] [--log-level=INFO]

Configuration:
    A valid .env file is required either in the current directory or specified via --config
    All settings are loaded via RuntimeSettingsReader from the configuration file
    Command line arguments take precedence over configuration file settings
"""

import os
import sys
import signal
import asyncio
import logging
import argparse
import platform
from typing import Optional, Any
from io import BytesIO

# Add parent directory to sys.path so we can use absolute imports
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Import our modules
from src.db_recorder import HealthCheckDatabase
from src.health_check_runner import HealthCheckRunner
from src.runtime_settings import RuntimeSettingsReader, DatabaseSettings, HealthCheckSettings
from src.errors import DatabaseError, InitialConnectionError

# Configure logging - this will be updated based on settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("health_checker")

# Global variables with proper type annotations
runner: Optional[HealthCheckRunner] = None
settings_reader: Optional[RuntimeSettingsReader] = None
db: Optional[HealthCheckDatabase] = None
shutdown_event: Optional[asyncio.Event] = None
restart_event: Optional[asyncio.Event] = None

async def setup_database(db_settings: DatabaseSettings) -> HealthCheckDatabase:
    """
    Set up database connection and initialize schema.
    
    Args:
        db_settings: Database connection settings
        
    Returns:
        Initialized HealthCheckDatabase instance
        
    Raises:
        InitialConnectionError: If database connection fails
    """
    logger.info("Setting up database connection")
    
    # Construct DSN from settings if needed
    connection_string = f"postgresql://{db_settings['user']}:{db_settings['password']}@{db_settings['host']}:{db_settings['port']}/{db_settings['name']}"
    
    # Append SSL mode if SSL is enabled
    if db_settings.get('use_ssl', False):
        connection_string += "?sslmode=require"
    
    database = HealthCheckDatabase(
        connection_string=connection_string,
        min_pool_size=1,
        max_pool_size=db_settings.get('pool_size', 10),
        logger=logger
    )
    
    try:
        # Initialize the database connection and schema
        await database.initialize()
        logger.info("Database connection established")
        return database
    except InitialConnectionError as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise
    except DatabaseError as e:
        logger.error(f"Database error during setup: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during database setup: {str(e)}", exc_info=True)
        raise InitialConnectionError(f"Unexpected error: {str(e)}")

async def setup_runner(
    database: HealthCheckDatabase, 
    health_settings: HealthCheckSettings
) -> HealthCheckRunner:
    """
    Set up and start the health check runner.
    
    Args:
        database: Initialized database instance
        health_settings: Health check settings
        
    Returns:
        Running HealthCheckRunner instance
    """
    logger.info("Setting up health check runner")
    
    # Pass health check settings to the runner using the correct field names
    # from HealthCheckSettings TypedDict in runtime_settings.py
    runner_instance = HealthCheckRunner(
        db=database, 
        logger=logger,
        health_settings=health_settings
    )
    
    # Start the runner
    await runner_instance.start()
    logger.info("Health check runner started")
    
    return runner_instance

async def shutdown_runner() -> None:
    """Gracefully shut down the health check runner and database."""
    global runner, db
    
    if runner:
        logger.info("Shutting down health check runner")
        try:
            await runner.stop()
        except Exception as e:
            logger.error(f"Error shutting down runner: {str(e)}", exc_info=True)
    
    if db:
        logger.info("Closing database connection")
        try:
            await db.close()
        except Exception as e:
            logger.error(f"Error closing database: {str(e)}", exc_info=True)

async def restart_runner() -> None:
    """Restart the health check runner to refresh settings."""
    global runner, db, settings_reader, restart_event
    
    logger.info("Restarting health check runner")
    
    try:
        # Stop the current runner
        if runner:
            await runner.stop()
            logger.info("Runner stopped for restart")
        
        if not settings_reader or not db:
            logger.error("Cannot restart: settings reader or database not initialized")
            return
            
        # Reload settings
        settings_reader.reload_settings()
        health_settings = settings_reader.get_health_check_settings()
        
        # Create a new runner and start it
        runner = await setup_runner(db, health_settings)
        logger.info("Runner restarted successfully")
    except Exception as e:
        logger.error(f"Failed to restart runner: {str(e)}", exc_info=True)
        # Clear the restart event
        if restart_event:
            restart_event.clear()
        
        # Try to restart the runner if it was stopped
        if runner and not runner.running:
            try:
                await runner.start()
            except Exception as restart_error:
                logger.error(f"Failed to recover runner after restart failure: {str(restart_error)}", exc_info=True)

def handle_sighup(*args: Any) -> None:
    """Signal handler for SIGHUP (reload configuration)."""
    logger.info("Received SIGHUP signal, triggering restart")
    if restart_event:
        restart_event.set()

def handle_sigterm(*args: Any) -> None:
    """Signal handler for SIGTERM/SIGINT (graceful shutdown)."""
    logger.info("Received termination signal, initiating shutdown")
    if shutdown_event:
        shutdown_event.set()

async def main_loop() -> int:
    """
    Main application loop.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    global runner, db, settings_reader, shutdown_event, restart_event
    
    shutdown_event = asyncio.Event()
    restart_event = asyncio.Event()
    
    try:
        if not settings_reader:
            logger.error("Settings reader not initialized")
            return 1
            
        # Get settings
        db_settings = settings_reader.get_db_connection_settings()
        health_settings = settings_reader.get_health_check_settings()
        
        # Setup the database
        db = await setup_database(db_settings)
        
        # Setup the health check runner
        runner = await setup_runner(db, health_settings)
        
        logger.info("Health checker started successfully. Press Ctrl+C to stop.")
        
        # Main loop
        while not shutdown_event.is_set():
            # Wait for either restart or shutdown event
            restart_task = asyncio.create_task(restart_event.wait())
            shutdown_task = asyncio.create_task(shutdown_event.wait())
            
            # Wait for one of the events or a timeout (to periodically check status)
            done, pending = await asyncio.wait(
                [restart_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=60  # Check status every minute
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
            
            # Handle restart event
            if restart_event.is_set():
                logger.info("Restart event triggered")
                restart_event.clear()
                await restart_runner()
            
            # Check database health periodically
            if db and not shutdown_event.is_set():
                healthy = await db.is_healthy()
                if not healthy:
                    logger.warning("Database connection not healthy, attempting to reconnect")
                    try:
                        await db.close()
                        await db.initialize()
                    except Exception as e:
                        logger.error(f"Failed to reconnect to database: {str(e)}", exc_info=True)
        
        logger.info("Main loop exited, shutting down")
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)
        return 1
    finally:
        # Ensure we always shut down gracefully
        await shutdown_runner()
    
    return 0

def setup_signal_handlers() -> None:
    """Set up signal handlers for different platforms."""
    # Common signals for all platforms
    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    # SIGHUP is not available on Windows
    if platform.system() != "Windows":
        signal.signal(signal.SIGHUP, handle_sighup)
    else:
        # On Windows, we can use SIGBREAK (Ctrl+Break) as an alternative to SIGHUP
        signal.signal(signal.SIGBREAK, handle_sighup)
        logger.info("Windows platform detected: Use Ctrl+Break to reload configuration")

def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Health Checker Application')
    parser.add_argument('--config', type=str, help='Path to configuration .env file (required)')
    parser.add_argument('--log-level', type=str, default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set logging level (overrides settings from .env)')
    
    return parser.parse_args()

def load_env_file(file_path: str) -> Optional[BytesIO]:
    """
    Load environment file into BytesIO.
    
    Args:
        file_path: Path to .env file
        
    Returns:
        BytesIO object containing file contents, or None if file not found
    """
    try:
        if os.path.exists(file_path):
            logger.info(f"Loading configuration from: {file_path}")
            with open(file_path, 'rb') as f:
                return BytesIO(f.read())
        else:
            logger.error(f"Configuration file not found: {file_path}")
            return None
    except Exception as e:
        logger.error(f"Error loading config file: {str(e)}")
        return None

def main() -> int:
    """
    Main entry point for the application.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    global settings_reader
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set initial logging level from command line
    log_level = getattr(logging, args.log_level.upper())
    logger.setLevel(log_level)
    logger.info(f"Initial log level set to {args.log_level} from command line")
    
    # Load config file - either specified or default
    config_path = args.config if args.config else os.path.join(os.getcwd(), '.env')
    
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        logger.error("A valid .env configuration file is required")
        return 1
        
    # Load the configuration file
    env_source = load_env_file(config_path)
    if not env_source:
        logger.error(f"Failed to load configuration from {config_path}")
        return 1
    
    # Initialize settings reader
    settings_reader = RuntimeSettingsReader(env_source)
    
    # Check if log level should be overridden by settings
    # Command line args take precedence, so only update if --log-level wasn't explicitly provided
    if args.log_level == 'INFO' and not args.config:  # Default value and no explicit config
        general_settings = settings_reader.get_general_settings()
        if 'log_level' in general_settings:
            log_level_str = general_settings['log_level']
            logger.setLevel(getattr(logging, log_level_str))
            logger.info(f"Log level updated to {log_level_str} from settings")
    
    # Set up signal handlers
    setup_signal_handlers()
    
    # Run main loop
    try:        
        return asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        return 0
    except Exception as e:
        logger.error(f"Application failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())