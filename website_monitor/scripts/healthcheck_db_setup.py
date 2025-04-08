#!/usr/bin/env python3
"""
Healthcheck Database Settings Script

This script adds healthcheck settings entries to the PostgreSQL database for specified URLs/routes.

Usage:
    python healthcheck_db_setup.py --base-url=127.0.0.1:8080 --routes=/a,/b,/c,/d,/e

Configuration:
    A valid .env file is required in the current directory
    All settings are loaded via RuntimeSettingsReader from the configuration file
"""

import os
import sys
import argparse
import asyncio
import logging
from io import BytesIO
import psycopg
from psycopg.rows import dict_row
import json

# Add parent directory to sys.path so we can use absolute imports
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Import RuntimeSettingsReader
from src.runtime_settings import RuntimeSettingsReader, DatabaseSettings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def add_healthcheck_settings(dsn, base_url, routes, check_interval=60, timeout_ms=5000):
    """Add healthcheck settings to the database for each route."""
    logger.info(f"Adding healthcheck settings for {len(routes)} routes to database")
    
    # Ensure base_url doesn't have a trailing slash
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    
    # Add protocol if not present
    if not base_url.startswith('http://') and not base_url.startswith('https://'):
        raise ValueError("Base URL must start with 'http://' or 'https://'")
    
    try:
        # Connect to the database asynchronously
        async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
            async with conn.cursor() as cur:
                # Prepare insert query
                insert_query = """
                INSERT INTO healthcheck_settings 
                (url, expected_status_code, check_interval, timeout_ms, headers, active)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
                """
                
                for route in routes:
                    # Ensure route starts with a slash
                    if not route.startswith('/'):
                        route = f"/{route}"
                    
                    url = f"{base_url}{route}"
                    headers = json.dumps({"Accept": "text/html"})
                    
                    # Execute the insert query
                    await cur.execute(
                        insert_query, 
                        (url, 200, check_interval, timeout_ms, headers, True)
                    )
                    
                    result = await cur.fetchone()
                    logger.info(f"Added healthcheck setting for URL {url} with ID {result['id']}")
                
                # Commit the transaction
                await conn.commit()
                logger.info("Successfully added all healthcheck settings to the database")
                
    except Exception as e:
        logger.error(f"Error adding healthcheck settings to database: {e}")
        raise

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Healthcheck Database Settings Setup')
    
    parser.add_argument('--base-url', type=str, required=True,
                       help='Base URL for the healthcheck endpoints (e.g., 127.0.0.1:8080 or example.com)')
    parser.add_argument('--routes', type=str, required=True,
                       help='Comma-separated list of routes (e.g., /a,/b,/c)')
    parser.add_argument('--check-interval', type=int, default=60,
                       help='Interval in seconds between health checks')
    parser.add_argument('--timeout', type=int, default=5000,
                       help='Timeout in milliseconds for health checks')
    parser.add_argument('--config', type=str, default=None,
                       help='Path to configuration .env file')
    
    args = parser.parse_args()
    
    # Parse routes from comma-separated string to list
    routes_list = args.routes.split(',')
    
    return args.base_url, routes_list, args.check_interval, args.timeout, args.config

def load_env_file(file_path):
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

async def main():
    """Main entry point of the script."""
    # Parse command line arguments
    base_url, routes, check_interval, timeout, config_path = parse_arguments()
    
    # Load config file - either specified or default in current directory
    if not config_path:
        config_path = os.path.join(os.getcwd(), '.env')
    
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        logger.error("A valid .env configuration file is required")
        sys.exit(1)
        
    # Load the configuration file
    env_source = load_env_file(config_path)
    if not env_source:
        logger.error(f"Failed to load configuration from {config_path}")
        sys.exit(1)
    
    # Initialize settings reader
    settings_reader = RuntimeSettingsReader(env_source)
    
    # Get database settings
    try:
        db_settings = settings_reader.get_db_connection_settings()
    except Exception as e:
        logger.error(f"Error getting database settings: {e}")
        sys.exit(1)
    
    # Construct DSN from settings
    dsn = f"postgresql://{db_settings['user']}:{db_settings['password']}@{db_settings['host']}:{db_settings['port']}/{db_settings['name']}"
    
    # Append SSL mode if SSL is enabled
    if db_settings.get('use_ssl', False):
        dsn += "?sslmode=require"
    
    try:
        # Add healthcheck settings to the database
        await add_healthcheck_settings(dsn, base_url, routes, check_interval, timeout)
        logger.info("Database setup complete.")
            
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())