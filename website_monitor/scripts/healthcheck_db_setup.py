#!/usr/bin/env python3
"""
Healthcheck Database Settings Script

This script manages healthcheck settings entries in the PostgreSQL database.

Usage:
    # Add a new healthcheck setting
    python healthcheck_db_setup.py add --url=https://example.com/path [--regex-match="Success"] [--expected-status-code=200] 
                                       [--check-interval=60] [--timeout-ms=5000] 
                                       [--headers='{"User-Agent": "MyBot"}'] [--active=true]
    
    # List all healthcheck settings (with filters)
    python healthcheck_db_setup.py list [--active-only] [--url-filter=example.com]
    
    # Update existing healthcheck settings
    python healthcheck_db_setup.py update --id=1 [--url=https://new-url.com] [--regex-match="NewPattern"] 
                                          [--active=false] [--check-interval=120] [--timeout-ms=10000]
                                          [--headers='{"User-Agent": "MyBot"}'] [--expected-status-code=200]
    
    # Delete healthcheck settings
    python healthcheck_db_setup.py delete --id=1

Configuration:
    A valid .env file is required in the current directory.
    All settings are loaded via RuntimeSettingsReader from the configuration file.

Constraints:
    check_interval must be between 5 and 300 seconds
    URL must start with 'http://' or 'https://'
    Headers must be in valid JSON format
    Timeout is in milliseconds
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
from src.runtime_settings import RuntimeSettingsReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def add_healthcheck_setting(dsn, url, regex_match=None, expected_status_code=200, check_interval=60, timeout_ms=5000, headers=None, active=True):
    """Add a healthcheck setting to the database for the specified URL."""
    logger.info(f"Adding healthcheck setting for URL: {url}")
    
    # Validate URL format
    if not url.startswith('http://') and not url.startswith('https://'):
        raise ValueError(f"URL must start with 'http://' or 'https://': {url}")
    
    # Process headers
    if headers is None:
        headers = json.dumps({"Accept": "text/html"})
    elif not isinstance(headers, str):
        headers = json.dumps(headers)
        
    try:
        # Connect to the database asynchronously
        async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
            async with conn.cursor() as cur:
                # Prepare insert query
                insert_query = """
                INSERT INTO healthcheck_settings 
                (url, regex_match, expected_status_code, check_interval, timeout_ms, headers, active)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """
                
                # Execute the insert query
                await cur.execute(
                    insert_query, 
                    (url, regex_match, expected_status_code, check_interval, timeout_ms, headers, active)
                )
                
                result = await cur.fetchone()
                logger.info(f"Added healthcheck setting for URL {url} with ID {result['id']}")
            
                # Commit the transaction
                await conn.commit()
                logger.info("Successfully added healthcheck setting to the database")
                return result['id']
                
    except Exception as e:
        logger.error(f"Error adding healthcheck setting to database: {e}")
        raise

async def list_healthcheck_settings(dsn, active_only=False, url_filter=None):
    """
    List healthcheck settings from the database with optional filters.
    
    Args:
        dsn: Database connection string
        active_only: If True, only return active settings
        url_filter: Filter results by URLs containing this string
        
    Returns:
        List of healthcheck settings as dictionaries
    """
    logger.info("Retrieving healthcheck settings from database")
    
    try:
        async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
            async with conn.cursor() as cur:
                # Build the query with filters
                query = "SELECT * FROM healthcheck_settings"
                where_clauses = []
                params = []
                
                if active_only:
                    where_clauses.append("active = TRUE")
                
                if url_filter:
                    where_clauses.append("url LIKE %s")
                    params.append(f"%{url_filter}%")
                
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
                
                query += " ORDER BY id"
                
                # Execute the query
                await cur.execute(query, tuple(params))
                results = await cur.fetchall()
                
                logger.info(f"Retrieved {len(results)} healthcheck settings")
                
                # Format output for display
                for setting in results:
                    # Parse the JSON headers for better display
                    try:
                        if isinstance(setting['headers'], str):
                            setting['headers'] = json.loads(setting['headers'])
                    except (json.JSONDecodeError, TypeError):
                        # Keep as is if parsing fails
                        pass
                    
                    # Format timestamps for better readability
                    if 'created_at' in setting and setting['created_at']:
                        setting['created_at'] = setting['created_at'].strftime("%Y-%m-%d %H:%M:%S")
                    if 'updated_at' in setting and setting['updated_at']:
                        setting['updated_at'] = setting['updated_at'].strftime("%Y-%m-%d %H:%M:%S")
                
                return results
                
    except Exception as e:
        logger.error(f"Error retrieving healthcheck settings: {e}")
        raise

async def update_healthcheck_setting(dsn, setting_id, updates):
    """
    Update an existing healthcheck setting.
    
    Args:
        dsn: Database connection string
        setting_id: ID of the setting to update
        updates: Dictionary of field:value pairs to update
        
    Returns:
        True if update was successful, False if setting was not found
    """
    logger.info(f"Updating healthcheck setting with ID {setting_id}")
    
    # Validate updates
    valid_fields = {
        'url', 'regex_match', 'expected_status_code', 
        'check_interval', 'timeout_ms', 'headers', 'active'
    }
    
    # Filter out invalid fields
    valid_updates = {k: v for k, v in updates.items() if k in valid_fields}
    
    if not valid_updates:
        logger.warning("No valid fields to update")
        return False
    
    # Process headers if present
    if 'headers' in valid_updates and not isinstance(valid_updates['headers'], str):
        valid_updates['headers'] = json.dumps(valid_updates['headers'])
    
    try:
        async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
            async with conn.cursor() as cur:
                # Check if the setting exists
                await cur.execute(
                    "SELECT id FROM healthcheck_settings WHERE id = %s",
                    (setting_id,)
                )
                
                if not await cur.fetchone():
                    logger.warning(f"Healthcheck setting with ID {setting_id} not found")
                    return False
                
                # Build the update query
                set_clauses = []
                params = []
                
                for field, value in valid_updates.items():
                    set_clauses.append(f"{field} = %s")
                    params.append(value)
                
                # Add updated_at timestamp
                set_clauses.append("updated_at = NOW()")
                
                # Add the ID parameter
                params.append(setting_id)
                
                # Execute the update
                query = f"""
                UPDATE healthcheck_settings 
                SET {', '.join(set_clauses)}
                WHERE id = %s
                """
                
                await cur.execute(query, tuple(params))
                
                # Check if a row was affected
                if cur.rowcount == 0:
                    logger.warning(f"No rows affected when updating setting with ID {setting_id}")
                    return False
                
                # Commit the transaction
                await conn.commit()
                logger.info(f"Successfully updated healthcheck setting with ID {setting_id}")
                return True
                
    except Exception as e:
        logger.error(f"Error updating healthcheck setting: {e}")
        raise

async def delete_healthcheck_setting(dsn, setting_id):
    """
    Delete a healthcheck setting.
    
    Args:
        dsn: Database connection string
        setting_id: ID of the setting to delete
        
    Returns:
        True if deletion was successful, False if setting was not found
    """
    logger.info(f"Deleting healthcheck setting with ID {setting_id}")
    
    try:
        async with await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as conn:
            # Start a transaction
            async with conn.transaction():
                # First check if there are any results referencing this setting
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT COUNT(*) as count FROM healthcheck_results WHERE target_id = %s",
                        (setting_id,)
                    )
                    result = await cur.fetchone()
                    result_count = result['count'] if result else 0
                    
                    if result_count > 0:
                        logger.info(f"Found {result_count} results referencing this setting")
                        
                        # Ask for confirmation if running in interactive mode
                        if sys.stdin.isatty():
                            confirm = input(f"Deleting this setting will also delete {result_count} associated results. Proceed? (y/N): ")
                            if not confirm.lower().startswith('y'):
                                logger.info("Deletion cancelled by user")
                                return False
                        
                        # Delete associated results first
                        await cur.execute(
                            "DELETE FROM healthcheck_results WHERE target_id = %s",
                            (setting_id,)
                        )
                        logger.info(f"Deleted {cur.rowcount} associated results")
                
                # Now delete the setting
                async with conn.cursor() as cur:
                    await cur.execute(
                        "DELETE FROM healthcheck_settings WHERE id = %s",
                        (setting_id,)
                    )
                    
                    if cur.rowcount == 0:
                        logger.warning(f"Healthcheck setting with ID {setting_id} not found")
                        return False
                    
                    logger.info(f"Successfully deleted healthcheck setting with ID {setting_id}")
                    return True
                
    except Exception as e:
        logger.error(f"Error deleting healthcheck setting: {e}")
        raise

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Healthcheck Database Settings Management')
    
    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new healthcheck setting')
    add_parser.add_argument('--url', type=str, required=True,
                       help='Complete URL to monitor (e.g., https://example.com/path)')
    add_parser.add_argument('--regex-match', type=str,
                       help='Regular expression pattern to match in the response body')
    add_parser.add_argument('--expected-status-code', type=int, default=200,
                       help='Expected HTTP status code for a successful response (default: 200)')
    add_parser.add_argument('--check-interval', type=int, default=60,
                       help=f'Interval in seconds between health checks')
    add_parser.add_argument('--timeout-ms', type=int, default=5000,
                       help='Timeout in milliseconds for health checks')
    add_parser.add_argument('--headers', type=str,
                       help='Custom headers in JSON format (e.g., \'{"User-Agent": "MyBot", "Accept": "application/json"}\')')
    add_parser.add_argument('--active', type=str, choices=['true', 'false'], default='true',
                       help='Set if the healthcheck is active (default: true)')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List healthcheck settings')
    list_parser.add_argument('--active-only', action='store_true',
                         help='Only show active healthcheck settings')
    list_parser.add_argument('--url-filter', type=str,
                         help='Filter settings by URL (partial match)')
    
    # Update command
    update_parser = subparsers.add_parser('update', help='Update healthcheck settings')
    update_parser.add_argument('--id', type=int, required=True,
                          help='ID of the healthcheck setting to update')
    update_parser.add_argument('--url', type=str,
                          help='New URL for the healthcheck')
    update_parser.add_argument('--regex-match', type=str,
                          help='New regex pattern to match in the response')
    update_parser.add_argument('--expected-status-code', type=int,
                          help='New expected HTTP status code')
    update_parser.add_argument('--check-interval', type=int,
                          help=f'New interval in seconds between health checks')
    update_parser.add_argument('--timeout-ms', type=int,
                          help='New timeout in milliseconds for health checks')
    update_parser.add_argument('--headers', type=str,
                          help='New headers in JSON format')
    update_parser.add_argument('--active', type=str, choices=['true', 'false'],
                          help='Set check to active or inactive')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete healthcheck settings')
    delete_parser.add_argument('--id', type=int, required=True,
                          help='ID of the healthcheck setting to delete')
    
    # Common arguments for all commands
    parser.add_argument('--config', type=str, default=None,
                       help='Path to configuration .env file')
    
    args = parser.parse_args()
    
    # Validate that a command was provided
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    return args

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

def format_output(settings):
    """Format healthcheck settings for display."""
    if not settings:
        return "No healthcheck settings found."
        
    output = []
    for setting in settings:
        output.append(f"ID: {setting['id']}")
        output.append(f"URL: {setting['url']}")
        output.append(f"Status Code: {setting['expected_status_code']}")
        output.append(f"Check Interval: {setting['check_interval']} seconds")
        output.append(f"Timeout: {setting['timeout_ms']} ms")
        output.append(f"Active: {setting['active']}")
        
        # Add regex match if present
        if setting.get('regex_match'):
            output.append(f"Regex Match: {setting['regex_match']}")
            
        # Add headers if present
        if setting.get('headers'):
            if isinstance(setting['headers'], str):
                try:
                    headers = json.loads(setting['headers'])
                    output.append("Headers:")
                    for k, v in headers.items():
                        output.append(f"  {k}: {v}")
                except:
                    output.append(f"Headers: {setting['headers']}")
            else:
                output.append("Headers:")
                for k, v in setting['headers'].items():
                    output.append(f"  {k}: {v}")
        
        # Add timestamps
        if 'created_at' in setting:
            output.append(f"Created: {setting['created_at']}")
        if 'updated_at' in setting:
            output.append(f"Updated: {setting['updated_at']}")
            
        output.append("-" * 40)
        
    return "\n".join(output)

async def main():
    """Main entry point of the script."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Load config file - either specified or default in current directory
    if not args.config:
        config_path = os.path.join(os.getcwd(), '.env')
    else:
        config_path = args.config
    
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
        # Execute the appropriate command
        if args.command == 'add':
            # Process headers if provided
            headers = None
            if args.headers is not None:
                try:
                    headers = json.loads(args.headers)
                except json.JSONDecodeError:
                    logger.error("Invalid JSON format for headers")
                    sys.exit(1)
            
            # Convert active string to boolean
            active = args.active.lower() == 'true' if args.active else True
            
            setting_id = await add_healthcheck_setting(
                dsn, 
                args.url, 
                regex_match=args.regex_match,
                expected_status_code=args.expected_status_code,
                check_interval=args.check_interval, 
                timeout_ms=args.timeout_ms,
                headers=headers,
                active=active
            )
            print(f"Successfully added healthcheck setting with ID {setting_id}")
            logger.info("Database setup complete.")
            
        elif args.command == 'list':
            settings = await list_healthcheck_settings(dsn, args.active_only, args.url_filter)
            print(format_output(settings))
            
        elif args.command == 'update':
            # Build updates dictionary from provided arguments
            updates = {}
            if args.url is not None:
                updates['url'] = args.url
            if args.regex_match is not None:
                updates['regex_match'] = args.regex_match
            if args.expected_status_code is not None:
                updates['expected_status_code'] = args.expected_status_code
            if args.check_interval is not None:
                updates['check_interval'] = args.check_interval
            if args.timeout_ms is not None:
                updates['timeout_ms'] = args.timeout_ms
            if args.headers is not None:
                try:
                    updates['headers'] = json.loads(args.headers)
                except json.JSONDecodeError:
                    logger.error("Invalid JSON format for headers")
                    sys.exit(1)
            if args.active is not None:
                updates['active'] = args.active.lower() == 'true'
                
            if not updates:
                logger.error("No update parameters provided")
                sys.exit(1)
                
            success = await update_healthcheck_setting(dsn, args.id, updates)
            if success:
                print(f"Successfully updated healthcheck setting with ID {args.id}")
            else:
                print(f"Failed to update healthcheck setting with ID {args.id}")
                sys.exit(1)
                
        elif args.command == 'delete':
            success = await delete_healthcheck_setting(dsn, args.id)
            if success:
                print(f"Successfully deleted healthcheck setting with ID {args.id}")
            else:
                print(f"Failed to delete healthcheck setting with ID {args.id}")
                sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user.")
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())