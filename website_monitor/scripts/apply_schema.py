import os
import sys
import psycopg
from io import BytesIO

# Add parent directory to sys.path so we can use absolute imports
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Import RuntimeSettingsReader
from src.runtime_settings import RuntimeSettingsReader, DatabaseSettings

def reset_database(conn):
    """
    Drop the public schema and recreate it.
    This effectively cancels (removes) all existing objects in the database.
    """
    with conn.cursor() as cur:
        # Drop the public schema and all its objects, then recreate the schema.
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    print("Database schema dropped and recreated.")

def apply_sql_file(conn, sql_file_path):
    """
    Read and execute the SQL file that defines the schema.
    """
    with open(sql_file_path, "r") as file:
        sql_content = file.read()
    with conn.cursor() as cur:
        # Execute the SQL file contents.
        cur.execute(sql_content)
    print(f"SQL file '{sql_file_path}' applied successfully.")

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
            print(f"Loading configuration from: {file_path}")
            with open(file_path, 'rb') as f:
                return BytesIO(f.read())
        else:
            print(f"Configuration file not found: {file_path}")
            return None
    except Exception as e:
        print(f"Error loading config file: {str(e)}")
        return None

def main():
    # Load config file - either specified or default in current directory
    config_path = os.path.join(os.getcwd(), '.env')
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        print("A valid .env configuration file is required")
        return
        
    # Load the configuration file
    env_source = load_env_file(config_path)
    if not env_source:
        print(f"Failed to load configuration from {config_path}")
        return
    
    # Initialize settings reader
    settings_reader = RuntimeSettingsReader(env_source)
    
    # Get database settings
    try:
        db_settings = settings_reader.get_db_connection_settings()
    except Exception as e:
        print(f"Error getting database settings: {e}")
        return
    
    # Construct DSN from settings
    dsn = f"postgresql://{db_settings['user']}:{db_settings['password']}@{db_settings['host']}:{db_settings['port']}/{db_settings['name']}"
    
    # Append SSL mode if SSL is enabled
    if db_settings.get('use_ssl', False):
        dsn += "?sslmode=require"

    try:
        # Connect to PostgreSQL; autocommit is enabled for DDL operations.
        conn = psycopg.connect(dsn, autocommit=True)
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return

    try:
        reset_database(conn)
        apply_sql_file(conn, "pgschema/pgschema.sql")
        print("Database reset and schema applied successfully.")
    except Exception as e:
        print(f"Error during database reset or schema application: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()