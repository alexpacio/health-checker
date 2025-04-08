import os
import sys
import time
import psycopg
import matplotlib.pyplot as plt
from datetime import datetime
from io import BytesIO

# Add parent directory to sys.path so we can use absolute imports
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Import RuntimeSettingsReader
from src.runtime_settings import RuntimeSettingsReader, DatabaseSettings

def fetch_healthchecks(conn):
    """
    Fetch healthcheck records ordered by check time.
    Adjust the table/column names if they differ from your schema.
    """
    query = """
    SELECT target_id, check_time, response_time 
    FROM healthcheck_results
    WHERE check_time >= NOW() - INTERVAL '1 hour'
    ORDER BY check_time;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
    return results

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
    # Parse command line arguments for config path
    config_path = None
    if len(sys.argv) > 1 and sys.argv[1].startswith('--config='):
        config_path = sys.argv[1].split('=')[1]
    
    # Load config file - either specified or default in current directory
    if not config_path:
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

    # Connect to PostgreSQL
    try:
        conn = psycopg.connect(dsn)
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return

    # Enable interactive mode for matplotlib
    plt.ion()
    fig, ax = plt.subplots()
    
    while True:
        try:
            # Fetch data from the database
            records = fetch_healthchecks(conn)
        except Exception as e:
            print(f"Error fetching data: {e}")
            break

        # Group data by target
        data_by_target = {}
        for target, check_time, response_time in records:
            # Ensure check_time is a datetime instance
            if not isinstance(check_time, datetime):
                try:
                    check_time = datetime.fromisoformat(str(check_time))
                except Exception as ex:
                    print(f"Error parsing time {check_time}: {ex}")
                    continue

            if target not in data_by_target:
                data_by_target[target] = {"times": [], "response_times": []}
            data_by_target[target]["times"].append(check_time)
            data_by_target[target]["response_times"].append(response_time)

        # Clear the axes for the new plot
        ax.clear()
        # Plot each target's time series
        for target, series in data_by_target.items():
            ax.plot(series["times"], series["response_times"], marker='o', label=target)
        ax.legend(loc="upper left")
        ax.set_title("Response Time Over Time")
        ax.set_xlabel("Check Time")
        ax.set_ylabel("Response Time")
        plt.tight_layout()

        # Redraw the plot
        plt.draw()
        plt.pause(3)  # Pause for 3 seconds before updating

        # Check if the plot window is closed, then exit the loop
        if not plt.fignum_exists(fig.number):
            break

    # Clean up
    conn.close()

if __name__ == "__main__":
    main()