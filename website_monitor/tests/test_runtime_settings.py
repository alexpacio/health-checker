import unittest
from io import BytesIO
import sys
import os
from unittest import mock

# Add the parent directory to sys.path to import the module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the module to test
from src.runtime_settings import (
    RuntimeSettingsReader, 
    DatabaseSettings, 
    HealthCheckSettings,
    GeneralSettings,
    BoundsRegistry,
    NumericBound,
    StringBound,
    DatabaseDefaultSettings,
    HealthCheckDefaultSettings,
    GeneralDefaultSettings,
    DB_SETTINGS_PREFIX,
    HC_RUNTIME_SETTINGS_PREFIX,
    GENERAL_SETTINGS_PREFIX
)


class TestRuntimeSettingsReader(unittest.TestCase):
    """Test cases for the RuntimeSettingsReader class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Create sample env content with correctly named fields
        self.sample_env_content = b"""
        # Database settings
        DB_HOST=testhost.example.com
        DB_PORT=5433
        DB_NAME=testdb
        DB_USER=testuser
        DB_PASSWORD=testpassword
        DB_TIMEOUT=45
        DB_CONNECT_TIMEOUT=15
        DB_USE_SSL=false
        DB_POOL_SIZE=10

        # Health check settings - using the correct field names from HealthCheckSettings
        HC_CHECKER_LOOP_FREQUENCY_IN_SEC=2
        HC_CHECK_INTERVAL_IN_SEC=8
        HC_CONNECTION_TIMEOUT_IN_SEC=15
        
        # General settings
        RUNTIME_LOG_LEVEL=INFO
        """
        
        # Create a BytesIO buffer with sample content
        self.env_buffer = BytesIO(self.sample_env_content)
        
        # Create the settings reader
        self.reader = RuntimeSettingsReader(self.env_buffer)

    def test_initialization(self):
        """Test that the reader initializes correctly."""
        self.assertIsInstance(self.reader, RuntimeSettingsReader)
        self.assertEqual(self.reader.env_source, self.env_buffer)
        self.assertIsInstance(self.reader.parsed_env, dict)
        self.assertIsInstance(self.reader.db_settings, dict)
        self.assertIsInstance(self.reader.health_settings, dict)
        self.assertIsInstance(self.reader.general_settings, dict)

    def test_parse_env_content(self):
        """Test that env content is parsed correctly."""
        # Reset the buffer position
        self.env_buffer.seek(0)
        
        # Parse content directly
        self.reader._parse_env_content(self.env_buffer.read().decode('utf-8'))
        
        # Check that all expected keys were parsed
        expected_keys = [
            'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
            'DB_TIMEOUT', 'DB_CONNECT_TIMEOUT', 'DB_USE_SSL', 'DB_POOL_SIZE',
            'HC_CHECKER_LOOP_FREQUENCY_IN_SEC', 'HC_CHECK_INTERVAL_IN_SEC', 'HC_CONNECTION_TIMEOUT_IN_SEC',
            'RUNTIME_LOG_LEVEL'
        ]
        
        for key in expected_keys:
            self.assertIn(key, self.reader.parsed_env)
        
        # Check specific values
        self.assertEqual(self.reader.parsed_env['DB_HOST'], 'testhost.example.com')
        self.assertEqual(self.reader.parsed_env['DB_PORT'], '5433')
        self.assertEqual(self.reader.parsed_env['HC_CHECKER_LOOP_FREQUENCY_IN_SEC'], '2')
        self.assertEqual(self.reader.parsed_env['RUNTIME_LOG_LEVEL'], 'INFO')

    def test_get_db_connection_settings(self):
        """Test retrieving database connection settings."""
        db_settings = self.reader.get_db_connection_settings()
        
        # Check that it's the correct type
        self.assertIsInstance(db_settings, dict)
        
        # Check that the settings were correctly converted
        self.assertEqual(db_settings['host'], 'testhost.example.com')
        self.assertEqual(db_settings['port'], 5433)  # Converted to int
        self.assertEqual(db_settings['use_ssl'], False)  # Converted to bool
        self.assertEqual(db_settings['pool_size'], 10)  # Converted to int

    def test_get_health_check_settings(self):
        """Test retrieving health check settings."""
        health_settings = self.reader.get_health_check_settings()
        
        # Check that it's the correct type
        self.assertIsInstance(health_settings, dict)
        
        # Check that the settings were correctly converted using the correct field names
        self.assertEqual(health_settings['checker_loop_frequency_in_sec'], 2)
        self.assertEqual(health_settings['check_interval_in_sec'], 8)
        self.assertEqual(health_settings['connection_timeout_in_sec'], 15)

    def test_get_general_settings(self):
        """Test retrieving general application settings."""
        general_settings = self.reader.get_general_settings()
        
        # Check that it's the correct type
        self.assertIsInstance(general_settings, dict)
        
        # Check only log_level, as it's the only field defined in GeneralSettings
        self.assertEqual(general_settings['log_level'], 'INFO')

    def test_validate_numeric_bounds(self):
        """Test validation of numeric bounds for all settings types."""
        # Test DB port validation
        converted_value = self.reader._convert_value('5433', int)
        port = self.reader._validate_value(converted_value, 'port', DB_SETTINGS_PREFIX.rstrip('_'), int)
        self.assertEqual(port, 5433)
        
        # Test port below minimum
        converted_value = self.reader._convert_value('0', int)
        port = self.reader._validate_value(converted_value, 'port', DB_SETTINGS_PREFIX.rstrip('_'), int)
        self.assertEqual(port, BoundsRegistry.DB.PORT.min)
        
        # Test port above maximum
        converted_value = self.reader._convert_value('70000', int)
        port = self.reader._validate_value(converted_value, 'port', DB_SETTINGS_PREFIX.rstrip('_'), int)
        self.assertEqual(port, BoundsRegistry.DB.PORT.max)
        
        # Test HC checker_loop_frequency_in_sec validation
        converted_value = self.reader._convert_value('30', int)
        checker_freq = self.reader._validate_value(
            converted_value, 
            'checker_loop_frequency_in_sec', 
            HC_RUNTIME_SETTINGS_PREFIX.rstrip('_'), 
            int
        )
        self.assertEqual(checker_freq, 30)
        
        # Test checker_loop_frequency_in_sec below minimum
        converted_value = self.reader._convert_value('0', int)
        checker_freq = self.reader._validate_value(
            converted_value,
            'checker_loop_frequency_in_sec', 
            HC_RUNTIME_SETTINGS_PREFIX.rstrip('_'), 
            int
        )
        self.assertEqual(checker_freq, BoundsRegistry.HC.CHECKER_LOOP_FREQUENCY_IN_SEC.min)
        
        # Test checker_loop_frequency_in_sec above maximum
        converted_value = self.reader._convert_value('7200', int)
        checker_freq = self.reader._validate_value(
            converted_value, 
            'checker_loop_frequency_in_sec', 
            HC_RUNTIME_SETTINGS_PREFIX.rstrip('_'), 
            int
        )
        self.assertEqual(checker_freq, BoundsRegistry.HC.CHECKER_LOOP_FREQUENCY_IN_SEC.max)

    def test_validate_string_bounds(self):
        """Test validation of string bounds."""
        # Test valid log level
        log_level = self.reader._validate_value('INFO', 'log_level', GENERAL_SETTINGS_PREFIX.rstrip('_'), str)
        self.assertEqual(log_level, 'INFO')
        
        # Test invalid log level - should default to first allowed value
        log_level = self.reader._validate_value('TRACE', 'log_level', GENERAL_SETTINGS_PREFIX.rstrip('_'), str)
        self.assertEqual(log_level, BoundsRegistry.GEN.LOG_LEVEL.allowed_values[0])
        
        # Test string without bounds - should return unchanged
        app_name = self.reader._validate_value('TestApp', 'app_name', GENERAL_SETTINGS_PREFIX.rstrip('_'), str)
        self.assertEqual(app_name, 'TestApp')

    def test_default_values(self):
        """Test that default values are applied correctly."""
        # Create a buffer with missing fields
        minimal_env = BytesIO(b"DB_HOST=minimal.example.com")
        
        # Create a new reader with minimal env
        minimal_reader = RuntimeSettingsReader(minimal_env)
        
        # Get settings
        db_settings = minimal_reader.get_db_connection_settings()
        health_settings = minimal_reader.get_health_check_settings()
        general_settings = minimal_reader.get_general_settings()
        
        # Check that default values were applied for database settings
        self.assertEqual(db_settings['host'], 'minimal.example.com')  # From env
        self.assertEqual(db_settings['port'], DatabaseDefaultSettings.PORT)  # Default
        self.assertEqual(db_settings['name'], DatabaseDefaultSettings.NAME)  # Default
        
        # Check that default values were applied for health check settings
        self.assertEqual(
            health_settings['checker_loop_frequency_in_sec'], 
            HealthCheckDefaultSettings.CHECKER_LOOP_FREQUENCY_IN_SEC
        )
        self.assertEqual(
            health_settings['check_interval_in_sec'], 
            HealthCheckDefaultSettings.CHECK_INTERVAL_IN_SEC
        )
        self.assertEqual(
            health_settings['connection_timeout_in_sec'], 
            HealthCheckDefaultSettings.CONNECTION_TIMEOUT_IN_SEC
        )
        
        # Check that default values were applied for general settings
        self.assertEqual(general_settings['log_level'], GeneralDefaultSettings.LOG_LEVEL)

    def test_reload_settings(self):
        """Test that settings can be reloaded."""
        # Store original settings
        original_health_settings = self.reader.get_health_check_settings().copy()
        
        # Update the buffer with new content
        new_env_content = b"""
        DB_HOST=newhost.example.com
        HC_CHECKER_LOOP_FREQUENCY_IN_SEC=3
        RUNTIME_LOG_LEVEL=DEBUG
        """
        new_buffer = BytesIO(new_env_content)
        
        # Update the reader
        self.reader.env_source = new_buffer
        self.reader.reload_settings()
        
        # Get settings
        db_settings = self.reader.get_db_connection_settings()
        health_settings = self.reader.get_health_check_settings()
        general_settings = self.reader.get_general_settings()
        
        # Check that new values were loaded
        self.assertEqual(db_settings['host'], 'newhost.example.com')
        self.assertEqual(health_settings['checker_loop_frequency_in_sec'], 3)
        self.assertEqual(general_settings['log_level'], 'DEBUG')
        
        # Check that other health settings were reset to defaults
        self.assertEqual(
            health_settings['check_interval_in_sec'], 
            HealthCheckDefaultSettings.CHECK_INTERVAL_IN_SEC
        )
        self.assertEqual(
            health_settings['connection_timeout_in_sec'], 
            HealthCheckDefaultSettings.CONNECTION_TIMEOUT_IN_SEC
        )

    def test_malformed_lines(self):
        """Test handling of malformed lines in env content."""
        malformed_env = BytesIO(b"""
        # Good line
        DB_HOST=good.example.com
        # Malformed line
        BROKEN_LINE
        # Another good line
        DB_PORT=5555
        RUNTIME_LOG_LEVEL=ERROR
        """)
        
        # Create a new reader with malformed content
        malformed_reader = RuntimeSettingsReader(malformed_env)
        
        # Get settings
        db_settings = malformed_reader.get_db_connection_settings()
        general_settings = malformed_reader.get_general_settings()
        
        # Check that good lines were processed
        self.assertEqual(db_settings['host'], 'good.example.com')
        self.assertEqual(db_settings['port'], 5555)
        self.assertEqual(general_settings['log_level'], 'ERROR')
        
        # Check that malformed line was ignored
        self.assertNotIn('BROKEN_LINE', malformed_reader.parsed_env)

    def test_type_conversion(self):
        """Test conversion of values to the correct types."""
        # Create env with values that need conversion
        conversion_env = BytesIO(b"""
        DB_PORT=not_a_number
        DB_USE_SSL=not_a_boolean
        HC_CHECKER_LOOP_FREQUENCY_IN_SEC=invalid
        RUNTIME_LOG_LEVEL=INVALID_LEVEL
        """)
        
        # Create a reader with this content
        conversion_reader = RuntimeSettingsReader(conversion_env)
        
        # Get settings
        db_settings = conversion_reader.get_db_connection_settings()
        health_settings = conversion_reader.get_health_check_settings()
        general_settings = conversion_reader.get_general_settings()
        
        # Check that values were handled gracefully
        # The _convert_value method returns 0 for invalid int conversions
        self.assertEqual(db_settings['port'], BoundsRegistry.DB.PORT.min)  # Min value after bounds validation
        
        # For health settings, check that values were properly bounded
        self.assertEqual(
            health_settings['checker_loop_frequency_in_sec'], 
            BoundsRegistry.HC.CHECKER_LOOP_FREQUENCY_IN_SEC.min
        )
        
        # For invalid log level, it should default to the first allowed value
        self.assertEqual(general_settings['log_level'], BoundsRegistry.GEN.LOG_LEVEL.allowed_values[0])

    def test_complete_settings(self):
        """Test that default values don't override provided values."""
        # Create a buffer with all fields
        complete_env = BytesIO(b"""
        DB_HOST=custom.example.com
        DB_PORT=9999
        DB_NAME=customdb
        DB_USER=customuser
        DB_PASSWORD=custompass
        DB_TIMEOUT=120
        DB_CONNECT_TIMEOUT=30
        DB_USE_SSL=true
        DB_POOL_SIZE=25
        
        HC_CHECKER_LOOP_FREQUENCY_IN_SEC=2
        HC_CHECK_INTERVAL_IN_SEC=6
        HC_CONNECTION_TIMEOUT_IN_SEC=10
        
        RUNTIME_LOG_LEVEL=WARNING
        """)
        
        # Create a new reader with complete env
        complete_reader = RuntimeSettingsReader(complete_env)
        
        # Get settings
        db_settings = complete_reader.get_db_connection_settings()
        health_settings = complete_reader.get_health_check_settings()
        general_settings = complete_reader.get_general_settings()
        
        # Check that database settings were applied correctly
        self.assertEqual(db_settings['host'], 'custom.example.com')
        self.assertEqual(db_settings['port'], 9999)
        self.assertEqual(db_settings['name'], 'customdb')
        self.assertEqual(db_settings['user'], 'customuser')
        self.assertEqual(db_settings['password'], 'custompass')
        self.assertEqual(db_settings['timeout'], 120)
        self.assertEqual(db_settings['connect_timeout'], 30)
        self.assertEqual(db_settings['use_ssl'], True)
        self.assertEqual(db_settings['pool_size'], 25)
        
        # Check that health check settings were applied correctly
        self.assertEqual(health_settings['checker_loop_frequency_in_sec'], 2)
        self.assertEqual(health_settings['check_interval_in_sec'], 6)
        self.assertEqual(health_settings['connection_timeout_in_sec'], 10)
        
        # Check that general settings were applied correctly
        self.assertEqual(general_settings['log_level'], 'WARNING')


if __name__ == '__main__':
    unittest.main()