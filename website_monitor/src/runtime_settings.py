from typing import Dict, Any, TypedDict, Type, get_type_hints, Union, Callable
from io import BytesIO

"""
Prefixes that identify environment variables groups. EnvVar keys must start with these prefixes.
"""
DB_SETTINGS_PREFIX = 'DB_'
HC_RUNTIME_SETTINGS_PREFIX = 'HC_'
GENERAL_SETTINGS_PREFIX = 'RUNTIME_'

class NumericBound:
    """Container for numeric min/max bounds."""
    def __init__(self, min_val: int, max_val: int):
        self.min = min_val
        self.max = max_val


class StringBound:
    """Container for string allowed values."""
    def __init__(self, allowed_values: list[str]):
        self.allowed_values = allowed_values


class BoundsRegistry:
    """
    Central registry for all setting bounds (numeric and string).
    This removes the need for multiple bounds classes.
    """
    # Strip the trailing underscore from prefixes
    DB_GROUP = DB_SETTINGS_PREFIX.rstrip('_')
    HC_GROUP = HC_RUNTIME_SETTINGS_PREFIX.rstrip('_')
    GEN_GROUP = GENERAL_SETTINGS_PREFIX.rstrip('_')
    
    # Database bounds
    class DB:
        """Database bounds"""
        PORT = NumericBound(1, 65535)
        TIMEOUT = NumericBound(1, 300)           # Maximum: 5 minutes
        CONNECT_TIMEOUT = NumericBound(1, 60)    # Maximum: 1 minute
        POOL_SIZE = NumericBound(1, 100)
    
    # Health check bounds
    class HC:
        """Health check bounds"""
        CHECKER_LOOP_FREQUENCY_IN_SEC = NumericBound(1, 3600)         # 1 second to 1 hour
        CHECK_INTERVAL_IN_SEC = NumericBound(1, 60)        # Maximum: 1 minute
        EXPECTED_STATUS_CODE = NumericBound(100, 599)                 # HTTP status codes range
        CONNECTION_TIMEOUT_IN_SEC = NumericBound(1, 30) # Maximum: 30 seconds
        TARGETS_SETTINGS_REFRESH_RATE_IN_SEC = NumericBound(1, 3600) # Maximum: 1 hour
    
    # General settings bounds
    class GEN:
        """General settings bounds"""
        LOG_LEVEL = StringBound(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    
    # Mapping from prefix groups to nested classes - using the constants
    _GROUP_MAP = {
        DB_GROUP: DB,
        HC_GROUP: HC,
        GEN_GROUP: GEN
    }
    
    @classmethod
    def get_numeric_bound(cls, group: str, setting_name: str) -> NumericBound | None:
        """Get numeric bound for a setting if it exists."""
        if group not in cls._GROUP_MAP:
            return None
        
        group_class = cls._GROUP_MAP[group]
        if hasattr(group_class, setting_name.upper()):
            bound = getattr(group_class, setting_name.upper())
            if isinstance(bound, NumericBound):
                return bound
        return None
    
    @classmethod
    def get_string_bound(cls, group: str, setting_name: str) -> StringBound | None:
        """Get string bound for a setting if it exists."""
        if group not in cls._GROUP_MAP:
            return None
        
        group_class = cls._GROUP_MAP[group]
        if hasattr(group_class, setting_name.upper()):
            bound = getattr(group_class, setting_name.upper())
            if isinstance(bound, StringBound):
                return bound
        return None


class DatabaseDefaultSettings:
    """
    Defines default values for database settings.
    """
    HOST = 'localhost'
    PORT = 5432
    NAME = ''
    USER = ''
    PASSWORD = ''
    TIMEOUT = 30
    CONNECT_TIMEOUT = 10
    USE_SSL = False
    POOL_SIZE = 5


class HealthCheckDefaultSettings:
    """
    Defines default values for health check settings.
    """
    CHECKER_LOOP_FREQUENCY_IN_SEC = 1  # the frequency of the health check loop in seconds
    CHECK_INTERVAL_IN_SEC = 5  # 5 seconds, if not defined in the database, the target is evaluated each x seconds
    EXPECTED_STATUS_CODE = 200  # the expected status code for a successful health check
    CONNECTION_TIMEOUT_IN_SEC = 5  # 5 seconds
    TARGETS_SETTINGS_REFRESH_RATE_IN_SEC = 60  # 1 minute, the frequency of the targets settings refresh in seconds


class GeneralDefaultSettings:
    """
    Defines default values for general application settings.
    """
    LOG_LEVEL = 'INFO'

class DatabaseSettings(TypedDict, total=False):
    """Type definition for database settings."""
    host: str
    port: int
    name: str
    user: str
    password: str
    timeout: int
    connect_timeout: int
    use_ssl: bool
    pool_size: int

class HealthCheckSettings(TypedDict, total=False):
    """Type definition for health check settings."""
    checker_loop_frequency_in_sec: int
    check_interval_in_sec: int
    expected_status_code: int
    connection_timeout_in_sec: int
    targets_settings_refresh_rate_in_sec: int


class GeneralSettings(TypedDict, total=False):
    """Type definition for general application settings."""
    log_level: str

class RuntimeSettingsReader:
    """
    A class to read database connection settings, health check frequency,
    and general application settings from environment variables or .env file/buffer.
    
    Handles parsing and type conversion of environment variables from
    system environment or .env buffer content based on TypedDict definitions.
    """

    def __init__(self, env_source: BytesIO):
        """
        Initialize the settings reader with a BytesIO buffer containing .env file content.
        
        Args:
            env_source: BytesIO buffer containing .env content
        """
        self.env_source = env_source
        self.parsed_env: Dict[str, str] = {}
        self.db_settings: DatabaseSettings = {}
        self.health_settings: HealthCheckSettings = {}
        self.general_settings: GeneralSettings = {}
        self.reload_settings()
    
    def _load_settings(self) -> None:
        """
        Load settings from BytesIO buffer and parse them into internal dictionaries.
        """
        # Read and decode the buffer content
        content = self.env_source.read().decode('utf-8')
        
        # Parse the content line by line and build environment dictionary
        self._parse_env_content(content)
        
        # Populate typed dictionaries from parsed content
        self._populate_db_settings()
        self._populate_health_settings()
        self._populate_general_settings()
    
    def _parse_env_content(self, content: str) -> None:
        """
        Parse .env file content into internal dictionary.
        
        Args:
            content: String content of the .env file
        """
        self.parsed_env = {}
        
        for line in content.splitlines():
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Parse key-value pairs
            try:
                key, value = line.split('=', 1)
                self.parsed_env[key.strip()] = value.strip()
            except ValueError:
                # Skip malformed lines
                continue
    
    def _convert_value(self, value: str, expected_type: Type) -> Any:
        """
        Convert a string value to the expected type.
        
        Args:
            value: String value from environment
            expected_type: Type to convert to
            
        Returns:
            Converted value
        """
        if expected_type == bool:
            return value.lower() == 'true'
        elif expected_type == int:
            try:
                return int(value)
            except ValueError:
                return 0
        elif expected_type == float:
            try:
                return float(value)
            except ValueError:
                return 0.0
        else:
            # Default to string
            return value
    
    def _validate_value(self, value: Any, setting_name: str, prefix_key: str, expected_type: Type) -> Any:
        """
        Unified validation function for any setting value.
        Handles both numeric and string validation.
        
        Args:
            value: The value to validate
            setting_name: The name of the setting
            prefix_key: The prefix key for the setting group (DB, HC, GEN)
            expected_type: The expected type of the value
            
        Returns:
            The validated value
        """
        # Strip the prefix from the prefix key
        group = prefix_key.replace('_', '')
        
        # For integer values, check numeric bounds
        if expected_type == int:
            bound = BoundsRegistry.get_numeric_bound(group, setting_name)
            if bound:
                return max(bound.min, min(value, bound.max))
        
        # For string values, check allowed values
        elif expected_type == str:
            bound = BoundsRegistry.get_string_bound(group, setting_name)
            if bound and value not in bound.allowed_values:
                # If invalid, return first allowed value as default
                if bound.allowed_values:
                    return bound.allowed_values[0]
        
        # For other types or if no bounds defined, return as is
        return value
    
    def _populate_settings(self, 
                          prefix: str, 
                          settings_dict: Dict[str, Any], 
                          settings_type: Type,
                          validator_func: Union[Callable[[int, str], int], None] = None) -> None:
        """
        Generic function to populate settings dictionaries from parsed environment variables.
        
        Args:
            prefix: The environment variable prefix to look for
            settings_dict: The settings dictionary to populate
            settings_type: The TypedDict class defining the settings structure
            validator_func: Optional function to validate numeric values
        """
        # Clear the dictionary
        settings_dict.clear()
        
        # Get type hints for the settings type
        type_hints = get_type_hints(settings_type)
        
        # Check for prefixed variables
        for env_key, env_value in self.parsed_env.items():
            if env_key.startswith(prefix):
                # Convert prefixed key to setting name (e.g., DB_HOST to host)
                setting_name = env_key[len(prefix):].lower()
                
                # Only process if this is a valid field in the settings TypedDict
                if setting_name in type_hints:
                    expected_type = type_hints[setting_name]
                    # Convert value to the expected type
                    converted_value = self._convert_value(env_value, expected_type)
                    
                    # Always validate using _validate_value first
                    prefix_key = prefix.rstrip('_')  # Remove trailing underscore
                    validated_value = self._validate_value(converted_value, setting_name, prefix_key, expected_type)
                    
                    # Apply additional validator function if provided
                    if expected_type == int and validator_func is not None:
                        validated_value = validator_func(validated_value, setting_name)
                    
                    settings_dict[setting_name] = validated_value
        
        # Set default values for any missing but required fields
        self._set_default_values(settings_dict, settings_type)
    
    def _populate_db_settings(self) -> None:
        self._populate_settings(
            prefix=DB_SETTINGS_PREFIX,
            settings_dict=self.db_settings,
            settings_type=DatabaseSettings
        )
    
    def _populate_health_settings(self) -> None:
        self._populate_settings(
            prefix=HC_RUNTIME_SETTINGS_PREFIX,
            settings_dict=self.health_settings,
            settings_type=HealthCheckSettings
        )
    
    def _populate_general_settings(self) -> None:
        self._populate_settings(
            prefix=GENERAL_SETTINGS_PREFIX,
            settings_dict=self.general_settings,
            settings_type=GeneralSettings
        )
    
    def _set_default_values(self, settings_dict: Dict[str, Any], settings_type: Type) -> None:
        """
        Set default values for any missing fields in settings dictionary.
        
        Args:
            settings_dict: The settings dictionary to populate with defaults
            settings_type: The TypedDict class defining the settings structure
        """
        # Get all fields from the TypedDict
        type_hints = get_type_hints(settings_type)
        
        # Determine which default settings class to use
        default_settings_class = None
        if settings_type == DatabaseSettings:
            default_settings_class = DatabaseDefaultSettings
        elif settings_type == HealthCheckSettings:
            default_settings_class = HealthCheckDefaultSettings
        elif settings_type == GeneralSettings:
            default_settings_class = GeneralDefaultSettings
        else:
            return  # No defaults for unknown settings type
        
        # Add default values for missing fields
        for field_name in type_hints:
            if field_name not in settings_dict:
                # Get the attribute from the default settings class using the uppercase field name
                attr_name = field_name.upper()
                if hasattr(default_settings_class, attr_name):
                    settings_dict[field_name] = getattr(default_settings_class, attr_name)
    
    def get_db_connection_settings(self) -> DatabaseSettings:
        """
        Get database connection settings.
        
        Returns:
            DatabaseSettings object containing database connection parameters
        
        Raises:
            KeyError: If no database settings are found
        """
        if not self.db_settings:
            raise KeyError("No database settings found in environment variables")
        
        return self.db_settings
    
    def get_health_check_settings(self) -> HealthCheckSettings:
        """
        Get the health check settings.
        
        Returns:
            HealthCheckSettings object containing health check parameters
        """
        return self.health_settings
    
    def get_general_settings(self) -> GeneralSettings:
        """
        Get the general application settings.
        
        Returns:
            GeneralSettings object containing general application parameters
        """
        return self.general_settings
    
    def reload_settings(self) -> None:
        """Reload settings from the BytesIO buffer."""
        # Reset buffer position to the beginning
        self.env_source.seek(0)
        self._load_settings()