import os
import yaml
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime, timedelta


class ConfigManager:
    """Manages configuration loading and validation."""
    
    REQUIRED_KEYS = {
        "project", "project_nq", "dataset", "dataset_nq",
        "startdate", "enddate", "startdate_nq", "enddate_nq"
    }
    OPTIONAL_KEYS = {
        "output_csv": "output/",
        "skip_tables": [],
        "deltaload_tables": []
    }
    
    # Default values when config is empty
    DEFAULT_VALUES = {
        "project": "ncau-data-newsquery-prd",
        "project_nq": "ncau-data-newsquery-sit",
        "dataset": "sdm_think",
        "dataset_nq": "sdm_think"
    }

    @staticmethod
    def _get_default_dates() -> Dict[str, str]:
        """Calculate default dates based on current date."""
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        
        return {
            "startdate": yesterday.strftime("%Y-%m-%d"),      # CURRENT_DATE - 1
            "enddate": yesterday.strftime("%Y-%m-%d"),        # CURRENT_DATE - 1
            "startdate_nq": today.strftime("%Y-%m-%d"),       # CURRENT_DATE
            "enddate_nq": today.strftime("%Y-%m-%d")          # CURRENT_DATE
        }

    @staticmethod
    def load(config_file: str = "config/config.yaml") -> Dict[str, Any]:
        """
        Load and validate configuration from YAML file.
        
        Args:
            config_file: Path to YAML config file
            
        Returns:
            Validated configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If required keys are missing
        """
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file '{config_file}' not found")

        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        # Get default dates
        default_dates = ConfigManager._get_default_dates()
        
        # Apply defaults for empty or missing values
        for key in ConfigManager.REQUIRED_KEYS:
            value = config.get(key)
            
            # Check if value is None or empty string
            if value is None or (isinstance(value, str) and value.strip() == ""):
                # Apply default value
                if key in ConfigManager.DEFAULT_VALUES:
                    config[key] = ConfigManager.DEFAULT_VALUES[key]
                    print(f"ℹ️  Using default for '{key}': {config[key]}")
                elif key in default_dates:
                    config[key] = default_dates[key]
                    print(f"ℹ️  Using default for '{key}': {config[key]}")
                else:
                    raise ValueError(f"No default value available for required key: {key}")
        
        # Add optional keys with defaults
        for key, default_value in ConfigManager.OPTIONAL_KEYS.items():
            config.setdefault(key, default_value)

        # Ensure output directories exist
        Path(config["output_csv"]).mkdir(parents=True, exist_ok=True)

        return config

    @staticmethod
    def build_skip_tables_sql(skip_tables: List[str]) -> str:
        """Convert skip_tables list to SQL-friendly format."""
        if not skip_tables:
            return ""
        return ", ".join(f"'{table}'" for table in skip_tables)

    @staticmethod
    def build_deltaload_tables_sql(deltaload_tables: List[str]) -> str:
        """Convert deltaload_tables list to SQL-friendly format."""
        if not deltaload_tables:
            return ""
        return ", ".join(f"'{table}'" for table in deltaload_tables)