import os
import yaml
from typing import Dict, List, Any
from pathlib import Path


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

        # Validate required keys
        missing_keys = ConfigManager.REQUIRED_KEYS - set(config.keys())
        if missing_keys:
            raise ValueError(f"Missing required config keys: {missing_keys}")

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