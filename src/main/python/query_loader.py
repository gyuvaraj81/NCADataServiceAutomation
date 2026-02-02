import os
from typing import Dict, Any
from config import ConfigManager


def load_query_from_file(
    query_name: str,
    config: Dict[str, Any],
    sql_folder: str = "sqls"
) -> str:
    """
    Load SQL template and inject configuration parameters.
    
    Args:
        query_name: Name of the query (without .sql extension)
        config: Configuration dictionary
        sql_folder: Folder containing SQL files
        
    Returns:
        Formatted SQL query with parameters substituted
        
    Raises:
        FileNotFoundError: If SQL file not found
    """
    file_path = os.path.join(sql_folder, f"{query_name}.sql")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file '{file_path}' not found")

    with open(file_path, "r", encoding="utf-8") as f:
        sql_template = f.read()

    # Build extended config with SQL-friendly skip_tables
    extended_config = {
        **config,
        "skip_tables_sql": ConfigManager.build_skip_tables_sql(
            config.get("skip_tables", [])
        ),
        "deltaload_tables_sql": ConfigManager.build_deltaload_tables_sql(
            config.get("deltaload_tables", [])
        )
    }

    return sql_template.format(**extended_config)