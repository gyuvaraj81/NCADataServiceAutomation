import pandas as pd
from google.cloud import bigquery
import google.api_core.exceptions
from spinner import Spinner


class QueryExecutor:
    """Executes BigQuery queries with spinner feedback."""

    def __init__(self, client: bigquery.Client):
        """
        Initialize query executor.
        
        Args:
            client: BigQuery client instance
        """
        self.client = client

    def execute(self, query: str, timeout_sec: int = 300) -> pd.DataFrame:
        """
        Execute a BigQuery query with timeout.
        
        Args:
            query: SQL query to execute
            timeout_sec: Timeout in seconds
            
        Returns:
            Query results as DataFrame
            
        Raises:
            google.api_core.exceptions.GoogleAPICallError: On API errors
            Exception: On other execution errors
        """
        spinner = Spinner("Executing query")
        spinner.start()
        try:
            df = self.client.query(query).result(timeout=timeout_sec).to_dataframe()
            return df
        finally:
            spinner.stop()