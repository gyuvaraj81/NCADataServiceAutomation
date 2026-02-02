import argparse
import os
import sys
import time
from typing import Optional
from google.cloud import bigquery
import pandas as pd

from config import ConfigManager
from query_loader import load_query_from_file
from query_executor import QueryExecutor
from file_manager import FileManager
from sql_formatter import SQLFormatter
from test_executor import TestExecutor
from logger import get_logger

# Configure stdout encoding for Windows
sys.stdout.reconfigure(encoding='utf-8')

logger = get_logger("RUN_QUERY")
sql_formatter = SQLFormatter()


class QueryRunner:
    """Orchestrates query execution and result handling."""

    def __init__(self, config: dict, client: bigquery.Client):
        self.config = config
        self.client = client
        self.executor = QueryExecutor(client)
        self.test_executor = TestExecutor(client, logger)

    def process_count_results(
        self,
        df: pd.DataFrame,
        failed_sql_file: str,
        sql: str
    ) -> pd.DataFrame:
        """
        Process count query results and detect mismatches.
        
        Args:
            df: Query results
            failed_sql_file: Path to save failed SQL
            sql: Original SQL query
            
        Returns:
            Processed DataFrame
        """
        # Ensure columns exist
        for col in ["sit_count", "prd_count"]:
            if col not in df.columns:
                df[col] = 0

        # Convert to numeric
        df["sit_count"] = pd.to_numeric(df["sit_count"], errors="coerce").fillna(0)
        df["prd_count"] = pd.to_numeric(df["prd_count"], errors="coerce").fillna(0)

        # Calculate diff
        df["diff"] = df["sit_count"] - df["prd_count"]

        # Check for differences
        if df["diff"].ne(0).any():
            failed_dir = os.path.dirname(failed_sql_file)
            FileManager.save_count_mismatch(
                self.config["dataset"],
                sql,
                failed_dir,
                sql_formatter.beautify
            )
            print(f"\n❌ DIFF FOUND\n")
            print("▶ SQL Preview:\n")
            print(sql_formatter.beautify(sql))
            print("-" * 80)
            logger.warning("❌ DIFF FOUND in count query")
        else:
            print("\n✔ All counts matched (diff = 0). Test Passed.")
            logger.info("\n✔ All counts matched (diff = 0). Test Passed.")

        return df

    def run_query(
        self,
        query_name: str,
        tablename: Optional[str] = None
    ) -> None:
        """
        Execute a named query and handle results.
        
        Args:
            query_name: Name of query to run
            tablename: Optional table filter for testcases
        """
        try:
            logger.info(f"\nRunning query: {query_name}")
            print(f"\nRunning query: {query_name}")

            # Load SQL
            sql = load_query_from_file(query_name, self.config)

            # Create output paths
            csv_file, failed_sql_file, result_csv_file, schema_diff_file = \
                FileManager.create_output_paths(
                    self.config["dataset"],
                    query_name,
                    self.config["output_csv"]
                )
            base_name = os.path.splitext(os.path.basename(csv_file))[0]

            start_time = time.time()

            # Preview and execute
            print("\n▶ Generated SQL:\n")
            print(sql_formatter.beautify(sql))
            logger.info(f"▶ Generated SQL:\n{sql_formatter.beautify(sql)}")

            df = self.executor.execute(sql)

            # Handle query-specific logic
            if query_name == "count":
                # Save results for count query
                FileManager.save_dataframe_with_query(
                    df, result_csv_file, sql, sql_formatter.beautify
                )
                self.process_count_results(df, failed_sql_file, sql)
            elif query_name.startswith("except_distinct") and "testcase" in df.columns:
                # Save overall results with all testcases
                FileManager.save_dataframe_with_query(
                    df, result_csv_file, sql, sql_formatter.beautify
                )
                # Then execute filtered testcases (creates _testcase_results.csv if filtered)
                filtered_df = self.test_executor.filter_testcases_by_table(df, tablename)
                if tablename:  # Only create testcase_results if filtering by tablename
                    self.test_executor.execute_except_distinct_testcases(
                        filtered_df, base_name, result_csv_file, failed_sql_file,
                        self.config
                    )
            elif query_name == "schema_compare" and "diff_status" in df.columns:
                # Save full results for schema_compare
                FileManager.save_dataframe_with_query(
                    df, result_csv_file, sql, sql_formatter.beautify
                )
                # Also save only mismatches (if any) to failed_testcases
                mismatches = df[df["diff_status"].isin(["TYPE_MISMATCH", "NULLABILITY_MISMATCH"])]
                if not mismatches.empty:
                    FileManager.save_dataframe_with_query(
                        mismatches, schema_diff_file, sql, sql_formatter.beautify
                    )
                    print(f"\n📁 Schema differences saved: {schema_diff_file}")
                    logger.info(f"📁 Schema differences saved: {schema_diff_file}")
                else:
                    print("\n✔ No schema differences found")
                    logger.info("\n✔ No schema differences found")
            else:
                # Save results for other queries
                FileManager.save_dataframe_with_query(
                    df, result_csv_file, sql, sql_formatter.beautify
                )

            elapsed_time = time.time() - start_time
            msg = f"Finished query: {query_name} in {elapsed_time:.2f}s → {result_csv_file}"
            print(msg)
            logger.info(msg)

        except Exception as e:
            logger.error(f"Error running query {query_name}: {str(e)}")
            print(f"Error running query {query_name}: {str(e)}")
            raise


def discover_sql_files(sql_folder: str = "sqls") -> list:
    """Discover all SQL files in folder."""
    return [
        os.path.splitext(f)[0] for f in os.listdir(sql_folder)
        if f.endswith(".sql")
    ]


if __name__ == "__main__":
    print("Starting query execution framework...\n")
    logger.info("Starting query execution framework...\n")

    parser = argparse.ArgumentParser(
        description="Run BigQuery queries from SQL files"
    )
    parser.add_argument(
        "--query", type=str, default=None,
        help="Query name (e.g., count, except_distinct, schema_compare)"
    )
    parser.add_argument(
        "--tablename", type=str, default=None,
        help="Table filter for testcases (supports * wildcard)"
    )
    args = parser.parse_args()

    try:
        # Load config and initialize client
        config = ConfigManager.load()
        client = bigquery.Client()

        # Determine queries to run
        if args.query:
            queries_to_run = [args.query]
        else:
            queries_to_run = discover_sql_files()

        # Create runner and execute queries
        runner = QueryRunner(config, client)
        for query_name in queries_to_run:
            runner.run_query(query_name, args.tablename)

        logger.info("All queries completed successfully")
        print("\n✅ All queries completed successfully")

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"Fatal error: {str(e)}")
        sys.exit(1)