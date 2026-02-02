import os
from typing import Dict, List, Any, Optional
import pandas as pd
from google.cloud import bigquery
import google.api_core.exceptions
from spinner import Spinner
from query_executor import QueryExecutor
from file_manager import FileManager
from sql_formatter import SQLFormatter
from logger import get_logger


class TestExecutor:
    """Executes database validation test cases."""

    def __init__(self, client: bigquery.Client, logger=None):
        self.executor = QueryExecutor(client)
        self.logger = logger or get_logger("TestExecutor")
        self.sql_formatter = SQLFormatter()

    def filter_testcases_by_table(
        self,
        df: pd.DataFrame,
        table_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Filter testcases by table name with wildcard support.
        
        Args:
            df: DataFrame with testcases
            table_name: Table name to filter (supports * for prefix match)
            
        Returns:
            Filtered DataFrame
            
        Raises:
            ValueError: If no matching tables found
        """
        if not table_name:
            return df

        table_name = table_name.strip().lower()
        
        if "," in table_name:
            tables = [t.strip() for t in table_name.split(",") if t.strip()]
            filtered_df = df[df["table_name"].fillna("").str.lower().isin(tables)]
            filter_type = f"tables in {tables}"
        elif table_name.endswith("*"):
            prefix = table_name[:-1].lower()
            filtered_df = df[df["table_name"].str.lower().str.startswith(prefix)]
            filter_type = f"tables starting with '{prefix}'"
        else:
            filtered_df = df[df["table_name"].str.lower() == table_name.lower()]
            filter_type = f"table '{table_name}'"

        if filtered_df.empty:
            raise ValueError(f"❌ No testcases found matching {filter_type}.")

        msg = f"✅ Running testcases for {filter_type}"
        print(msg)
        self.logger.info(msg)
        
        return filtered_df

    def execute_except_distinct_testcases(
        self,
        df: pd.DataFrame,
        base_name: str,
        result_csv_file: str,
        failed_sql_file: str,
        config: Dict[str, Any],
        timeout_sec: int = 60
    ) -> None:
        """
        Execute EXCEPT DISTINCT validation testcases.
        
        Args:
            df: DataFrame with testcases
            base_name: Base filename for results
            result_csv_file: Output CSV file path
            failed_sql_file: Failed SQL file path
            config: Configuration dictionary
            timeout_sec: Query timeout in seconds
        """
        results = []
        success_queries = []
        results_dir = os.path.dirname(result_csv_file)
        failed_dir = os.path.dirname(failed_sql_file)
        combined_file = os.path.join(results_dir, f"{base_name}_filtered_tables_result.csv")
        
        skip_list = [tbl.lower() for tbl in config.get("skip_tables", [])]

        print("\nExecuting EXCEPT DISTINCT testcases...\n")
        self.logger.info("Executing EXCEPT DISTINCT testcases...\n")

        for idx, row in df.iterrows():
            scenario = row.get("scenario", "")
            table_name = row.get("table_name", "")
            testcase_sql = row.get("testcase", "")

            if table_name.lower() in skip_list:
                print(f"⚠️ Skipping table (configured skip): {table_name}")
                self.logger.info(f"⚠️ Skipping table (configured skip): {table_name}")
                continue

            self._execute_single_testcase(
                scenario, table_name, testcase_sql,
                results, success_queries,
                failed_dir, timeout_sec
            )

        # Save results
        self._save_testcase_results(
            results, success_queries,
            combined_file, result_csv_file
        )

    def _execute_single_testcase(
        self,
        scenario: str,
        table_name: str,
        testcase_sql: str,
        results: List,
        success_queries: List,
        failed_dir: str,
        timeout_sec: int
    ) -> None:
        """Execute a single testcase and record results."""
        print(f"\n▶▶▶ Running testcase [{scenario}] for table → {table_name} ▶▶▶")
        print("\n▶ Generated SQL:\n")
        print(self.sql_formatter.beautify(testcase_sql))
        print("-" * 80)

        try:
            result_df = self.executor.execute(testcase_sql, timeout_sec)
            diff_count = len(result_df)

            if diff_count > 0:
                FileManager.save_failed_sql(
                    f"{scenario}_{table_name}",
                    testcase_sql,
                    failed_dir,
                    f"{diff_count} differing row(s) found",
                    self.sql_formatter.beautify,
                    result_df
                )
                
                status = "YES"
                print(f"❌ Testcase failed → {diff_count} differing row(s) found")
                self.logger.info(f"❌ Testcase failed → {diff_count} differing row(s) found")
            else:
                status = "NO"
                print(f"✔ Testcase passed → no differences found")
                self.logger.info(f"✔ Testcase passed → no differences found")

        except google.api_core.exceptions.GoogleAPICallError as e:
            FileManager.save_failed_sql(
                f"{scenario}_{table_name}",
                testcase_sql,
                failed_dir,
                str(e),
                self.sql_formatter.beautify
            )
            status = "TIMEOUT"
            diff_count = -1
            print(f"⏳ Timeout or API error → testcase marked failed")
            self.logger.warning(f"⏳ Timeout or API error: {str(e)}")

        except Exception as e:
            FileManager.save_failed_sql(
                f"{scenario}_{table_name}",
                testcase_sql,
                failed_dir,
                str(e),
                self.sql_formatter.beautify
            )
            status = "ERROR"
            diff_count = -1
            print(f"❌ SQL execution failed → testcase marked failed")
            self.logger.error(f"❌ SQL execution failed: {str(e)}")

        results.append({
            "scenario": scenario,
            "table_name": table_name,
            "diff_count": diff_count,
            "has_diff": status
        })
        success_queries.append(testcase_sql)

    def _save_testcase_results(
        self,
        results: List,
        success_queries: List,
        combined_file: str,
        result_csv_file: str
    ) -> None:
        """Save testcase results to CSV with filtered dataset naming."""
        results_df = pd.DataFrame(results)
        results_df.to_csv(combined_file, index=False, encoding="utf-8-sig")

        # Append queries
        with open(combined_file, "a", encoding="utf-8-sig") as f:
            f.write("\n" + "-" * 59 + "\n")
            f.write("               ---*** Generated Query(ies) ***---        \n")
            f.write("-" * 59 + "\n")
            f.write("\n".join(success_queries))
            f.write("\n")

        print(f"\n📁 Filtered dataset results saved: {combined_file}")
        self.logger.info(f"\n📁 Filtered dataset results saved: {combined_file}")