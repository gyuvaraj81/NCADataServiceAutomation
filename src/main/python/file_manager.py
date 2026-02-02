import os
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional
import pandas as pd


class FileManager:
    """Manages file operations and output paths."""

    @staticmethod
    def create_output_paths(
        dataset: str,
        query_name: str,
        output_base: str = "output/"
    ) -> Tuple[str, str, str, str]:
        """
        Create consistent output file paths with timestamp.
        
        Args:
            dataset: Dataset name from config
            query_name: Name of the query
            output_base: Base output directory
            
        Returns:
            Tuple of (csv_file, failed_sql_file, result_csv_file, schema_diff_file)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{dataset}_{query_name}_{timestamp}"

        # Create directories
        failed_dir = os.path.join(output_base, "failed_testcases")
        result_dir = os.path.join(output_base, "testcase_results")
        
        Path(output_base).mkdir(parents=True, exist_ok=True)
        Path(failed_dir).mkdir(parents=True, exist_ok=True)
        Path(result_dir).mkdir(parents=True, exist_ok=True)

        csv_file = os.path.join(output_base, f"{base_filename}.csv")
        failed_sql_file = os.path.join(failed_dir, f"{base_filename}.sql")
        result_csv_file = os.path.join(result_dir, f"{base_filename}.csv")
        schema_diff_file = os.path.join(failed_dir, f"{base_filename}_schema_diff.csv")

        return csv_file, failed_sql_file, result_csv_file, schema_diff_file

    @staticmethod
    def save_dataframe_with_query(
        df: pd.DataFrame,
        output_file: str,
        query: str,
        beautifier=None
    ) -> None:
        """
        Save DataFrame to CSV with query appended.
        
        Args:
            df: DataFrame to save
            output_file: Output file path
            query: SQL query to append
            beautifier: Optional function to format query
        """
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False, encoding="utf-8-sig", na_rep="NULL")
        
        FileManager._append_query_to_file(output_file, query, beautifier)

    @staticmethod
    def save_failed_sql(
        testcase_name: str,
        sql: str,
        failed_dir: str,
        error_message: Optional[str] = None,
        beautifier=None,
        result_df: Optional[pd.DataFrame] = None
    ) -> str:
        """
        Save failed SQL query with error details and data differences.
        
        Args:
            testcase_name: Name of failed testcase
            sql: SQL query that failed
            failed_dir: Directory to save file
            error_message: Optional error message
            beautifier: Optional function to format query
            result_df: Optional DataFrame with difference rows
            
        Returns:
            Path to saved file
        """
        Path(failed_dir).mkdir(parents=True, exist_ok=True)
        file_path = os.path.join(failed_dir, f"{testcase_name}.sql")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("---*** SQL FAILED DURING EXECUTION ***---\n\n")
            formatted_sql = beautifier(sql) if beautifier else sql
            f.write(formatted_sql)

            if error_message:
                f.write("\n\n" + "-" * 59 + "\n")
                f.write("                ---*** ERROR MESSAGE ***---                \n")
                f.write("-" * 59 + "\n")
                f.write(f"{error_message}\n")
                f.write("-" * 59 + "\n")
            
            # Append difference data if provided
            if result_df is not None and not result_df.empty:
                f.write("\n" + "=" * 59 + "\n")
                f.write("             ---*** DIFFERENCE DATA ***---                \n")
                f.write("=" * 59 + "\n\n")
                # Convert DataFrame to string with tabular format
                f.write(result_df.to_string(index=False, max_rows=1000))
                f.write(f"\n\n(Showing up to 1000 rows of {len(result_df)} total differences)\n")
                f.write("=" * 59 + "\n\n")

        print(f"⚠️ Saved failing SQL → {file_path}")
        return file_path
    
    @staticmethod
    def save_count_mismatch(
        dataset: str,
        sql: str,
        failed_dir: str,
        beautifier=None
    ) -> str:
        """
        Save count mismatch SQL with dataset prefix (no timestamp).
        
        Args:
            dataset: Dataset name from config
            sql: SQL query
            failed_dir: Directory to save file
            beautifier: Optional function to format query
            
        Returns:
            Path to saved file
        """
        Path(failed_dir).mkdir(parents=True, exist_ok=True)
        file_path = os.path.join(failed_dir, f"{dataset}_count_mismatch.sql")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("---*** SQL FAILED DURING EXECUTION ***---\n\n")
            formatted_sql = beautifier(sql) if beautifier else sql
            f.write(formatted_sql)
            f.write("\n\n" + "-" * 59 + "\n")
            f.write("                ---*** ERROR MESSAGE ***---                \n")
            f.write("-" * 59 + "\n")
            f.write("Diff detected in count query\n")
            f.write("-" * 59 + "\n\n")

        print(f"⚠️ Saved count mismatch SQL → {file_path}")
        return file_path

    @staticmethod
    def _append_query_to_file(output_file: str, query: str, beautifier=None) -> None:
        """Append query to file with formatting."""
        formatted_query = beautifier(query) if beautifier else query
        
        with open(output_file, "a", encoding="utf-8-sig") as f:
            f.write("\n" + "-" * 59 + "\n")
            f.write("        ---*** Generated Query (Debug Preview) ***---        \n")
            f.write("-" * 59 + "\n\n")
            f.write(formatted_query)
            f.write("\n")