"""SQL formatting utilities."""


class SQLFormatter:
    """Formats SQL queries for display."""

    @staticmethod
    def beautify(sql: str) -> str:
        """
        Format SQL query for readability.
        
        Args:
            sql: Raw SQL query
            
        Returns:
            Formatted SQL query
        """
        try:
            import sqlparse
            return sqlparse.format(sql, reindent=True, keyword_case="upper")
        except ImportError:
            return sql