from abc import ABC, abstractmethod
from typing import Any, Sequence
from enum import Enum
from dataclasses import dataclass

@dataclass
class DataFrameInsertResult:
    attempted: int
    succeeded: int
    failed: int
    errors: list[tuple[int, Exception]]  # (ro


class DatabaseType(Enum):
    POSTGRESQL = "postgresql"
    MSSQL = "mssql"
    MYSQL = "mysql"
    SQLITE = "sqlite"

#DBBase is the base class for all database connections.
class DBBase(ABC):
    """Abstract base class for database connections."""

    def __init__(self, host: str, database: str, user: str, password: str, port: int):
        """Initialize the database connection.
        Args:
            host: The host of the database.
            database: The name of the database.
            user: The user of the database.
            password: The password of the database.
            port: The port of the database.
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.conn = None

    @abstractmethod
    def connect(self):
        """Return a live database connection."""
        pass

    @abstractmethod
    def select(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a SELECT query and return rows."""
        pass
    @abstractmethod
    def select_where(self, query_or_table: str, columns: Sequence[str] | None = None, where: str | None = None, params: tuple = ()) -> list[dict[str, Any]]:
        pass
    @abstractmethod
    def execute(self, query: str, params: tuple = ()) -> None:
        """Execute an INSERT, UPDATE, or DELETE query."""
        pass

    @abstractmethod
    def insert(self, table: str, data: dict[str, Any]) -> None:
        """Insert a row into the given table."""
        pass

    @abstractmethod
    def update(self, table: str, data: dict[str, Any], where: str, params: tuple = ()) -> None:
        """Update rows in the given table matching the WHERE condition."""
        pass

    @abstractmethod
    def delete(self, table: str, where: str, params: tuple = ()) -> None:
        """Delete rows in the given table matching the WHERE condition."""
        pass
    
    def __enter__(self):
        """Optional: for use in `with` statements."""
        self.conn = self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Ensure connection is closed."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            



    def insert_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        raise_on_error: bool = True,
    ) -> dict[str, Any]:
        """
        Insert all rows from a pandas DataFrame into the given table.

        Returns a dict with attempted/succeeded/failed and errors.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"insert_dataframe expects a pandas DataFrame, got {type(df)}")

        attempted = 0
        succeeded = 0
        errors: list[tuple[int, Exception]] = []

        for idx, row in df.iterrows():
            attempted += 1
            row_dict = row.to_dict()

            print(f"[DEBUG] Inserting row {idx}: {row_dict}")  # debug

            try:
                # this calls your DB-specific insert()
                self.insert(table, row_dict)
                succeeded += 1
            except Exception as e:
                print(f"[DEBUG] ERROR inserting row {idx} into {table}: {e!r}")
                errors.append((idx, e))
                if raise_on_error:
                    # re-raise so your caller sees the failure
                    raise

        failed = attempted - succeeded
        result = {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "errors": errors,
        }
        print("[DEBUG] insert_dataframe result:", result)
        return result

    # def insert_dataframe(self, table: str, df, batch_size: int = 1000) -> int:
    #     """
    #     Insert all rows from a (pandas) DataFrame into the given table.

    #     Args:
    #         table: Target table name.
    #         df: A pandas.DataFrame (or similar with .to_dict(orient="records")).
    #         batch_size: How many rows to insert per chunk (still per-row insert,
    #                     but lets you hook in batching later if you want).

    #     Returns:
    #         Total number of rows inserted.
    #     """
    #     try:
    #         import pandas as pd  # type: ignore
    #     except ImportError as e:
    #         raise RuntimeError("pandas is required for insert_dataframe") from e

    #     if not isinstance(df, pd.DataFrame):
    #         raise TypeError(f"insert_dataframe expects a pandas DataFrame, got: {type(df)}")

    #     total = 0
    #     # simple chunking to avoid huge loops, but still calls self.insert per row
    #     for start in range(0, len(df), batch_size):
    #         chunk = df.iloc[start:start + batch_size]
    #         records = chunk.to_dict(orient="records")
    #         for row in records:
    #             # row is dict: {column_name: value}
    #             self.insert(table, row)
    #         total += len(records)
    #     return total
