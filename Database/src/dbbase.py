from abc import ABC, abstractmethod
from typing import Any, Sequence
from enum import Enum
from dataclasses import dataclass
import math
from datetime import datetime, date

import pandas as pd
import hashlib
import pandas as pd

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
    @abstractmethod
    def get_table_schema(self, table: str) -> dict[str, str]:
        """
        Returns: {column_name: data_type}
        Example: {"timeofcreation": "timestamp", "accountnumber": "integer"}
        """
        pass
    ### Private helper methods ###
    def __enter__(self):
        """Optional: for use in `with` statements."""
        self.conn = self.connect()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        """Ensure connection is closed."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
    ## Utility methods ##
    def normalize_value(self, value: Any, pg_type: str) -> Any:
        if value is None:
            return None

        # pandas NaN → SQL NULL
        if isinstance(value, float) and math.isnan(value):
            return None

        t = pg_type.lower()

        if "timestamp" in t:
            if isinstance(value, (int, str)):
                s = str(value)
                if len(s) == 10 and s.isdigit():  # YYMMDDHHMM
                    return datetime(
                        2000 + int(s[:2]),
                        int(s[2:4]),
                        int(s[4:6]),
                        int(s[6:8]),
                        int(s[8:10]),
                    )
            if isinstance(value, datetime):
                return value

        if t == "date":
            if isinstance(value, str) and len(value) == 6:
                return date(
                    2000 + int(value[4:6]),
                    int(value[2:4]),
                    int(value[0:2]),
                )

        if "int" in t:
            return int(value)

        if "double" in t or "numeric" in t:
            if isinstance(value, str) and value.strip() == "":
                return float('0')
            return float(value)

        # varchar / text / default
        return str(value)    

    def parse_timeofcreation(self,v) -> datetime | None:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None

        # Expect YYMMDDHHMM (10 digits)
        # e.g. 2512090506 -> 2025-12-09 05:06
        if len(s) == 10 and s.isdigit():
            yy = int(s[0:2])
            year = 2000 + yy
            month = int(s[2:4])
            day = int(s[4:6])
            hour = int(s[6:8])
            minute = int(s[8:10])
            return datetime(year, month, day, hour, minute)

        raise ValueError(f"Unsupported TimeOfCreation format: {v!r}")

    def insert_dataframe(self, table: str, df: pd.DataFrame) -> dict[str, Any]:

        schema = self.get_table_schema(table)
        attempted = 0
        succeeded = 0
        errors: list[tuple[int, Exception]] = []
        idx=1
        for _, row in df.iterrows():
            attempted += 1
            raw = row.to_dict()
            cleaned = {}

            for col, col_type in schema.items():
                if col in raw:
                    cleaned_value = self.normalize_value(raw[col], col_type)
                    cleaned[col] = cleaned_value

            insert_result = self.insert(table, cleaned)
            if insert_result.get("success"):
                succeeded += 1
            else:
                errors.append((idx, Exception(insert_result.get("error"))))
            idx +=1

        failed = attempted - succeeded
        result = {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "errors": errors,
        }
        return result

    # def get_existing_hashes(self, table: str, hashes: list[str]) -> set[str]:
    #     if not hashes:
    #         return set()


    #     # chunk to avoid too many parameters
    #     out: set[str] = set()
    #     chunk_size = 500
    #     for i in range(0, len(hashes), chunk_size):
    #         chunk = hashes[i:i+chunk_size]

    #         placeholders = ",".join(["%s"] * len(chunk))  # change to "?" or ":p1" based on your driver
    #         sql = f"SELECT row_hash FROM {table} WHERE row_hash IN ({placeholders})"

    #         rows = self.select(sql, chunk)  # you need a method that returns rows
    #         out.update(r["row_hash"] if isinstance(r, dict) else r[0] for r in rows)

    #     return out
    





    # def _stable_row_hash(self, values: list[Any]) -> str:
    #     parts = []
    #     for v in values:
    #         if v is None or (isinstance(v, float) and pd.isna(v)):
    #             parts.append("<NULL>")
    #         else:
    #             parts.append(str(v))
    #     return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


    # def _df_with_hash(
    #     self,
    #     table: str,
    #     df: pd.DataFrame,
    #     exclude_columns: list[str] | None = None,
    #     keep_non_schema_columns: bool = False,
    # ) -> pd.DataFrame:
        
    #     if "row_hash" not in exclude_columns:
    #         exclude_columns.append("row_hash")
        
        
    #     schema = self.get_table_schema(table)
    #     exclude_columns = exclude_columns or []

    #     schema_cols = list(schema.keys())              # all cols we want in output
    #     hash_cols = [c for c in schema_cols if c not in exclude_columns]  # cols used for hash

    #     # Normalize into ALL schema columns (even excluded ones)
    #     normalized_rows = []
    #     for _, row in df.iterrows():
    #         raw = row.to_dict()
    #         cleaned = {}
    #         for col, col_type in schema.items():
    #             cleaned[col] = self.normalize_value(raw.get(col), col_type)
    #         normalized_rows.append(cleaned)

    #     cleaned_df = pd.DataFrame(normalized_rows, columns=schema_cols)

    #     # Optionally keep extra columns from original df (not in schema)
    #     if keep_non_schema_columns:
    #         extra_cols = [c for c in df.columns if c not in cleaned_df.columns]
    #         if extra_cols:
    #             cleaned_df = pd.concat([cleaned_df, df[extra_cols].reset_index(drop=True)], axis=1)

    #     # Hash uses ONLY hash_cols, but excluded cols still remain in cleaned_df
    #     try:
    #         # Hash uses ONLY hash_cols, but excluded cols still remain in cleaned_df
    #         cleaned_df["row_hash"] = cleaned_df.apply(
    #             lambda r: self._stable_row_hash([r[c] for c in hash_cols]),
    #             axis=1,
    #         )
    #     except Exception as e:
    #         print(f"Error hashing rows: {e}")
    #         print(cleaned_df)
    #         print(hash_cols)
    #         raise e

    #     return cleaned_df



















#     def insert_dataframe(
#         self,
#         table: str,
#         df: pd.DataFrame,
#         raise_on_error: bool = True,
#     ) -> dict[str, Any]:
#         """
#         Insert all rows from a pandas DataFrame into the given table.

#         Returns a dict with attempted/succeeded/failed and errors.
#         """
#         if not isinstance(df, pd.DataFrame):
#             raise TypeError(f"insert_dataframe expects a pandas DataFrame, got {type(df)}")

#         attempted = 0
#         succeeded = 0
#         errors: list[tuple[int, Exception]] = []

#         for idx, row in df.iterrows():
#             attempted += 1
#             row_dict = row.to_dict()

#             # print(f"[DEBUG] Inserting row {idx}: {row_dict}")  # debug

#             # this calls your DB-specific insert()
#             insert_result = self.insert(table, row_dict)
#             if insert_result.get("success"):
#                 succeeded += 1
#             else:
#                 errors.append((idx, Exception(insert_result.get("error"))))
# #                raise Exception(insert_result.get("error"))

#         failed = attempted - succeeded
#         result = {
#             "attempted": attempted,
#             "succeeded": succeeded,
#             "failed": failed,
#             "errors": errors,
#         }
#         # print("[DEBUG] insert_dataframe result:", result)
        
#         return result




