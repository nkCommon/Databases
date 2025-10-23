import sqlite3
from typing import Any
from Database.src.dbbase import DBBase


class SQLiteDatabase(DBBase):
    """SQLite implementation using the built-in sqlite3 module."""

    def __init__(self, database: str):
        """SQLite only needs a database file path."""
        super().__init__(host="", database=database, user="", password="", port=0)

    def connect(self):
        """Connect to an SQLite database file."""
        conn = sqlite3.connect(self.database)
        conn.row_factory = sqlite3.Row  # return dict-like rows
        return conn
 
    def select(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a SELECT query and return all rows as dicts."""
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]

    def select_where(
        self,
        query_or_table: str,
        columns: list[str] | None = None,
        where: str | None = None,
        params: tuple = ()
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            cur = conn.cursor()
            if columns is not None or where is not None:
                col_str = ", ".join(columns) if columns else "*"
                query = f"SELECT {col_str} FROM {query_or_table}"
                if where:
                    query += f" WHERE {where}"
            else:
                query = query_or_table

            cur.execute(query, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]



    def execute(self, query: str, params: tuple = ()) -> None:
        """Execute INSERT/UPDATE/DELETE."""
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
    
    def insert(self, table: str, data: dict[str, Any]) -> None:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        values = tuple(data.values())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(query, values)

    def update(self, table: str, data: dict[str, Any], where: str, params: tuple = ()) -> None:
        set_clause = ", ".join([f"{col} = ?" for col in data.keys()])
        values = tuple(data.values()) + params
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        self.execute(query, values)
        
    def delete(self, table: str, where: str, params: tuple = ()) -> None:
        query = f"DELETE FROM {table} WHERE {where}"
        return self.execute(query, params)