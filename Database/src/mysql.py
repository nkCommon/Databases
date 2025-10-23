import mysql.connector
from typing import Any
from Database.src.dbbase import DBBase


class MySQLDatabase(DBBase):
    """MySQL implementation using mysql-connector-python."""

    def __init__(self, host, database, user, password, port=3306):
        super().__init__(host, database, user, password, port)

    def connect(self):
        """Create a MySQL connection."""
        return mysql.connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )

    def select(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute SELECT and return all rows as list of dicts."""
        with self.connect() as conn:
            cur = conn.cursor(dictionary=True)
            cur.execute(query, params)
            return cur.fetchall()

    def select_where(
            self,
            query_or_table: str,
            columns: list[str] | None = None,
            where: str | None = None,
            params: tuple = ()
        ) -> list[dict[str, Any]]:
            """
            Execute a SELECT query.
            - If `columns` or `where` are provided, builds a query automatically.
            - Otherwise, treats the first argument as a raw SQL string.
            """
            with self.connect() as conn:
                cur = conn.cursor(dictionary=True)
                if columns is not None or where is not None:
                    col_str = ", ".join(columns) if columns else "*"
                    query = f"SELECT {col_str} FROM {query_or_table}"
                    if where:
                        query += f" WHERE {where}"
                else:
                    query = query_or_table

                cur.execute(query, params)
                return cur.fetchall()

    def execute(self, query: str, params: tuple = ()) -> None:
        """Execute INSERT/UPDATE/DELETE and commit."""
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
    
    def insert(self, table: str, data: dict[str, Any]) -> None:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(query, values)

    def update(self, table: str, data: dict[str, Any], where: str, params: tuple = ()) -> None:
        set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
        values = tuple(data.values()) + params
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        self.execute(query, values)
        
    def delete(self, table: str, where: str, params: tuple = ()) -> None:
        query = f"DELETE FROM {table} WHERE {where}"
        return self.execute(query, params)