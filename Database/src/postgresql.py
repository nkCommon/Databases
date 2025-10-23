import psycopg
from psycopg.rows import dict_row
from typing import Any
from Database.src.dbbase import DBBase


class PostgreSQLDatabase(DBBase):
    """PostgreSQL implementation using psycopg[binary]."""

    def __init__(self, host, database, user, password, port=5432):
        """Initialize the PostgreSQL connection.
        Args:
            host: The host of the database.
            database: The name of the database.
            user: The user of the database.
            password: The password of the database.
            port: The port of the database.
        """
        super().__init__(host, database, user, password, port)

    def connect(self):
        """
        Establish a new PostgreSQL connection using psycopg[binary].
        Returns a psycopg.Connection object.
        """
        return psycopg.connect(
            host=self.host,
            dbname=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
            row_factory=dict_row,  # results as dicts instead of tuples
            autocommit=False
        )

    def select(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Run a SELECT query and return results as list of dicts."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
            
    def execute(self, query: str, params: tuple = ()) -> None:
        """Run INSERT, UPDATE, or DELETE queries."""
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                conn.commit()
            return {"success": True, "rows_affected": cur.rowcount, "error": None}            
        except Exception as e:
            return {"success": False, "rows_affected": 0, "error": str(e)}
            
    def insert(self, table: str, data: dict[str, Any]) -> None:
        """Insert a row using a dictionary of column-value pairs."""
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())

        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return self.execute(query, values)

    def update(self, table: str, data: dict[str, Any], where: str, params: tuple = ()) -> None:
        """Update rows with provided data and WHERE condition."""
        set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
        values = tuple(data.values()) + params

        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        return self.execute(query, values)