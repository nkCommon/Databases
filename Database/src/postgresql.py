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
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()

    def select_where(
        self,
        query_or_table: str,
        columns: list[str] | None = None,
        where: str | None = None,
        params: tuple = ()
    ) -> list[dict[str, Any]]:
        """Execute SELECT from raw SQL or from table name + filters."""
        with self.connect() as conn:
            with conn.cursor() as cur:
                if columns is not None or where is not None:
                    col_str = ", ".join(columns) if columns else "*"
                    query = f"SELECT {col_str} FROM {query_or_table}"
                    if where:
                        query += f" WHERE {where}"
                else:
                    # treat first argument as a raw SQL query
                    query = query_or_table

                cur.execute(query, params)
                return cur.fetchall()            
            
    def execute(self, query: str, params: tuple = ()) -> None:
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                conn.commit()
            return {"success": True, "rows_affected": cur.rowcount, "error": None}            
        except Exception as e:
            return {"success": False, "rows_affected": 0, "error": str(e)}
            
    def insert(self, table: str, data: dict[str, Any]) -> None:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())

        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        return self.execute(query, values)

    def update(self, table: str, data: dict[str, Any], where: str, params: tuple = ()) -> None:
        set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
        values = tuple(data.values()) + params

        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        return self.execute(query, values)

    def delete(self, table: str, where: str, params: tuple = ()) -> None:
        query = f"DELETE FROM {table} WHERE {where}"
        return self.execute(query, params)
    
    def get_table_schema(self, table: str) -> dict[str, str]:
        schema, table_name = table.split(".", 1)

        query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        ORDER BY ordinal_position
        """

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (schema, table_name))
                rows = cur.fetchall()

        # rows are dicts because of dict_row row_factory
        return {r["column_name"]: r["data_type"] for r in rows}