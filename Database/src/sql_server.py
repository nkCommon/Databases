import pytds
from Database.src.dbbase import DBBase
from typing import Any

class MSSQLDatabase(DBBase):
    """Microsoft SQL Server implementation of BaseDatabase (no ODBC)."""

    def __init__(self, host, database, user, password, port=1433):
        super().__init__(host, database, user, password, port)

    def connect(self):
        return pytds.connect(
            server=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
            as_dict=True
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
            """
            Execute a SELECT query.
            - If `columns` or `where` are provided, builds a query automatically.
            - Otherwise, treats the first argument as a raw SQL string.
            """
            with self.connect() as conn:
                with conn.cursor() as cur:
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