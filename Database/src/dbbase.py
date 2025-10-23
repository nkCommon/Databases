from abc import ABC, abstractmethod
from typing import Any, Sequence
from enum import Enum

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
    def select_where(
        self,
        query_or_table: str,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        params: tuple = ()
    ) -> list[dict[str, Any]]:
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