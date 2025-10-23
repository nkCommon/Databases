from Database.src.dbbase import DBBase
from Database.src.postgresql import PostgreSQLDatabase
from Database.src.sql_server import MSSQLDatabase
from Database.src.mysql import MySQLDatabase
from Database.src.sqlite import SQLiteDatabase
from Database.src.dbbase import DatabaseType


class DatabaseFactory:
    """
    Factory to create the correct DBBase subclass instance.
    Contains Only static functions
    """

    @staticmethod
    def create(
        db_type: DatabaseType,
        host: str = "",
        database: str = "",
        user: str = "",
        password: str = "",
        port: int | None = None
    ) -> DBBase:
        """
        Create an instance of a database/connection

        Args:
            db_type (DatabaseType): select between supported databases enum { POSTGRESQL, MSSQL, MYSQL, SQLITE }
            host (str, optional): database server eks. db0001.samdrift.dk. Defaults to "".
            database (str, optional): name of database or file path for SQLite. Defaults to "".
            user (str, optional): username. Defaults to "".
            password (str, optional): user password. Defaults to "".
            port (int | None, optional): port number. Defaults to None.

        Raises:
            ValueError: _description_
            ValueError: _description_

        Returns:
            DBBase: instance of the correct database subclass
        """
        if db_type == DatabaseType.POSTGRESQL:
            return PostgreSQLDatabase(host, database, user, password, port or 5432)
        elif db_type == DatabaseType.MSSQL:
            return MSSQLDatabase(host, database, user, password, port or 1433)
        elif db_type == DatabaseType.MYSQL:
            return MySQLDatabase(host, database, user, password, port or 3306)
        elif db_type == DatabaseType.SQLITE:
            if not database:
                raise ValueError("SQLite requires a file path for 'database'")
            return SQLiteDatabase(database)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")