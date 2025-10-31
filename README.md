# Databases
implement database interfaces

## supported database
* SQL Server
* PostgreSQL
* MySql ( not tested )
* SQLite ( not tested )

## Installation

### Forudsætninger

- Python 3.12 eller nyere  
- uv


### Opsætning

1. Klon repository’et:
```bash
git clone https://github.com/nkCommon/Databases.git
cd Databases
uv sync
````
### Brug af biblioteket

#### Standard installation
1. installer whl fil:
```uv
 uv add Databases@git+https://github.com/nkCommon/Databases.git
```

2. importer database factory and types:
```python
from Database.src.dbbase import DatabaseType
from Database.src.dbfactory import DatabaseFactory
```
3. Opret .env fil
```bash
DATABASE_SERVER="localhost"
DATABASE="test"
USER="testuser"
PASSWORD="***********"
```

### Funktioner
+ def select(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
+ def select_where(self, query_or_table: str, columns: Sequence[str] | None = None, where: str | None = None, params: tuple = ()) -> list[dict[str, Any]]:
 
    select from table where.

    **Retur værdi**
    Row(s)

- def execute(self, query: str, params: tuple = ()) -> None:
  
    execute sql statements

    **Retur værdier**
    
    Ingen fejl
    ```bash
    {"success": True, "rows_affected": -1, "error": None}
    ```
    Ved fejl
    ```bash
    {"success": False, "rows_affected": 0, "error": exception} 
    ```

+ def insert(self, table: str, data: dict[str, Any]) -> None:

    Insert row into table
    **Retur værdier**

    << Se execute funktion >>    

+ def update(self, table: str, data: dict[str, Any], where: str, params: tuple = ()) -> None:

    Insert row into table
    **Retur værdier**

    << Se execute funktion >>    

+ def delete(self, table: str, where: str, params: tuple = ()) -> None:

    Insert row into table
    **Retur værdier**

    << Se execute funktion >>    


## Eksempel kode
```python
from Database.src.dbbase import DatabaseType
from Database.src.dbfactory import DatabaseFactory
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    db = DatabaseFactory.create(
            db_type=DatabaseType.POSTGRESQL,
            host=os.getenv("DATABASE_SERVER"),
            database=os.getenv("DATABASE"),
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            port=5432
        )

    ### INSERT INTO SOME TABLE
    timestamp = "2024-01-01 12:00:00"
    result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('1', '1Name', '1Value', 0, '{timestamp}');")
    result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('2', '2Name', '2Value', 0, '{timestamp}');")
    result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('3', '3Name', '3Value', 1, '{timestamp}');")
    result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('4', '4Name', '4Value', 2, '{timestamp}');")

    rows = db.select("SELECT * FROM tst.test;")
    print(f"Total rows in tst.test: {len(rows)}")
    
    rows = db.select("SELECT * FROM tst.test WHERE state = %s;", (0,))
    print(f"Total rows in tst.test: {len(rows)}")
    
    rows = db.select_where(
        query_or_table="tst.test",
        columns=["id", "name"],
        where="state = %s",
        params=(0,)
    )
    print(f"Rows with state=0: {len(rows)}")
    for row in rows:
        print(row["name"])

    ### UPDATE SOME TABLE
    result = db.execute(f"UPDATE tst.test set name='U1Name' WHERE id = '1';")
    print(result["success"])


    db.update("tst.test",
            data={"name": "TName", "value": "UValue"},
            where="id = %s",
            params=("1",))
    print(result["success"])

    ### DELETE FROM SOME TABLE
    result = db.execute(f"DELETE FROM tst.test WHERE id = '4';")
    print(result["success"])
    rows = db.select("SELECT * FROM tst.test;")
    print(f"Total rows in tst.test: {len(rows)}")

    db.execute("DELETE FROM tst.test;")
    rows = db.select("SELECT * FROM tst.test;")
    print(f"Total rows in tst.test: {len(rows)}")

if __name__ == "__main__":
    main()

```





