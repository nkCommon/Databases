from Database.src.dbfactory import DatabaseFactory, DatabaseType
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
