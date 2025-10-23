import unittest
#from Database.src.dbbase import DBBase
from Database.src.dbfactory import DatabaseFactory, DatabaseType
########################################################################################################################
### Tests for PostgreSQL
########################################################################################################################

class TestPostgreSQL(unittest.TestCase):
    def setUp(self):
        # Setup code: create resources needed for tests
        pass
    def tearDown(self):
        # Teardown code: clean up resources
        # Example: del self.invoice
        db = self._make_db()
        db.execute("DELETE FROM tst.test;")
        
    def _make_db(self):
        db = DatabaseFactory.create(
            db_type=DatabaseType.POSTGRESQL,
            host="localhost",
            database="test",
            user="testuser",
            password="testuser",
            port=5432
        )
        return db
    ########################################################################################################################
    ### Tests        
    ########################################################################################################################
    # *************************************************************************************************************
    def test_construction(self):
        db = self._make_db()
        self.assertIsNotNone(db)
        test = type(db).__name__
        self.assertEqual(test, "PostgreSQLDatabase")
        
    # *************************************************************************************************************
    def test_connect(self):
        db = self._make_db()
        conn = db.connect()
        self.assertIsNotNone(conn)
    # *************************************************************************************************************
    def test_insert(self):
        db = self._make_db()
        timestamp = "2024-01-01"
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('1', 'TestName', 'TestValue', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.insert("tst.test", {
            "id": "2",
            "name": "Name",
            "value": "Value",
            "state": 0,
            "updated": timestamp
        })
        self.assertTrue(result["success"])
    # *************************************************************************************************************
    def test_update(self):
        db = self._make_db()
        timestamp = "2024-01-01 12:00:00"
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('1', 'TestName', 'TestValue', 0, '{timestamp}');")
        self.assertTrue(result["success"])

        result = db.execute(f"UPDATE tst.test set name='U1Name' WHERE id = '1';")
        self.assertTrue(result["success"])


        db.update("tst.test",
                  data={"name": "TName", "value": "UValue"},
                  where="id = %s",
                  params=("1",))
        self.assertTrue(result["success"])

    # *************************************************************************************************************
    def test_delete(self):
        db = self._make_db()
        timestamp = "2024-01-01 12:00:00"
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('1', '1Name', '1Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('2', '2Name', '2Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('3', '3Name', '3Value', 1, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('4', '4Name', '4Value', 2, '{timestamp}');")
        self.assertTrue(result["success"])
        
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (0,))
        self.assertEqual(len(result), 2)
        result = db.execute("DELETE FROM tst.test WHERE state = %s;", (0,))
        self.assertTrue(result["success"])
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (0,))
        self.assertEqual(len(result), 0)
        

        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (1,))
        self.assertEqual(len(result), 1)
        result = db.delete("tst.test", "state = %s", (1,))
        self.assertTrue(result["success"])
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (1,))
        self.assertEqual(len(result), 0)
        
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (2,))
        self.assertEqual(len(result), 1)
        result = db.delete("tst.test", "state = %s", (2,))
        self.assertTrue(result["success"])
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (2,))
        self.assertEqual(len(result), 0)
                
        
    # *************************************************************************************************************
    def test_select(self):
        db = self._make_db()
        timestamp = "2024-01-01 12:00:00"
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('1', '1Name', '1Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('2', '2Name', '2Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('3', '3Name', '3Value', 1, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO tst.test (id, name, value, state, updated) VALUES ('4', '4Name', '4Value', 2, '{timestamp}');")
        self.assertTrue(result["success"])

        result = db.select("SELECT * FROM tst.test;")
        self.assertEqual(len(result), 4)
        
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (0,))
        self.assertEqual(len(result), 2)
        
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (1,))
        self.assertEqual(len(result), 1)
        
        result = db.select("SELECT * FROM tst.test WHERE state = %s;", (2,))
        self.assertEqual(len(result), 1)

    # *************************************************************************************************************

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
### Tests for SQL Server
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

class TesSQLServer(unittest.TestCase):
    def setUp(self):
        # Setup code: create resources needed for tests
        pass
    def tearDown(self):
        # Teardown code: clean up resources
        # Example: del self.invoice
        db = self._make_db()
        db.execute("DELETE dbo.test;")

    def _make_db(self):
        db = DatabaseFactory.create(
            db_type=DatabaseType.MSSQL,
            host="nkSQLTest.samdrift.dk",
            database="test",
            user="testuser",
            password="testuser",
            port=1433
        )
        return db
    ########################################################################################################################
    ### Tests        
    ########################################################################################################################
    # *************************************************************************************************************
    def test_construction(self):
        db = self._make_db()
        self.assertIsNotNone(db)
        test = type(db).__name__
        self.assertEqual(test, "MSSQLDatabase")
    # *************************************************************************************************************
    def test_connect(self):
        db = self._make_db()        
        conn = db.connect()
        self.assertIsNotNone(conn)
    # *************************************************************************************************************
    def test_insert(self):
        db = self._make_db()
        timestamp = "2024-01-01 12:00:00"
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('1', 'TestName', 'TestValue', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        
        result = db.insert("dbo.test", {
            "id": "2",
            "name": "Name",
            "value": "Value",
            "state": 0,
            "updated": timestamp
        })
        self.assertTrue(result["success"])
        
    # *************************************************************************************************************
    def test_update(self):
        db = self._make_db()
        timestamp = "2024-01-01 12:00:00"
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('1', 'TestName', 'TestValue', 0, '{timestamp}');")
        self.assertTrue(result["success"])

        result = db.execute(f"UPDATE dbo.test set name='U1Name' WHERE id = '1';")
        self.assertTrue(result["success"])


        db.update("dbo.test",
                  data={"name": "TName", "value": "UValue"},
                  where="id = %s",
                  params=("1",))
        self.assertTrue(result["success"])
        
        
        
    # *************************************************************************************************************
    def test_delete(self):
        db = self._make_db()
        timestamp = "2024-01-01 12:00:00"
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('1', '1Name', '1Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('2', '2Name', '2Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('3', '3Name', '3Value', 1, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('4', '4Name', '4Value', 2, '{timestamp}');")
        self.assertTrue(result["success"])
        
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (0,))
        self.assertEqual(len(result), 2)
        result = db.execute("DELETE FROM dbo.test WHERE state = %s;", (0,))
        self.assertTrue(result["success"])
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (0,))
        self.assertEqual(len(result), 0)
        

        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (1,))
        self.assertEqual(len(result), 1)
        result = db.delete("dbo.test", "state = %s", (1,))
        self.assertTrue(result["success"])
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (1,))
        self.assertEqual(len(result), 0)
        
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (2,))
        self.assertEqual(len(result), 1)
        result = db.delete("dbo.test", "state = %s", (2,))
        self.assertTrue(result["success"])
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (2,))
        self.assertEqual(len(result), 0)

    # *************************************************************************************************************
    def test_select(self):
        db = self._make_db()
        timestamp = "2024-01-01 12:00:00"
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('1', '1Name', '1Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('2', '2Name', '2Value', 0, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('3', '3Name', '3Value', 1, '{timestamp}');")
        self.assertTrue(result["success"])
        result = db.execute(f"INSERT INTO dbo.test (id, name, value, state, updated) VALUES ('4', '4Name', '4Value', 2, '{timestamp}');")
        self.assertTrue(result["success"])
        
        result = db.select("SELECT * FROM dbo.test;")
        self.assertEqual(len(result), 4)
        
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (0,))
        self.assertEqual(len(result), 2)
        
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (1,))
        self.assertEqual(len(result), 1)
        
        result = db.select("SELECT * FROM dbo.test WHERE state = %s;", (2,))
        self.assertEqual(len(result), 1)
    # *************************************************************************************************************


if __name__ == '__main__':
    unittest.main()