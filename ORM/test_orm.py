import unittest
from datetime import datetime
import sqlite3
import psycopg2
from ORM.pythonORM import BaseModel, ClockInOut  # Import from the ORM file

class TestORM(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test database connections for SQLite and PostgreSQL."""
        cls.local_conn = sqlite3.connect('employee_tracker.db')  # Test SQLite DB
        cls.server_conn = psycopg2.connect(
            dbname='employee_tracker',
            user='postgres',
            password='md',
            host='localhost',
            port='5432'
        )

        ClockInOut.create_table()

        # Check if there are any records in the database
        cursor = cls.local_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM clock_in_out")
        record_count = cursor.fetchone()[0]
        cursor.close()

        if record_count == 0:
            print("No records found in the database. Stopping tests.")
            raise unittest.SkipTest("No records found in the database. Tests stopped.")

    @classmethod
    def tearDownClass(cls):
        """Clean up test databases after all tests are done."""
        cls.local_conn.close()
        cls.server_conn.close()

    def setUp(self):
        """Set up individual test case with a fresh connection."""
        self.local_conn = sqlite3.connect('employee_tracker.db')
        self.cursor = self.local_conn.cursor()

    def tearDown(self):
        """Clean up after each test case."""
        self.cursor.execute("DELETE FROM clock_in_out")
        self.local_conn.commit()
        self.local_conn.close()

    def test_create_table(self):
        """Test table creation in SQLite."""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='clock_in_out'")
        result = self.cursor.fetchone()
        self.assertIsNotNone(result, "Table 'clock_in_out' should be created.")

    def test_save_record(self):
        """Test saving a record in SQLite."""
        record = ClockInOut(employee_id=1, clock_in=datetime.now())
        record.save()

        self.cursor.execute("SELECT * FROM clock_in_out WHERE employee_id = 1")
        saved_record = self.cursor.fetchone()
        self.assertIsNotNone(saved_record, "Record should be saved in SQLite.")

    def test_fetch_all(self):
        """Test fetching all records from SQLite."""
        record1 = ClockInOut(employee_id=1, clock_in=datetime.now())
        record2 = ClockInOut(employee_id=2, clock_in=datetime.now())
        record1.save()
        record2.save()

        all_records = ClockInOut.fetch_all()
        self.assertEqual(len(all_records), 2, "There should be 2 records fetched from SQLite.")

    def test_sync_data_to_postgres(self):
        """Test syncing data from SQLite to PostgreSQL."""
        record = ClockInOut(employee_id=3, clock_in=datetime.now())
        record.save()

        ClockInOut.sync_data_to_postgres(batch_size=100)

        with self.server_conn.cursor() as server_cursor:
            server_cursor.execute("SELECT * FROM clock_in_out WHERE employee_id = 3")
            server_record = server_cursor.fetchone()

        self.assertIsNotNone(server_record, "Record should be synced to PostgreSQL.")

    def test_sync_updates_local_records(self):
        """Test that local records are marked as synced after syncing."""
        record = ClockInOut(employee_id=4, clock_in=datetime.now())
        record.save()

        ClockInOut.sync_data_to_postgres(batch_size=100)

        self.cursor.execute("SELECT synced FROM clock_in_out WHERE employee_id = 4")
        synced_status = self.cursor.fetchone()[0]

        self.assertEqual(synced_status, 1, "Record should be marked as synced in SQLite.")

    def test_error_handling_on_save(self):
        """Test error handling when saving a record with missing fields."""
        record = ClockInOut(employee_id=None, clock_in=datetime.now())
        
        with self.assertLogs('root', level='ERROR') as log:
            record.save()

        log_messages = [entry for entry in log.output if "Failed to save record" in entry]
        self.assertGreater(len(log_messages), 0, "Error should be logged when saving fails.")

if __name__ == '__main__':
    unittest.main()
