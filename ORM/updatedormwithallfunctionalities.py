import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database Configuration
DB_CONFIG = {
    'local': {
        'name': 'employee_tracker.db'
    },
    'server': {
        'dbname': 'employee_tracker',
        'user': 'postgres',
        'password': 'md',
        'host': 'localhost',
        'port': '5432'
    }
}

# Register custom SQLite adapter for datetime
sqlite3.register_adapter(datetime, lambda ts: ts.isoformat())

# Field Class
class Field:
    def __init__(self, column_type: str, primary_key=False, default=None):
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

# Base ORM Class
class BaseModel:
    table_name: str = None
    columns: Dict[str, Field] = {}

    def __init__(self, **kwargs):
        for column in self.columns:
            setattr(self, column, kwargs.get(column, self.columns[column].default))

    @classmethod
    def _get_local_connection(cls):
        return sqlite3.connect(DB_CONFIG['local']['name'])

    @classmethod
    def _get_server_connection(cls):
        return psycopg2.connect(
            dbname=DB_CONFIG['server']['dbname'],
            user=DB_CONFIG['server']['user'],
            password=DB_CONFIG['server']['password'],
            host=DB_CONFIG['server']['host'],
            port=DB_CONFIG['server']['port']
        )

    @classmethod
    def create_table(cls):
        column_defs = ", ".join(
            [f"{col} {field.column_type}{' PRIMARY KEY' if field.primary_key else ''}"
             for col, field in cls.columns.items()]
        )
        sql = f"CREATE TABLE IF NOT EXISTS {cls.table_name} ({column_defs})"
        
        conn = cls._get_local_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logging.info(f"Table {cls.table_name} created (if not exists).")

    def save(self):
        columns = ", ".join(self.columns.keys())
        placeholders = ", ".join(["?" for _ in self.columns])
        values = tuple(getattr(self, col) for col in self.columns)

        sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        
        conn = self._get_local_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, values)
            conn.commit()
            logging.info(f"Record saved in {self.table_name}.")
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to save record: {e}")
        finally:
            conn.close()

    @classmethod
    def fetch_all(cls) -> List['BaseModel']:
        sql = f"SELECT * FROM {cls.table_name}"
        return cls._execute_fetch(sql)

    @classmethod
    def fetch_by_id(cls, record_id: int) -> Optional['BaseModel']:
        sql = f"SELECT * FROM {cls.table_name} WHERE id = ?"
        records = cls._execute_fetch(sql, (record_id,))
        return records[0] if records else None

    @classmethod
    def search(cls, column: str, value: Any) -> List['BaseModel']:
        sql = f"SELECT * FROM {cls.table_name} WHERE {column} LIKE ?"
        return cls._execute_fetch(sql, (f"%{value}%",))

    @classmethod
    def sort(cls, column: str, ascending=True) -> List['BaseModel']:
        order = 'ASC' if ascending else 'DESC'
        sql = f"SELECT * FROM {cls.table_name} ORDER BY {column} {order}"
        return cls._execute_fetch(sql)

    @classmethod
    def filter_by_date_range(cls, column: str, start_date: datetime, end_date: datetime) -> List['BaseModel']:
        sql = f"SELECT * FROM {cls.table_name} WHERE {column} BETWEEN ? AND ?"
        return cls._execute_fetch(sql, (start_date, end_date))

    @classmethod
    def update(cls, record_id: int, **kwargs) -> bool:
        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        sql = f"UPDATE {cls.table_name} SET {set_clause} WHERE id = ?"
        params = tuple(kwargs.values()) + (record_id,)

        conn = cls._get_local_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
            logging.info(f"Record with ID {record_id} updated in {cls.table_name}.")
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to update record: {e}")
            return False
        finally:
            conn.close()

    @classmethod
    def delete(cls, record_id: int) -> bool:
        sql = f"DELETE FROM {cls.table_name} WHERE id = ?"

        conn = cls._get_local_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (record_id,))
            conn.commit()
            logging.info(f"Record with ID {record_id} deleted from {cls.table_name}.")
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"Failed to delete record: {e}")
            return False
        finally:
            conn.close()

    @classmethod
    def sync_data_to_postgres(cls, batch_size=100):
        if not cls.is_server_reachable():
            logging.warning("Server not reachable, sync aborted.")
            return

        conn_local = cls._get_local_connection()
        cursor_local = conn_local.cursor()

        cursor_local.execute(f"SELECT * FROM {cls.table_name} WHERE synced = 0 LIMIT ?", (batch_size,))
        unsynced_data = cursor_local.fetchall()

        if not unsynced_data:
            logging.info("All data is already migrated to the server.")
            return

        conn_server = cls._get_server_connection()
        cursor_server = conn_server.cursor()

        columns = [col for col in cls.columns if col not in ('id', 'synced')]
        insert_sql = f"INSERT INTO {cls.table_name} ({', '.join(columns)}) VALUES %s"

        try:
            formatted_data = []
            for record in unsynced_data:
                filtered_record = [value for value in record[1:-1] if value is not None]

                if len(filtered_record) != len(columns):
                    logging.info(f"Skipping record due to null values: {record}")
                    continue

                formatted_data.append(tuple(filtered_record))

            if formatted_data:
                execute_values(cursor_server, insert_sql, formatted_data)
                conn_server.commit()

                for record in unsynced_data:
                    if all(value is not None for value in record[1:-1]):
                        cursor_local.execute(f"UPDATE {cls.table_name} SET synced = 1 WHERE id = ?", (record[0],))
                conn_local.commit()

                logging.info(f"Synced {len(formatted_data)} records to server and updated locally.")
            else:
                logging.info("No valid records to sync.")

        except Exception as e:
            conn_server.rollback()
            conn_local.rollback()
            logging.error(f"Failed to sync data to server: {e}")
        finally:
            cursor_server.close()
            conn_server.close()
            cursor_local.close()
            conn_local.close()

    @staticmethod
    def is_server_reachable() -> bool:
        try:
            conn = BaseModel._get_server_connection()
            conn.close()
            return True
        except psycopg2.OperationalError:
            return False

    @classmethod
    def _execute_fetch(cls, sql: str, params: Union[tuple, None] = None) -> List['BaseModel']:
        conn = cls._get_local_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params or ())
            rows = cursor.fetchall()
            records = [cls(**dict(zip(cls.columns.keys(), row))) for row in rows]
            logging.info(f"Fetched {len(records)} records from {cls.table_name}.")
            return records
        except Exception as e:
            logging.error(f"Failed to fetch records: {e}")
            return []
        finally:
            conn.close()

# Example Model Definition
class ClockInOut(BaseModel):
    table_name = 'clock_in_out'
    columns = {
        'id': Field('INTEGER', primary_key=True),
        'employee_id': Field('INTEGER'),
        'clock_in': Field('TIMESTAMP'),
        'clock_out': Field('TIMESTAMP'),
        'synced': Field('BOOLEAN', default=0)
    }

# Initialize tables
ClockInOut.create_table()

# Function to perform all testing
def perform_tests():
    print("\n1. Adding New Records:")
    record1 = ClockInOut(employee_id=1, clock_in=datetime.now(), clock_out=None)
    record1.save()

    record2 = ClockInOut(employee_id=2, clock_in=datetime.now(), clock_out=None)
    record2.save()

    all_records = ClockInOut.fetch_all()
    print("\nAll Records:")
    for record in all_records:
        print(vars(record))

    print("\n2. Searching Records (employee_id=1):")
    searched_records = ClockInOut.search('employee_id', 1)
    for record in searched_records:
        print(vars(record))

    print("\n3. Sorting Records (by clock_in descending):")
    sorted_records = ClockInOut.sort('clock_in', ascending=False)
    for record in sorted_records:
        print(vars(record))

    print("\n4. Filtering by Date Range:")
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now() + timedelta(days=1)
    filtered_records = ClockInOut.filter_by_date_range('clock_in', start_date, end_date)
    for record in filtered_records:
        print(vars(record))

    print("\n5. Updating Record (ID=1):")
    updated = ClockInOut.update(record_id=1, clock_out=datetime.now())
    print("Record Updated:", updated)

    updated_record = ClockInOut.fetch_by_id(1)
    print("Updated Record (ID=1):")
    if updated_record:
        print(vars(updated_record))

    print("\n6. Deleting Record (ID=2):")
    deleted = ClockInOut.delete(record_id=2)
    print("Record Deleted:", deleted)

    remaining_records = ClockInOut.fetch_all()
    print("\nRemaining Records after Deletion:")
    for record in remaining_records:
        print(vars(record))

    print("\n7. Synchronizing Data to PostgreSQL:")
    ClockInOut.sync_data_to_postgres(batch_size=100)

# Run all tests
if __name__ == "__main__":
    perform_tests()
