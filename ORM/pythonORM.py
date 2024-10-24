import sqlite3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, List
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
        
        conn = cls._get_local_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.close()

        records = [cls(**dict(zip(cls.columns.keys(), row))) for row in rows]
        logging.info(f"Fetched {len(records)} records from {cls.table_name}.")
        return records

    @classmethod
    def sync_data_to_postgres(cls, batch_size=100):
        if not cls.is_server_reachable():
            logging.warning("Server not reachable, sync aborted.")
            return

        conn_local = cls._get_local_connection()
        cursor_local = conn_local.cursor()

        # Fetch unsynced records from SQLite in batches
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
                # Filter out null values
                filtered_record = [value for value in record[1:-1] if value is not None]  # Exclude 'id' and 'synced'
                
                # Skip if any column value is None
                if len(filtered_record) != len(columns):
                    logging.info(f"Skipping record due to null values: {record}")
                    continue

                formatted_data.append(tuple(filtered_record))

            if formatted_data:
                # Use execute_values for bulk insert
                execute_values(cursor_server, insert_sql, formatted_data)
                conn_server.commit()

                # Mark synced records in SQLite
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
