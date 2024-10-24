import sys
import sqlite3
import psycopg2
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# PostgreSQL connection configuration
POSTGRES_CONFIG = {
    'dbname': 'employee_tracker',
    'user': 'postgres',
    'password': 'md',
    'host': 'localhost',
    'port': '5432'
}

# Connect to PostgreSQL
def connect_postgres():
    try:
        return psycopg2.connect(**POSTGRES_CONFIG)
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None

# Fetch unsynced data from SQLite
def fetch_unsynced_data(db_name, table_name, batch_size=100):
    local_conn = sqlite3.connect(f'{db_name}.db')
    local_cursor = local_conn.cursor()
    
    try:
        local_cursor.execute(f"SELECT * FROM {table_name} WHERE synced = 0 LIMIT ?", (batch_size,))
        unsynced_data = local_cursor.fetchall()
        logging.info(f"Fetched {len(unsynced_data)} unsynced records from '{table_name}'.")
        return unsynced_data
    except Exception as e:
        logging.error(f"Error fetching unsynced data from '{table_name}': {e}")
        return []
    finally:
        local_conn.close()

def sync_data_to_postgres(db_name, table_name, batch_size=100):
    unsynced_data = fetch_unsynced_data(db_name, table_name, batch_size)
    if not unsynced_data:
        logging.info(f"No unsynced data found in '{table_name}'.")
        return

    # Extract column names from the SQLite table
    columns = [desc[1].lower() for desc in sqlite3.connect(f'{db_name}.db').execute(f'PRAGMA table_info({table_name})')]
    if 'id' in columns:
        columns.remove('id')  # Exclude 'id' from column list

    # Prepare data for insertion and convert 'synced' to boolean
    data_to_insert = []
    for row in unsynced_data:
        row = list(row[1:])  # Exclude 'id'
        # Convert the 'synced' field to a boolean
        if isinstance(row[3], int):
            row[3] = bool(row[3])
        data_to_insert.append(tuple(row))

    logging.info(f"Columns for insert: {columns}")
    logging.info(f"Data to insert into PostgreSQL: {data_to_insert}")

    # Connect to PostgreSQL
    conn = connect_postgres()
    if not conn:
        logging.error("Failed to connect to PostgreSQL for syncing.")
        return

    try:
        with conn.cursor() as cursor:
            quoted_columns = ', '.join([f'"{col}"' for col in columns])
            placeholders = ', '.join(['%s' for _ in columns])
            insert_query = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'

            # Insert data into PostgreSQL
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()

            # After successful insertion, delete the synced records
            ids_to_delete = [row[0] for row in unsynced_data]
            delete_synced_data(db_name, table_name, ids_to_delete)
            logging.info(f"Synced and deleted {len(data_to_insert)} records from '{table_name}'.")

    except Exception as e:
        logging.error(f"Error during data sync to PostgreSQL: {e}")
        conn.rollback()
    finally:
        conn.close()


# Delete synced data from SQLite
def delete_synced_data(db_name, table_name, ids_to_delete):
    if not ids_to_delete:
        return  # No records to delete

    local_conn = sqlite3.connect(f'{db_name}.db')
    local_cursor = local_conn.cursor()

    try:
        placeholders = ', '.join(['?' for _ in ids_to_delete])
        delete_query = f"DELETE FROM {table_name} WHERE id IN ({placeholders})"
        local_cursor.execute(delete_query, ids_to_delete)
        local_conn.commit()
        logging.info(f"Deleted {len(ids_to_delete)} synced records from '{table_name}'.")
    except Exception as e:
        logging.error(f"Error deleting synced data from '{table_name}': {e}")
    finally:
        local_conn.close()

# Main function to handle command-line arguments
def main():
    if len(sys.argv) >= 2:
        command = sys.argv[1]

        if command == 'sync' and len(sys.argv) == 4:
            db_name = sys.argv[2]
            table_name = sys.argv[3]
            sync_data_to_postgres(db_name, table_name)

        else:
            print("Usage:")
            print("  python automatedsync.py sync <db_name> <table_name>")
            sys.exit(1)

if __name__ == '__main__':
    main()

# python automatedsync.py sync employee_tracker clock_in_out
