import sqlite3
import psycopg2
from contextlib import contextmanager

def connect(db_type='sqlite', db_name='database.db', db_params=None):
    if db_type == 'sqlite':
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
    elif db_type == 'postgres':
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
    else:
        raise ValueError("Unsupported database type. Use 'sqlite' or 'postgres'.")
    return conn, cursor

@contextmanager
def get_connection(db_type='sqlite', db_name='database.db', db_params=None):
    conn, cursor = None, None
    try:
        conn, cursor = connect(db_type, db_name, db_params)
        yield conn, cursor
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
