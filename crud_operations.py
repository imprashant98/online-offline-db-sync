import logging
from db_connection import get_connection

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def safe_execute(cursor, sql, params=None):
    try:
        cursor.execute(sql, params or ())
    except Exception as e:
        logging.error(f"Error executing SQL: {sql} | Error: {e}")
        raise

def create_employees_table(db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        sql = """
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            department TEXT,
            position TEXT,
            is_synced BOOLEAN,
            is_active BOOLEAN
        )
        """ if db_type == 'sqlite' else """
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            department VARCHAR(100),
            position VARCHAR(100),
            is_synced BOOLEAN,
            is_active BOOLEAN
        )
        """
        safe_execute(cursor, sql)
        conn.commit()

def store(table_name, data_dict, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        keys = ', '.join(data_dict.keys())
        placeholders = ', '.join(['%s' if db_type == 'postgres' else '?' for _ in data_dict])
        sql = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})"
        safe_execute(cursor, sql, tuple(data_dict.values()))
        conn.commit()

def get_all(table_name, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        sql = f"SELECT * FROM {table_name}"
        safe_execute(cursor, sql)
        rows = cursor.fetchall()
    return rows



def store(table_name, data_dict, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        keys = ', '.join(data_dict.keys())
        placeholders = ', '.join(['%s' if db_type == 'postgres' else '?' for _ in data_dict])
        sql = f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})"
        safe_execute(cursor, sql, tuple(data_dict.values()))
        conn.commit()


def get_all(table_name, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        sql = f"SELECT * FROM {table_name}"
        safe_execute(cursor, sql)
        rows = cursor.fetchall()
    return rows


def get_by_condition(table_name, conditions, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        condition_str = ' AND '.join([f"{key} = %s" if db_type == 'postgres' else f"{key} = ?" for key in conditions])
        sql = f"SELECT * FROM {table_name} WHERE {condition_str}"
        safe_execute(cursor, sql, tuple(conditions.values()))
        rows = cursor.fetchall()
    return rows


def update(table_name, record_id, data_dict, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        updates = ', '.join([f"{key} = %s" if db_type == 'postgres' else f"{key} = ?" for key in data_dict])
        sql = f"UPDATE {table_name} SET {updates} WHERE id = %s" if db_type == 'postgres' else f"UPDATE {table_name} SET {updates} WHERE id = ?"
        safe_execute(cursor, sql, (*data_dict.values(), record_id))
        conn.commit()


def delete(table_name, record_id, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        sql = f"DELETE FROM {table_name} WHERE id = %s" if db_type == 'postgres' else f"DELETE FROM {table_name} WHERE id = ?"
        safe_execute(cursor, sql, (record_id,))
        conn.commit()

def get_paginated(table_name, limit=10, offset=0, db_type='sqlite', db_name='database.db', db_params=None):
    with get_connection(db_type, db_name, db_params) as (conn, cursor):
        sql = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s" if db_type == 'postgres' else f"SELECT * FROM {table_name} LIMIT ? OFFSET ?"
        safe_execute(cursor, sql, (limit, offset))
        rows = cursor.fetchall()
    return rows
