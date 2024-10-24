import sqlite3
from datetime import datetime, timedelta

# Database configuration
LOCAL_DB_NAME = 'employee_tracker.db'

def populate_local_db():
    """Populate the local database with sample data."""
    conn = sqlite3.connect(LOCAL_DB_NAME)
    cursor = conn.cursor()

    # Sample data to be inserted
    sample_data = [
        (1, datetime.now(), datetime.now() + timedelta(hours=8), 0),
        (2, datetime.now() - timedelta(hours=1), datetime.now() + timedelta(hours=7), 0),
        (3, datetime.now() - timedelta(days=1), datetime.now() - timedelta(days=1, hours=8), 0),
        (4, datetime.now() - timedelta(hours=2), datetime.now() + timedelta(hours=6), 0),
        (5, datetime.now() - timedelta(days=2), datetime.now() - timedelta(days=2, hours=8), 0)
    ]

    try:
        # Insert sample data into clock_in_out table
        for employee_id, clock_in, clock_out, synced in sample_data:
            cursor.execute("""
                INSERT INTO clock_in_out (employee_id, clock_in, clock_out, synced) 
                VALUES (?, ?, ?, ?)
            """, (employee_id, clock_in, clock_out, synced))

        conn.commit()
        print(f"{len(sample_data)} records have been successfully added to the 'clock_in_out' table.")
    
    except Exception as e:
        conn.rollback()
        print(f"Error while populating the database: {e}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    populate_local_db()
