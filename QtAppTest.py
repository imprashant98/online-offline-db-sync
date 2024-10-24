import sys
import sqlite3
import psycopg2
import logging
from datetime import datetime
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QWidget, QLabel, QPushButton, QLineEdit, QMessageBox
)
from PyQt5.QtCore import QTimer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(asctime)s - %(message)s')

# Connect to local SQLite database
local_conn = sqlite3.connect('employee_tracker.db')
local_cursor = local_conn.cursor()

# Create local tables if not already present
def initialize_local_db():
    local_cursor.execute('''
        CREATE TABLE IF NOT EXISTS clock_in_out (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER,
            clock_in TIMESTAMP,
            clock_out TIMESTAMP,
            synced BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP,
            modified_at TIMESTAMP
        )
    ''')
    local_conn.commit()

initialize_local_db()

# PostgreSQL server connection configuration
SERVER_DB_CONFIG = {
    'dbname': 'employee_tracker',
    'user': 'postgres',
    'password': 'md',
    'host': 'localhost',
    'port': '5432'
}

# Function to create tables in PostgreSQL
def initialize_server_db():
    try:
        server_conn = psycopg2.connect(**SERVER_DB_CONFIG)
        server_cursor = server_conn.cursor()

        # Create the clock_in_out table if it doesn't exist
        server_cursor.execute('''
            CREATE TABLE IF NOT EXISTS clock_in_out (
                id SERIAL PRIMARY KEY,
                employee_id INTEGER,
                clock_in TIMESTAMP,
                clock_out TIMESTAMP,
                created_at TIMESTAMP,
                modified_at TIMESTAMP
            );
        ''')
        server_conn.commit()
        logging.info("Initialized clock_in_out table in PostgreSQL.")

        server_cursor.close()
        server_conn.close()

    except psycopg2.Error as e:
        logging.error(f"Error initializing PostgreSQL tables: {e}")

# Initialize the PostgreSQL tables
initialize_server_db()

# Function to check server connectivity
def is_server_reachable():
    try:
        server_conn = psycopg2.connect(**SERVER_DB_CONFIG)
        server_conn.close()
        return True
    except psycopg2.Error:
        return False

# Function to save data directly to the PostgreSQL server
def save_data_to_server(employee_id, clock_in):
    try:
        server_conn = psycopg2.connect(**SERVER_DB_CONFIG)
        server_cursor = server_conn.cursor()

        server_cursor.execute('''
            INSERT INTO clock_in_out (employee_id, clock_in, created_at, modified_at)
            VALUES (%s, %s, %s, %s)
        ''', (employee_id, clock_in, datetime.now(), datetime.now()))
        
        server_conn.commit()
        logging.info(f"Data saved to PostgreSQL for employee ID {employee_id}")

        server_cursor.close()
        server_conn.close()
        return True

    except psycopg2.Error as e:
        logging.error(f"Failed to save data to server: {e}")
        return False

# Function to save data locally in SQLite
def save_data_locally(employee_id, clock_in):
    try:
        local_cursor.execute('''
            INSERT INTO clock_in_out (employee_id, clock_in, synced, created_at, modified_at)
            VALUES (?, ?, FALSE, ?, ?)
        ''', (employee_id, clock_in, datetime.now(), datetime.now()))
        local_conn.commit()
        logging.info(f"Saved data locally for employee ID {employee_id}")
        return True

    except Exception as e:
        logging.error(f"Failed to save data locally: {e}")
        return False

# Main function to save data based on connectivity
def save_data(employee_id, clock_in):
    if is_server_reachable():
        success = save_data_to_server(employee_id, clock_in)
        if success:
            return True
        else:
            logging.error("Failed to save to server. Attempting to save locally.")
    
    save_data_locally(employee_id, clock_in)
    return True

# Function to sync data from SQLite to PostgreSQL when online
def sync_local_to_server():
    if not is_server_reachable():
        return  # Exit if the server is not reachable

    try:
        # Fetch unsynced data from local SQLite
        local_cursor.execute("SELECT id, employee_id, clock_in, clock_out, created_at, modified_at FROM clock_in_out WHERE synced = FALSE")
        unsynced_data = local_cursor.fetchall()

        if not unsynced_data:
            logging.info("No unsynced data found.")
            return

        server_conn = psycopg2.connect(**SERVER_DB_CONFIG)
        server_cursor = server_conn.cursor()

        # Insert data to PostgreSQL, excluding the ID column (ID will be auto-generated)
        for record in unsynced_data:
            server_cursor.execute('''
                INSERT INTO clock_in_out (employee_id, clock_in, clock_out, created_at, modified_at)
                VALUES (%s, %s, %s, %s, %s)
            ''', (record[1], record[2], record[3], record[4], record[5]))
            
            # Commit after each record is successfully inserted
            server_conn.commit()

            # Delete the synced record from SQLite
            local_cursor.execute("DELETE FROM clock_in_out WHERE id = ?", (record[0],))
            local_conn.commit()
            logging.info(f"Deleted synced record with ID {record[0]} from local SQLite.")

        logging.info("All unsynced data successfully synced to PostgreSQL and deleted locally.")

        server_cursor.close()
        server_conn.close()

    except psycopg2.Error as e:
        logging.error(f"Error syncing local data to server: {e}")


# PyQt5 Application
class EmployeeTrackerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Employee Tracker App')
        self.setGeometry(100, 100, 300, 200)

        # Main layout
        main_layout = QVBoxLayout()

        # Employee ID input
        self.employee_id_input = QLineEdit(self)
        self.employee_id_input.setPlaceholderText('Enter Employee ID')
        main_layout.addWidget(QLabel('Employee ID:'))
        main_layout.addWidget(self.employee_id_input)

        # Clock-in Button
        clock_in_button = QPushButton('Clock In', self)
        clock_in_button.clicked.connect(self.handle_clock_in)
        main_layout.addWidget(clock_in_button)

        # Sync Button
        sync_button = QPushButton('Sync Local Data', self)
        sync_button.clicked.connect(sync_local_to_server)
        main_layout.addWidget(sync_button)

        # Status Label
        self.status_label = QLabel('', self)
        main_layout.addWidget(self.status_label)

        # Set central widget
        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

        # Timer for periodic sync (every 10 seconds)
        self.sync_timer = QTimer(self)
        self.sync_timer.timeout.connect(sync_local_to_server)
        self.sync_timer.start(10000)

    def handle_clock_in(self):
        employee_id = self.employee_id_input.text()

        if not employee_id.isdigit():
            self.show_message('Error', 'Please enter a valid Employee ID.')
            return

        employee_id = int(employee_id)
        clock_in_time = datetime.now()

        # Save data based on server connectivity
        if save_data(employee_id, clock_in_time):
            self.show_message('Success', f'Clock-in saved for Employee ID {employee_id}')
        else:
            self.show_message('Error', 'Failed to save data.')

    def show_message(self, title, message):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle(title)
        msg.setText(message)
        msg.exec_()

# Run the PyQt5 Application
if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = EmployeeTrackerApp()
    ex.show()
    sys.exit(app.exec_())
