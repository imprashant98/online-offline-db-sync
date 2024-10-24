import os
from dotenv import load_dotenv
from crud_operations import store, get_all, create_employees_table
import argparse

# Load environment variables from the .env file
load_dotenv()

# Set PostgreSQL connection parameters using environment variables
pg_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def main(args):
    # Create the employees table in SQLite
    create_employees_table(db_type='sqlite', db_name='database.db')

    # Create the employees table in PostgreSQL
    create_employees_table(db_type='postgres', db_params=pg_params)

    if args.operation == 'store':
        # Perform the store operation
        employee_data = {
            'first_name': args.first_name,
            'last_name': args.last_name,
            'email': args.email,
            'department': args.department,
            'position': args.position,
            'is_synced': args.is_synced,
            'is_active': args.is_active
        }
        store('employees', employee_data, db_type='sqlite', db_name='database.db')
        print(f"Employee '{args.first_name} {args.last_name}' added to the database.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform CRUD operations on the Employee database.')
    parser.add_argument('operation', choices=['store'], help='CRUD operation to perform')
    parser.add_argument('--first_name', required=True, help='First name of the employee')
    parser.add_argument('--last_name', required=True, help='Last name of the employee')
    parser.add_argument('--email', required=True, help='Email of the employee')
    parser.add_argument('--department', required=True, help='Department of the employee')
    parser.add_argument('--position', required=True, help='Position of the employee')
    parser.add_argument('--is_synced', type=bool, default=False, help='Sync status of the employee')
    parser.add_argument('--is_active', type=bool, default=True, help='Active status of the employee')

    args = parser.parse_args()
    main(args)
    
# python main.py store --first_name "John" --last_name "Doe" --email "john.doe@example.com" --department "IT" --position "Developer" --is_synced False --is_active True
# python main.py get_all
# python main.py get_by_condition --field "department" --value "IT"
# python main.py update --id 1 --first_name "Jane" --last_name "Doe" --department "HR" --position "Manager" --is_synced True --is_active True
# python main.py delete --id 1
