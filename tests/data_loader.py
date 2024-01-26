import csv
import mysql.connector
from mysql.connector import Error

# Function to load a CSV file into a MySQL table
def load_csv_to_mysql(filename, table_name, cursor):
    # Define the path to the CSV file
    csv_file_path = f'../data/{filename}'

    # Open the CSV file
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        # Create a CSV reader object using the csv module
        csv_reader = csv.reader(csv_file)

        # Get the header row
        header_row = next(csv_reader)

        # Loop through the rows in the CSV reader object
        for row in csv_reader:
            # Create a string of placeholders for the INSERT query
            placeholders = ', '.join(['%s'] * len(row))

            # Define the INSERT statement with ON DUPLICATE KEY UPDATE
            update_clause = ', '.join([f"{col} = VALUES({col})" for col in header_row])
            insert_stmt = f"INSERT INTO {table_name} ({', '.join(header_row)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"

            try:
                # Execute the INSERT statement for each row
                cursor.execute(insert_stmt, row)
            except Error as e:
                print(f"Error: {e}")
                break  # If an error occurs, break out of the loop

# Database connection parameters
db_config = {
    'user': 'root',
    'password': 'qazwsxHMH',
    'host': '127.0.0.1',
    'database': 'spotify'
}

# CSV files and corresponding table names
csv_files_tables = {
    'category_data.csv': 'Categories',
    'product_data.csv': 'Products',
    'user_data.csv': 'Users',
    'order_data.csv': 'Orders',
    'order_items_data.csv': 'Order_Items',
    'cart_data.csv': 'Cart',
    'cart_item_data.csv': 'Cart_Items',
    'payment_data.csv': 'Payments',
    'shipping_data.csv': 'Shipping',
    'review_data.csv': 'Reviews',
}

try:
    # Establish a database connection
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Loop through the CSV files and corresponding tables
    for csv_file, table_name in csv_files_tables.items():
        print(f"Loading {csv_file} into {table_name}")
        load_csv_to_mysql(csv_file, table_name, cursor)

    # Commit the transaction
    conn.commit()
    print("Data has been loaded successfully.")

except Error as e:
    print(f"Error connecting to MySQL: {e}")
finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed.")
