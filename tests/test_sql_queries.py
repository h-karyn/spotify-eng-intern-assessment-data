import unittest
from _pydecimal import Decimal

# import psycopg2  # Replace with appropriate database connector based on your database
import mysql.connector

from tests.data_loader import csv_files_tables, load_csv_to_mysql


class TestSQLQueries(unittest.TestCase):

    def setUp(self):
        # Establish a connection to your test database
        self.conn = mysql.connector.connect(
            database='spotify',
            user='root',
            password='qazwsxHMH',
            host='127.0.0.1',
            port='3306'
        )
        self.cur = self.conn.cursor()

        # Load data from CSV files into the database
        for csv_file, table_name in csv_files_tables.items():
            load_csv_to_mysql(csv_file, table_name, self.cur)

        self.conn.commit()

    def tearDown(self):
        # Close the database connection
        self.cur.close()
        self.conn.close()

    def test_task1(self):
        # Open task1.sql and read all queries
        with open('../sql/task1.sql', 'r') as file:
            sql_queries = file.read().split(
                ';')

        # Remove any empty queries after splitting
        sql_queries = [query.strip() for query in sql_queries if query.strip()]

        expected_result_1 = [(15, 'Mountain Bike',
                              'Conquer the trails with this high-performance mountain bike.',
                              1000.00),
                             (16, 'Tennis Racket',
                              'Take your tennis game to the next level with this professional-grade racket.',
                              54.00)]
        expected_result_2 = [
            (1, 'johndoe', 1),
            (2, 'janesmith', 1),
            (3, 'maryjones', 1),
            (4, 'robertbrown', 1),
            (5, 'sarahwilson', 1),
            (6, 'michaellee', 1),
            (7, 'lisawilliams', 1),
            (8, 'chrisharris', 1),
            (9, 'emilythompson', 1),
            (10, 'davidmartinez', 1),
            (11, 'amandajohnson', 1),
            (12, 'jasonrodriguez', 1),
            (13, 'ashleytaylor', 1),
            (14, 'matthewthomas', 1),
            (15, 'sophiawalker', 1),
            (16, 'jacobanderson', 1),
            (17, 'olivialopez', 1),
            (18, 'ethanmiller', 1),
            (19, 'emilygonzalez', 1),
            (20, 'williamhernandez', 1),
            (21, 'sophiawright', 1),
            (22, 'alexanderhill', 1),
            (23, 'madisonmoore', 1),
            (24, 'jamesrogers', 1),
            (25, 'emilyward', 1),
            (26, 'benjamincarter', 1),
            (27, 'gracestewart', 1),
            (28, 'danielturner', 1),
            (29, 'elliecollins', 1),
            (30, 'williamwood', 1)
        ]
        expected_result_3 = [
            (1, 'Smartphone X', 5.0000),
            (2, 'Wireless Headphones', 4.0000),
            (3, 'Laptop Pro', 3.0000),
            (4, 'Smart TV', 5.0000),
            (5, 'Running Shoes', 2.0000),
            (6, 'Designer Dress', 4.0000),
            (7, 'Coffee Maker', 5.0000),
            (8, 'Toaster Oven', 3.0000),
            (9, 'Action Camera', 4.0000),
            (10, 'Board Game Collection', 1.0000),
            (11, 'Yoga Mat', 5.0000),
            (12, 'Skincare Set', 4.0000),
            (13, 'Vitamin C Supplement', 2.0000),
            (14, 'Weighted Blanket', 3.0000),
            (15, 'Mountain Bike', 5.0000),
            (16, 'Tennis Racket', 4.0000)
        ]
        expected_result_4 = [
            (12, 'jasonrodriguez', 160.00),
            (4, 'robertbrown', 155.00),
            (24, 'jamesrogers', 150.00),
            (8, 'chrisharris', 150.00),
            (17, 'olivialopez', 145.00)
        ]

        # Define expected results for each query in task1.sql
        expected_results = [
            expected_result_1,
            expected_result_2,
            expected_result_3,
            expected_result_4
        ]

        # Execute each query and compare the result with the expected outcome
        for i, query in enumerate(sql_queries):
            with self.subTest(query=i):
                self.cur.execute(query)
                result = self.cur.fetchall()
                self.assertEqual(result, expected_results[i],
                                 f"Query {i + 1} in task1.sql output doesn't match expected result.")

    def test_task2(self):
        with open('../sql/task2.sql', 'r') as file:
            sql_queries = file.read().split(';')

        # Remove any empty queries after splitting
        sql_queries = [query.strip() for query in sql_queries if query.strip()]

        expected_result_5 = [(1, 'Smartphone X', 5.0000),
                             (4, 'Smart TV', 5.0000),
                             (7, 'Coffee Maker', 5.0000),
                             (11, 'Yoga Mat', 5.0000),
                             (15, 'Mountain Bike', 5.0000)]

        expected_result_6 = []

        expected_result_7 = []

        expected_result_8 = []

        # Define the expected outcomes for Task 2 and compare
        expected_results = [
            expected_result_5,
            expected_result_6,
            expected_result_7,
            expected_result_8
        ]

        # Execute each query and compare the result with the expected outcome
        for i, query in enumerate(sql_queries):
            with self.subTest(query=i):
                self.cur.execute(query)
                result = self.cur.fetchall()
                self.assertEqual(result, expected_results[i],
                                 f"Query {i + 5} in task2.sql output doesn't match expected result.")

    # Add more test methods for additional SQL tasks
    def test_task3(self):
        with open('../sql/task3.sql', 'r') as file:
            sql_queries = file.read().split(';')

        # Remove any empty queries after splitting
        sql_queries = [query.strip() for query in sql_queries if query.strip()]

        expected_result_9 = [(8, 'Sports & Outdoors', 155.00),
                             (4, 'Home & Kitchen', 145.00),
                             (1, 'Electronics', 125.00)]

        expected_result_10 = [(5, 'sarahwilson')]

        expected_result_11 = [(1, 'Smartphone X', 1, 500.00),
                              (3, 'Laptop Pro', 2, 1200.00),
                              (6, 'Designer Dress', 3, 300.00),
                              (7, 'Coffee Maker', 4, 80.00),
                              (9, 'Action Camera', 5, 200.00),
                              (12, 'Skincare Set', 6, 150.00),
                              (14, 'Weighted Blanket', 7, 100.00),
                              (15, 'Mountain Bike', 8, 1000.00)]

        expected_result_12 = []

        expected_results = [
            expected_result_9,
            expected_result_10,
            expected_result_11,
            expected_result_12
        ]

        # Execute each query and compare the result with the expected outcome
        for i, query in enumerate(sql_queries):
            with self.subTest(query=i):
                self.cur.execute(query)
                result = self.cur.fetchall()
                self.assertEqual(result, expected_results[i],
                                 f"Query {i + 9} in task3.sql output doesn't match expected result.")


if __name__ == '__main__':
    unittest.main()
