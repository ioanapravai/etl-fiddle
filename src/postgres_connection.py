import pandas as pd
import psycopg2

from clean_and_transform_data import clean_dataframe, transform_dataframe
from extract_data import get_sales_dataframe


def connect_and_create_table():
    print("Connecting to the database...\n")

    db_host = 'sales-mock-database.c36qyqoqe9yz.eu-north-1.rds.amazonaws.com'
    db_user = 'postgres'
    db_password = 'Nalaeugen'

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=db_host,
            database='postgres',
            user=db_user,
            password=db_password
        )

        print("Connected to the database!")

        # Create a cursor object
        cur = conn.cursor()

        # Create a table if it doesn't exist
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS sales (
            order_id INT PRIMARY KEY,
            customer_id INT,
            order_date DATE,
            product VARCHAR(100),
            quantity NUMERIC(10, 2),
            price NUMERIC(10, 2),
            discount NUMERIC(10, 2),
            region VARCHAR(50),
            total_sales NUMERIC(10, 2)
        );
        '''
        cur.execute(create_table_query)
        conn.commit()
        print("Table created successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Database connection closed.")


def insert_dataframe_to_db(df: pd.DataFrame):
    print("Inserting data into the database...\n")

    db_host = 'sales-mock-database.c36qyqoqe9yz.eu-north-1.rds.amazonaws.com'
    db_user = 'postgres'
    db_password = 'Nalaeugen'

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=db_host,
            database='postgres',
            user=db_user,
            password=db_password
        )

        print("Connected to the database!")

        # Create a cursor object
        cur = conn.cursor()

        # Insert data into the sales table
        insert_query = '''
        INSERT INTO sales (order_id, customer_id, order_date, product, quantity, price, discount, region, total_sales)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''

        for index, row in df.iterrows():
            cur.execute(insert_query, (
                row['order_id'], row['customer_id'], row['order_date'], row['product'],
                row['quantity'], row['price'], row['discount'], row['region'], row['total_sales']
            ))

        # Commit the transaction
        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Database connection closed.")


def get_sales_data():
    print("Retrieving sales data from the database...\n")

    db_host = 'sales-mock-database.c36qyqoqe9yz.eu-north-1.rds.amazonaws.com'
    db_user = 'postgres'
    db_password = 'Nalaeugen'

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=db_host,
            database='postgres',
            user=db_user,
            password=db_password
        )

        print("Connected to the database!")

        # Create a cursor object
        cur = conn.cursor()

        # Query the database
        sales_data_query = 'SELECT * FROM sales;'
        cur.execute(sales_data_query)
        data = cur.fetchall()
        print(data)
    except Exception as e:
        print("Error:", e)

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Database connection closed.")

def main():
    df = get_sales_dataframe()
    df = clean_dataframe(df)
    df = transform_dataframe(df)

    connect_and_create_table()
    insert_dataframe_to_db(df)
    get_sales_data()

if __name__ == '__main__':
    main()