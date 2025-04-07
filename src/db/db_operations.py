from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
import time
import os
from dotenv import load_dotenv

# Load environment variables (if needed for your database configuration)
load_dotenv()

# Database connection string
DB_USER = os.getenv("DB_USER", "stockuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "stockpassword")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")


def create_db_engine():
    """
    Creates and returns a SQLAlchemy engine for the PostgreSQL database.
    """
    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def init_db(max_retries=5, retry_interval=5):
    """
    Initializes the database by retrying the connection a limited number of times.
    Returns the database engine if successful.
    """
    db_engine = create_db_engine()
    attempt = 0

    while attempt < max_retries:
        try:
            # Test the connection to ensure the database is reachable
            with db_engine.connect() as conn:
                print("Successfully connected to the database.")
            return db_engine  # Return the engine, not the connection
        except OperationalError as e:
            attempt += 1
            print(f"Database connection failed (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                print("Max retry attempts reached. Could not establish connection.")
                raise Exception("Failed to connect to the database after multiple retries.")

def insert_raw_data(db_engine, raw_df, table_name='raw_data'):
    """
    Insert raw data from a pandas DataFrame into the database table
    
    db_connection: PostgreSQL connection object
    stock_df: Pandas DataFrame containing raw stock data
    """
    try:
        with db_engine.connect() as conn:  # Create a fresh connection
            raw_df.to_sql(table_name, db_engine, if_exists='replace', index=True)
            print("Raw data successfully inserted!")
    except Exception as e:
        print(f"Error inserting raw data: {e}")
    
def insert_raw_data_with_cdc(db_engine, raw_df, table_name='raw_data'):
    """
    Insert raw data into a temporary staging table and merge it into the main table.

    db_engine: SQLAlchemy engine object
    raw_df: Pandas DataFrame containing raw stock data
    table_name: Name of the main database table
    """
    staging_table_name = f"staging_{table_name}"

    try:
        with db_engine.connect() as conn:
            # Step 1: Check if the main table exists
            inspector = inspect(db_engine)
            if table_name not in inspector.get_table_names():
                print(f"Table '{table_name}' does not exist. Creating it...")
                raw_df.columns = raw_df.columns.str.lower() 
                raw_df.to_sql(table_name, conn, if_exists='replace', index=False)  # Create the table schema
                print(f"Table '{table_name}' created successfully.")
                # Create the unique constraint / primary key to allow for CDC
                pk_query = (f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT unique_date_ticker UNIQUE (date, ticker);
                """)
                conn.execute(text(pk_query))
                print(f"Unique constraint added to main table: {table_name}")

            else:
                # Step 2: As main _table already exists, create the staging table
                print(f"Creating staging table: {staging_table_name}")
                raw_df.columns = raw_df.columns.str.lower() # Ensure column names are lowercase
                raw_df.to_sql(staging_table_name, conn, if_exists='replace', index=False)
                print(f"Data inserted into staging table: {staging_table_name}")
                

                # Step 3: Merge data from the staging table into the main table
                print(f"Merging data into main table: {table_name}")
                merge_query = text(f"""
                INSERT INTO {table_name} (date, ticker, open, high, low, close, volume, curr_timestamp)
                SELECT date, ticker, open, high, low, close, volume, curr_timestamp
                FROM {staging_table_name}
                ON CONFLICT (date, ticker) DO UPDATE
                SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    curr_timestamp = EXCLUDED.curr_timestamp
                WHERE
                    {table_name}.open <> EXCLUDED.open OR
                    {table_name}.high <> EXCLUDED.high OR
                    {table_name}.low <> EXCLUDED.low OR
                    {table_name}.close <> EXCLUDED.close OR
                    {table_name}.volume <> EXCLUDED.volume;
                """)
                conn.execute(merge_query)
                print(f"Data merged into main table: {table_name}")

                # Step 4: Drop the staging table
                # print(f"Dropping staging table: {staging_table_name}")
                # conn.execute(text(f"DROP TABLE IF EXISTS {staging_table_name}"))
                # print(f"Staging table dropped: {staging_table_name}")

    except Exception as e:
        print(f"Error during CDC operation: {e}")

def aggregate_stock_data(db_engine, table_name='agg_stock_data'):
    """
    Creates an aggregated stock data table with calculated metrics.

    This function generates a new table in the database that contains aggregated stock data
    with additional calculated metrics such as daily returns, average daily price, 
    rolling averages, and volatilities. The data is derived from the `raw_data` table.

    Args:
        db_engine (sqlalchemy.engine.Engine): The SQLAlchemy database engine object.
        table_name (str): The name of the table to be created in the database. Defaults to 'agg_stock_data'.

    The function performs the following steps:
        1. Calculates daily returns and average daily price for each stock ticker.
        2. Computes rolling averages and volatilities for daily returns and prices over 7-day and 10-day windows.
        3. Creates a new table in the database with the aggregated data and calculated metrics.

    The resulting table includes the following columns:
        - curr_timestamp: The timestamp when the data was processed.
        - date: The date of the stock data.
        - ticker: The stock ticker symbol.
        - avg_daily_price: The average daily price of the stock.
        - daily_return: The daily return of the stock.
        - avg_return_7d: The 7-day rolling average of daily returns.
        - avg_return_10d: The 10-day rolling average of daily returns.
        - price_volatility_7d: The 7-day rolling standard deviation of average daily prices.
        - return_volatility_7d: The 7-day rolling standard deviation of daily returns.
        - return_volatility_10d: The 10-day rolling standard deviation of daily returns.
    """

    agg_query_str = f"""
        CREATE TABLE {table_name} AS
        WITH with_returns AS (
            SELECT
                *,
                (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) / LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS daily_return,
                (open + high + low + close) / 4 AS avg_daily_price
            FROM raw_data rd
        ),
        rolling_metrics AS (
            SELECT *,
                AVG(daily_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_return_7d,
                AVG(daily_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS avg_return_10d,
                STDDEV(avg_daily_price) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS price_volatility_7d,
                STDDEV(daily_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS return_volatility_7d,
                STDDEV(daily_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS return_volatility_10d
            FROM with_returns
        )
        SELECT
            curr_timestamp,
            date::DATE AS date,
            ticker,
            avg_daily_price,
            daily_return,
            avg_return_7d,
            avg_return_10d,
            price_volatility_7d,
            return_volatility_7d,
            return_volatility_10d
        FROM rolling_metrics
        ORDER BY ticker, date DESC;
    """

    # Execute the query and create the table
    with db_engine.connect() as conn:
        conn.execute(text(agg_query_str))
        conn.commit()

    print(f"Table {table_name} has been created successfully.")


if __name__ == "__main__":
    try:
        db_engine = init_db()
        aggregate_stock_data(db_engine)
    except Exception as e:
        print("Database connection failed. Data not inserted.")
