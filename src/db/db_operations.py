from sqlalchemy import create_engine
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
    



if __name__ == "__main__":
    db_connection = init_db()
