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
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")



def create_db_engine():
    """
    Creates and returns a SQLAlchemy engine for the PostgreSQL database.
    """
    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def connect_to_db(db_engine):
    """
    Attempts to connect to the database using the provided engine.
    Returns the connection if successful, raises OperationalError if not.
    """
    try:
        with db_engine.connect() as conn:
            print("Successfully connected to the database.")
            return conn
    except OperationalError as e:
        print(f"Error connecting to the database: {e}")
        raise

def retry_connection(max_retries, retry_interval):
    """
    Attempts to connect to the database with retries and a defined interval.
    If successful, returns the connection; otherwise, raises an error after max_retries.
    """
    db_engine = create_db_engine()
    attempt = 0
    
    while attempt < max_retries:
        try:
            return connect_to_db(db_engine)
        except OperationalError:
            attempt += 1
            print(f"Retrying... Attempt {attempt} of {max_retries}.")
            if attempt < max_retries:
                time.sleep(retry_interval)  # Wait before retrying
            else:
                print("Max retry attempts reached. Could not establish connection.")
                raise  # Raise the error if all attempts fail

def init_db(max_retries=5, retry_interval=5):
    """
    Initializes the database by retrying the connection a limited number of times.
    """
    try:
        # Attempt to connect with retry logic
        connection = retry_connection(max_retries, retry_interval)
        return connection  # Return the established connection
    except Exception as e:
        print(f"Failed to initialize the database: {e}")
        return None

def insert_raw_data(stock_raw_dict):
    pass



if __name__ == "__main__":
    db_connection = init_db()