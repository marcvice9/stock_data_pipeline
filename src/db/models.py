from sqlalchemy import create_engine, Column, Integer, String, Float, Date, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables (if needed for your database configuration)
load_dotenv()

# Database connection string
DB_USER = os.getenv("DB_USER", "stockuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "yourpassword")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Define the StockData model
class RawStockData(Base):
    __tablename__ = 'raw_stock'
    
    # Map dictionary fields to columns
    date = Column(Date, nullable=False)
    ticker = Column(String(10), nullable=False)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)

    # Composite primary key: combination of date and ticker
    __table_args__ = (PrimaryKeyConstraint('date', 'ticker', name='pk_raw_stock_date_ticker'),)

    def __repr__(self):
        return f"<RawStockData(ticker='{self.ticker}', date='{self.date}')>"

# Function to initialize the database and create tables
def init_db():
    Base.metadata.create_all(engine)
    print("Database initialized and table created.")