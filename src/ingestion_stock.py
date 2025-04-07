import requests
from dotenv import load_dotenv
from src.config.constants import TICKERS
import time
import os
import logging


# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("ingestion_stock.log", mode="a")  # Log to a file
    ]
)

# Get the API key from the environment variable
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = os.getenv("ALPHA_VANTAGE_BASE_URL")

if not API_KEY:
    logging.error("API key not found. Please make sure to set the ALPHA_VANTAGE_API_KEY in your .env file.")
    raise ValueError("API key not found. Please make sure to set the ALPHA_VANTAGE_API_KEY in your .env file.")
if not BASE_URL:
    logging.error("BASE_URL not found. Please make sure to set the ALPHA_VANTAGE_BASE_URL in your .env file.")
    raise ValueError("BASE_URL not found. Please make sure to set the ALPHA_VANTAGE_BASE_URL in your .env file.")

# Function to fetch stock data for a specific ticker
def fetch_stock_data(ticker):
    """
    Fetches daily stock data for a specific ticker from the Alpha Vantage API.

    Args:
        ticker (str): The stock ticker symbol.

    Returns:
        list: A list of dictionaries containing stock data for each day.
    """

    logging.info(f"Fetching data for ticker: {ticker}")

    # Define the parameters for the API request
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': ticker,
        'apikey': API_KEY,
        'outputsize': 'compact' # 'compact' returns 100 data points (last 100 days)
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raise an excpeption for HTTP errors

        data = response.json()

        # Parse the data to get the daily time series
        time_series = data.get('Time Series (Daily)', {})

        if time_series:
            
            records = []
            for date, stats in time_series.items():
                record = {
                    "Date": date,
                    "Ticker": str(ticker),
                    "Open": float(stats['1. open']),
                    "High": float(stats['2. high']),
                    "Low": float(stats['3. low']),
                    "Close": float(stats['4. close']),
                    "Volume": int(stats['5. volume'])
                }
                records.append(record)
            
            logging.info(f"Data for {ticker} fetched successfully.")
            return records
        
        else:
            logging.warning(f"No time series data found for ticker: {ticker}.")
            return []

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error for ticker {ticker}: {e}")
        return []

    except requests.exceptions.JSONDecodeError as e:
        logging.error(f"JSON decode error for ticker {ticker}: {e}")
        return []

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for ticker {ticker}: {e}")
        raise Exception(f"Error fetching data for ticker {ticker}: {e}")

if __name__ == "__main__":
    
    all_tickers = []

    for ticker in TICKERS:
        logging.info(f"Fetching data for {ticker}...")
        records=fetch_stock_data(ticker)
        all_tickers.extend(records)  # Collect data for all tickers
        time.sleep(12)  # Sleep for 12 seconds to avoid hitting the API rate limit
    
    logging.info(f"Data fetching completed. Total records fetched: {len(all_tickers)}")