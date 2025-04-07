# Stock Data Pipeline

A data engineering pipeline that fetches stock market data from an external API, stores it in a PostgreSQL database, and computes key financial metrics like volatility and returns. It includes automated scheduling for data ingestion and validation.

## Table of Contents
1. [Setup Instructions](#setup-instructions)
2. [How to Trigger the Data Pipeline Manually](#how-to-trigger-the-data-pipeline-manually)
3. [Database Schemas](#database-schemas)
4. [Example API Requests or Output](#example-api-requests-or-output)

## Setup Instructions

### 1. Prerequisites
- Python 3.12 or higher
- PostgreSQL installed locally
- `pip` (Python package manager)
- `git` (to clone the repository)

### 2. Clone the Repository
```bash
git clone https://github.com/your-username/stock_data_pipeline.git
cd stock_data_pipeline
```

### 3. Set Up the Environment
Create a virtual environment:
```bash
python -m venv .venv
```

Activate the virtual environment:

On Windows:
```bash
.venv\Scripts\activate
```

On macOS/Linux:
```bash
source .venv/bin/activate
```

Install the required dependencies:
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Copy the .env.example file to .env:
```bash
cp .env.example .env
```

Update the .env file with your API key, database credentials, and scheduling configuration.

### 5. Set Up the Database
Ensure PostgreSQL is running locally.

Create a database using the credentials specified in the .env file:
```bash
psql -U postgres -c "CREATE DATABASE stock_pipeline;"
```

## How to Trigger the Data Pipeline Manually

### 1. Run the Scheduler
The scheduler automatically triggers the pipeline at regular intervals based on the configuration in the .env file.

To start the scheduler:
```bash
python -m src.run_scheduler
```

### 2. Trigger the Pipeline Manually
To manually fetch, validate, and process stock data:
```bash
python -m src.main
```

## Database Schemas

### 1. raw_data Table
Stores the raw stock data fetched from the API.

| Column | Data Type | Description |
|--------|-----------|-------------|
| date | DATE | The date of the stock data. |
| ticker | VARCHAR | The stock ticker symbol (e.g., AAPL). |
| open | FLOAT | Opening price of the stock. |
| high | FLOAT | Highest price of the stock. |
| low | FLOAT | Lowest price of the stock. |
| close | FLOAT | Closing price of the stock. |
| volume | INTEGER | Number of shares traded. |
| curr_timestamp | TIMESTAMP | Timestamp when the data was fetched. |

### 2. agg_stock_data Table
Stores aggregated stock data with calculated metrics.

| Column | Data Type | Description |
|--------|-----------|-------------|
| curr_timestamp | TIMESTAMP | Timestamp when the data was processed. |
| date | DATE | The date of the stock data. |
| ticker | VARCHAR | The stock ticker symbol. |
| avg_daily_price | FLOAT | Average daily price of the stock. |
| daily_return | FLOAT | Daily return of the stock. |
| avg_return_7d | FLOAT | 7-day average return of the stock. |
| avg_return_10d | FLOAT | 10-day average return of the stock. |
| price_volatility_7d | FLOAT | 7-day price volatility of the stock. |
| return_volatility_7d | FLOAT | 7-day return volatility of the stock. |
| return_volatility_10d | FLOAT | 10-day return volatility of the stock. |

## Example API Requests or Output

### 1. Example API Request
The pipeline fetches stock data using the Alpha Vantage API. Below is an example request:
```
GET https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey=your_api_key&outputsize=compact
```

### 2. Example Output

**Raw Data (raw_data Table)**

| date | ticker | open | high | low | close | volume | curr_timestamp |
|------|--------|------|------|-----|-------|--------|---------------|
| 2025-04-04 | AAPL | 193.89 | 199.88 | 187.34 | 189.92 | 125910913 | 2025-04-07 13:38:28.41 |

**Aggregated Data (agg_stock_data Table)**

| curr_timestamp | date | ticker | avg_daily_price | daily_return | avg_return_7d | avg_return_10d | price_volatility_7d | return_volatility_7d | return_volatility_10d |
|----------------|------|--------|----------------|-------------|--------------|---------------|-------------------|---------------------|----------------------|
| 2025-04-07 13:38:28.41 | 2025-04-04 | AAPL | 192.75 | 0.0123 | 0.0105 | 0.0098 | 1.23 | 0.98 | 1.12 |

## Notes
- The pipeline is configured to fetch data for the tickers specified in constants.py.
- Ensure the API key and database credentials are correctly set in the .env file before running the pipeline.
- Logs for each component are stored in separate log files (e.g., main.log, scheduler.log, ingestion_stock.log, db_operations.log).
