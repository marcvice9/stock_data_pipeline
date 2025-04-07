import schedule
import time
import os
import logging
from dotenv import load_dotenv
from src.main import fetch_and_process_data

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scheduler.log", mode="a")
    ]
)

def start_scheduler():
    """
    Starts the scheduler based on environment variables.
    """
    # Fetch the scheduling interval and unit from environment variables
    schedule_interval = int(os.getenv("SCHEDULE_INTERVAL", 24))  # Default to 24
    schedule_unit = os.getenv("SCHEDULE_UNIT", "hours").lower()  # Default to "hours"

    # Validate the schedule unit
    valid_units = ["seconds", "minutes", "hours", "days"]
    if schedule_unit not in valid_units:
        logging.error(f"Invalid SCHEDULE_UNIT: {schedule_unit}. Must be one of {valid_units}.")
        raise ValueError(f"Invalid SCHEDULE_UNIT: {schedule_unit}. Must be one of {valid_units}.")

    # Dynamically schedule the task based on the interval and unit
    if schedule_unit == "seconds":
        schedule.every(schedule_interval).seconds.do(fetch_and_process_data, debug=False)
    elif schedule_unit == "minutes":
        schedule.every(schedule_interval).minutes.do(fetch_and_process_data, debug=False)
    elif schedule_unit == "hours":
        schedule.every(schedule_interval).hours.do(fetch_and_process_data, debug=False)
    elif schedule_unit == "days":
        schedule.every(schedule_interval).days.do(fetch_and_process_data, debug=False)

    logging.info(f"Scheduler started. Running every {schedule_interval} {schedule_unit}.")

    # Run the task immediately on startup
    logging.info("Running the task immediately on startup...")
    fetch_and_process_data(debug=False)

    # Keep the script running to execute scheduled jobs
    while True:
        schedule.run_pending()
        time.sleep(1)