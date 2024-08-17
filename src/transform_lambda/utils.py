import pandas as pd
import logging, json, boto3
from datetime import datetime
from typing import Optional


def log_message(name: str, level: int, message: str = ""):
    """
    Sends a message to the logger at a specified level.

    :param name: The name of the logger.
    :param level: The logging level (one of 10, 20, 30, 40, 50).
    :param message: The message to log.
    """
    logger = logging.getLogger(name)

    # Define a mapping of level integers to logging methods
    level_map = {
        10: logger.debug,
        20: logger.info,
        30: logger.warning,
        40: logger.error,
        50: logger.critical,
    }

    # Get the logging method from the map
    log_method = level_map.get(level)

    if log_method:
        log_method(message)
    else:
        logger.error("Invalid log level: %d", level)


def create_df_from_json(source_bucket: str, file_name: str) -> Optional[pd.DataFrame]:
    """
    Reads a JSON file from an S3 bucket and converts it to a pandas DataFrame.

    Args:
        source_bucket (str): The name of the S3 bucket.
        file_name (str): The key (path) of the JSON file within the S3 bucket.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the data from the JSON file,
        or None if there are issues with the file or its content.
    """
    if not file_name.endswith(".json"):
        print(f"File {file_name} is not a JSON file.")
        return None

    s3_client = boto3.client("s3")

    try:
        # Retrieve the JSON file from S3
        json_file = s3_client.get_object(Bucket=source_bucket, Key=file_name)
        json_contents = json_file["Body"].read().decode("utf-8")
        json_data = json.loads(json_contents)

        # Determine the table name from the file path
        table = file_name.split("/")[0]
        data = json_data.get(table, [])
        df = pd.DataFrame(data)

        return df

    except Exception as e:
        print(f"Error reading or processing file {file_name}: {e}")
        return None


def create_dim_date(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Creates a dimension date DataFrame for a given range of dates.

    Args:
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.

    Returns:
        pd.DataFrame: DataFrame containing the date dimension table.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = pd.date_range(start=start, end=end)

    dim_date = pd.DataFrame(date_range, columns=["date_id"])
    dim_date["year"] = dim_date["date_id"].dt.year
    dim_date["month"] = dim_date["date_id"].dt.month
    dim_date["day"] = dim_date["date_id"].dt.day
    dim_date["day_of_week"] = (
        dim_date["date_id"].dt.weekday + 1
    )  # 1 for Monday, 7 for Sunday
    dim_date["day_name"] = dim_date["date_id"].dt.day_name()
    dim_date["month_name"] = dim_date["date_id"].dt.month_name()
    dim_date["quarter"] = dim_date["date_id"].dt.quarter

    dim_date["date_id"] = dim_date["date_id"].dt.strftime(
        "%Y%m%d"
    )  # Format date_id as YYYYMMDD

    return dim_date
