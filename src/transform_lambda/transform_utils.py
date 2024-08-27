import pandas as pd
import logging, json, boto3, time
from datetime import datetime
from typing import Optional
from botocore.exceptions import ClientError
from io import BytesIO

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


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


def list_s3_files_by_prefix(bucket: str, prefix: str = "", s3_client=None) -> list:
    """
    Lists files in an S3 bucket. If a prefix is provided, it filters the files by that prefix.
    If no prefix is provided, it lists all files in the bucket.

    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str, optional): The prefix to filter the S3 objects. Defaults to an empty string, which lists all files.
        s3_client (boto3.client, optional): The S3 client. If not provided, a new client will be created.

    Returns:
        list: A list of keys (file paths) in the S3 bucket that match the prefix or all files if no prefix is provided.
    """
    if not s3_client:
        s3_client = boto3.client("s3")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        # Check if 'Contents' is in the response
        if "Contents" in response:
            file_list = [content["Key"] for content in response["Contents"]]
            log_message(
                __name__,
                20,
                f"Found {len(file_list)} files in bucket '{bucket}' with prefix '{prefix}'",
            )
            return file_list
        else:
            log_message(
                __name__,
                20,
                f"No files found in bucket '{bucket}' with prefix '{prefix}'",
            )
            return []

    except Exception as e:
        log_message(
            __name__,
            40,
            f"Failed to list files in S3 bucket '{bucket}' with prefix '{prefix}': {e}",
        )
        return []


def create_df_from_json_in_bucket(
    source_bucket: str, file_name: str, s3_client=None
) -> Optional[pd.DataFrame]:
    """
    Reads a JSON file from an S3 bucket and converts it to a pandas DataFrame.

    Args:
        source_bucket (str): The name of the S3 bucket.
        file_name (str): The key (path) of the JSON file within the S3 bucket.
        s3_client (client): AWS S3 client.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the data from the JSON file,
        or None if there are issues with the file or its content.
    """
    if not s3_client:
        s3_client = boto3.client("s3")

    if not file_name.endswith(".json"):
        log_message(__name__, 20, f"File {file_name} is not a JSON file.")
        return None

    try:
        # Retrieve the JSON file from S3
        json_file = s3_client.get_object(Bucket=source_bucket, Key=file_name)
        json_contents = json_file["Body"].read().decode("utf-8")
        json_data = json.loads(json_contents)

        # Determine the table name from the file path
        table = file_name.split("/")[0]
        data = json_data.get(table, [])

        if not data:
            log_message(
                __name__, 30, f"No data found for table {table} in file {file_name}."
            )
            return None

        df = pd.DataFrame(data)
        return df

    except json.JSONDecodeError as e:
        log_message(__name__, 40, f"JSON decoding error for file {file_name}: {e}")
    except ClientError as e:
        log_message(
            __name__,
            40,
            f"Client error when accessing {file_name} in bucket {source_bucket}: {e}",
        )
    except Exception as e:
        log_message(
            __name__,
            40,
            f"Unexpected error reading or processing file {file_name}: {e}",
        )


def create_dim_date(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Creates a dimension date DataFrame for a given range of dates.

    Args:
        start_date (str): Start date in "YYYY-MM-DD" format.
        end_date (str): End date in "YYYY-MM-DD" format.

    Returns:
        pd.DataFrame: DataFrame containing the date dimension table.
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        date_range = pd.date_range(start=start, end=end)

        dim_date = pd.DataFrame(date_range, columns=["date_id"])
        dim_date["year"] = dim_date["date_id"].dt.year
        dim_date["month"] = dim_date["date_id"].dt.month
        dim_date["day"] = dim_date["date_id"].dt.day
        dim_date["day_of_week"] = dim_date["date_id"].dt.weekday + 1
        dim_date["day_name"] = dim_date["date_id"].dt.day_name()
        dim_date["month_name"] = dim_date["date_id"].dt.month_name()
        dim_date["quarter"] = dim_date["date_id"].dt.quarter

        dim_date["date_id"] = dim_date["date_id"].dt.date

        return dim_date

    except Exception as e:
        log_message(__name__, 40, f"Error creating dim_date DataFrame: {e}")
        raise


def process_table(
    df: pd.DataFrame, file: str, bucket: str, timer: int = 60, s3_client=None
):
    """
    Process specific table based on the file name, converts it to a dataframe.

    Args:
        source_bucket (str): The name of the S3 bucket containing the source JSON files.
        file (str): str of file path (key) within the source bucket.
        output_bucket (str): The name of the S3 bucket to upload processed files to.
        timer (int): delay timer in order to allow files to be created before joining on another.

    Returns:
        pd.DataFrame: DataFrame containing the processed dim or fact table.
    """
    if not s3_client:
        s3_client = boto3.client("s3")

    table = file.split("/")[0]
    output_table = ""

    try:
        if table == "address":
            df = df.rename(columns={"address_id": "location_id"}).drop(
                ["created_at", "last_updated"], axis=1
            )
            output_table = "dim_location"

        elif table == "design":
            df = df.drop(["created_at", "last_updated"], axis=1)
            output_table = "dim_design"

        elif table == "department":
            df = df.drop(["created_at", "last_updated"], axis=1)
            output_table = "dim_department"

        elif table == "payment_type":
            df = df.drop(["created_at", "last_updated"], axis=1)
            output_table = "dim_payment_type"

        elif table == "transaction":
            df = df.drop(["created_at", "last_updated"], axis=1)
            output_table = "dim_transaction"

        elif table == "currency":
            currency_mapping = {
                "GBP": "British Pound Sterling",
                "USD": "United States Dollar",
                "EUR": "Euros",
            }
            df = df.drop(["created_at", "last_updated"], axis=1).assign(
                currency_name=df["currency_code"].map(currency_mapping)
            )
            if df["currency_name"].isnull().any():
                log_message(__name__, 30, f"Unmapped currency codes found in {file}.")
            output_table = "dim_currency"

        elif table == "counterparty":  # combine counterparty with address table
            log_message(__name__, 20, "Entered counterparty inside process_table")
            time.sleep(timer)
            # print(dim_counterparty_df)
            dim_location_df = combine_parquet_from_s3(bucket, "dim_location")
            # print(dim_location_df)
            df = df.merge(
                dim_location_df,
                how="inner",
                left_on="legal_address_id",
                right_on="location_id",
            )
            df = df.drop(
                [
                    "commercial_contact",
                    "delivery_contact",
                    "created_at",
                    "last_updated",
                    "legal_address_id",
                    "location_id",
                ],
                axis=1,
            )
            df = df.rename(
                columns={
                    "address_line_1": "counterparty_legal_address_line_1",
                    "address_line_2": "counterparty_legal_address_line_2",
                    "district": "counterparty_legal_district",
                    "city": "counterparty_legal_city",
                    "postal_code": "counterparty_legal_postal_code",
                    "country": "counterparty_legal_country",
                    "phone": "counterparty_legal_phone_number",
                }
            )
            output_table = "dim_counterparty"

        elif table == "staff":
            log_message(__name__, 10, "Entered staff inside process_table")
            time.sleep(timer)
            dim_department_df = combine_parquet_from_s3(bucket, "dim_department")
            df = df.merge(dim_department_df, how="inner", on="department_id")
            log_message(__name__, 10, "merged staff data frame with dim_department")
            df = df.drop(
                [
                    "department_id",
                    "created_at",
                    "last_updated",
                ],
                axis=1,
            )
            log_message(__name__, 10, "created staff data frame")
            output_table = "dim_staff"

        elif table == "sales_order":
            # split the Name column into two columns using pd.Series.str.split()
            df[["created_date", "created_time"]] = df["created_at"].str.split(
                " ", expand=True
            )
            df[["last_updated_date", "last_updated_time"]] = df[
                "last_updated"
            ].str.split(" ", expand=True)
            output_table = "fact_sales_order"

        elif table == "purchase_order":
            # split the Name column into two columns using pd.Series.str.split()
            df[["created_date", "created_time"]] = df["created_at"].str.split(
                " ", expand=True
            )
            df[["last_updated_date", "last_updated_time"]] = df[
                "last_updated"
            ].str.split(" ", expand=True)
            output_table = "fact_purchase_order"

        elif table == "payment":
            # split the Name column into two columns using pd.Series.str.split()
            df[["created_date", "created_time"]] = df["created_at"].str.split(
                " ", expand=True
            )
            df[["last_updated_date", "last_updated_time"]] = df[
                "last_updated"
            ].str.split(" ", expand=True)
            df = df.drop(
                [
                    "company_ac_number",
                    "counterparty_ac_number",
                ],
                axis=1,
            )
            output_table = "fact_payment"

        else:
            log_message(
                __name__, 20, f"Unknown table encountered: {table}, skipping..."
            )
            return

        return (df, output_table)

    except Exception as e:
        log_message(__name__, 40, f"Error processing table {table}: {e}")


def combine_parquet_from_s3(bucket: str, directory: str, s3_client=None):
    """
    Reads a S3 bucket containing parquet files and combines them into a dataframe
    Removes duplicate rows and keeps the last modification.

    Args:
        bucket (str): The name of the S3 bucket containing the source parquet files.
        directory (str): The directory (table name) containing the parquet files.
        s3_client (optional): AWS S3 client.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the data from the combined parquet files,
        or None if there are issues with the files or its content.
    """
    log_message(__name__, 20, "Entered combine_parquet_from_s3")
    if not s3_client:
        s3_client = boto3.client("s3")
    directory_files = list_s3_files_by_prefix(bucket, directory)
    sorted_directory_files = sorted(directory_files)
    dfs = []
    for file in sorted_directory_files:
        response = s3_client.get_object(Bucket=bucket, Key=file)
        data = response["Body"].read()
        df = pd.read_parquet(BytesIO(data))
        dfs.append(df)
    log_message(__name__, 10, "appended dfs in combine_parquet_from_s3")
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.drop_duplicates(keep="last", inplace=True)
    log_message(__name__, 10, "concatted and dropped dups")
    return combined_df
