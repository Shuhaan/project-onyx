import pandas as pd
import boto3, logging, json, os
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, inspect, DateTime, Boolean
from sqlalchemy.exc import SQLAlchemyError
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def get_secret(
    secret_name: str = "project-onyx/warehouse-login",
    region_name: str = "eu-west-2",
):
    """
    Retrieves warehouse login credentials from AWS Secrets Manager and
    returns it as a sqlalchemy engine db string

    :param secret_name: The name of the secret to retrieve.
    :param region_name: The AWS region where the secret is stored.
    :raises ClientError: If there is an error retrieving the secret.
    :return: The secret DB string
    """
    log_message(__name__, 10, "Entered get_secret_for_warehouse")
    try:
        client = boto3.client(service_name="secretsmanager", region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_dict = get_secret_value_response["SecretString"]
        secret = json.loads(secret_dict)
        result = f"postgresql+pg8000://{secret['username']}:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}"
        log_message(__name__, 20, "Secret retrieved")
        return result

    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        log_message(__name__, 40, e.response["Error"]["Message"])
        raise e


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


def read_parquets_from_s3(
    s3_client, table, last_load, bucket="onyx-processed-data-bucket"
):
    """Reads parquet files from an s3 bucket and converts to pandas dataframe

    Args:
        s3_client (boto3('s3')): Boto3 s3 client to access s3 bucket
        last_load (string): Read from .txt file containing timestamp
                            when load function last ran
        bucket (str, optional): The name of the s3 bucket to be read.
                                Defaults to "onyx-processed-data-bucket".

    Returns:
        tuple: A tuple containing lists of dimension table names, fact table names,
               dimension dataframes, and fact dataframes.
    """
    try:
        bucket_contents = s3_client.list_objects_v2(Bucket=bucket, Prefix=table).get(
            "Contents", []
        )
        if not bucket_contents:
            log_message(__name__, 20, "No files found in the bucket.")
            return []

        last_load = datetime.strptime(last_load, "%Y-%m-%d %H:%M:%S%z")

        new_files = [
            file['Key']
            for file in bucket_contents
            if last_load
            and last_load < file["LastModified"]
            and ".txt" not in file["Key"]
        ]
        if not new_files:
            log_message(__name__, 20, "No new files to process.")
            return []
        
        df_list = []
        for parquet_file_name in new_files:
            response = s3_client.get_object(Bucket=bucket, Key=parquet_file_name)
            data = response["Body"].read()
            df = pd.read_parquet(BytesIO(data))
            df_list.append(df)

        log_message(__name__, 20, f"Parquet {table} files read and dataframes created.")
        return df_list

    except ClientError as e:
        log_message(
            __name__, 40, f"Error reading from S3: {e.response['Error']['Message']}"
        )
        raise e
    except Exception as e:
        log_message(__name__, 40, f"Unexpected error: {str(e)}")
        raise e


def write_df_to_warehouse(df_list, table, engine_string=os.getenv("TEST-ENGINE")):
    """
    Writes the DataFrames to the associated tables in a PostgreSQL database.

    Args:
        read_parquet (list): List of lists received via output of
                             read_parquets_from_s3 function.
        engine_string (str, optional): Database credentials in SQLAlchemy db string
                                       format. Defaults to the 'TEST-ENGINE' env variable.
    """
    try:
        if not engine_string:
            engine_string = get_secret()

        if not df_list:
            log_message(__name__, 20, "No data to write to the warehouse.")
            return

        for df in df_list:
            log_message(__name__, 20, f"Writing data to {table}.")

            upload_dataframe_to_table(df, table)
            log_message(__name__, 20, f"Data written to {table} successfully.")

    except SQLAlchemyError as e:
        log_message(__name__, 40, f"SQLAlchemy error: {str(e)}")
        raise e
    except Exception as e:
        log_message(__name__, 40, f"Unexpected error: {str(e)}")
        raise e


def upload_dataframe_to_table(df, table):
    """
    Converts dataframe values to match those in the reference table and uploads it, ensuring no duplicates.

    Parameters:
    df (pd.DataFrame): The dataframe to be processed and uploaded.
    table (str): The name of the target table in the database.

    Returns:
    None
    """

    engine_url = get_secret()
    log_message(__name__, 20, "Retrieved engine URL.")

    engine = create_engine(engine_url)
    log_message(__name__, 20, f"Created engine for table: {table}.")

    try:
        with engine.begin() as connection:
            # Use Inspector to get table schema
            inspector = inspect(connection)
            columns = inspector.get_columns(table)
            
            # Create a dictionary of column names and types
            # Ensure dataframe columns match table columns
            if table.startswith('fact'):
                table_columns = {col["name"]: col["type"] for col in columns[1:]}
                df = df[list(table_columns.keys())]
                log_message(__name__, 20, f"Confirmed correct columns in {table} ")
            else:
                table_columns = {col["name"]: col["type"] for col in columns}
                df = df[list(table_columns.keys())]
                log_message(__name__, 20, f"Confirmed correct columns in {table} ")
                
            log_message(__name__, 20, f"Table columns: {table_columns}")
            log_message(__name__, 20, f"Dataframe columns: {df.columns} ")

            # Convert dataframe columns to the correct types
            for col_name, col_type in table_columns.items():
                if isinstance(col_type, DateTime):
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce").dt.date
                elif col_type.__class__.__name__ == "Integer":
                    df[col_name] = pd.to_numeric(
                        df[col_name], errors="coerce", downcast="integer"
                    )
                elif col_type.__class__.__name__ == "Float":
                    df[col_name] = pd.to_numeric(
                        df[col_name], errors="coerce", downcast="float"
                    )
                elif col_type.__class__.__name__ == "String":
                    df[col_name] = df[col_name].astype(str)
                    
                elif isinstance(col_type, Boolean):
                # Convert to boolean
                    df[col_name] = df[col_name].astype(bool)
    
                log_message(
                    __name__, 20, f"Converted column {col_name} to type {col_type}."
                )
                
            primary_key_column = df.columns[0]
            log_message(
                __name__, 20, f"Primary key column identified: {primary_key_column}."
            )

            existing_data = pd.read_sql_table(
                table, con=connection, schema="project_team_3"
            )
            log_message(__name__, 20, f"Retrieved existing data from {table}.")

            df = df[~df[primary_key_column].isin(existing_data[primary_key_column])]
            log_message(__name__, 20, f"Removed duplicate rows from dataframe.")
            log_message(__name__, 10, f"Dataframe being uploaded: {df.head()}")

            df.to_sql(
                table,
                con=connection,
                schema="project_team_3",
                if_exists="append",
                index=False,
            )
            log_message(__name__, 20, f"Uploaded data to {table} successfully.")

    except SQLAlchemyError as e:
        log_message(__name__, 40, f"SQLAlchemy error: {str(e)}")
        raise e
    except Exception as e:
        log_message(__name__, 40, f"Unexpected error: {str(e)}")
        raise e
