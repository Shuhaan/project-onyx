import pandas as pd
import boto3, logging, json, os
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
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


def read_parquets_from_s3(s3_client, last_load, bucket="onyx-processed-data-bucket"):
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
        bucket_contents = s3_client.list_objects(Bucket=bucket).get("Contents", [])
        if not bucket_contents:
            log_message(__name__, 20, "No files found in the bucket.")
            return [], [], [], []

        last_load = datetime.strptime(last_load, "%Y-%m-%d %H:%M:%S%z")

        new_files = [
            file
            for file in bucket_contents
            if last_load
            and last_load < file["LastModified"]
            and ".txt" not in file["Key"]
        ]
        if not new_files:
            log_message(__name__, 20, "No new files to process.")
            return [], [], [], []

        dim_table_names = [
            obj["Key"].split(".")[0] for obj in new_files if "dim_" in obj["Key"]
        ]
        fact_table_names = [
            obj["Key"].split(".")[0] for obj in new_files if "fact_" in obj["Key"]
        ]
        dim_parquet_files_list = [
            file_data["Key"] for file_data in new_files if "dim_" in file_data["Key"]
        ]

        fact_parquet_files_list = [
            file_data["Key"] for file_data in new_files if "fact_" in file_data["Key"]
        ]
        dim_df_list = []
        for parquet_file_name in dim_parquet_files_list:
            response = s3_client.get_object(Bucket=bucket, Key=parquet_file_name)
            data = response["Body"].read()
            df = pd.read_parquet(BytesIO(data))
            dim_df_list.append(df)

        fact_df_list = []
        for parquet_file_name in fact_parquet_files_list:
            response = s3_client.get_object(Bucket=bucket, Key=parquet_file_name)
            data = response["Body"].read()
            df = pd.read_parquet(BytesIO(data))
            fact_df_list.append(df)

        log_message(__name__, 20, "Parquet files read and dataframes created.")
        return (dim_table_names, fact_table_names, dim_df_list, fact_df_list)

    except ClientError as e:
        log_message(
            __name__, 40, f"Error reading from S3: {e.response['Error']['Message']}"
        )
        raise e
    except Exception as e:
        log_message(__name__, 40, f"Unexpected error: {str(e)}")
        raise e


def write_df_to_warehouse(read_parquet, engine_string=os.getenv("TEST-ENGINE")):
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

        dim_table_names, fact_table_names, dim_df_list, fact_df_list = read_parquet
        if not dim_table_names and not fact_table_names:
            log_message(__name__, 20, "No data to write to the warehouse.")
            return

        log_message(__name__, 20, "Connecting to the database.")
        engine = create_engine(engine_string, connect_args={"ssl_context": False})

        for i, file in enumerate(dim_table_names):
            table_name = file.split("/")[0]
            print(table_name)
            new_df = dim_df_list[i]

            # Optimising the read to only necessary columns
            with engine.begin() as connection:  # Use a transaction block
                current_df = pd.read_sql(
                    text(f"SELECT currency_id from {table_name};"), connection
                )["currency_id"]
                new_df = new_df[~new_df["currency_id"].isin(current_df)]
                print(current_df.to_string())

                log_message(__name__, 20, f"Writing data to {table_name}.")
                new_df = new_df.astype(
                    {
                        "currency_id": "int",
                        "currency_code": "str",
                        "currency_name": "str",
                    }
                )
                print(new_df.to_string())
                # Set the schema path
                connection.execute(text("SET search_path TO project_team_3;"))
                if not new_df.empty:
                    new_df.to_sql(
                        table_name,
                        connection,
                        schema="project_team_3",
                        if_exists="append",
                        index=False,
                    )
            log_message(__name__, 20, f"Data written to {table_name} successfully.")

        for i, file in enumerate(fact_table_names):
            table_name = file.split("/")[0]
            fact_df_list[i].to_sql(
                table_name,
                engine,
                schema="project_team_3",
                if_exists="append",
                index=False,
            )
            log_message(
                __name__, 20, f"Fact data written to {table_name} successfully."
            )

    except SQLAlchemyError as e:
        log_message(__name__, 40, f"SQLAlchemy error: {str(e)}")
        raise e
    except Exception as e:
        log_message(__name__, 40, f"Unexpected error: {str(e)}")
        raise e
