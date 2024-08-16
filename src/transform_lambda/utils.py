import pandas as pd
import logging, json, boto3


def log_message(name, level, message=""):
    """
    Sends a message to the logger.

    :param name: The name of the logger.
    :param level: The logging level (one of 0, 10, 20, 30, 40, 50).
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


def create_df_from_json(source_bucket, file_name):
    """_summary_

    Args:
        source_bucket (_type_): _description_
        file_name (_type_): _description_

    Returns:
        _type_: _description_
    """

    if file_name.endswith(".json"):
        s3_client = boto3.client("s3")

        table = file_name.split("/")[0]
        json_file = s3_client.get_object(Bucket=source_bucket, Key=file_name)
        json_contents = json_file["Body"].read().decode("utf-8")
        data = json.loads(json_contents).get(table, [])

        if not data:  # Skip if the JSON content does not contain expected table data
            print(f"No data found for table: {table}")
        else:
            df = pd.DataFrame(data)
            return df
