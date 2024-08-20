import boto3
from botocore.exceptions import ClientError
import pandas
from datetime import datetime
from warehouse_connection import connect_to_warehouse


def load(client=None):
    log_message(__name__, 10, "Entered load function")
    if not s3_client:
        s3_client = boto3.client("s3")
    conn = None
    
    try:
        # connect to warehouse
        conn = connect_to_warehouse()
        log_message(__name__, 20, "Connection to data warehouse made")
        
        
        try:
            last_extract_file = s3_client.get_object(
                Bucket=bucket, Key="last_load.txt"
            )
            last_load = last_load_file["Body"].read().decode("utf-8")
            log_message(__name__, 20, f"Load function last ran at {last_load}")
        except s3_client.exceptions.NoSuchKey:
            lastload = None
            log_message(__name__, 20, "Load function running for the first time")
        # check for new parquet files since last upload
        
        # read new parquets from s3 one at a time in loop
        
        
        
        # convert parquet to df

        # extract columns and data from df
        
        # write sql query using extracted info
        
        table_name = file.split(".")[0]
        columns = (', ').join(list(df.columns))
        values_lst = df.values.tolist()
        
        df.to_sql(table_name, conn, if_exists="append", index=False)
        
        query = f"""
                INSERT INTO {table_name} ({column}) VALUES ({values});
                    
        """
                
    
                # write to warehouse
                
        store_last_load = date.strftime("%Y-%m-%d %H:%M:%S")
        s3_client.put_object(
            Bucket=bucket, Key="last_load.txt", Body=store_last_load
        )
        
    except ClientError as e:
        log_message(__name__, 40, f"Error: {e.response['Error']['Message']}")
    finally:
        if conn:
            conn.close()
            log_message(__name__, 20, "Warehouse connection closed")
    
