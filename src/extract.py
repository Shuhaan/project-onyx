from src.connection import connect_to_db
from src.utils import format_response
import logging
import boto3
from datetime import datetime
from pprint import pprint
import json


# logger = logging.getLogger("OnyxLogger")
# logger.setLevel(logging.INFO)



extract_time = datetime.now()
extract_year = extract_time.year
extract_month = extract_time.month
extract_day = extract_time.day
extract_hour = extract_time.hour
extract_minute = extract_time.minute

most_recent_extract = ''

def extract_from_db():
    totesys_table_list = ['address', 'design', 'transaction', 'sales_order', 
                      'counterparty', 'payment', 'staff', 'purchase_order', 
                      'payment_type', 'currency', 'department' ]
    s3_client = boto3.client('s3')
    conn = connect_to_db()

    for table in totesys_table_list:
        query = f'''SELECT * FROM {table}
                    LIMIT 2 ;
                    '''
        # logic required to check timestamps for updates
        
        response = conn.run(query)
        columns = [col['name'] for col in conn.columns]
        formatted_response = {table: format_response(columns, response)}
        extracted_json = json.dumps(formatted_response, indent=4)
        print(extracted_json)
        
        s3_key = f"""{table}/{extract_year}/
                    {extract_month}/{extract_day}/
                    {extract_hour}/{extract_minute}.json"""
                    
        most_recent_extract = s3_key
        
        # response = s3_client.put_object(
        #     Bucket='onyx_ingestion_bucket',
        #     Key='s3_key',
        #     Body=extracted_json,
        # )
        
        print(s3_key)
    conn.close()
        
extract_from_db()


