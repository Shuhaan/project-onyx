from src.connection import connect_to_db
from src.utils import format_response
import logging
import boto3
from datetime import datetime
from pprint import pprint
import json


# logger = logging.getLogger("OnyxLogger")
# logger.setLevel(logging.INFO)

totesys_table_list = ['address', 'design', 'transaction', 'sales_order', 
                      'counterparty', 'payment', 'staff', 'purchase_order', 
                      'payment_type', 'currency', 'department' ]

def extract_from_db():
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
        
        # write extracted_json to ingestion s3
        

        response = s3_client.put_object(
            Bucket='onyx_ingestion_bucket',
            Key='',
            Body=extracted_json,
        )
    conn.close()
        
extract_from_db()


