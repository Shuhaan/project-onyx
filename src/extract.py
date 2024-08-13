from src.connection import connect_to_db
from src.utils import format_response
import logging
import boto3
from datetime import datetime
from pprint import pprint


# logger = logging.getLogger("OnyxLogger")
# logger.setLevel(logging.INFO)

totesys_table_list = ['address', 'design', 'transaction', 'sales_order', 
                      'counterparty', 'payment', 'staff', 'purchase_order', 
                      'payment_type', 'currency', 'department' ]

def extract_from_db():
    conn = connect_to_db()
    time_stamp = 0
    
    for table in totesys_table_list:
        query = f'''SELECT * FROM {table}
                    LIMIT 1 ;
                    '''
        response = conn.run(query)
        headers = [col['name'] for col in conn.columns]
        formatted_response = format_response(headers, response)
        print(formatted_response)
        
extract_from_db()