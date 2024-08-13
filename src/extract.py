from src.connection import connect_to_db
import logging
import boto3


logger = logging.getLogger("OnyxLogger")
logger.setLevel(logging.INFO)

def extract_from_db():
    conn = connect_to_db()
    for 
        query = '''SELECT * FROM payment
                   LIMIT 1;
                '''
    return conn.run(query)