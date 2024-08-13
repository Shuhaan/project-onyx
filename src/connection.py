from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
# from dotenv import load_dotenv
import os
from src.utils import get_secret

# load_dotenv()

# {
#     'username': 'project_team_3',
#     'password': '3oOhexQmO2Rafbf',
#     'engine': 'postgres',
#     'host': 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com',
#     'port': '5432',
#     'dbname': 'totesys'
# }

#if __name__ == "__main__":

def connect_to_db():
    try:
        credentials = get_secret()
        return Connection(
            user=credentials['username'],
            password=credentials['password'],
            database=credentials["dbname"],
            host=credentials['host'],
            port=int(credentials['port'])
        )
    
    except DatabaseError as e:
        print(e)
        raise e

def test():
    conn = connect_to_db()
    query = '''SELECT * FROM payment
limit 1;
'''
    return conn.run(query)