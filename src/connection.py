from pg8000.native import Connection
from dotenv import load_dotenv
import os

load_dotenv()

def connect_to_db():
    return Connection(
        user=os.getenv("Username"),
        password=os.getenv("Password"),
        database=os.getenv("Database"),
        host=os.getenv("Hostname"),
        port=int(os.getenv("Port"))
    )
    
