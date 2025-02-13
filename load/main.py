from loader import Loader
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    env_path = "../resources/secret.env"
    load_dotenv(env_path)

    # Define the database configuration
    db_config = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

    # Create an instance of CSVLoader and load data
    loader = Loader(db_config)
    loader.connect_db()
    loader.load_csv_files_to_db()
    loader.close_connect()