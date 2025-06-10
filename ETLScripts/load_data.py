import pandas as pd
from sqlalchemy import create_engine
import os


def load_weather_data(input_file_path, table_name="weather_data"):

    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_name = os.getenv("POSTGRES_DB", "airflow")
    db_user = os.getenv("POSTGRES_USER", "admin")
    db_password = os.getenv("POSTGRES_PASSWORD", "admin")
    db_port = os.getenv("POSTGRES_PORT", "5432")

    print(f"Loading data from {input_file_path} into PostgreSQL table '{table_name}'...")
    try:
        df = pd.read_csv(input_file_path, parse_dates=['data_time', 'sunrise', 'sunset', 'record_ingestion_time'])

        db_connection_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        db_connection = create_engine(db_connection_str)
        df.to_sql(table_name, db_connection, if_exists='append', index=False, method='multi')

        print(f"Data from {input_file_path} loaded successfully into table '{table_name}'.")
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
        raise
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
        raise


if __name__ == "__main__":
    # For local testing outside of Airflow
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["POSTGRES_DB"] = "airflow"
    os.environ["POSTGRES_USER"] = "admin"
    os.environ["POSTGRES_PASSWORD"] = "admin"
    os.environ["POSTGRES_PORT"] = "5432"


    local_input_file = os.path.join(os.path.dirname(__file__),
                                    '../Data/processed/weather_processed_Hanoi_20250610_103425.csv')
    print(f"Testing load_data.py locally. Input file: {local_input_file}")

    print("Please provide a valid input file path to test load_data.py")