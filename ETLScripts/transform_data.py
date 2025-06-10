import pandas as pd
import json
import os
from datetime import datetime, timezone


def transform_weather_data(input_file_path, output_dir="/opt/airflow/data/processed"):
    """
    Reads raw weather JSON data, transforms it, and saves it to a processed CSV file.
    """
    print(f"Transforming data from {input_file_path}...")
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        city = data.get('name', 'Unknown')  # Get city name from data

        # Extract and transform relevant fields
        weather_data = {
            'city': city,
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon'],
            'main_weather': data['weather'][0]['main'],
            'description': data['weather'][0]['description'],
            'temp': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'temp_min': data['main']['temp_min'],
            'temp_max': data['main']['temp_max'],
            'pressure': data['main']['pressure'],
            'humidity': data['main']['humidity'],
            'visibility': data.get('visibility'),  # visibility can be missing
            'wind_speed': data['wind']['speed'],
            'wind_deg': data['wind']['deg'],
            'clouds_all': data['clouds']['all'],
            'data_time': datetime.fromtimestamp(data['dt'], tz=timezone.utc),
            'sunrise': datetime.fromtimestamp(data['sys']['sunrise'], tz=timezone.utc),
            'sunset': datetime.fromtimestamp(data['sys']['sunset'], tz=timezone.utc),
            'record_ingestion_time': datetime.now(timezone.utc),
            'rain_1h': data['rain']['1h'] if 'rain' in data and '1h' in data['rain'] else None,
            'snow_1h': data['snow']['1h'] if 'snow' in data and '1h' in data['snow'] else None
        }

        df = pd.DataFrame([weather_data])
        print("Data transformed to DataFrame.")

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Generate unique filename for processed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_path = os.path.join(output_dir, f"weather_processed_{city}_{timestamp}.csv")

        df.to_csv(output_file_path, index=False, encoding='utf-8')
        print(f"Processed data saved to {output_file_path}")
        return output_file_path  # Return path for the next step
    except KeyError as e:
        print(f"Error during data transformation - missing key: {e}. Raw data sample: {data.get('main', {})}")
        raise
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
        raise
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {input_file_path}. Is it valid JSON?")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during transformation: {e}")
        raise


if __name__ == "__main__":
    # --- Local Testing Setup for a SPECIFIC Input File ---
    # This block allows you to run this script directly to test the transform logic
    # using an existing file from your data pipeline.

    # **IMPORTANT:** Use the exact path to your existing raw JSON file.
    # On Windows, you might need to use raw strings (r"...") or double backslashes.
    # If running from WSL, the path to your D: drive would be /mnt/d/...
    # For example, if you are running this script from inside WSL,
    # the Windows path D:\ELT(FETCH API)\Data\raw\weather_raw_Hanoi_20250610_101802.json
    # becomes /mnt/d/ELT(FETCH API)/Data/raw/weather_raw_Hanoi_20250610_101802.json in WSL.
    # Assuming you are running this from your WSL terminal.
    local_input_file = "../Data/raw/weather_raw_Hanoi_20250610_101802.json"

    # Define the output directory relative to the current script for local testing
    # This will place the processed CSV file in your project's 'data/processed' folder
    local_output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')

    # Ensure the local output directory exists
    os.makedirs(local_output_dir, exist_ok=True)

    print(f"\nTesting transform_data.py locally.")
    print(f"Attempting to transform data from: {local_input_file}")
    print(f"Output will be saved to: {local_output_dir}")

    try:
        # Call the transform function with the specified input file
        transformed_file_path = transform_weather_data(
            input_file_path=local_input_file,
            output_dir=local_output_dir
        )
        print(f"\nLocal test complete. Transformed data saved to: {transformed_file_path}")

    except FileNotFoundError:
        print(f"\nERROR: The specified input file was not found at: {local_input_file}")
        print("Please ensure the path is correct and the file exists.")
    except Exception as e:
        print(f"\nAn unexpected error occurred during local testing: {e}")