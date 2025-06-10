import requests
import os
from datetime import datetime

def extract_weather_data(city="Hanoi", output_dir="/opt/airflow/data/raw"):
    """
    Fetches weather data from OpenWeatherMap API and saves it to a raw JSON file.
    """
    api_key = os.getenv("OPENWEATHER_API_KEY")
    base_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY environment variable not set.")

    print(f"Fetching weather data for {city}...")
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        raw_data = response.json()
        print("Data fetched successfully.")

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Generate unique filename using timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file_path = os.path.join(output_dir, f"weather_raw_{city}_{timestamp}.json")

        import json
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)
        print(f"Raw data saved to {output_file_path}")
        return output_file_path # Return path for the next step
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from OpenWeatherMap API: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during extraction: {e}")
        raise

if __name__ == "__main__":
    # For local testing outside of Airflow
    os.environ["OPENWEATHER_API_KEY"] = "dce14144727f2f938937d572708fe335"
    # Ensure a local 'data/raw' directory exists for testing
    local_output_dir = os.path.join(os.path.dirname(__file__), '../Data/raw')
    print(f"Testing extract_data.py locally. Output dir: {local_output_dir}")
    extract_weather_data(output_dir=local_output_dir)