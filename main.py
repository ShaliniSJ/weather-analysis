import os
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
API_KEY = os.getenv("WORLD_WEATHER_KEY")

if not API_KEY:
    raise ValueError("❌ WORLD_WEATHER_KEY not found in .env file")

BASE_URL = "https://api.worldweatheronline.com/premium/v1/past-weather.ashx"

# List of locations
LOCATIONS = [
    "Houston,Texas,US",
    "London,UK",
    "Pune,India",
    "Chennai,India"
]

def fetch_historical_weather(city, start_date, end_date):
    """Fetch historical weather data for a city between given dates (1 data point per day)."""
    params = {
        "key": API_KEY,
        "q": city,
        "format": "json",
        "date": start_date,
        "enddate": end_date,
        "tp": 24,
    }

    response = requests.get(BASE_URL, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"Failed for {city}: {response.status_code} - {response.text}")

    data = response.json().get("data", {})
    records = []

    for day in data.get("weather", []):
        records.append({
            "city": city,
            "date": day.get("date"),
            "avgTempC": day.get("avgtempC"),
            "maxtempC": day.get("maxtempC"),
            "mintempC": day.get("mintempC"),
            "sunHour": day.get("sunHour"),
            "uvIndex": day.get("uvIndex"),
            "totalSnow_cm": day.get("totalSnow_cm"),
            "precipMM": day.get("hourly", [{}])[0].get("precipMM"),
            "humidity": day.get("hourly", [{}])[0].get("humidity"),
            "pressure": day.get("hourly", [{}])[0].get("pressure"),
            "windspeedKmph": day.get("hourly", [{}])[0].get("windspeedKmph"),
            "weatherDesc": day.get("hourly", [{}])[0].get("weatherDesc", [{}])[0].get("value")
        })

    return pd.DataFrame(records)


def fetch_two_years_of_data(city):
    """Fetch data in 30-day chunks for 2 years."""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=730)
    current_start = start_date

    all_data = []

    print(f"📆 Fetching 2 years of data for {city}")

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=30), end_date)
        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")

        try:
            df = fetch_historical_weather(city, start_str, end_str)
            all_data.append(df)
        except Exception as e:
            print(f"⚠️ Skipping {start_str} to {end_str} due to error: {e}")

        current_start = current_end + timedelta(days=1)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def save_to_csv(df, city):
    """Save DataFrame to CSV file."""
    city_name = city.replace(",", "_").replace(" ", "_")
    filename = f"data_{city_name}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Saved: {filename}")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.chdir("data")

    for city in tqdm(LOCATIONS, desc="Fetching weather data"):
        try:
            df = fetch_two_years_of_data(city)
            if not df.empty:
                save_to_csv(df, city)
            else:
                print(f"⚠️ No data found for {city}")
        except Exception as e:
            print(f"❌ Error fetching data for {city}: {e}")

    print("\n🌤️ All city data fetched and saved successfully!")
