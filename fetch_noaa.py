import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

# Import mapping functions from your station mapper
from map_naoo_stations import parse_station_file, find_nearest_station, CITIES

# Load NOAA API token from .env
load_dotenv()
NOAA_TOKEN = os.getenv("NOAA_TOKEN")

if not NOAA_TOKEN:
    raise ValueError("❌ NOAA_TOKEN not found in .env file")

BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
HEADERS = {"token": NOAA_TOKEN}
DATASET = "GHCND"  # Daily summaries
DATATYPE = ["TMAX", "TMIN", "TAVG", "PRCP", "AWND"]
LIMIT = 1000  # Max records per request

# Path to your GHCND stations file
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
STATION_FILE = os.path.join(PROJECT_DIR, "ghcnd-stations.txt")
DATA_DIR = os.path.join(PROJECT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)  # Ensure data folder exists


def fetch_noaa_data(station_id, start_date, end_date):
    """Fetch daily historical weather data for a station from NOAA."""
    params = {
        "datasetid": DATASET,
        "stationid": f"GHCND:{station_id}",
        "startdate": start_date,
        "enddate": end_date,
        "datatypeid": DATATYPE,
        "limit": LIMIT,
        "units": "metric",
        "format": "json",
    }

    response = requests.get(BASE_URL, headers=HEADERS, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"❌ Failed to fetch NOAA data: {response.status_code} - {response.text}")

    results = response.json().get("results", [])
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def save_noaa_csv(df, city):
    """Save NOAA data to CSV file in the data folder."""
    safe_name = city.replace(",", "").replace(" ", "_")
    csv_path = os.path.join(DATA_DIR, f"data_noaa_{safe_name}.csv")
    df.to_csv(csv_path, index=False)
    print(f"✅ Saved NOAA data for {city} -> {csv_path}")


def fetch_for_all_cities(start_date, end_date):
    """Fetch NOAA data for all cities using nearest GHCND station."""
    stations_df = parse_station_file(STATION_FILE)
    city_station_map = {}
    for city, latlon in CITIES.items():
        station_id = find_nearest_station(city, latlon, stations_df)
        city_station_map[city] = station_id

    for city, station_id in tqdm(city_station_map.items(), desc="Fetching NOAA data"):
        try:
            df = fetch_noaa_data(station_id, start_date, end_date)
            if not df.empty:
                save_noaa_csv(df, city)
            else:
                print(f"⚠️ No data found for {city}")
        except Exception as e:
            print(f"❌ Error fetching data for {city}: {e}")


if __name__ == "__main__":
    # Fetch last 3 months as example
    end_date = datetime.today()
    start_date = end_date - timedelta(days=90)

    fetch_for_all_cities(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
