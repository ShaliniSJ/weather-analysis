import pandas as pd
from geopy.distance import geodesic

# Path to NOAA station file (ghcnd-stations.txt or your custom list)
STATION_FILE = "ghcnd-stations.txt"  # replace with your file path

# List of cities with approximate lat/lon
CITIES = {
    "Houston, TX, US": (29.76, -95.37),
    "London, UK": (51.51, -0.13),
    "Pune, India": (18.52, 73.85),
    "Chennai, India": (13.08, 80.27),
}

def parse_station_file(file_path):
    """
    Parse GHCND stations file into a DataFrame with columns:
    id, name, lat, lon, elevation, country
    """
    stations = []
    with open(file_path, "r") as f:
        for line in f:
            # Official ghcnd-stations.txt format
            station_id = line[0:11].strip()
            lat = float(line[12:20].strip())
            lon = float(line[21:30].strip())
            elev = line[31:37].strip()
            state = line[38:40].strip()
            name = line[41:71].strip()
            country = line[72:74].strip()
            stations.append({
                "station_id": station_id,
                "name": name,
                "lat": lat,
                "lon": lon,
                "elevation": elev,
                "state": state,
                "country": country
            })
    return pd.DataFrame(stations)

def find_nearest_station(city_name, city_latlon, stations_df):
    """
    Find the nearest GHCND station to a city using geodesic distance.
    """
    min_dist = float("inf")
    nearest_station = None
    for _, row in stations_df.iterrows():
        station_latlon = (row["lat"], row["lon"])
        dist = geodesic(city_latlon, station_latlon).km
        if dist < min_dist:
            min_dist = dist
            nearest_station = row
    print(f"Nearest station to {city_name} is {nearest_station['name']} ({nearest_station['station_id']}) at {min_dist:.2f} km")
    return nearest_station["station_id"]

if __name__ == "__main__":
    stations_df = parse_station_file(STATION_FILE)
    city_station_map = {}
    for city, latlon in CITIES.items():
        station_id = find_nearest_station(city, latlon, stations_df)
        city_station_map[city] = station_id

    print("\n✅ City to GHCND station mapping:")
    for city, sid in city_station_map.items():
        print(f"{city}: {sid}")
