import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone

from weather_pipeline.config.settings import MONGO_URI, API_KEY
from weather_pipeline.config.cities import CITIES

def extract(CITY_ID):
    params = {
        "id": CITY_ID,
        "units": "metric", 
        "appid": API_KEY
    }

    response = requests.get(f"https://api.openweathermap.org/data/2.5/weather", params=params)

    if (response.status_code == 200):

        data = response.json()

        city_id = data.get("id", None)
        city_name = data.get("name", None)

        if city_id is not None:

            observation_ts = data.get("dt",None)
            return {
                "city_id": city_id,
                "city_name": city_name,
                "source": "OWM",
                "observation_ts":observation_ts,
                "payload": data
            }
        else:
            print("Error city_id is None")

    else:
        print(f"Status not 200, Response: {response.status_code}, {response.reason}")


def save_raw(data, collection):


    try:
        data["ingested_at"] = datetime.now(timezone.utc)
        collection.insert_one(
            data  
        )
    except DuplicateKeyError:
        print("Error duplicate collection")

def main():
    with MongoClient(MONGO_URI) as mongo:
        weather_collection = mongo.weather_raw.raw_weather
        for city in CITIES:
            raw_data = extract(city["id"])
            save_raw(raw_data, weather_collection)