import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone

from config.settings import MONGO_URI, API_KEY, CITY_ID

mongo = MongoClient(MONGO_URI)
weather_collection = mongo.weather_raw.raw_weather

def extract():
    response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?id={CITY_ID}&units=metric&appid={API_KEY}")

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


raw_data = extract()
save_raw(raw_data, weather_collection)