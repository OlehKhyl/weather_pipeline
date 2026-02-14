import requests
import logging
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone

from weather_pipeline.config.settings import MONGO_URI, API_KEY
from weather_pipeline.config.cities import CITIES

logger = logging.getLogger(__name__)

def extract(CITY_ID):
    params = {
        "id": CITY_ID,
        "units": "metric", 
        "appid": API_KEY
    }

    logger.info("Requesting weather data for city_id: %s", CITY_ID)
    response = requests.get(f"https://api.openweathermap.org/data/2.5/weather", params=params, timeout=(5,15))

    response.raise_for_status()

    logger.info("Weather fetched successfully for city_id: %s", CITY_ID)

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
        raise ValueError("Error city_id is None - invalid configuration or mapping error")


def save_raw(data, collection):


    try:
        data["ingested_at"] = datetime.now(timezone.utc)

        logger.debug("Saving raw weather for city_id=%s", data["city_id"])

        collection.insert_one(
            data  
        )
    except DuplicateKeyError:
        logger.warning("Duplicate entry for city_id=%s at observation_ts=%s. Skipping.",
                       data["city_id"], data["observation_ts"])

def main():
    with MongoClient(MONGO_URI) as mongo:

        success_count = 0
        failure_count = 0

        weather_collection = mongo.weather_raw.raw_weather
        for city in CITIES:
            raw_data = extract(city["id"])
            if raw_data is None:
                failure_count += 1
                continue

            save_raw(raw_data, weather_collection)
            success_count += 1

        logger.info("Extraction completed for cities count: %s. Success: %s, Failure: %s", len(CITIES), success_count, failure_count)