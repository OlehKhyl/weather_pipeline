from pymongo import MongoClient

from config.settings import MONGO_URI

with MongoClient(MONGO_URI) as mongo:
    weather_collection = mongo.weather_raw.raw_weather

    weather_collection.create_index([("city_id", 1), ("observation_ts", 1), ("source", 1)], unique=True)