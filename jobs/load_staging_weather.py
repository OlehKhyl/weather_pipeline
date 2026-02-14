import psycopg2
import logging
from psycopg2 import extras
from pymongo import MongoClient
from pytz import utc
from datetime import datetime

from weather_pipeline.config.settings import POSTGRESQL_URI
from weather_pipeline.config.settings import MONGO_URI

logger = logging.getLogger(__name__)

def get_last_loaded_ts(pg_conn) -> datetime:
    with pg_conn.cursor() as cursor:
        cursor.execute("SELECT MAX(observation_ts) FROM staging.stg_weather_observations;")

        return cursor.fetchone()[0]


def extract_from_mongo(mongo_client, since_ts: datetime):
    collection = mongo_client.weather_raw.raw_weather

    since_ts_epoch = int(since_ts.astimezone(utc).timestamp())
    result = collection.find({"observation_ts": {"$gt": since_ts_epoch}}).sort("observation_ts", 1)
    return result



def transform(raw_doc: dict) -> dict:
    payload = raw_doc.get("payload",None)

    if payload is None:
        raise ValueError("Payload is None")

    ingested_at = raw_doc.get("ingested_at", None)

    if ingested_at is not None:
        ingested_at = ingested_at.astimezone(utc)

    staging_record ={
        "city_id": safe_int(payload.get("id", None)),
        "observation_ts": datetime.fromtimestamp(payload.get("dt", None), tz=utc),
        "temperature": safe_float(payload.get("main", {}).get("temp", None)),
        "feels_like": safe_float(payload.get("main", {}).get("feels_like", None)),
        "humidity": safe_int(payload.get("main", {}).get("humidity", None)),
        "pressure": safe_int(payload.get("main", {}).get("pressure", None)),
        "wind_speed": safe_float(payload.get("wind", {}).get("speed", None)),
        "weather_main": payload.get("weather", [{}])[0].get("main", None),
        "weather_description": payload.get("weather", [{}])[0].get("description", None),
        "source": raw_doc.get("source", None),
        "ingested_at": ingested_at
    }

    if staging_record["city_id"] is None:
        raise ValueError("City_id is None")
    
    if staging_record["observation_ts"] is None:
        raise ValueError("Observation_ts is None")

    if staging_record["source"] is None:
        raise ValueError("Source is None")
    
    if staging_record["source"] not in ["OWM"]:
        raise ValueError("Source is invalid")

    if staging_record["temperature"] < -60 or staging_record["temperature"] > 60:
        staging_record["temperature"] = None
    
    if staging_record["feels_like"] < -60 or staging_record["feels_like"] > 60:
        staging_record["feels_like"] = None

    if staging_record["humidity"] < 0 or staging_record["humidity"] > 100:
        staging_record["humidity"] = None

    if staging_record["pressure"] <= 800:
        staging_record["pressure"] = None

    return staging_record


def load_to_postgresql(pg_conn, records: list[dict]):
    if records:
        columns = records[0].keys()
        query = "INSERT INTO staging.stg_weather_observations ({}) " \
        "VALUES %s " \
        "ON CONFLICT (city_id, observation_ts, source) DO NOTHING".format(",".join(columns))

        values = [[value for value in record.values()] for record in records]

        with pg_conn.cursor() as cursor:
            psycopg2.extras.execute_values(
                cursor, query, values, template=None
            )

        pg_conn.commit()

def safe_float(value):
    return float(value) if value is not None else None

def safe_int(value):
    return int(value) if value is not None else None

def main():
    with psycopg2.connect(POSTGRESQL_URI) as pg_conn:
        with MongoClient(MONGO_URI) as mongo_client:

            since_ts = get_last_loaded_ts(pg_conn)

            if since_ts is None:
                since_ts = datetime.fromtimestamp(0, tz=utc)

            logger.info("Starting load to staging")

            logger.info("Loading records with observation_ts > %s", since_ts)
            raw_data = list(extract_from_mongo(mongo_client, since_ts))

            logger.info("Extracted %s records from MongoDB with observation_ts > %s", len(raw_data), since_ts)

            staging_data = []

            reject_count = 0

            for document in raw_data:
                try :
                    staging_data.append(transform(document))
                except ValueError as e:
                    reject_count += 1
                    logger.warning("Error transforming document: %s", e)
                    continue

            if reject_count > 0:        
                logger.warning("Rejected records during transform: %s", reject_count)

            logger.info("Inserting %s records into staging", len(staging_data))

            load_to_postgresql(pg_conn, staging_data)

            logger.info("Load to staging completed")