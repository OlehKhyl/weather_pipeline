import psycopg2

from weather_pipeline.config.settings import POSTGRESQL_URI
from weather_pipeline.config.cities import CITIES
from datetime import datetime
from pytz import utc

def get_current_dag_start_time(**context):
    return context.get("data_interval_start").astimezone(utc)


def check_data_uploaded(pg_conn, current_dag_start_time):
    sql_statement = "SELECT COUNT(*)\
                     FROM staging.stg_weather_observations\
                     WHERE ingested_at >= %s;"

    with pg_conn.cursor() as cursor:
        cursor.execute(sql_statement, (current_dag_start_time,))
        number = cursor.fetchone()[0]
        if (number == 0):
            raise ValueError("No data was uploaded since dag start")


def check_cities_count(pg_conn, current_dag_start_time):
    sql_statement = "SELECT distinct city_id\
                     FROM staging.stg_weather_observations\
                     WHERE ingested_at >= %s;"

    with pg_conn.cursor()  as cursor:
        cursor.execute(sql_statement, (current_dag_start_time,))
        result = set(r[0] for r in cursor.fetchall())

        expected = set(c['id'] for c in CITIES)

        if result != expected:
            raise ValueError("city_id not match CITIES dict in config/cities")


def check_constraints(pg_conn, current_dag_start_time):
    sql_statement = "SELECT COUNT(*) FROM staging.stg_weather_observations WHERE ingested_at >= %s AND (city_id is null OR observation_ts is null OR source is null);"

    with pg_conn.cursor()  as cursor:
        cursor.execute(sql_statement, (current_dag_start_time,))
        number = cursor.fetchone()[0]
        if (number != 0):
            raise ValueError("There is data constaints issues in table")


def main(**context):
    with psycopg2.connect(POSTGRESQL_URI) as pg_conn:

        current_dag_start_time = get_current_dag_start_time(**context)

        check_data_uploaded(pg_conn,current_dag_start_time)
        check_cities_count(pg_conn,current_dag_start_time)
        check_constraints(pg_conn,current_dag_start_time)