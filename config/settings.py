import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
CITY_ID = '709930'
MONGO_URI = os.getenv("mongoURI")
POSTGRESQL_URI = os.getenv("POSTGRESQL_URI")
