import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
MONGO_URI = os.getenv("mongoURI")
POSTGRESQL_URI = f"postgresql://{os.getenv('POSTGRESQL_USER')}:{os.getenv('POSTGRESQL_PASSWORD')}@{os.getenv('POSTGRESQL_IP')}:{os.getenv('POSTGRESQL_PORT')}/{os.getenv('POSTGRESQL_DB')}"