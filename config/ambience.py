import os
from dotenv import load_dotenv

load_dotenv()


class EnvConfig:
    TWELVE_API_KEY = os.getenv("TWELVE_API_KEY")
    FRED_API_KEY = os.getenv("FRED_API_KEY")
    AWESOME_API_KEY = os.getenv("AWESOME_API_KEY")
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
