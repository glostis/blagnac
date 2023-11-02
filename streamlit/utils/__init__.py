from pathlib import Path

import pytz

TABLE = "flights"
DB_PATH = Path("db.db")
LOG_PATH = Path("log.log")
TZ = pytz.timezone("Europe/Paris")
