from pathlib import Path

import pytz

TABLE = "flights"
DB_PATH = Path("db.db")
LOG_PATH = Path("log.log")
TZ = pytz.timezone("Europe/Paris")

RWY_HDG = 142.8

AIRBORNE_WHERE = "(on_ground = 0) and (ground_speed >= 20)"
TOFF_LAN_WHERE = f"({AIRBORNE_WHERE}) and (abs(heading % 180 - {RWY_HDG}) < 5) and (altitude < 10000)"


def db_query(db_conn, query, where=None, groupby=None, limit=None, ttl=3600):
    sql = query
    sql += f" from {TABLE}"
    if where:
        sql += f" where {where}"
    if groupby:
        sql += f" group by {groupby}"
    if limit:
        sql += f" limit {limit}"
    sql += ";"
    return db_conn.query(sql, ttl=ttl)
