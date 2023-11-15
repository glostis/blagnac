from pathlib import Path

import pytz

TABLE = "flights"
DB_PATH = Path("db.db")
LOG_PATH = Path("log.log")
TZ = pytz.timezone("Europe/Paris")

AIRBORNE_WHERE = "(on_ground = 0) and (ground_speed >= 20)"


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
