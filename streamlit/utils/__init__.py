from pathlib import Path

import pytz
from pyproj import Geod
from shapely import Polygon

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


def airport_zones(
    lon=1.3642,
    lat=43.6287,
    azimuth=RWY_HDG,
    long_axis=15_000,
    short_axis=400,
):
    opp_azimuth = (azimuth + 180) % 360

    geod = Geod(ellps="WGS84")

    nw0 = geod.fwd(lons=lon, lats=lat, az=opp_azimuth, dist=long_axis)[:2]
    nw1 = geod.fwd(lons=nw0[0], lats=nw0[1], az=opp_azimuth + 90, dist=short_axis)[:2]
    nw2 = geod.fwd(lons=nw0[0], lats=nw0[1], az=opp_azimuth - 90, dist=short_axis)[:2]

    se0 = geod.fwd(lons=lon, lats=lat, az=azimuth, dist=long_axis)[:2]
    se1 = geod.fwd(lons=se0[0], lats=se0[1], az=azimuth + 90, dist=short_axis)[:2]
    se2 = geod.fwd(lons=se0[0], lats=se0[1], az=azimuth - 90, dist=short_axis)[:2]

    return Polygon((nw1, nw2, se1, se2))
