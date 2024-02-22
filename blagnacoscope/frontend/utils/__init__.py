import json
from collections import Counter
from pathlib import Path

import geopandas as gpd
import pandas as pd
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

with open("data/airlines.json") as f:
    AIRLINES = json.load(f)

with open("data/airports.json") as f:
    AIRPORTS = json.load(f)

with open("data/aircraft.json") as f:
    AIRCRAFT = json.load(f)


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


def _fr24_pings_to_flights():
    import duckdb

    df = duckdb.sql(f"SELECT * FROM 'fr24_history/feed/playback/*.parquet' WHERE {TOFF_LAN_WHERE}").df()
    zone = airport_zones()
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    gdf = gdf[gdf.geometry.within(zone)]
    gdf.groupby("flightid", as_index=False)["timestamp"].first().to_csv("flights_to_dl.csv", index=False)


def aggregate_takeoffs_landings():
    import streamlit as st

    df = db_query(st.connection("db", type="sql"), "select *", where=TOFF_LAN_WHERE)
    zone = airport_zones()
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    gdf = gdf[gdf.geometry.within(zone)]

    gdf["datetime"] = pd.to_datetime(gdf["time"], unit="s", utc=True).dt.tz_convert("Europe/Paris")
    gdf["hour"] = gdf["datetime"].dt.hour
    gdf["ping_delta_t"] = gdf.groupby("fr_id")[["time"]].diff().fillna(0)
    gdf["subflight_nb"] = (gdf["ping_delta_t"] > 3 * 60).cumsum()
    gdf["subflight_id"] = gdf.apply(lambda x: f"{x['fr_id']}_{x['subflight_nb']}", axis=1)
    cols = [
        "fr_id",
        "aircraft_code",
        "registration",
        "time",
        "origin_airport_iata",
        "destination_airport_iata",
        "number",
        "airline_iata",
        "callsign",
        "airline_icao",
        "datetime",
        "hour",
        "subflight_id",
    ]

    agg_dict = {column: "first" for column in cols}
    agg_dict.update({"vertical_speed": "mean", "heading": lambda headings: Counter(headings).most_common(1)[0][0]})
    toff_land: pd.DataFrame = gdf.groupby("subflight_id").agg(agg_dict)
    toff_land["rwy_event"] = toff_land["vertical_speed"].apply(lambda x: "takeoff" if x >= 0 else "landing")
    toff_land["rwy_direction"] = toff_land["heading"].apply(
        lambda heading: "14" if heading <= (RWY_HDG + 180) / 2 else "32"
    )
    toff_land["airline"] = toff_land["airline_icao"].apply(lambda x: f"{AIRLINES.get(x, '')} ({x})")
    toff_land["aircraft"] = toff_land["aircraft_code"].apply(lambda x: f"{AIRCRAFT.get(x, {}).get('model', '')} ({x})")
    toff_land["aircraft_type"] = toff_land["aircraft_code"].apply(lambda x: AIRCRAFT.get(x, {}).get("type", "Unknown"))

    def _get_airport_name(airport_iata):
        if not airport_iata:
            airport_iata = "N/A"
        return f"{AIRPORTS.get(airport_iata, {'name': ''})['name']} ({airport_iata})"

    toff_land["origin_airport"] = toff_land["origin_airport_iata"].apply(_get_airport_name)
    toff_land["destination_airport"] = toff_land["destination_airport_iata"].apply(_get_airport_name)

    def _get_connecting_airport(row):
        match row["rwy_event"]:
            case "landing":
                connecting_airport = row["origin_airport"]
            case "takeoff":
                connecting_airport = row["destination_airport"]
            case _:
                connecting_airport = "N/A"
        return connecting_airport

    toff_land["connecting_airport"] = toff_land.apply(_get_connecting_airport, axis=1)

    return toff_land[
        [
            "datetime",
            "airline",
            "aircraft",
            "origin_airport",
            "destination_airport",
            "registration",
            "callsign",
            "number",
            "rwy_event",
            "hour",
            "connecting_airport",
            "fr_id",
            "rwy_direction",
            "heading",
            "aircraft_type",
        ]
    ]
