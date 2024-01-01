import json

import altair as alt
import geopandas as gpd
import pandas as pd
import pydeck as pdk
from pyproj import Geod
from shapely import Polygon
from utils import RWY_HDG, TOFF_LAN_WHERE, db_query

import streamlit as st

st.set_page_config(
    page_title="Blagnacoscope · Takeoffs & Landings",
    page_icon="✈️",
)

DB_CONN = st.connection("db", type="sql")

with open("data/airlines.json") as f:
    AIRLINES = json.load(f)

with open("data/airports.json") as f:
    AIRPORTS = json.load(f)

with open("data/aircraft.json") as f:
    AIRCRAFT = json.load(f)


def intro():
    st.markdown(
        """
Now that we've seen the properties of the data at hand, we can get down to business and start filtering the data to
only keep ADS-B pings for aircraft that are in a takeoff or landing phase at LFBO.

For this, we add several conditions to the previous filtering condition for airborne aircraft:
1. The aircraft's heading must be close to LFBO's runways heading (143° or 323°)
2. The aircraft's altitude must be below a certain threshold (to avoid false positives of aircraft overflying LFBO at
  high altitude)
3. The aircraft's geographical position must be aligned with the axis of LFBO's runways
"""
    )


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


def map(df):
    st.markdown(
        f"""
The first two filtering conditions can be applied with the following SQL `where` clause:
```
{TOFF_LAN_WHERE}
```
The third one is a bit more tricky. Ideally, it would also be done in SQL using
[Spatialite](https://www.gaia-gis.it/fossil/libspatialite/index) (a geospatial extension for SQLite), but loading
Spatialite in SQLAlchemy is a bit of a pain (requires a python version compiled to allow for SQLite extensions for
example), and I can't be bothered with it right now.
We'll therefore stick with a plain old `point.within(polygon)` in [shapely](https://shapely.readthedocs.io/) /
[geopandas](https://geopandas.org/en/stable/) after the SQL query.
                """
    )

    zone = airport_zones()
    x, y = zone.exterior.coords.xy
    coordinates = [(xx, yy) for xx, yy in zip(x, y)]
    df2 = pd.DataFrame({"coordinates": [coordinates]})

    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=43.62,
                longitude=1.36,
                zoom=9.8,
            ),
            layers=[
                pdk.Layer(
                    "HeatmapLayer",
                    data=df,
                    get_position="[longitude, latitude]",
                    opacity=0.9,
                    radius_pixels=20,
                ),
                pdk.Layer(
                    "PolygonLayer",
                    data=df2,
                    get_polygon="coordinates",
                    get_fill_color=(21, 130, 55, 100),
                ),
            ],
        )
    )

    st.markdown(
        """
The heatmap above shows:
- the points resulting from the SQL query shown above
- the polygon that is used afterwards in python to only keep points within it
"""
    )


def aggregate_takeoffs_landings(df):
    zone = airport_zones()
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    gdf = gdf[gdf.geometry.within(zone)]

    gdf["datetime"] = pd.to_datetime(gdf["time"], unit="s")
    # gdf = gdf[gdf['datetime'] >= datetime.now() - timedelta(days=7)]
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
        "subflight_id",
    ]

    agg_dict = {column: "first" for column in cols}
    agg_dict.update({"vertical_speed": "mean"})
    toff_land: pd.DataFrame = gdf.groupby("subflight_id").agg(agg_dict)
    toff_land["direction"] = toff_land["vertical_speed"].apply(lambda x: "takeoff" if x >= 0 else "landing")
    toff_land["airline"] = toff_land["airline_icao"].apply(lambda x: AIRLINES.get(x, x))
    toff_land["aircraft"] = toff_land["aircraft_code"].apply(lambda x: AIRCRAFT.get(x, x))
    toff_land["is_airbus"] = toff_land["airline_icao"].apply(lambda x: x in ["AIB", "BGA"])
    toff_land["origin_airport"] = toff_land["origin_airport_iata"].apply(lambda x: AIRPORTS.get(x, x))
    toff_land["destination_airport"] = toff_land["destination_airport_iata"].apply(lambda x: AIRPORTS.get(x, x))

    return toff_land


def stats_time(df):
    c = alt.Chart(df).mark_bar().encode(x=alt.X("hours(datetime):O"), y="count()", color="direction").interactive()
    st.altair_chart(c, use_container_width=True)
    c = alt.Chart(df).mark_bar().encode(x=alt.X("day(datetime):O"), y="count()", color="is_airbus").interactive()
    st.altair_chart(c, use_container_width=True)


def stats_airlines(df):
    d = df.groupby("airline").agg(
        {
            "fr_id": lambda x: round(len(x) / len(df) * 100, 2),
            "registration": "nunique",
            "aircraft_code": "nunique",
            "origin_airport_iata": "nunique",
            "aircraft": lambda x: set(x),
        }
    )
    st.dataframe(
        d.sort_values(by="fr_id", ascending=False).rename(
            columns={
                "fr_id": "% of flights",
                "registration": "# of aircraft",
                "aircraft_code": "# of aircraft models",
                "origin_airport_iata": "# of destinations",
            }
        ),
        use_container_width=True,
    )


def stats_airports(df):
    st.dataframe(
        df[df["direction"] == "landing"]
        .groupby("origin_airport")["fr_id"]
        .count()
        .sort_values(ascending=False)
        .rename("% of landings")
        / len(df)
        * 100,
        use_container_width=True,
    )
    st.dataframe(
        df[df["direction"] == "takeoff"]
        .groupby("destination_airport")["fr_id"]
        .count()
        .sort_values(ascending=False)
        .rename("% of takeoffs")
        / len(df)
        * 100,
        use_container_width=True,
    )


def stats_aircraft(df):
    st.dataframe(df.groupby("aircraft")["fr_id"].count().sort_values().rename("Aircraft type"))


st.title("Takeoffs & Landings")
intro()
df = db_query(DB_CONN, "select *", where=TOFF_LAN_WHERE)
map(df)
df = aggregate_takeoffs_landings(df)
stats_airlines(df)
stats_airports(df)
stats_aircraft(df)
