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


def map(db_conn):
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

    df = db_query(db_conn, "select *", where=TOFF_LAN_WHERE)

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

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    gdf = gdf[gdf.geometry.within(zone)]
    gdf.to_file("g.gpkg")


st.title("Takeoffs & Landings")
intro()
map(DB_CONN)
