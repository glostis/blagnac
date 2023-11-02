import altair as alt
from pyproj import Geod
from osmnx import features_from_bbox
import pandas as pd
import streamlit as st
import pydeck as pdk
from pydeck.types import String

from utils import TABLE

st.set_page_config(
    page_title="Blagnacoscope · Data exploration",
    page_icon="✈️",
)

DB_CONN = st.experimental_connection("db", type="sql")


def intro():
    st.markdown(
        """
    The database of flights is a single table in an SQlite database which contains a snapshot of flights in the airspace
    around Toulouse Blagnac airport every 30 seconds.

    Each record in the database corresponds to an
    [ADS-B](https://en.wikipedia.org/wiki/Automatic_Dependent_Surveillance%E2%80%93Broadcast) ping of an aircraft,
    containing it 3D position, speed, heading, and metadata relevant to the aircraft and the flight.

    Below are tables and charts giving an overview of the columns of the database and some of their statistics.
    """
    )


def histogram(db_conn, table, column):
    query = f"select {column}, count({column}) as count from {table} where on_ground = 0 group by {column};"
    df = db_conn.query(query)

    c = (
        alt.Chart(df, title=f"Histogram of {column}")
        .mark_bar()
        .encode(
            x=column,
            y="count",
        )
        .interactive()
    )
    st.altair_chart(c, use_container_width=True)


def two_dee_histogram(db_conn, table, column1, column2, bins=50):
    query = f"select {column1}, {column2} from {table} where on_ground = 0;"
    df = db_conn.query(query)

    c = (
        alt.Chart(df, title=f"2D histogram of {column2} vs {column1}")
        .mark_rect()
        .encode(
            alt.X(f"{column1}:Q").bin(maxbins=bins),
            alt.Y(f"{column2}:Q").bin(maxbins=bins),
            alt.Color("count():Q").scale(scheme="greenblue"),
        )
        .interactive()
    )
    st.altair_chart(c, use_container_width=True)


def table_structure(db_conn, table):
    st.header("Table structure", divider=True)
    st.markdown(f"The `{table}` table looks like this:")
    df = (
        db_conn.query(f"select * from {table} where altitude != 0 limit 1;")
        .transpose()
        .rename(columns={0: "value"})
    )
    st.dataframe(df)

    st.subheader("Statistics of the `on_ground` column", divider=True)
    df = db_conn.query(
        (
            "select on_ground as on_ground_value, "
            "count(*) * 100.0/ sum(count(*)) over () as on_ground_percent "
            f"from {table} "
            "group by on_ground;"
        )
    )
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df, hide_index=True)
    with col2:
        st.markdown(
            f"""
        The database contains {df[df.on_ground_value == 1].on_ground_percent.iloc[0]:.0f}% of records with aircraft that are
        on the ground.

        **We are not interested in such data points, and will therefore remove them from all further analysis.**
        """
        )

    st.subheader(
        "Statistics of the other columns",
        help="Only considering records with `on_ground = 0`",
        divider=True,
    )
    dfs = []
    for column in [
        "latitude",
        "longitude",
        "heading",
        "altitude",
        "ground_speed",
        "vertical_speed",
    ]:
        query = (
            "select "
            f"min({column}) as min, "
            f"max({column}) as max, "
            f"round(avg({column}), 2) as avg "
            f"from {table} "
            "where on_ground = 0;"
        )
        df = db_conn.query(query).transpose().rename(columns={0: column})
        dfs.append(df)
    col1, col2 = st.columns(2)
    with col1:
        st.caption("Columns with numbers")
        st.dataframe(pd.concat(objs=dfs, axis=1).transpose())
    query = "select "
    for column in [
        "icao_24bit",
        "squawk",
        "aircraft_code",
        "registration",
        "origin_airport_iata",
        "destination_airport_iata",
        "number",
        "airline_iata",
        "callsign",
        "airline_icao",
    ]:
        query += f" round(avg(case when {column} != 'N/A' then 100.0 else 0 end), 1) as {column}, "
    query = query[:-2]
    query += f" from {table} where on_ground = 0;"
    df = db_conn.query(query).transpose().rename(columns={0: "% of valid data"})
    with col2:
        st.caption("Columns with strings")
        st.dataframe(df)


def histograms(db_conn, table):
    st.header("Histograms of various columns", divider=True)
    columns = ["heading", "ground_speed", "altitude"]
    tabs = st.tabs(columns)
    for column, tab in zip(columns, tabs):
        with tab:
            histogram(db_conn, table, column)

    columns = [("heading", "altitude"), ("ground_speed", "altitude")]
    tabs = st.tabs([f"{el2} vs {el1}" for (el1, el2) in columns])
    for column, tab in zip(columns, tabs):
        with tab:
            two_dee_histogram(db_conn, table, *column)


def heatmap(db_conn, table):
    st.header("Heatmap", divider=True)

    df = db_conn.query(f"select latitude, longitude from {table} where on_ground = 0;")

    gdf = features_from_bbox(
        43.76, 43.49, 1.18, 1.55, tags={"aeroway": ["aerodrome", "runway"]}
    )
    runways = gdf[
        (gdf.aeroway == "runway")
        & (gdf.geom_type == "LineString")
        & (gdf.surface == "asphalt")
    ]
    runways["path"] = runways.geometry.apply(lambda geom: list(geom.coords))
    # Remove tiny "aeromodelism" runway
    runways = runways[runways.to_crs(runways.estimate_utm_crs()).length > 300]

    def _runway_heading(linestring):
        coords = linestring.coords
        p1 = coords[0]
        p2 = coords[-1]
        geod = Geod(ellps="WGS84")
        az, *_ = geod.inv(*p1, *p2)
        return az

    runways["heading"] = runways.geometry.apply(_runway_heading)

    airports = gdf[(gdf.aeroway == "aerodrome")]
    airports["coordinates"] = airports.centroid.apply(
        lambda centroid: (centroid.x, centroid.y)
    )

    airports = airports[["geometry", "name", "icao", "coordinates"]]
    runways = runways[["geometry", "heading", "path"]]

    j = airports.sjoin(runways, how="inner")
    icao_heading_lookup = dict(j.groupby("icao")["heading"].mean())

    airports["heading"] = airports.icao.apply(lambda x: icao_heading_lookup[x])
    airports["angle"] = airports.heading.apply(lambda x: (90 - x) % 180 + 180)

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
                    "PathLayer",
                    data=runways,
                    get_path="path",
                    get_color=(255, 0, 0),
                    get_width=5,
                    width_min_pixels=2,
                ),
                pdk.Layer(
                    "TextLayer",
                    data=airports,
                    character_set=String("auto"),
                    get_position="coordinates",
                    get_color=(255, 255, 255),
                    get_size=15,
                    get_text="icao",
                    get_angle="angle",
                    outline_width=40,
                    outline_color=(0, 0, 0),
                    font_settings={"sdf": True, "cutoff": 0.1},
                ),
            ],
        )
    )


st.title("Data exploration")
intro()
table_structure(DB_CONN, TABLE)
heatmap(DB_CONN, TABLE)
histograms(DB_CONN, TABLE)
