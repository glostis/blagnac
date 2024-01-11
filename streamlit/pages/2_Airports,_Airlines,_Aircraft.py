import json

import altair as alt
import geopandas as gpd
import pandas as pd
import pydeck as pdk
from matplotlib import colormaps
from scipy import stats
from streamlit_extras.dataframe_explorer import dataframe_explorer
from utils import TOFF_LAN_WHERE, airport_zones, db_query

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Blagnacoscope · Airports, Airlines, Aircraft",
    page_icon="✈️",
    layout="wide",
)

DB_CONN = st.connection("db", type="sql")

with open("data/airlines.json") as f:
    AIRLINES = json.load(f)

with open("data/airports.json") as f:
    AIRPORTS = json.load(f)

with open("data/aircraft.json") as f:
    AIRCRAFT = json.load(f)


def aggregate_takeoffs_landings(df):
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
    agg_dict.update({"vertical_speed": "mean"})
    toff_land: pd.DataFrame = gdf.groupby("subflight_id").agg(agg_dict)
    toff_land["rwy_event"] = toff_land["vertical_speed"].apply(lambda x: "takeoff" if x >= 0 else "landing")
    toff_land["airline"] = toff_land["airline_icao"].apply(lambda x: f"{AIRLINES.get(x, '')} ({x})")
    toff_land["aircraft"] = toff_land["aircraft_code"].apply(lambda x: f"{AIRCRAFT.get(x, '')} ({x})")

    def _get_airport_name(airport_iata):
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
        ]
    ]


def stats_time(df):
    # TODO: not very satisfying to have to use `ambiguous=True` here...
    df["dt"] = df["datetime"].dt.tz_localize("Europe/Paris", ambiguous=True)
    c = alt.Chart(df).mark_bar().encode(x=alt.X("hours(dt):O"), y="count()", color="rwy_event").interactive()
    st.altair_chart(c, use_container_width=True)
    c = alt.Chart(df).mark_bar().encode(x=alt.X("day(dt):O"), y="count()").interactive()
    st.altair_chart(c, use_container_width=True)


def stats_airlines(df):
    d = df.groupby("airline").agg(
        {
            "fr_id": lambda x: round(len(x) / len(df) * 100, 2),
            "registration": "nunique",
            "aircraft": "nunique",
            "origin_airport": "nunique",
        }
    )
    st.dataframe(
        d.sort_values(by="fr_id", ascending=False).rename(
            columns={
                "fr_id": "% of flights",
                "registration": "# of aircraft",
                "aircraft": "# of aircraft models",
                "origin_airport": "# of destinations",
            }
        ),
        use_container_width=True,
    )


def stats_airports(df):
    cols = st.columns(3)
    for col, var, data, title in zip(
        cols,
        ["connecting_airport", "origin_airport", "destination_airport"],
        [df, df[df["rwy_event"] == "landing"], df[df["rwy_event"] == "takeoff"]],
        ["flights", "landings", "takeoffs"],
    ):
        col.dataframe(
            data.groupby(var)["fr_id"].count().sort_values(ascending=False).rename(f"% of {title}") / len(df) * 100,
        )


def stats_aircraft(df):
    st.dataframe(
        df.groupby("aircraft")
        .agg({"fr_id": "count", "registration": "nunique"})
        .sort_values(by="fr_id", ascending=False)
        .rename(columns={"fr_id": "# of flights", "registration": "# of aircraft"})
    )


def map(airports):
    view_mode = st.radio(
        "Select display mode",
        options=["Globe", "Map"],
        captions=["3D globe", "Flat map, that you can tilt and rotate using Ctrl+click"],
    )
    split_rwy_event = st.toggle("Split takeoffs and landings", disabled=view_mode == "Globe")

    groupby = ["connecting_airport"]
    if split_rwy_event:
        groupby.append("rwy_event")
    data = airports[airports["connecting_airport"] != " (N/A)"].groupby(groupby, as_index=False).agg({"fr_id": "count"})

    data["airport_code"] = data["connecting_airport"].apply(lambda x: x[-4:-1])
    data["latitude"] = data["airport_code"].apply(lambda x: AIRPORTS[x]["latitude"])
    data["longitude"] = data["airport_code"].apply(lambda x: AIRPORTS[x]["longitude"])
    data["latitude_tls"] = AIRPORTS["TLS"]["latitude"]
    data["longitude_tls"] = AIRPORTS["TLS"]["longitude"]
    count_max = data["fr_id"].max()
    count_min = data["fr_id"].min()
    data["count_norm"] = data["fr_id"].apply(lambda x: (x - count_min) / count_max)
    data["width"] = data["count_norm"].apply(lambda x: 2 + x * 30)
    if not split_rwy_event:
        data["rwy_event"] = "N/A"
    data["tilt"] = data["rwy_event"].apply(lambda x: {"takeoff": 15, "landing": -15}.get(x, 0))

    def _count_to_info(row):
        count = row["fr_id"]
        event = row["rwy_event"]
        word = event if event in {"takeoff", "landing"} else "flight"
        if count > 1:
            word += "s"
        pct = count / data["fr_id"].sum() * 100
        return f"{count} {word} ({pct:.2f}%)"

    data["info"] = data.apply(_count_to_info, axis=1)

    def _count_to_color(count, count_series, rwy_event):
        gradient = colormaps[{"takeoff": "Reds", "landing": "Greens"}.get(rwy_event, "cividis")]
        rank = stats.percentileofscore(count_series, count, kind="weak")
        color = gradient(rank / 100)[:3]
        color = [int(el * 255) for el in color]
        return color

    data["color"] = data.apply(
        lambda row: _count_to_color(row["count_norm"], data["count_norm"], row["rwy_event"]), axis=1
    )
    data["color_start"] = data["color"].apply(lambda x: x + [25])
    data["color_end"] = data["color"].apply(lambda x: x + [255])

    st.dataframe(data)

    arc_layer = pdk.Layer(
        "ArcLayer",
        data=data,
        great_circle=(view_mode == "Globe"),
        get_width="width",
        get_height=0.3,
        get_tilt="tilt",
        get_source_position=["longitude_tls", "latitude_tls"],
        get_target_position=["longitude", "latitude"],
        get_source_color="color_start",
        get_target_color="color_end",
        pickable=True,
        auto_highlight=True,
    )

    # This is needed in GlobeView, because otherwise the globe is empty (no basemap) and you can see through it.
    globe_layers = [
        pdk.Layer(
            "PolygonLayer",
            data=pd.DataFrame({"coordinates": [[[-180, 90], [0, 90], [180, 90], [180, -90], [0, -90], [-180, -90]]]}),
            get_polygon="coordinates",
            get_fill_color=[25, 26, 26],
        ),
        pdk.Layer(
            "GeoJsonLayer",
            "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_50m_admin_0_scale_rank.geojson",
            get_fill_color=[52, 51, 50],
            stroked=True,
            get_line_color=[69, 69, 69],
            get_line_width=1000,
            line_width_min_pixels=2,
            line_joint_rounded=True,
        ),
    ]

    # This is completely illegible because text labels are too dense and overlap one another.
    # There's a CollisionFilterExtension in deck.gl to solve this problem, but I haven't found
    # a way to make it work with `pydeck` (https://github.com/visgl/deck.gl/discussions/8329).

    # text_df = pd.DataFrame.from_dict(AIRPORTS, orient="index")
    # text_df["iata"] = text_df.index
    # text_df["fullname"] = text_df.apply(lambda x: f"{x['name']} ({x['iata']})", axis=1)
    # text_df["coordinates"] = text_df.apply(lambda row: (row.longitude, row.latitude), axis=1)
    # text_df = text_df[text_df["iata"].apply(lambda x: x in set(data["airport_code"]))]
    # airports_layer = (
    #     pdk.Layer(
    #         "TextLayer",
    #         data=text_df,
    #         character_set=String("auto"),
    #         get_position="coordinates",
    #         get_color=(255, 255, 255),
    #         get_size=10,
    #         get_text="fullname",
    #         outline_width=40,
    #         outline_color=(0, 0, 0),
    #         font_settings={"sdf": True, "cutoff": 0.1},
    #     ),
    # )

    view_state = pdk.ViewState(
        latitude=43.62,
        longitude=1.36,
        zoom=4,
    )

    deck_args = dict(
        initial_view_state=view_state,
        tooltip={"html": "{connecting_airport}<br/>{info}"},
    )
    if view_mode == "Globe":
        d = pdk.Deck(
            views=pdk.View(type="_GlobeView", controller=True),
            layers=globe_layers + [arc_layer],
            map_provider=None,
            **deck_args,
        )

        # Streamlit's pydeck integration doesn't handle non-MapView views.
        # This workaround uses raw HTML instead, which gets rendered as in iframe in Streamlit.
        # (see https://github.com/streamlit/streamlit/issues/2302)
        components.html(d.to_html(as_string=True), height=800)
    else:
        d = pdk.Deck(
            map_style=None,
            layers=[arc_layer],
            **deck_args,
        )
        st.pydeck_chart(d)


# Dirty hack to make Altair/Vega chart tooltips still visible when viewing a chart in fullscreen/expanded mode
# (taken from https://discuss.streamlit.io/t/tool-tips-in-fullscreen-mode-for-charts/6800/9)
st.markdown("<style>#vg-tooltip-element{z-index: 1000051}</style>", unsafe_allow_html=True)

st.title("Airports, Airlines, Aircraft")
df = db_query(DB_CONN, "select *", where=TOFF_LAN_WHERE)
df = aggregate_takeoffs_landings(df)
filtered_df = dataframe_explorer(df)
st.dataframe(filtered_df, hide_index=True, use_container_width=True)
stats_time(filtered_df)
stats_airlines(filtered_df)
stats_airports(filtered_df)
stats_aircraft(filtered_df)
map(filtered_df)
