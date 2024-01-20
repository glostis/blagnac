import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import pandas as pd
from utils import aggregate_takeoffs_landings

import streamlit as st

st.set_page_config(
    page_title="Blagnacoscope · Runway direction",
    page_icon="✈️",
)

# Dirty hack to make Altair/Vega chart tooltips still visible when viewing a chart in fullscreen/expanded mode
# (taken from https://discuss.streamlit.io/t/tool-tips-in-fullscreen-mode-for-charts/6800/9)
st.markdown("<style>#vg-tooltip-element{z-index: 1000051}</style>", unsafe_allow_html=True)


def _scrape_wind_data(start: datetime, end: datetime):
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
    url += "station=LFBO&"
    url += (
        # `drct` is wind direction, and `snkt` is wind speed in knots
        f"data=drct&data=sknt&"
        f"year1={start.year}&"
        f"month1={start.month}&"
        f"day1={start.day}&"
        f"year2={end.year}&"
        f"month2={end.month}&"
        f"day2={end.day}&"
        f"tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&"
        f"missing=null&trace=T&direct=no&report_type=3&report_type=4"
    )
    df = pd.read_csv(url).rename(columns={"valid": "datetime", "drct": "wind_direction", "sknt": "wind_speed"})
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    return df


def _get_wind_data(start: datetime, end: datetime):
    start = start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    csv_path = Path("data") / "lfbo_wind.csv"
    scraped = []
    if csv_path.is_file():
        df = pd.read_csv(csv_path)
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        c_start = df["datetime"].min()
        c_end = df["datetime"].max()
        if start >= c_start and end - timedelta(minutes=30) <= c_end:
            return df
        else:
            if start < c_start:
                scraped.append(_scrape_wind_data(start, c_start))
            if end - timedelta(minutes=30) > c_end:
                scraped.append(_scrape_wind_data(c_end, end))
        scraped.append(df)
    else:
        scraped.append(_scrape_wind_data(start, end))
    all = pd.concat(scraped)
    all = all.drop_duplicates(keep=False)
    all = all.sort_values(by="datetime")
    all.to_csv(csv_path, index=False)
    return all


def intro():
    st.markdown(
        """
    Toulouse Blagnac airport has two parallel runways: 32L/14R and 32R/14L.
    These runways are aligned with the 140° / 320° axis.

    The goal of this page is to study the correlation between takeoff/landing direction and the wind's direction.

    The theory is that airplanes tend to takeoff/land **against** the wind in order to benefit from an increased
    apparent wind which gives them more lift. If planes takeoff or land in the 14 direction, we expect the wind
    direction to be closer to 140° than 320°.

    Let's find out if we see that in the data!
    """
    )


def flight_direction_stats(df):
    st.subheader("Runway direction statistics", divider=True)
    col1, col2 = st.columns(2)
    c = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("rwy_event").title(None),
            y=alt.Y("count()"),
            color=alt.Color("rwy_direction").legend(None),
            column=alt.Column("rwy_direction").title("Runway direction"),
        )
    )
    col1.altair_chart(c)
    col2.markdown(
        """
    On the chart to the left, you can see that there are twice as many runway events in the direction 320° than
    there are in the direction 140°.
    Landings and takeoffs are quite evenly distributed in both directions.

    On the chart below, you can see that the distribution of runway direction throughout the day is not uniform.
    The proportion of use of runway direction 32 peaks during the afternoon. Could this be a
    [thermal wind](https://en.wikipedia.org/wiki/Thermal_wind) effect that kicks in during the afternoon?
    """
    )

    c = (
        # Only show values during daytime as there are not enough flights during the night to be meaningful
        alt.Chart(df[df.datetime.dt.hour >= 6])
        .mark_bar()
        .encode(
            x=alt.X("hours(datetime)"),
            y=alt.Y("count()").stack("normalize"),
            color=alt.Color("rwy_direction"),
        )
    )
    st.altair_chart(c, use_container_width=True)


def wind_stats(df):
    st.subheader("Wind direction and speed statistics", divider=True)
    st.markdown(
        """
    Now, let's look at statistics about the wind at Toulouse Blagnac.

    The data source for wind speed and direction that we will use is taken from
    [METAR](https://en.wikipedia.org/wiki/METAR) reports, which are automated weather aviation reports published by
    weather stations present in airports.

    Every 30 minutes, we have a message giving — among other information — the wind direction (rounded at the nearest
    10°) and wind speed in knots.
    """
    )

    def deg_to_rad(degrees):
        return (degrees / 360) * 2 * math.pi

    column = "wind_direction"

    grouped = df.groupby("wind_direction", as_index=False)["station"].count().rename(columns={"station": "count"})
    grouped["theta"] = grouped[column].apply(deg_to_rad)
    grouped["theta2"] = grouped[column].apply(lambda x: deg_to_rad(x + 10))
    total_count = grouped["count"].sum()
    grouped["percent"] = grouped["count"].apply(lambda x: x / total_count)
    grouped["text"] = grouped[column].apply(lambda x: f"{x}°")

    base = alt.Chart(grouped, title="Radial histogram of wind direction")
    c1 = (
        base.encode(
            theta=alt.Theta("theta", scale=alt.Scale(domain=[0, 2 * math.pi])),
            theta2="theta2",
            radius=alt.Radius("count"),
            color=alt.Color("count", legend=None),
            order=column,
            tooltip=[column, alt.Tooltip("percent", format=".2%")],
        )
        .mark_arc(stroke=None)
        .interactive()
    )

    c2 = (
        alt.Chart(df[df["wind_speed"] != 0], title="Wind direction versus speed")
        .mark_rect()
        .encode(
            x=alt.X("wind_direction").bin(maxbins=36).title("Wind direction (°)"),
            y=alt.Y("wind_speed").bin(maxbins=36).title("Wind speed"),
            color=alt.Color("count()").legend(None),
        )
    )
    col1, col2 = st.columns(2)
    col1.altair_chart(c1)
    col2.altair_chart(c2)


def runway_vs_wind(events, wind):
    st.subheader("Runway versus wind direction", divider=True)

    st.markdown(
        """
    Now, let's merge the runway direction and wind direction data, and see if we get something that makes sense.

    The graph below shows the distribution of wind direction of all flights split by runway direction.

    The trend we see is what we would expect: planes use runway direction 14 when the wind is below 220°, and runway
    direction 32 when the wind direction is above 220°.
    """
    )
    wind["datetime2"] = pd.to_datetime(wind["datetime"], utc=True).dt.tz_convert("Europe/Paris")
    wind.drop(columns="datetime", inplace=True)
    d = pd.merge_asof(events.sort_values(by="datetime"), wind, left_on="datetime", right_on="datetime2")
    # Black magic taken from https://altair-viz.github.io/gallery/violin_plot.html to make a violin plot
    c = (
        alt.Chart(d[d["wind_direction"] == d["wind_direction"]])
        .transform_density("wind_direction", as_=["wind_direction", "density"], groupby=["rwy_direction"])
        .mark_area(orient="horizontal")
        .encode(
            alt.X("density:Q")
            .stack("center")
            .impute(None)
            .title(None)
            .axis(labels=False, values=[0], grid=False, ticks=True),
            alt.Y("wind_direction:Q", scale=alt.Scale(domain=[0, 360])).title("Wind direction (°)"),
            alt.Color("rwy_direction:N").legend(None),
            alt.Column("rwy_direction:N")
            .spacing(0)
            .header(titleOrient="bottom", labelOrient="bottom", labelPadding=0)
            .title("Runway direction"),
        )
        # For some reason, I have to specify a width here, otherwise the chart is too large
        # and overflows its streamlit container
        .properties(width=250)
    )
    st.altair_chart(c)

    st.markdown(
        """
    The graph below is a bit more dense. It shows a timeseries of runway direction used by flights, superimposed with
    a timeseries of wind direction. The runway direction line width is proportional to the number of flights in a given
    time period (the thicker the line, the more frequent the flights). The wind direction line width is proportional
    to the wind speed (the thicker the line, the stronger the wind).

    You can see that the runway direction follows the wind direction, but not always perfectly.
    """
    )
    c = (
        alt.Chart(events)
        .mark_trail()
        .encode(
            x=alt.X("yearmonthdatehours(datetime)").title("Datetime"),
            y=alt.Y("median(heading)").title("Runway direction"),
            size=alt.Size("count()", legend=None),
            # Hack to have a legend
            color=alt.datum("Runway direction"),
        )
        .interactive()
    )
    c2 = (
        alt.Chart(wind[wind["wind_speed"] != 0])
        .mark_trail()
        .encode(
            x=alt.X("yearmonthdatehours(datetime2)").title("Datetime"),
            y=alt.Y("average(wind_direction)").title("Wind direction (°)"),
            size=alt.Size("average(wind_speed)", legend=None),
            color=alt.datum("Wind direction (°)"),
        )
        .interactive()
    )
    st.altair_chart(c + c2, use_container_width=True)


st.title("Runway direction")
intro()
events = aggregate_takeoffs_landings()
wind = _get_wind_data(events["datetime"].min(), events["datetime"].max())
flight_direction_stats(events)
wind_stats(wind)
runway_vs_wind(events, wind)
