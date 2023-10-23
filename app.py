from pathlib import Path
from datetime import datetime

import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.sql import text


def chart(db_path):
    periodicity = st.sidebar.selectbox("Périodicité", ["Heure", "Minute"])
    date_strings = {"Minute": "%Y-%m-%d %H:%M", "Heure": "%Y-%m-%d %H"}
    if not periodicity:
        periodicity = "Heure"
    date_string = date_strings[periodicity]
    time_agg = f"strftime('{date_string}', datetime(time, 'unixepoch'))"
    engine = create_engine(f"sqlite:///{db_path.absolute()}", echo=False)
    with engine.connect() as con:
        stmt = text(f"select {time_agg}, count(*) from flights group by {time_agg};")
        res = con.execute(stmt)

    data = []
    for time, count in res:
        dt = datetime.strptime(time, date_string)
        data.append({"date": dt, "count": count})

    st.line_chart(data, x="date", y="count")


def db_stats(db_path: Path):
    def sizeof_fmt(num, suffix="B"):
        """From https://stackoverflow.com/a/1094933"""
        for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f}Yi{suffix}"

    engine = create_engine(f"sqlite:///{db_path.absolute()}", echo=False)
    with engine.connect() as con:
        stmt = text("select count(*) from flights;")
        res = con.execute(stmt)
    total_count = next(res)[0]
    size = db_path.stat().st_size
    return st.text(f"Sqlite database - {total_count} rows / {sizeof_fmt(size)}")


db_path = Path("db.db")
st.title("Traffic aérien autour de Toulouse Blagnac")
chart(db_path)
db_stats(db_path)
