import asyncio
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fr24.core import FR24
from fr24.livefeed import (
    livefeed_flightdata_dict,
    livefeed_message_create,
    livefeed_playback_message_create,
    livefeed_playback_request_create,
    livefeed_playback_response_parse,
    livefeed_post,
)
from fr24.types.cache import LiveFeedRecord, livefeed_schema
from httpx import HTTPStatusError
from tqdm import tqdm


def log(message):
    with open("fr24_log.log", "a") as f:
        f.write(f"{datetime.now().isoformat()} - {message}\n")


async def catch_pings_of_day(client, year, month, day, interval=30):
    timestamp = int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())
    duration = 7
    all_records = []
    for i in tqdm(range(int(24 * 60 * 60 / interval)), disable=True):
        ts = timestamp + i * interval
        try:
            livefeed_records = await query_playback_snapshot(client, ts, duration)
            all_records.extend(livefeed_records)
        except AssertionError:
            log(f"Failed for {ts} / {datetime.utcfromtimestamp(ts).isoformat()}")

    fp = client.cache_dir / "feed" / "playback" / f"{year}{month}{day}.parquet"
    pq.write_table(pa.Table.from_pylist(all_records, schema=livefeed_schema), fp)
    # await asyncio.sleep(0.1)


async def query_playback_snapshot(
    client,
    timestamp,
    duration,
) -> list[LiveFeedRecord]:
    north, south, west, east = 43.76, 43.49, 1.18, 1.55
    message = livefeed_message_create(
        north=north,
        west=west,
        south=south,
        east=east,
        fields=["flight", "reg", "route", "type", "vspeed"],
    )
    pb_message = livefeed_playback_message_create(
        message,
        timestamp=timestamp,
        prefetch=timestamp + duration,
        hfreq=0,
    )
    request = livefeed_playback_request_create(pb_message, auth=client.auth)
    data = await livefeed_post(client.client, request)
    return [livefeed_flightdata_dict(lfr) for lfr in livefeed_playback_response_parse(data).flights_list]


async def get_flight_playback(client, flightid, timestamp):
    flight_id = f"{flightid:x}"
    flight_id = flight_id.lower()
    rootdir = client.cache_dir / "playback"
    fp_metadata = rootdir / "metadata" / f"{flight_id}.parquet"
    if fp_metadata.exists():
        log(f"Skipping {flight_id}")
        return
    try:
        await client.cache_playback_upsert(
            flight_id=flightid,
            timestamp=timestamp,
            overwrite=False,
        )
    except HTTPStatusError as e:
        code = e.response.status_code
        if code == 429:
            retry = int(e.response.headers.get("retry-after"))
            log(f"Too many requests, sleeping for {retry} seconds")
            await asyncio.sleep(retry)
        else:
            log(f"HTTP error {code} on {flightid} {timestamp}")


async def async_get_pings():
    print("Getting pings")
    async with FR24(cache_dir="fr24_pings") as fr24:
        start_dt = datetime(2023, 6, 1)
        end_dt = datetime(2024, 1, 31)
        dt = start_dt
        while dt <= end_dt:
            log(f"Starting {dt.date()}")
            await catch_pings_of_day(fr24, dt.year, dt.month, dt.day)
            log(f"Finished {dt.date()}")
            dt = dt + timedelta(days=1)


async def async_get_flights():
    print("Getting flights")
    df = pd.read_csv("./flights_to_dl.csv")
    async with FR24(cache_dir="fr24_flights") as fr24:
        for _, row in df.iterrows():
            await get_flight_playback(fr24, row.flightid, row.timestamp)


def sync_get_pings():
    asyncio.run(async_get_pings())


def sync_get_flights():
    asyncio.run(async_get_flights())


if __name__ == "__main__":
    sync_get_flights()
