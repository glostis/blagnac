import json
from io import StringIO

import pandas as pd
import requests
from FlightRadar24 import FlightRadar24API


def airlines():
    fr_api = FlightRadar24API()
    airlines_path = "airlines.json"
    airlines = {airline["ICAO"]: airline["Name"] for airline in fr_api.get_airlines()}
    with open(airlines_path, "w") as f:
        json.dump(airlines, f)


def airports():
    fr_api = FlightRadar24API()
    airports_path = "airports.json"
    airports = dict()
    for airport in fr_api.get_airports():
        name = airport.name.replace(" Airport", "")
        airports[airport.iata] = dict(
            name=name,
            latitude=airport.latitude,
            longitude=airport.longitude,
            country=airport.country,
        )
    with open(airports_path, "w") as f:
        json.dump(airports, f)


def aircraft():
    url = "http://www.flugzeuginfo.net/table_accodes_en.php"
    r = requests.get(url)
    tables = pd.read_html(StringIO(r.text))

    df = pd.concat(tables)

    def _aggregate_strings(models):
        s = set(models)
        ss = set(models)
        for m in s:
            for mm in s:
                if m != mm and mm in m:
                    try:
                        ss.remove(m)
                    except KeyError:
                        pass
        return " / ".join(sorted(ss))

    def _format_manufacturer_model(row):
        if " / " in row.Manufacturer:
            manufacturer = f"{row.Manufacturer} —"
        else:
            manufacturer = row.Manufacturer
        return f"{manufacturer} {row['Type/Model']}"

    aircraft_dict = (
        df.groupby("ICAO")
        .agg({"Wake": "count", "Manufacturer": _aggregate_strings, "Type/Model": _aggregate_strings})
        .apply(_format_manufacturer_model, axis=1)
        .to_dict()
    )

    url = "https://en.wikipedia.org/w/index.php?title=List_of_aircraft_type_designators&oldid=1167181019"
    r = requests.get(url)
    tables = pd.read_html(StringIO(r.text))
    assert len(tables) == 1

    df = tables[0]
    df = df.rename(columns={"ICAO code[3]": "ICAO"})

    aircraft_dict.update(df.groupby("ICAO")["Model"].agg(_aggregate_strings).to_dict())

    url = "https://opensky-network.org/datasets/metadata/doc8643AircraftTypes.csv"

    df = pd.read_csv(url)
    type_dict = df.groupby('Designator')['AircraftDescription'].first().to_dict()

    final_dict = {}
    for icao, model in aircraft_dict.items():
        final_dict[icao] = {"model": model, "type": type_dict.get(icao, "Unknown")}

    with open("aircraft.json", "w") as f:
        json.dump(final_dict, f)


if __name__ == "__main__":
    airlines()
    airports()
    aircraft()
