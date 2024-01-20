import math
from pathlib import Path

from FlightRadar24 import FlightRadar24API
from pyproj import Geod
from sqlalchemy import String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


class Flight(Base):
    __tablename__ = "flights"

    id: Mapped[int] = mapped_column(primary_key=True)
    latitude: Mapped[float]
    longitude: Mapped[float]
    fr_id: Mapped[str] = mapped_column(String(10))
    icao_24bit: Mapped[str] = mapped_column(String(6))
    heading: Mapped[int]
    altitude: Mapped[int]
    ground_speed: Mapped[int]
    squawk: Mapped[str] = mapped_column(String(6))
    aircraft_code: Mapped[str] = mapped_column(String(6))
    registration: Mapped[str] = mapped_column(String(10))
    time: Mapped[int]
    origin_airport_iata: Mapped[str] = mapped_column(String(6))
    destination_airport_iata: Mapped[str] = mapped_column(String(6))
    number: Mapped[str] = mapped_column(String(10))
    airline_iata: Mapped[str] = mapped_column(String(6))
    on_ground: Mapped[bool]
    vertical_speed: Mapped[int]
    callsign: Mapped[str] = mapped_column(String(10))
    airline_icao: Mapped[str] = mapped_column(String(6))


def airport_bounds(lon=1.3642, lat=43.6287, distance=15_000):
    geod = Geod(ellps="WGS84")
    x1, y2 = geod.fwd(lons=lon, lats=lat, az=360 - 45, dist=distance * math.sqrt(2))[:2]
    x2, y1 = geod.fwd(lons=lon, lats=lat, az=180 - 45, dist=distance * math.sqrt(2))[:2]
    return y1, y2, x1, x2


def main(db_path="db.db"):
    fr_api = FlightRadar24API()

    y1, y2, x1, x2 = airport_bounds()
    bounds = f"{y2:.2f},{y1:.2f},{x1:.2f},{x2:.2f}"
    flights = fr_api.get_flights(bounds=bounds)

    flights_dicts = []
    for flight in flights:
        flight_dict = flight.__dict__
        flight_dict["fr_id"] = flight_dict.pop("id")
        flights_dicts.append(flight_dict)

    db_path = Path(db_path)
    engine = create_engine(f"sqlite:///{db_path.absolute()}", echo=False)

    if not db_path.exists():
        Base.metadata.create_all(engine)

    with Session(engine) as session:
        for flight in flights_dicts:
            session.add(Flight(**flight))
        session.commit()
    return len(flights)


if __name__ == "__main__":
    # Taken from https://stackoverflow.com/a/25251804
    import time
    from datetime import datetime, timezone

    starttime = time.monotonic()
    periodicity = 30
    logfile = "log.log"
    while True:
        nb_flights = main()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with open("log.log", "a") as f:
            f.write(f"{now} - {nb_flights} flights\n")
        time.sleep(periodicity - ((time.monotonic() - starttime) % periodicity))
