-- install spatial;
-- load spatial;

-- CREATE METADATA
create table metadata as select * from 'metadata.parquet';

alter table metadata add column url TEXT;
update metadata set
    url = 'https://www.flightradar24.com/data/aircraft/' || lower(registration) || '#' || lower(hex(flight_id));
--

-- CREATE PINGS
create table pings as
select
    cast(
        '0x' || parse_filename(filename, true, 'system') as BIGINT
    ) as flight_id,
    timestamp,
    altitude,
    ground_speed,
    vertical_speed,
    heading,
    squawk,
    st_point(longitude, latitude) as point
from 'track.parquet';

create sequence ping_id_seq start 1;
alter table pings add column id INT default nextval('ping_id_seq');

alter table pings add column in_rwy_poly BOOL;

update pings set in_rwy_poly = coalesce(st_intersects(
    point,
    st_geomfromtext(
        'POLYGON ((1.374827 43.610997, 1.357907 43.61213, 1.345193 43.631459, 1.34177 43.642005, 1.340938 43.644966, 1.315268 43.669529, 1.32317 43.673883, 1.345539 43.652474, 1.352039 43.654319, 1.362308 43.655522, 1.36358 43.643775, 1.390182 43.623388, 1.393312 43.616874, 1.385374 43.614351, 1.413062 43.587853, 1.405172 43.583499, 1.376035 43.611381, 1.374827 43.610997))'
    )
) and altitude < 5000, false);

alter table pings add column rwy_io INT;
with cte as (
    select
        id,
        cast(in_rwy_poly as INT)
        - lag(cast(in_rwy_poly as INT))
            over (partition by flight_id order by timestamp)
        as change
    from pings
)

update pings set rwy_io = cte.change
from cte
where cte.id = pings.id;

alter table pings add column rwy_event TEXT;
with cte as (
    select
        id,
        flight_id,
        rwy_io,
        lag(rwy_io)
            over (partition by flight_id order by timestamp)
        as prev_rwy_io,
        lead(rwy_io)
            over (partition by flight_id order by timestamp)
        as next_rwy_io,
        case
            when rwy_io = -1 and prev_rwy_io = 1 then 'touch-n-go'
            when rwy_io = 1 and next_rwy_io is null then 'landing'
            when rwy_io = -1 and prev_rwy_io is null then 'takeoff'
        end as rwy_event
    from pings where rwy_io != 0
)

update pings set rwy_event = cte.rwy_event
from cte
where cte.id = pings.id;
-- END CREATE PINGS

-- Try to investigate spurious altitude measurements
with cte as (
    select
        flight_id,
        (
            altitude
            - (lag(altitude) over (partition by flight_id order by timestamp))
        )
        / (
            epoch(timestamp)
            - epoch(
                lag(timestamp) over (partition by flight_id order by timestamp)
            )
        ) as delta
    from pings
)

select
    delta,
    count(*)
from cte
where delta != 0
group by delta
order by delta;

select
    registration,
    count(registration)
from pings
inner join metadata on pings.flight_id = metadata.flight_id
where rwy_event = 'touch-n-go'
group by registration
order by count(registration);

with cte as (
    select
        flight_id,
        timestamp,
        point,
        altitude,
        cast(in_rwy_poly as INT)
        - lag(cast(in_rwy_poly as INT))
            over (partition by flight_id order by timestamp)
        as change
    from pings
)

select *
from cte
where flight_id = '820785193';

with cte as (
    select
        cast(in_rwy_poly as INT)
        - lag(cast(in_rwy_poly as INT))
            over (partition by flight_id order by timestamp)
        as change
    from pings
)

select count(*)
from cte
where change != 0;


-- See flights that never ping in rwy_poly but depart from or arrive to LFBO
select
    p.flight_id,
    any_value(origin),
    any_value(destination),
    any_value(url),
    any_value(to_timestamp(status_time))
from pings as p
inner join metadata on p.flight_id = metadata.flight_id
where (origin = 'LFBO' or destination = 'LFBO')
group by p.flight_id
having sum(cast(in_rwy_poly as INT)) = 0;

-- See flights with most takeoffs/landings per flight
select
    pings.flight_id,
    any_value(url),
    any_value(timestamp),
    count(*) as count
from pings
inner join metadata on pings.flight_id = metadata.flight_id
where rwy_io != 0
group by pings.flight_id
order by count;
