-- Append-only positions, de-duplicated on (vehicle_id, position_ts): the feed repeats a vehicle's
-- last known position until it moves, and the producer polls more often than vehicles report.
create table if not exists vehicle_positions (
    vehicle_id    text        not null,
    position_ts   timestamptz not null,
    route_id      text,
    trip_id       text,
    direction_id  smallint,
    latitude      double precision not null,
    longitude     double precision not null,
    bearing       real,
    speed_mps     real,
    stop_id       text,
    current_status text,
    occupancy     text,
    ingested_at   timestamptz not null default now(),
    primary key (vehicle_id, position_ts)
);
create index if not exists ix_positions_route_ts on vehicle_positions (route_id, position_ts desc);

-- Latest position per vehicle (what a live map reads).
create or replace view v_latest_positions as
select distinct on (vehicle_id) *
from vehicle_positions
order by vehicle_id, position_ts desc;

-- Active vehicles per route in the last 5 minutes.
create or replace view v_route_activity as
select route_id,
       count(distinct vehicle_id) as active_vehicles,
       max(position_ts)           as last_update
from vehicle_positions
where position_ts > now() - interval '5 minutes'
group by route_id
order by active_vehicles desc;

-- Speed derived from consecutive positions (haversine), because the feed's own speed field is often null.
create or replace view v_vehicle_speeds as
with ordered as (
    select vehicle_id, route_id, position_ts, latitude, longitude,
           lag(position_ts) over (partition by vehicle_id order by position_ts) as prev_ts,
           lag(latitude)    over (partition by vehicle_id order by position_ts) as prev_lat,
           lag(longitude)   over (partition by vehicle_id order by position_ts) as prev_lon
    from vehicle_positions
)
select vehicle_id, route_id, position_ts,
       extract(epoch from position_ts - prev_ts) as seconds,
       2 * 6371000 * asin(sqrt(
           power(sin(radians(latitude - prev_lat) / 2), 2)
         + cos(radians(prev_lat)) * cos(radians(latitude)) * power(sin(radians(longitude - prev_lon) / 2), 2)
       )) as metres,
       case when extract(epoch from position_ts - prev_ts) between 5 and 600 then
           round((2 * 6371000 * asin(sqrt(
               power(sin(radians(latitude - prev_lat) / 2), 2)
             + cos(radians(prev_lat)) * cos(radians(latitude)) * power(sin(radians(longitude - prev_lon) / 2), 2)
           )) / extract(epoch from position_ts - prev_ts) * 3.6)::numeric, 1) end as km_h
from ordered
where prev_ts is not null;

-- Per-route average speed over the last 15 minutes: the "is Line X crawling" question.
create or replace view v_route_speed_15m as
select route_id,
       count(*)                 as samples,
       round(avg(km_h)::numeric, 1) as avg_km_h,
       round(percentile_cont(0.5) within group (order by km_h)::numeric, 1) as median_km_h
from v_vehicle_speeds
where position_ts > now() - interval '15 minutes' and km_h is not null and km_h < 120
group by route_id
having count(*) >= 5
order by avg_km_h;
