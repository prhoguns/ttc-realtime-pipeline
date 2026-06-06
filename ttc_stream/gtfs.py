"""Parse a GTFS-Realtime VehiclePositions feed into plain dicts (one per vehicle)."""

from __future__ import annotations

from datetime import UTC, datetime

from google.transit import gtfs_realtime_pb2

VEHICLES_URL = "https://bustime.ttc.ca/gtfsrt/vehicles"

STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}
OCCUPANCY = {
    0: "EMPTY",
    1: "MANY_SEATS_AVAILABLE",
    2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY",
    4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL",
    6: "NOT_ACCEPTING_PASSENGERS",
}


def parse_feed(payload: bytes) -> tuple[datetime, list[dict]]:
    """Return (feed header timestamp, list of vehicle position dicts)."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    header_ts = datetime.fromtimestamp(feed.header.timestamp, tz=UTC)
    out = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        v = entity.vehicle
        if not v.HasField("position"):
            continue
        out.append(
            {
                "vehicle_id": v.vehicle.id or entity.id,
                "position_ts": datetime.fromtimestamp(v.timestamp or feed.header.timestamp, tz=UTC).isoformat(),
                "route_id": v.trip.route_id or None,
                "trip_id": v.trip.trip_id or None,
                "direction_id": v.trip.direction_id if v.trip.HasField("direction_id") else None,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "bearing": v.position.bearing if v.position.HasField("bearing") else None,
                "speed_mps": v.position.speed if v.position.HasField("speed") else None,
                "stop_id": v.stop_id or None,
                "current_status": STATUS.get(v.current_status) if v.HasField("current_status") else None,
                "occupancy": OCCUPANCY.get(v.occupancy_status) if v.HasField("occupancy_status") else None,
            }
        )
    return header_ts, out
