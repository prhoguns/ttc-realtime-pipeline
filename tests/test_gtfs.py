from pathlib import Path

from ttc_stream.gtfs import parse_feed

FIXTURE = Path(__file__).parent / "fixtures" / "vehicles_sample.pb"


def test_parse_real_feed_sample():
    header_ts, vehicles = parse_feed(FIXTURE.read_bytes())
    assert header_ts.year >= 2026
    assert len(vehicles) > 100
    v = vehicles[0]
    assert set(v) >= {"vehicle_id", "position_ts", "route_id", "latitude", "longitude"}
    assert 43.0 < v["latitude"] < 44.5 and -80.0 < v["longitude"] < -78.5  # Toronto


def test_vehicle_ids_unique_within_a_feed():
    _, vehicles = parse_feed(FIXTURE.read_bytes())
    ids = [v["vehicle_id"] for v in vehicles]
    assert len(ids) == len(set(ids))
