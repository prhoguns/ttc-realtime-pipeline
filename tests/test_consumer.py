"""Dedupe contract of the sink, exercised against a throwaway Postgres if one is available."""

import os
from datetime import UTC, datetime

import psycopg
import pytest

from ttc_stream.consumer import write_batch

DSN = os.getenv("POSTGRES_DSN")


@pytest.mark.skipif(not DSN, reason="POSTGRES_DSN not set")
def test_upsert_is_idempotent():
    row = {
        "vehicle_id": "test-1",
        "position_ts": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "route_id": "1",
        "trip_id": None,
        "direction_id": 0,
        "latitude": 43.7,
        "longitude": -79.4,
        "bearing": None,
        "speed_mps": None,
        "stop_id": None,
        "current_status": None,
        "occupancy": None,
    }
    with psycopg.connect(DSN) as conn:
        conn.execute("delete from vehicle_positions where vehicle_id = 'test-1'")
        conn.commit()
        assert write_batch(conn, [row, row]) == 1
        assert write_batch(conn, [row]) == 0
        conn.execute("delete from vehicle_positions where vehicle_id = 'test-1'")
        conn.commit()
