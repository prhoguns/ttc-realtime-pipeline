"""Consume vehicle positions and upsert them into PostgreSQL in batches.

Idempotent by design: the primary key (vehicle_id, position_ts) plus ON CONFLICT DO NOTHING means
replaying the topic, or a producer that re-sends unchanged positions, never duplicates rows.
Offsets are committed only after the batch is written, so a crash mid-batch replays it (at-least-once),
which the upsert makes safe.
"""

from __future__ import annotations

import json
import logging
import os
import time

import psycopg
from confluent_kafka import Consumer, KafkaError

log = logging.getLogger("consumer")
TOPIC = os.getenv("KAFKA_TOPIC", "ttc.vehicle_positions")
BATCH = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "5"))

INSERT = """
insert into vehicle_positions
    (vehicle_id, position_ts, route_id, trip_id, direction_id, latitude, longitude,
     bearing, speed_mps, stop_id, current_status, occupancy)
values (%(vehicle_id)s, %(position_ts)s, %(route_id)s, %(trip_id)s, %(direction_id)s, %(latitude)s, %(longitude)s,
        %(bearing)s, %(speed_mps)s, %(stop_id)s, %(current_status)s, %(occupancy)s)
on conflict (vehicle_id, position_ts) do nothing
"""


def write_batch(conn: psycopg.Connection, rows: list[dict]) -> int:
    with conn.cursor() as cur:
        cur.executemany(INSERT, rows)
        written = cur.rowcount
    conn.commit()
    return written


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            "group.id": os.getenv("KAFKA_GROUP", "ttc-postgres-sink"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    conn = psycopg.connect(os.getenv("POSTGRES_DSN", "host=localhost port=5434 dbname=ttc user=ttc password=ttc"))
    batch: list[dict] = []
    last_flush = time.monotonic()
    total_in = total_written = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error: %s", msg.error())
                    continue
                batch.append(json.loads(msg.value()))
            if batch and (len(batch) >= BATCH or time.monotonic() - last_flush >= FLUSH_SECONDS):
                written = write_batch(conn, batch)
                consumer.commit(asynchronous=False)
                total_in += len(batch)
                total_written += written
                log.info(
                    "batch: %d received, %d new rows (totals: %d received, %d new)",
                    len(batch),
                    written,
                    total_in,
                    total_written,
                )
                batch.clear()
                last_flush = time.monotonic()
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
