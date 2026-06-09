"""Poll the TTC vehicle-positions feed and publish one message per vehicle to Kafka.

Key = vehicle_id, so all positions for a vehicle land on the same partition in order.
The feed header timestamp is sent as a message header so consumers can measure lag.
"""

from __future__ import annotations

import json
import logging
import os
import time

import requests
from confluent_kafka import Producer

from ttc_stream.gtfs import VEHICLES_URL, parse_feed

log = logging.getLogger("producer")
TOPIC = os.getenv("KAFKA_TOPIC", "ttc.vehicle_positions")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    poll = int(os.getenv("POLL_SECONDS", "20"))
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"), "linger.ms": 50})
    session = requests.Session()
    session.headers["User-Agent"] = "ttc-realtime-pipeline/1.0"
    last_header_ts = None

    while True:
        started = time.monotonic()
        try:
            resp = session.get(VEHICLES_URL, timeout=30)
            resp.raise_for_status()
            header_ts, vehicles = parse_feed(resp.content)
            if header_ts == last_header_ts:
                log.info("feed unchanged (header %s); skipping", header_ts.isoformat())
            else:
                for v in vehicles:
                    producer.produce(
                        TOPIC,
                        key=v["vehicle_id"],
                        value=json.dumps(v),
                        headers={"feed_ts": header_ts.isoformat()},
                    )
                producer.flush(10)
                log.info("published %d vehicle positions (feed header %s)", len(vehicles), header_ts.isoformat())
                last_header_ts = header_ts
        except Exception as exc:  # keep polling through transient failures
            log.warning("poll failed: %s", exc)
        time.sleep(max(0.0, poll - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
