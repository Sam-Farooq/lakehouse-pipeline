#!/usr/bin/env python3
"""Synthetic transaction producer.

Shapes the stream the way a real one misbehaves, because a pipeline tested on
clean data is a pipeline that has not been tested: a duplicate rate, a late
arrival rate, and a slice of rows with nulls the schema permits.
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer

from lakehouse.config import get_settings

COUNTRIES = ["DE", "FR", "NL", "ES", "IT", "PL", "SE", None]
CHANNELS = ["card", "sepa", "swift", "wallet", None]
CURRENCIES = ["EUR", "EUR", "EUR", "USD", "GBP", "CHF"]

DUPLICATE_RATE = 0.03   # at-least-once delivery, as seen in staging
LATE_RATE = 0.05        # mobile clients that buffer offline


def make_event(now: datetime) -> dict:
    occurred = now - (
        timedelta(minutes=random.randint(5, 110))
        if random.random() < LATE_RATE
        else timedelta(seconds=random.randint(0, 30))
    )
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC{random.randint(1, 5000):06d}",
        "counterparty_id": f"CP{random.randint(1, 900):05d}",
        "amount": round(random.lognormvariate(4.0, 1.4), 2),
        "currency": random.choice(CURRENCIES),
        "country": random.choice(COUNTRIES),
        "channel": random.choice(CHANNELS),
        "mcc": random.choice([5411, 5812, 4111, 6011, None]),
        "occurred_at": occurred.isoformat(),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=500, help="events per second")
    ap.add_argument("--seconds", type=int, default=0, help="0 runs until killed")
    args = ap.parse_args()

    cfg = get_settings()
    producer = Producer({
        "bootstrap.servers": cfg.kafka_brokers,
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "compression.type": "lz4",
    })

    sent, started = 0, time.time()
    while not args.seconds or time.time() - started < args.seconds:
        batch_started = time.time()
        for _ in range(args.rate):
            event = make_event(datetime.now(UTC))
            payload = json.dumps(event).encode()
            # Key on account so a given account's events keep their order
            # within a partition, which silver's merge relies on.
            producer.produce(cfg.topic, key=event["account_id"].encode(), value=payload)
            if random.random() < DUPLICATE_RATE:
                producer.produce(cfg.topic, key=event["account_id"].encode(), value=payload)
                sent += 1
            sent += 1
        producer.poll(0)
        time.sleep(max(0.0, 1.0 - (time.time() - batch_started)))
        print(f"sent={sent}", end="\r", flush=True)

    producer.flush()
    print(f"\nflushed {sent}")


if __name__ == "__main__":
    main()
