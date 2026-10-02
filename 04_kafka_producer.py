import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC = "orders"
DEFAULT_SOURCE = "data/orders.csv"


def parse_args():
    p = argparse.ArgumentParser(description="Send orders.csv to Kafka")
    p.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    p.add_argument("--topic", default=DEFAULT_TOPIC)
    p.add_argument("--source", default=DEFAULT_SOURCE)
    p.add_argument("--delay", type=float, default=0.3)
    p.add_argument("--repeat", type=int, default=1)
    return p.parse_args()


def main():
    args = parse_args()
    if not os.path.exists(args.source):
        raise FileNotFoundError(args.source)
    with open(args.source, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError("CSV không có dữ liệu")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        key_serializer=lambda v: str(v).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    sent = 0
    try:
        for round_no in range(1, args.repeat + 1):
            print(f"--- round {round_no}/{args.repeat} ---")
            for row in rows:
                event = dict(row)
                event["event_sent_at"] = datetime.now(timezone.utc).isoformat()
                metadata = producer.send(
                    args.topic, key=event.get("order_id"), value=event
                ).get(timeout=30)
                sent += 1
                print(f"order_id={event.get('order_id'):<4} partition={metadata.partition} offset={metadata.offset}")
                if args.delay:
                    time.sleep(args.delay)
            producer.flush()
    finally:
        producer.close()
    print(f"DONE: {sent} messages -> topic={args.topic}")


if __name__ == "__main__":
    main()
