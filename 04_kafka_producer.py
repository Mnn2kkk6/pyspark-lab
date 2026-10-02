"""
04_kafka_producer.py
Kafka producer cho PySpark Structured Streaming Lab.

Đọc data/orders.csv và gửi từng order thành một JSON message vào Kafka.
Producer in ra partition + offset của từng message để quan sát trực tiếp
cách Kafka lưu và định vị record.

Chạy từ thư mục gốc repo:
    pip install -r requirements-streaming.txt
    python 04_kafka_producer.py --topic orders --delay 0.3
"""

from __future__ import annotations

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send orders.csv to Kafka")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--delay", type=float, default=0.3)
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Số lần phát lại toàn bộ CSV; hữu ích để quan sát offset tăng lên",
    )
    return parser.parse_args()


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        key_serializer=lambda value: str(value).encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
    )


def main() -> None:
    args = parse_args()

    if not os.path.exists(args.source):
        raise FileNotFoundError(f"Không tìm thấy file input: {args.source}")

    if args.repeat < 1:
        raise ValueError("--repeat phải >= 1")
    if args.delay < 0:
        raise ValueError("--delay phải >= 0")

    with open(args.source, "r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        raise ValueError("CSV không có dữ liệu")

    print("=" * 78)
    print("KAFKA PRODUCER")
    print(f"bootstrap : {args.bootstrap}")
    print(f"topic     : {args.topic}")
    print(f"source    : {args.source}")
    print(f"records   : {len(rows)}")
    print(f"repeat    : {args.repeat}")
    print("=" * 78)

    producer = build_producer(args.bootstrap)

    sent = 0
    try:
        for round_no in range(1, args.repeat + 1):
            print(f"\n--- round {round_no}/{args.repeat} ---")
            for row in rows:
                # Giữ nguyên amount dưới dạng string để các giá trị như
                # "abc", "" hoặc "-50000" được Spark Streaming validate.
                event = dict(row)
                event["event_sent_at"] = datetime.now(
                    timezone.utc
                ).isoformat()

                key = event.get("order_id")
                future = producer.send(args.topic, key=key, value=event)
                metadata = future.get(timeout=30)

                sent += 1
                print(
                    f"order_id={key:<4} "
                    f"partition={metadata.partition} "
                    f"offset={metadata.offset}"
                )

                if args.delay:
                    time.sleep(args.delay)

            producer.flush()

    finally:
        producer.close()

    print("\nDONE.")
    print(f"Đã gửi {sent} messages vào topic '{args.topic}'.")


if __name__ == "__main__":
    main()
