# Structured Streaming + Kafka Lab

Phần mở rộng của PySpark Lab để phần tìm hiểu Streaming trở thành bài có thể chạy được.

```text
orders.csv -> Kafka Producer -> topic orders -> Spark readStream
                                           -> schema -> validate
                                           -> append / update / complete
                                           -> checkpoint
```

## Chạy Kafka

```powershell
docker compose -f docker-compose-kafka.yml up -d

docker exec -it pyspark-lab-kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it pyspark-lab-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

## Producer

```powershell
pip install -r requirements-streaming.txt
python 04_kafka_producer.py --topic orders --delay 0.3
```

Producer in `partition` và `offset` cho từng message. Dùng `--repeat 2` để thấy offset tiếp tục tăng.

## Spark Structured Streaming

Spark 3.5.9 dùng Kafka connector:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 05_structured_streaming.py --mode inspect --once
```

`inspect` cho thấy dữ liệu cùng `kafka_partition`, `kafka_offset`, `kafka_timestamp`.

### append

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 05_structured_streaming.py --mode append
```

Record hợp lệ được ghi vào `streaming_output/append_valid_orders/` và partition theo `province`.

### update

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 05_structured_streaming.py --mode update --once
```

Aggregate theo province chỉ xuất các dòng đã thay đổi trong micro-batch.

### complete

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 05_structured_streaming.py --mode complete --once
```

Mỗi micro-batch xuất toàn bộ Result Table.

## Checkpoint

Mỗi mode có checkpoint riêng:

```text
checkpoints/structured_streaming/
  inspect/
  append/
  update/
  complete/
```

Checkpoint giúp query ghi nhớ tiến độ/state để có thể phục hồi. Không dùng chung checkpoint giữa các query có semantics khác nhau.

## Batch vs Streaming trong repo

Batch đã có thật trong `02_main.py` và `03_main_full.py`:

```text
CSV -> schema -> validate -> Window dedup -> join -> aggregate -> Parquet -> readback/check
```

Streaming mới:

```text
Kafka -> readStream -> JSON/schema -> validate -> outputMode -> sink -> checkpoint
```

Cả hai dùng cùng `data/orders.csv`, vì vậy có thể đối chiếu trực tiếp Batch và Streaming.

## Chạy lại từ đầu

Xóa checkpoint của mode muốn chạy lại rồi chạy query. Dùng `--starting-offsets earliest` để đọc backlog hiện có; dùng `latest` để bắt đầu từ message mới.
