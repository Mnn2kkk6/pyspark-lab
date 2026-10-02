from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
import argparse
import os

BOOTSTRAP = "localhost:9092"
TOPIC = "orders"
CHECKPOINT_ROOT = "checkpoints/structured_streaming"
OUTPUT_ROOT = "streaming_output"

EVENT_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("province", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("event_sent_at", StringType(), True),
])


def args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["inspect", "append", "update", "complete"], default="inspect")
    p.add_argument("--bootstrap", default=BOOTSTRAP)
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--starting-offsets", default="earliest")
    p.add_argument("--checkpoint-root", default=CHECKPOINT_ROOT)
    p.add_argument("--output-root", default=OUTPUT_ROOT)
    p.add_argument("--once", action="store_true")
    return p.parse_args()


def read_stream(spark, a):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap)
           .option("subscribe", a.topic)
           .option("startingOffsets", a.starting_offsets)
           .option("failOnDataLoss", "false")
           .load())
    return (raw.select(F.col("partition").alias("kafka_partition"),
                       F.col("offset").alias("kafka_offset"),
                       F.col("timestamp").alias("kafka_timestamp"),
                       F.col("value").cast("string").alias("raw_value"))
            .withColumn("event", F.from_json("raw_value", EVENT_SCHEMA))
            .select("kafka_partition", "kafka_offset", "kafka_timestamp", "raw_value", "event.*"))


def validate(df):
    amount = F.col("amount").cast("double")
    order_id = F.col("order_id").cast("int")
    date = F.to_date("order_date", "yyyy-MM-dd")
    date_ok = (F.col("order_date").isNotNull() & (F.length("order_date") == 10)
               & date.isNotNull() & (F.date_format(date, "yyyy-MM-dd") == F.col("order_date")))
    checked = (df.withColumn("order_id", order_id)
                 .withColumn("amount", amount)
                 .withColumn("status", F.upper(F.trim("status")))
                 .withColumn("order_date_ts", date)
                 .withColumn("updated_at_ts", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
                 .withColumn("error_reason", F.concat_ws(",", F.array(
                     F.when(order_id.isNull(), "INVALID_ORDER_ID"),
                     F.when(amount.isNull() | (amount <= 0), "INVALID_AMOUNT"),
                     F.when(~date_ok, "INVALID_DATE"),
                     F.when(F.col("province").isNull() | (F.trim("province") == ""), "MISSING_PROVINCE"),
                 )))
              )
    return checked


def main():
    a = args()
    spark = (SparkSession.builder.appName("PySparkKafkaStructuredStreamingLab")
             .master("local[*]").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        checked = validate(read_stream(spark, a))
        checkpoint = os.path.join(a.checkpoint_root, a.mode)
        if a.mode == "inspect":
            writer = (checked.select("order_id", "customer_id", "province", "amount", "status", "order_date",
                                     "error_reason", "kafka_partition", "kafka_offset", "kafka_timestamp")
                      .writeStream.format("console").outputMode("append")
                      .option("truncate", False).option("numRows", 30)
                      .option("checkpointLocation", checkpoint))
        elif a.mode == "append":
            valid = checked.filter("error_reason = ''")
            writer = (valid.writeStream.format("parquet").outputMode("append")
                      .option("path", os.path.join(a.output_root, "append_valid_orders"))
                      .option("checkpointLocation", checkpoint).partitionBy("province"))
        else:
            valid = checked.filter("error_reason = ''")
            report = (valid.groupBy("province").agg(
                F.count("*").alias("total_orders"),
                F.countDistinct("customer_id").alias("total_customers"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount"),
                F.sum(F.when(F.col("status") == "PAID", 1).otherwise(0)).alias("success_orders"),
                F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("failed_orders")
            ).orderBy("province"))
            writer = (report.writeStream.format("console").outputMode(a.mode)
                      .option("truncate", False).option("numRows", 50)
                      .option("checkpointLocation", checkpoint))
        writer = writer.trigger(availableNow=True) if a.once else writer.trigger(processingTime="5 seconds")
        print(f"mode={a.mode}; checkpoint={checkpoint}")
        writer.start().awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
