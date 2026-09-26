"""
03_main_full.py
BÀI TỔNG HỢP PySpark: read -> clean -> validate -> deduplicate -> join
-> transform -> aggregate -> write -> kiểm tra output.

Dữ liệu: data/orders.csv (có updated_at) + data/customers.csv
(sinh bởi 01_generate_data.py)
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from datetime import datetime
import shutil
import os

spark = (
    SparkSession.builder
    .appName("OrdersCustomersFullLab")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

ORDERS_PATH = "data/orders.csv"
CUSTOMERS_PATH = "data/customers.csv"
OUT_DIR = "output_bai2"

if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)

# =========================================================================
# YÊU CẦU 1 - READ + CLEAN
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 1: Read + Clean")
print("=" * 70)

orders_schema = StructType([
    StructField("order_id",    IntegerType(), True),
    StructField("customer_id", StringType(),  True),
    StructField("province",    StringType(),  True),
    StructField("amount",      DoubleType(),  True),
    StructField("status",      StringType(),  True),
    StructField("order_date",  StringType(),  True),   # validate thủ công bằng UDF
    StructField("updated_at",  StringType(),  True),   # parse thành timestamp sau
])

customers_schema = StructType([
    StructField("customer_id",   StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_type", StringType(), True),
])

orders_raw = (
    spark.read.option("header", True).schema(orders_schema).csv(ORDERS_PATH)
)
customers = (
    spark.read.option("header", True).schema(customers_schema).csv(CUSTOMERS_PATH)
)

print(">>> Schema orders:")
orders_raw.printSchema()
print(">>> Schema customers:")
customers.printSchema()

print(f">>> orders_raw count: {orders_raw.count()}")
print(f">>> customers count : {customers.count()}")

# UDF validate order_date đúng format yyyy-MM-dd và là ngày hợp lệ
# (không dùng to_date()/try_to_date() để tránh phụ thuộc phiên bản Spark / ANSI mode)
def _is_valid_date(s):
    if s is None or len(s) != 10:
        return False
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False

is_valid_date_udf = F.udf(_is_valid_date, "boolean")

# Chuẩn hóa status -> uppercase, parse updated_at -> timestamp thật để dùng cho Window
orders_clean = (
    orders_raw
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("updated_at_ts", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("is_date_valid", is_valid_date_udf(F.col("order_date")))
    .withColumn("is_amount_valid", F.col("amount").isNotNull() & (F.col("amount") > 0))
)

print(">>> Kiểm tra nhanh các record lỗi (amount/date) ngay sau bước clean:")
orders_clean.filter(~F.col("is_amount_valid") | ~F.col("is_date_valid")) \
    .select("order_id", "customer_id", "amount", "order_date", "is_amount_valid", "is_date_valid") \
    .show(10, truncate=False)

n_dup_ids = (
    orders_clean.groupBy("order_id").count().filter(F.col("count") > 1).count()
)
print(f">>> Số order_id bị duplicate: {n_dup_ids}")


# =========================================================================
# YÊU CẦU 2 - DEDUPLICATE (Window) + VALIDATE
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 2: Deduplicate bằng Window + tách valid/invalid")
print("=" * 70)

# Với order_id duplicate, giữ bản ghi có updated_at MỚI NHẤT
w = Window.partitionBy("order_id").orderBy(F.col("updated_at_ts").desc())
orders_dedup = (
    orders_clean
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f">>> Count trước dedup: {orders_clean.count()}  ->  sau dedup: {orders_dedup.count()}")

# Kiểm tra customer_id có tồn tại trong customers không (left_anti join)
missing_customers = orders_dedup.join(customers, "customer_id", "left_anti")
print(f">>> Số order có customer_id KHÔNG tồn tại trong customers: {missing_customers.count()}")
missing_customers.select("order_id", "customer_id").show(10, truncate=False)

cond_amount_ok = F.col("is_amount_valid")
cond_date_ok = F.col("is_date_valid")
cond_customer_ok = F.col("customer_id").isin(
    [r["customer_id"] for r in customers.select("customer_id").collect()]
)

orders_checked = (
    orders_dedup
    .withColumn("err_amount",   F.when(~cond_amount_ok, F.lit("INVALID_AMOUNT")))
    .withColumn("err_date",     F.when(~cond_date_ok, F.lit("INVALID_DATE")))
    .withColumn("err_customer", F.when(~cond_customer_ok, F.lit("CUSTOMER_NOT_FOUND")))
    .withColumn(
        "error_reason",
        F.array_join(
            F.array_except(
                F.array(F.col("err_amount"), F.col("err_date"), F.col("err_customer")),
                F.array(F.lit(None).cast("string"))
            ),
            ","
        )
    )
    .drop("err_amount", "err_date", "err_customer", "is_amount_valid", "is_date_valid")
)

valid_orders = orders_checked.filter(F.col("error_reason") == "").drop("error_reason")
invalid_orders = orders_checked.filter(F.col("error_reason") != "")

print(f">>> valid_orders  : {valid_orders.count()} dòng")
print(f">>> invalid_orders: {invalid_orders.count()} dòng")
invalid_orders.select("order_id", "customer_id", "amount", "order_date", "error_reason") \
    .show(15, truncate=False)


# =========================================================================
# YÊU CẦU 3 - JOIN + TRANSFORM
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 3: Left join orders với customers + order_level")
print("=" * 70)

orders_joined = valid_orders.join(customers, on="customer_id", how="left")

not_mapped = orders_joined.filter(F.col("customer_name").isNull())
print(f">>> Số valid_order KHÔNG map được customer (sau left join): {not_mapped.count()}")
# (kỳ vọng = 0 vì các order CUSTOMER_NOT_FOUND đã bị loại ở invalid_orders)

orders_joined = orders_joined.withColumn(
    "order_level",
    F.when(F.col("amount") >= 500_000, "HIGH")
     .when(F.col("amount") >= 200_000, "MEDIUM")
     .otherwise("LOW")
)

print(">>> Mẫu dữ liệu sau join + transform:")
orders_joined.select(
    "order_id", "customer_id", "customer_name", "customer_type",
    "province", "amount", "order_level", "status"
).show(10, truncate=False)

print(">>> Phân bố order_level:")
orders_joined.groupBy("order_level").count().orderBy("order_level").show()


# =========================================================================
# YÊU CẦU 4 - AGGREGATE (report theo province)
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 4: Aggregate report theo province")
print("=" * 70)

province_report = (
    orders_joined
    .groupBy("province")
    .agg(
        F.count("*").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("status") == "PAID", 1).otherwise(0)).alias("success_orders"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("failed_orders"),
    )
    .orderBy("province")
)

print(">>> province_report:")
province_report.show(truncate=False)


# =========================================================================
# YÊU CẦU 5 - WRITE + CHECK
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 5: Write + Check")
print("=" * 70)

# Ghi valid_orders (bản join+transform) ra Parquet - ghi thường
final_valid = orders_joined  # đã có customer_name, customer_type, order_level
final_valid.write.mode("overwrite").parquet(f"{OUT_DIR}/valid_orders")

# Ghi valid_orders với partitionBy("province")
final_valid.write.mode("overwrite").partitionBy("province").parquet(f"{OUT_DIR}/valid_orders_partitioned")

# Ghi invalid_orders ra output riêng
invalid_orders.write.mode("overwrite").parquet(f"{OUT_DIR}/invalid_orders")

# Ghi province_report ra Parquet
province_report.write.mode("overwrite").parquet(f"{OUT_DIR}/province_report")
# đồng thời ghi thêm bản CSV cho dễ mở xem nhanh
province_report.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT_DIR}/province_report_csv")

print(">>> Đã ghi xong: valid_orders/, valid_orders_partitioned/, invalid_orders/, province_report/")

# ---- Đọc lại và kiểm tra ----
print("\n>>> Đọc lại valid_orders và kiểm tra:")
readback = spark.read.parquet(f"{OUT_DIR}/valid_orders")
readback.printSchema()
print(f"count đọc lại: {readback.count()}  (trước khi ghi: {final_valid.count()})")

total_before = final_valid.agg(F.sum("amount")).first()[0]
total_after = readback.agg(F.sum("amount")).first()[0]
print(f"tổng amount trước khi ghi: {total_before}")
print(f"tổng amount đọc lại      : {total_after}")
print(f"KHỚP: {abs(total_before - total_after) < 1e-6}")

print("\n>>> Đọc lại valid_orders_partitioned, count theo province:")
spark.read.parquet(f"{OUT_DIR}/valid_orders_partitioned") \
    .groupBy("province").count().orderBy("province").show()

print(">>> Đọc lại province_report:")
spark.read.parquet(f"{OUT_DIR}/province_report").orderBy("province").show(truncate=False)

def list_dir(path, indent=0):
    for item in sorted(os.listdir(path)):
        full = os.path.join(path, item)
        print("  " * indent + ("[DIR] " if os.path.isdir(full) else "      ") + item)
        if os.path.isdir(full):
            list_dir(full, indent + 1)

print("\n>>> Cấu trúc output/:")
list_dir(OUT_DIR)

spark.stop()
print("\nDONE.")