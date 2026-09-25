"""
02_main.py
Bài thực hành: Read/Write nâng cao trong PySpark
Yêu cầu 1 -> 5 đầy đủ, có in log ở từng bước để kiểm tra.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)
from datetime import datetime
import shutil
import os

spark = (
    SparkSession.builder
    .appName("OrdersReadWriteLab")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

DATA_PATH = "data/orders.csv"
OUT_DIR = "output"

# dọn output cũ để chạy lại từ đầu cho sạch (chỉ chạy lần đầu tiên trong bài demo)
if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)

# =========================================================================
# YÊU CẦU 1 - ĐỌC DỮ LIỆU VỚI SCHEMA TỰ KHAI BÁO (KHÔNG dùng inferSchema)
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 1: Đọc dữ liệu với schema tự khai báo")
print("=" * 70)

orders_schema = StructType([
    StructField("order_id",    IntegerType(), True),
    StructField("customer_id", StringType(),  True),
    StructField("province",    StringType(),  True),
    StructField("amount",      DoubleType(),  True),   # amount "abc" -> Spark sẽ parse thành null
    StructField("status",      StringType(),  True),
    StructField("order_date",  StringType(),  True),   # để StringType, tự validate format sau
])

df_raw = (
    spark.read
    .option("header", True)
    .schema(orders_schema)          # <-- schema tự khai báo, KHÔNG dùng inferSchema
    .csv(DATA_PATH)
)

print(">>> Schema:")
df_raw.printSchema()

total_count = df_raw.count()
print(f">>> Tổng số dòng đọc được: {total_count}")

print(">>> Dữ liệu null / parse không đúng theo từng cột:")
df_raw.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_raw.columns
]).show()

print(">>> Toàn bộ dữ liệu thô sau khi áp schema:")
df_raw.show(truncate=False)


# =========================================================================
# YÊU CẦU 2 - XỬ LÝ DỮ LIỆU TRƯỚC KHI GHI
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 2: Chuẩn hóa + validate + tách valid/invalid")
print("=" * 70)

# Chuẩn hóa status -> uppercase (kể cả null giữ nguyên null)
df_norm = df_raw.withColumn("status", F.upper(F.trim(F.col("status"))))

# Điều kiện hợp lệ cho từng trường
cond_amount_ok = F.col("amount").isNotNull() & (F.col("amount") > 0)
def _is_valid_date(s):
    """Trả về True nếu s đúng format yyyy-MM-dd và là ngày hợp lệ (không dùng
    to_date()/try_to_date() để tránh phụ thuộc phiên bản Spark / ANSI mode)."""
    if s is None or len(s) != 10:
        return False
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False

is_valid_date_udf = F.udf(_is_valid_date, BooleanType())
cond_date_ok = is_valid_date_udf(F.col("order_date"))
cond_province_ok = F.col("province").isNotNull() & (F.trim(F.col("province")) != "")

# Gắn lý do lỗi (có thể có nhiều lý do cùng lúc -> nối chuỗi lại)
df_checked = (
    df_norm
    .withColumn("err_amount",   F.when(~cond_amount_ok, F.lit("INVALID_AMOUNT")))
    .withColumn("err_date",     F.when(~cond_date_ok, F.lit("INVALID_DATE")))
    .withColumn("err_province", F.when(~cond_province_ok, F.lit("MISSING_PROVINCE")))
    .withColumn(
        "error_reason",
        F.array_join(
            F.array_except(
                F.array(F.col("err_amount"), F.col("err_date"), F.col("err_province")),
                F.array(F.lit(None).cast("string"))
            ),
            ","
        )
    )
    .drop("err_amount", "err_date", "err_province")
)

valid_orders = df_checked.filter(F.col("error_reason") == "").drop("error_reason")
invalid_orders = df_checked.filter(F.col("error_reason") != "")

print(f">>> valid_orders: {valid_orders.count()} dòng")
valid_orders.show(truncate=False)

print(f">>> invalid_orders: {invalid_orders.count()} dòng")
invalid_orders.select(
    "order_id", "province", "amount", "status", "order_date", "error_reason"
).show(truncate=False)


# =========================================================================
# YÊU CẦU 3 - GHI PARQUET (2 CÁCH)
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 3: Ghi Parquet - cách thường vs partitionBy")
print("=" * 70)

# Cách 1: ghi bình thường
valid_orders.write.mode("overwrite").parquet(f"{OUT_DIR}/valid_orders")

# Cách 2: ghi có partitionBy("province")
valid_orders.write.mode("overwrite").partitionBy("province").parquet(f"{OUT_DIR}/valid_orders_partitioned")

# ghi invalid_orders để tham khảo
invalid_orders.write.mode("overwrite").parquet(f"{OUT_DIR}/invalid_orders")

def list_dir(path, indent=0):
    for item in sorted(os.listdir(path)):
        full = os.path.join(path, item)
        print("  " * indent + ("[DIR] " if os.path.isdir(full) else "      ") + item)
        if os.path.isdir(full):
            list_dir(full, indent + 1)

print(">>> Cấu trúc thư mục valid_orders (ghi bình thường):")
list_dir(f"{OUT_DIR}/valid_orders")

print("\n>>> Cấu trúc thư mục valid_orders_partitioned (partitionBy province):")
list_dir(f"{OUT_DIR}/valid_orders_partitioned")

n_part_plain = len([f for f in os.listdir(f"{OUT_DIR}/valid_orders") if f.startswith("part-")])
print(f"\n>>> Số file part-* trong valid_orders (không partition): {n_part_plain}")

province_dirs = [d for d in os.listdir(f"{OUT_DIR}/valid_orders_partitioned") if d.startswith("province=")]
print(f">>> Số thư mục con theo province trong valid_orders_partitioned: {len(province_dirs)} -> {province_dirs}")


# =========================================================================
# YÊU CẦU 4 - APPEND VÀ OVERWRITE
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 4: Append vs Overwrite")
print("=" * 70)

APPEND_TEST_DIR = f"{OUT_DIR}/append_overwrite_test"

# Ghi lần đầu bằng overwrite
valid_orders.write.mode("overwrite").parquet(APPEND_TEST_DIR)
c1 = spark.read.parquet(APPEND_TEST_DIR).count()
print(f">>> Sau overwrite lần 1: count = {c1}")

# Tạo thêm order mới
new_orders_data = [
    (101, "C101", "Hanoi", 200000.0, "PAID", "2024-02-01"),
    (102, "C102", "HCM",   350000.0, "PAID", "2024-02-02"),
]
new_orders = spark.createDataFrame(
    new_orders_data,
    schema=["order_id", "customer_id", "province", "amount", "status", "order_date"]
)

# Ghi tiếp bằng append
new_orders.write.mode("append").parquet(APPEND_TEST_DIR)
c2 = spark.read.parquet(APPEND_TEST_DIR).count()
print(f">>> Sau append thêm {new_orders.count()} order mới: count = {c2} "
      f"(kỳ vọng {c1} + {new_orders.count()} = {c1 + new_orders.count()})")

# Chạy lại bằng overwrite (ghi đè lại bằng valid_orders ban đầu, KHÔNG có new_orders)
valid_orders.write.mode("overwrite").parquet(APPEND_TEST_DIR)
c3 = spark.read.parquet(APPEND_TEST_DIR).count()
print(f">>> Sau overwrite lần 2 (ghi lại valid_orders gốc): count = {c3} "
      f"-> dữ liệu append trước đó {'CÒN' if c3 == c2 else 'ĐÃ BỊ THAY THẾ (mất)'}")


# =========================================================================
# YÊU CẦU 5 - ĐỌC LẠI VÀ VALIDATE OUTPUT
# =========================================================================
print("\n" + "=" * 70)
print("YÊU CẦU 5: Đọc lại Parquet và validate")
print("=" * 70)

df_readback = spark.read.parquet(f"{OUT_DIR}/valid_orders")
print(">>> Schema đọc lại:")
df_readback.printSchema()

print(f">>> Total count đọc lại: {df_readback.count()} "
      f"(so với valid_orders trước khi ghi: {valid_orders.count()})")

print(">>> Count theo province (đọc lại từ Parquet không-partition):")
df_readback.groupBy("province").count().orderBy("province").show()

print(">>> Tổng amount theo province (đọc lại):")
df_readback.groupBy("province").agg(F.sum("amount").alias("total_amount")).orderBy("province").show()

# So sánh với DataFrame gốc trước khi ghi
agg_before = valid_orders.groupBy("province").agg(F.sum("amount").alias("total_amount")).orderBy("province")
agg_after = df_readback.groupBy("province").agg(F.sum("amount").alias("total_amount")).orderBy("province")

diff = agg_before.exceptAll(agg_after).unionAll(agg_after.exceptAll(agg_before))
if diff.count() == 0:
    print(">>> KẾT LUẬN: Dữ liệu sau khi ghi/đọc lại KHỚP HOÀN TOÀN với DataFrame trước khi write.")
else:
    print(">>> KẾT LUẬN: CÓ SỰ KHÁC BIỆT giữa dữ liệu trước và sau khi ghi/đọc lại!")
    diff.show()

# Đọc lại bản partitioned và kiểm tra province nằm đúng partition
print("\n>>> Đọc lại valid_orders_partitioned, kiểm tra cột province tự suy ra từ folder:")
df_part_readback = spark.read.parquet(f"{OUT_DIR}/valid_orders_partitioned")
df_part_readback.groupBy("province").count().orderBy("province").show()

spark.stop()
print("\nDONE.")