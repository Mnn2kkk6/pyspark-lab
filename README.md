# PySpark Lab: Read/Write nâng cao & Bài tổng hợp (read → clean → validate → dedup → join → transform → aggregate → write → check)

Repo này gồm **2 bài thực hành nối tiếp nhau**, dùng chung một bộ dữ liệu
sinh ra từ `01_generate_data.py`:

- **Bài 1** (`02_main.py`): Read với schema, validate, ghi Parquet (thường &
  `partitionBy`), so sánh `append` vs `overwrite`, đọc lại kiểm tra.
- **Bài 2** (`03_main_full.py`): Ghép thêm `customers.csv`, dedup bằng
  `Window`, `left join`, transform, aggregate theo tỉnh, rồi write + check.

## 1. Cấu trúc thư mục

```
.
├── README.md                 # file này
├── 01_generate_data.py       # sinh data/orders.csv + data/customers.csv
├── 02_main.py                # BÀI 1: Read/Write nâng cao
├── 03_main_full.py           # BÀI 2: bài tổng hợp
├── data/
│   ├── orders.csv             # order_id, customer_id, province, amount,
│   │                           # status, order_date, updated_at
│   └── customers.csv          # customer_id, customer_name, customer_type
├── output_bai1/                # sinh ra khi chạy 02_main.py
│   ├── valid_orders/
│   ├── valid_orders_partitioned/
│   ├── invalid_orders/
│   └── append_overwrite_test/
└── output_bai2/                # sinh ra khi chạy 03_main_full.py
    ├── valid_orders/                # đã join với customers + có order_level
    ├── valid_orders_partitioned/
    ├── invalid_orders/
    ├── province_report/             # Parquet
    └── province_report_csv/         # CSV (1 file, để mở xem nhanh)
```

> **Lưu ý quan trọng:** `output_bai1/` và `output_bai2/` là 2 thư mục
> **riêng biệt**. Mỗi script chỉ xoá và ghi vào đúng thư mục output của
> chính nó khi chạy lại (`shutil.rmtree` chỉ target đúng `OUT_DIR` của
> script đó) — chạy `02_main.py` không ảnh hưởng đến `output_bai2/` và
> ngược lại, nên có thể chạy cả 2 bài tuần tự mà không lo mất kết quả của
> bài kia.

## 2. Yêu cầu cài đặt

- Python 3.9+
- Java 17/21 (Spark cần JVM)
- PySpark: `pip install pyspark`

## 3. Cách chạy

```bash
# Bước 1: sinh dữ liệu đầu vào (chung cho cả 2 bài)
python 01_generate_data.py

# Bước 2: chạy Bài 1 (Read/Write nâng cao) -> output_bai1/
python 02_main.py

# Bước 3: chạy Bài 2 (bài tổng hợp) -> output_bai2/
python 03_main_full.py
```

Có thể chạy `02_main.py` và `03_main_full.py` theo thứ tự bất kỳ, chạy đi
chạy lại nhiều lần — không đụng dữ liệu của nhau.

## 4. Dữ liệu đầu vào

`01_generate_data.py` dùng `random.seed(42)` nên **chạy lại luôn ra đúng
cùng một bộ dữ liệu** (để kết quả demo tái lập được).

**`data/customers.csv`** — 100 khách hàng (`C001`–`C100`):
`customer_id, customer_name, customer_type` (`customer_type` ∈
{Individual, Business}).

**`data/orders.csv`** — khoảng 111 dòng:
`order_id, customer_id, province, amount, status, order_date, updated_at`.

Các trường hợp lỗi/đặc biệt được **cài chủ đích**:

| Trường hợp | Cách cài | Dùng ở bài nào |
|---|---|---|
| `amount` null / ≤ 0 / sai kiểu | `""`, `"0"`, `"-50000"`, `"abc"` | Bài 1 & 2 |
| `order_date` sai format | `"05/01/2024"`, `"2024-13-40"`, `"not_a_date"`, `"2024/1/9"`, `"31/01/2024"` | Bài 1 & 2 |
| `province` null | Ô trống | Bài 1 |
| `status` hoa/thường lẫn lộn | `"paid"`, `"PAID"`, `"Paid"`, `"PaId"`... | Bài 1 & 2 |
| `order_id` duplicate | 6 `order_id` bị nhân đôi, bản ghi mới có `updated_at` khác (thường muộn hơn) và `status` khác | Bài 2 |
| `customer_id` không tồn tại | 5 dòng dùng `C901`/`C902`/`C903` — không có trong `customers.csv` | Bài 2 |

## 5. Bài 1 — `02_main.py` (Read/Write nâng cao)

- **Yêu cầu 1**: `StructType` tường minh cho `orders` (không `inferSchema`),
  đọc CSV, `printSchema()`, `count()`, đếm null theo từng cột.
- **Yêu cầu 2**: Chuẩn hoá `status` → uppercase, validate `amount > 0`,
  `order_date` đúng format (dùng UDF Python `datetime.strptime`, không phụ
  thuộc `to_date`/`try_to_date` theo phiên bản Spark), `province` không
  null/rỗng. Tách `valid_orders` / `invalid_orders` với cột `error_reason`
  (`INVALID_AMOUNT`, `INVALID_DATE`, `MISSING_PROVINCE`).
- **Yêu cầu 3**: Ghi `valid_orders` ra Parquet 2 cách (ghi thường và
  `partitionBy("province")`), in cấu trúc thư mục, đếm số file `part-*` và
  số thư mục partition.
- **Yêu cầu 4**: `overwrite` lần 1 → thêm order mới → `append` → đọc lại
  đếm count → `overwrite` lần 2 → kiểm chứng dữ liệu append bị mất.
- **Yêu cầu 5**: Đọc lại Parquet, so sánh `schema`, `count`, `count theo
  province`, `tổng amount theo province` với DataFrame trước khi ghi.

**Kết quả demo:** 105 dòng đọc vào → 90 valid / 15 invalid → ghi thường 1
file `part-*`, `partitionBy` ra 10 thư mục `province=.../` → append/
overwrite: 90 → 92 (append) → 90 (overwrite, dữ liệu append mất) → đọc lại
khớp 100% với dữ liệu trước khi ghi.

## 6. Bài 2 — `03_main_full.py` (bài tổng hợp)

- **Yêu cầu 1 – Read + Clean**: `StructType` riêng cho `orders` và
  `customers`. Chuẩn hoá `status` → uppercase, parse `updated_at` thành
  `timestamp` thật (`to_timestamp`), validate `order_date` bằng UDF,
  validate `amount > 0`.
- **Yêu cầu 2 – Deduplicate + Validate**: Dùng
  `Window.partitionBy("order_id").orderBy(updated_at_ts desc)` +
  `row_number() == 1` để **giữ bản ghi mới nhất** cho mỗi `order_id`
  duplicate (111 → 105 dòng). Tách `valid_orders` / `invalid_orders` với
  `error_reason` (`INVALID_AMOUNT`, `INVALID_DATE`, `CUSTOMER_NOT_FOUND`).
- **Yêu cầu 3 – Join + Transform**: `left join` `valid_orders` với
  `customers` theo `customer_id`; kiểm tra không còn dòng nào thiếu
  `customer_name` sau join (đúng, vì `CUSTOMER_NOT_FOUND` đã bị loại ở
  Yêu cầu 2). Thêm cột `order_level`: `amount >= 500_000` → HIGH,
  `>= 200_000` → MEDIUM, còn lại LOW.
- **Yêu cầu 4 – Aggregate**: `groupBy("province")` tính `total_orders`,
  `total_customers` (distinct), `total_amount`, `avg_amount`,
  `success_orders` (status = PAID), `failed_orders` (status = CANCELLED).
- **Yêu cầu 5 – Write + Check**: Ghi `valid_orders` (đã join, có
  `order_level`) ra Parquet 2 cách (thường & `partitionBy("province")`),
  ghi riêng `invalid_orders` và `province_report` (Parquet + CSV). Đọc lại
  tất cả, so sánh `schema`, `count`, và **tổng `amount`** trước/sau ghi.

**Kết quả demo:** 111 dòng đọc vào → dedup còn 105 → 86 valid / 19 invalid
→ left join: 0 dòng thiếu customer → report đủ 10 tỉnh với 6 chỉ số → đọc
lại Parquet: count khớp (86), tổng `amount` khớp tuyệt đối trước/sau ghi.

## 7. Tổng hợp phần trả lời lý thuyết

### Chung cho cả 2 bài

**`inferSchema` khác schema tự khai báo như thế nào?**
`inferSchema=True` bắt Spark quét thêm một lượt dữ liệu để tự đoán kiểu cho
từng cột — tốn thêm thời gian và có thể đoán sai (một giá trị rác trong cột
số có thể khiến cả cột bị suy ra kiểu String). Khai `StructType` tay thì
nhanh hơn (không cần lượt quét đoán kiểu) và ép kiểu tường minh: giá trị
không cast được sẽ luôn thành `null` một cách nhất quán, dễ kiểm soát.

**Tại sao ETL thực tế thường cần kiểm soát schema?**
Dữ liệu nguồn (CSV, log, API...) không đáng tin cậy về kiểu dữ liệu. Nếu để
Spark tự suy schema, chỉ cần nguồn phát sinh thêm một dòng rác là kiểu dữ
liệu của cả cột có thể đổi ở lần chạy sau, gây lỗi âm thầm cho các bước xử
lý phía sau (dashboard, model, báo cáo...). Khai schema cứng giúp pipeline
"fail sớm, fail rõ ràng" thay vì âm thầm sai lệch.

**`partitionBy` khi write dùng để làm gì?**
Chia dữ liệu vật lý thành các thư mục con theo giá trị của cột được chọn
(thường là cột hay dùng để lọc, ví dụ `province`). Khi truy vấn có
`WHERE province = 'HCM'`, engine chỉ cần đọc đúng thư mục `province=HCM/`
(partition pruning) thay vì quét toàn bộ dữ liệu, giúp truy vấn nhanh hơn
nhiều trên dữ liệu lớn.

**Nếu `partitionBy` một cột có cardinality cao thì sao?**
Sẽ tạo ra rất nhiều thư mục nhỏ (hàng nghìn/hàng triệu), mỗi thư mục chỉ
chứa vài KB dữ liệu — "small file problem". Việc này làm chậm cả ghi lẫn
đọc (quá nhiều metadata phải liệt kê/mở file), phản tác dụng so với lợi ích
partition pruning. Không nên partition theo cột như `customer_id` hay
`order_id`.

**Vì sao sau khi Spark write thường thấy nhiều file `part-*` thay vì một
file duy nhất?**
Spark xử lý dữ liệu phân tán theo nhiều partition (task) chạy song song
trong bộ nhớ. Khi ghi, mỗi partition trong DataFrame sinh ra một file
`part-*` riêng — số file gần bằng số partition của DataFrame lúc ghi. Có
thể `.coalesce(1)`/`.repartition(1)` để gộp về 1 file, nhưng với dữ liệu
lớn thường không nên vì mất tính song song.

### Riêng cho Bài 1

**Append khác Overwrite như thế nào?**
- `overwrite`: xoá sạch thư mục đích rồi ghi lại từ đầu — dữ liệu cũ mất
  hoàn toàn.
- `append`: giữ nguyên file cũ, chỉ thêm file `part-*` mới vào cùng thư
  mục — dữ liệu cộng dồn. Rủi ro: chạy lại cùng một batch nhiều lần bằng
  append sẽ tạo dữ liệu trùng lặp (Spark không tự khử trùng).

### Riêng cho Bài 2

**Vì sao dùng `Window` thay vì `dropDuplicates()`?**
`dropDuplicates()` chỉ loại các dòng **hoàn toàn trùng nhau** trên các cột
chỉ định, và nếu 2 dòng trùng `order_id` nhưng khác các cột khác (khác
`updated_at`, khác `status`...), Spark **không đảm bảo giữ lại dòng nào**
— kết quả không xác định. Với `Window`, ta chỉ định rõ ràng thứ tự ưu tiên
(`orderBy(updated_at desc)`) rồi lọc `row_number() == 1`, nên luôn giữ đúng
bản ghi mong muốn một cách nhất quán, có thể tái lập.

**Vì sao dùng `left join`?**
Mục tiêu là giữ toàn bộ `orders` (kể cả đơn không map được customer) để
phát hiện `CUSTOMER_NOT_FOUND`. Nếu dùng `inner join`, các đơn có
`customer_id` không tồn tại sẽ **biến mất âm thầm** khỏi kết quả mà không
có cách nào phát hiện ra.

**Nếu `amount`/`order_date` sai thì nên xử lý như nào trong ETL?**
Không nên âm thầm sửa/đoán giá trị (vd. tự set `amount = 0` hay
`order_date` = ngày hiện tại) vì sẽ làm sai lệch số liệu báo cáo mà không
ai biết. Cách đúng: tách riêng ra `invalid_orders` kèm `error_reason` rõ
ràng, để `valid_orders` luôn sạch (dùng an toàn cho báo cáo), còn
`invalid_orders` được lưu lại để tra soát, đối chiếu, sửa tại nguồn rồi
nạp lại.

**Trong bài này bước nào có khả năng gây shuffle?**
- `Window.partitionBy("order_id").orderBy(...)` — cần gom các dòng cùng
  `order_id` vào cùng partition trước khi tính `row_number()`.
- `join` (`orders` với `customers`) — cần shuffle theo khóa `customer_id`
  để ghép đúng cặp (trừ khi Spark tối ưu bằng broadcast join do
  `customers` khá nhỏ).
- `groupBy("province").agg(...)` — cần gom các dòng cùng `province` lại
  trước khi tính aggregate.
- `partitionBy("province")` khi ghi — Spark cũng phải sắp xếp/gom dữ liệu
  theo `province` trước khi ghi ra từng thư mục.
