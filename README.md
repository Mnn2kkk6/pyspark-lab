# PySpark Lab: Read/Write nâng cao (Schema, Validate, Parquet, Partition, Append/Overwrite)

Bài thực hành thao tác **read → validate → write → read lại** dữ liệu trong PySpark,
sử dụng schema tự khai báo, tách valid/invalid, ghi Parquet (thường & partitionBy),
và so sánh append vs overwrite.

## 1. Cấu trúc thư mục

```
.
├── README.md
├── 01_generate_data.py     # Sinh file data/orders.csv (~100+ khách hàng, có lỗi cài sẵn)
├── 02_main.py               # Toàn bộ Yêu cầu 1 -> 5
├── data/
│   └── orders.csv            # Dữ liệu đầu vào (sinh ra khi chạy bước 1)
└── output/                   # Sinh ra khi chạy bước 2
    ├── valid_orders/                 # Parquet ghi thường (Yêu cầu 3 - cách 1)
    ├── valid_orders_partitioned/     # Parquet ghi partitionBy("province") (Yêu cầu 3 - cách 2)
    ├── invalid_orders/               # Các dòng lỗi kèm error_reason
    └── append_overwrite_test/        # Thư mục demo cho Yêu cầu 4
```

## 2. Yêu cầu cài đặt

- Python 3.9+
- Java 17/21 (Spark cần JVM)
- PySpark: `pip install pyspark`

## 3. Cách chạy

```bash
# Bước 1: sinh dữ liệu đầu vào (khoảng 100-105 dòng, ~15 dòng lỗi có chủ đích)
python 01_generate_data.py

# Bước 2: chạy toàn bộ pipeline read -> validate -> write -> read lại
python 02_main.py
```

Script `02_main.py` sẽ tự xoá thư mục `output/` cũ (nếu có) trước khi chạy lại
để đảm bảo kết quả demo Yêu cầu 4 (append/overwrite) sạch và dễ kiểm chứng.

## 4. Dữ liệu đầu vào (`data/orders.csv`)

Cột: `order_id, customer_id, province, amount, status, order_date`

Được sinh ngẫu nhiên (có `seed` cố định để tái lập kết quả) cho khoảng
**102 khách hàng** (`C001` → `C102`), cộng thêm 3 dòng thủ công (`C201-C203`) —
tổng cộng khoảng **105 dòng**. Trong đó có khoảng **15 dòng bị cài lỗi chủ đích**,
rải đều các loại:

| Loại lỗi | Ví dụ |
|---|---|
| `amount` sai kiểu (không phải số) | `"abc"` |
| `amount` null (trống) | `""` |
| `amount` <= 0 | `"-50000"`, `"0"` |
| `order_date` sai format | `"05/01/2024"`, `"2024-13-40"`, `"not_a_date"`, `"2024/1/9"` |
| `province` null (trống) | `""` |
| `status` không đồng nhất hoa/thường | `"paid"`, `"PAID"`, `"Paid"`, `"PaId"` |

Muốn dữ liệu khác đi (nhiều/ít khách hàng hơn, tỉ lệ lỗi khác) → sửa các hằng số
`N_CUSTOMERS`, `PROVINCES`, hoặc tỉ lệ trong `error_indices` ở đầu file
`01_generate_data.py`.

## 5. Nội dung từng Yêu cầu trong `02_main.py`

- **Yêu cầu 1**: Khai báo `StructType` tường minh cho 6 cột (không dùng
  `inferSchema`), đọc CSV, in `printSchema()`, `count()`, và đếm số null theo
  từng cột.
- **Yêu cầu 2**: Chuẩn hoá `status` về uppercase, kiểm tra `amount > 0`,
  `order_date` đúng format `yyyy-MM-dd` (dùng `try_to_date` để tránh Spark ném
  exception khi ngày không hợp lệ ở chế độ ANSI), `province` không null/rỗng.
  Tách thành `valid_orders` / `invalid_orders`, gắn cột `error_reason`
  (`INVALID_AMOUNT`, `INVALID_DATE`, `MISSING_PROVINCE`, có thể gộp nhiều lý do
  cùng lúc bằng dấu phẩy).
- **Yêu cầu 3**: Ghi `valid_orders` ra Parquet 2 cách (ghi thường và
  `partitionBy("province")`), in cấu trúc thư mục, đếm số file `part-*` và số
  thư mục partition.
- **Yêu cầu 4**: `overwrite` lần 1 → thêm order mới → `append` → đọc lại đếm
  count → `overwrite` lần 2 → kiểm chứng dữ liệu append bị mất.
- **Yêu cầu 5**: Đọc lại Parquet, so sánh `schema`, `count`, `count theo
  province`, `tổng amount theo province` với DataFrame trước khi ghi bằng
  `exceptAll` hai chiều để xác nhận khớp tuyệt đối.

## 6. Tổng kết lý thuyết (trả lời các câu hỏi cuối bài)

**`inferSchema` khác schema tự khai báo như thế nào?**
`inferSchema=True` bắt Spark quét thêm một lượt dữ liệu để tự đoán kiểu cho
từng cột — tốn thêm thời gian và có thể đoán sai (một giá trị rác trong cột số
có thể khiến cả cột bị suy ra kiểu String). Khai `StructType` tay thì nhanh hơn
(không cần lượt quét đoán kiểu) và ép kiểu tường minh: giá trị không cast được
sẽ luôn thành `null` một cách nhất quán, dễ kiểm soát và dự đoán trước.

**Tại sao ETL thực tế thường cần kiểm soát schema?**
Dữ liệu nguồn (CSV, log, API...) không đáng tin cậy về kiểu dữ liệu. Nếu để
Spark tự suy schema, chỉ cần nguồn phát sinh thêm một dòng rác là kiểu dữ liệu
của cả cột có thể đổi ở lần chạy sau, gây lỗi âm thầm cho các bước xử lý phía
sau (dashboard, model, báo cáo...). Khai schema cứng giúp pipeline "fail sớm,
fail rõ ràng" (deterministic) thay vì âm thầm sai lệch.

**Append khác Overwrite như thế nào?**
- `overwrite`: xoá sạch thư mục đích rồi ghi lại từ đầu — dữ liệu cũ mất hoàn
  toàn, chỉ còn dữ liệu của lần ghi hiện tại.
- `append`: giữ nguyên các file cũ, chỉ thêm file `part-*` mới vào cùng thư
  mục — dữ liệu cộng dồn. Rủi ro: chạy lại cùng một batch nhiều lần bằng
  append sẽ tạo dữ liệu trùng lặp (Spark không tự khử trùng).

**`partitionBy` khi write dùng để làm gì?**
Chia dữ liệu vật lý thành các thư mục con theo giá trị của cột được chọn
(thường là cột hay dùng để lọc, ví dụ `province`, ngày). Khi truy vấn có
`WHERE province = 'HCM'`, engine chỉ cần đọc đúng thư mục `province=HCM/`
(partition pruning) thay vì quét toàn bộ dữ liệu, giúp truy vấn nhanh hơn
nhiều trên dữ liệu lớn.

**Nếu `partitionBy` một cột có quá nhiều giá trị khác nhau (cardinality cao)
thì sao?**
Sẽ tạo ra rất nhiều thư mục nhỏ (hàng nghìn/hàng triệu), mỗi thư mục chỉ chứa
vài KB dữ liệu — gọi là "small file problem". Việc này làm chậm cả ghi lẫn
đọc (quá nhiều metadata phải liệt kê/mở file), phản tác dụng so với lợi ích
partition pruning. Không nên partition theo cột như `customer_id` hay
`order_id`.

**Vì sao sau khi Spark write thường thấy nhiều file `part-*` thay vì một file
duy nhất?**
Spark xử lý dữ liệu phân tán theo nhiều partition (task) chạy song song trong
bộ nhớ. Khi ghi, mỗi partition trong DataFrame sẽ sinh ra một file `part-*`
riêng — số file gần bằng số partition của DataFrame lúc ghi. Có thể dùng
`.coalesce(1)` hoặc `.repartition(1)` để gộp về một file, nhưng với dữ liệu
lớn thường không nên làm vậy vì sẽ mất tính song song (một task duy nhất phải
gánh toàn bộ việc ghi).

## 7. Kết quả demo tham khảo (đã chạy thử với dữ liệu mẫu ~105 dòng)

```
valid_orders:   90 dòng
invalid_orders: 15 dòng
  - INVALID_AMOUNT   : phần lớn (amount null/âm/0/sai kiểu)
  - INVALID_DATE     : vài dòng (sai format ngày)
  - MISSING_PROVINCE : 1 dòng

Ghi thường  -> 1 file part-*
Ghi partitionBy("province") -> 10 thư mục province=... (tương ứng 10 tỉnh trong dữ liệu mẫu)

overwrite lần 1 -> count = 90
append +2 order  -> count = 92
overwrite lần 2  -> count = 90 (dữ liệu append đã bị thay thế/mất)

Đọc lại Parquet: schema, count, count/tổng amount theo province khớp 100%
với DataFrame trước khi ghi.
```

> Lưu ý: vì `01_generate_data.py` dùng `random.seed(42)`, chạy lại sẽ ra đúng
> cùng một bộ dữ liệu và cùng một kết quả như trên.