"""
01_generate_data.py
Tạo file orders.csv với khoảng 100 khách hàng (đơn hàng), trong đó CHỦ ĐỘNG
cài một số dòng lỗi rải rác để phục vụ bài thực hành:
- amount sai kiểu / null / <= 0
- order_date sai format
- province null
- status viết hoa/thường không đồng nhất

Chạy: python3 01_generate_data.py
Kết quả: data/orders.csv
"""

import csv
import os
import random

random.seed(42)  # để kết quả tái lập được (reproducible)

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

PROVINCES = ["Hanoi", "HCM", "Da Nang", "Can Tho", "Hai Phong",
             "Binh Duong", "Nghe An", "Hue", "Khanh Hoa", "Bac Ninh"]

STATUS_VARIANTS = {
    "paid":      ["paid", "PAID", "Paid", "PaId"],
    "pending":   ["pending", "PENDING", "Pending"],
    "cancelled": ["cancelled", "CANCELLED", "Cancelled"],
}

N_CUSTOMERS = 102  # ~100 người, cộng thêm 2-3 người cho tự nhiên

rows = []
order_id = 1

for i in range(1, N_CUSTOMERS + 1):
    customer_id = f"C{i:03d}"
    province = random.choice(PROVINCES)
    status_key = random.choices(
        ["paid", "pending", "cancelled"], weights=[0.7, 0.15, 0.15]
    )[0]
    status = random.choice(STATUS_VARIANTS[status_key])

    month = random.randint(1, 9)
    day = random.randint(1, 28)
    order_date = f"2024-{month:02d}-{day:02d}"

    amount = round(random.uniform(50_000, 800_000), -3)  # số tiền "đẹp"

    rows.append([str(order_id), customer_id, province, str(amount), status, order_date])
    order_id += 1

# ---- Cài lỗi có chủ đích vào khoảng 15% số dòng ----
n_rows = len(rows)
error_indices = random.sample(range(n_rows), k=max(12, n_rows // 8))

# chia đều các loại lỗi cho các index được chọn
error_types = (["bad_amount_text"] * (len(error_indices) // 4) +
               ["negative_or_zero_amount"] * (len(error_indices) // 4) +
               ["null_amount"] * (len(error_indices) // 4) +
               ["bad_date_format"] * (len(error_indices) // 4))
# bù cho tròn số nếu chia dư
while len(error_types) < len(error_indices):
    error_types.append(random.choice(
        ["bad_amount_text", "negative_or_zero_amount", "null_amount",
         "bad_date_format", "null_province"]))
random.shuffle(error_types)

for idx, err in zip(error_indices, error_types):
    if err == "bad_amount_text":
        rows[idx][3] = "abc"                       # amount sai kiểu
    elif err == "negative_or_zero_amount":
        rows[idx][3] = random.choice(["-50000", "0"])  # amount <= 0
    elif err == "null_amount":
        rows[idx][3] = ""                           # amount null
    elif err == "bad_date_format":
        rows[idx][5] = random.choice(["05/01/2024", "2024-13-40", "not_a_date", "2024/1/9"])
    elif err == "null_province":
        rows[idx][2] = ""                            # province null

# thêm vài dòng thủ công để chắc chắn có đủ các loại lỗi mẫu (giống bản demo ban đầu)
extra_rows = [
    [str(order_id),     "C201", "Da Nang", "-50000",  "Cancelled", "2024-01-07"],
    [str(order_id + 1), "C202", "",        "300000",  "Paid",      "2024-01-09"],
    [str(order_id + 2), "C203", "HCM",     "",        "PENDING",   "2024-01-10"],
]
rows.extend(extra_rows)

path = os.path.join(OUTPUT_DIR, "orders.csv")
with open(path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "province", "amount", "status", "order_date"])
    writer.writerows(rows)

print(f"Đã tạo {path} với {len(rows)} dòng ({N_CUSTOMERS} khách hàng gốc + {len(extra_rows)} dòng bổ sung).")
print(f"Số dòng được cài lỗi có chủ đích: {len(error_indices) + len(extra_rows)}")
