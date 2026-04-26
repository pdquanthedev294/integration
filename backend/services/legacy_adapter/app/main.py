import os
import time
import csv
import shutil
from datetime import datetime
from common.db.mysql import get_mysql_conn

# ==============================
# CẤU HÌNH CHUNG
# ==============================

INPUT_DIR = "/app/input"
PROCESSED_DIR = "/app/processed"
POLL_INTERVAL = 5


# ==============================
# HÀM CHUYỂN STRING → INT AN TOÀN
# ==============================
def safe_int(value):
    """
    Chuyển value sang số nguyên.
    Nếu lỗi → trả về None (tránh crash)
    """
    try:
        return int(value)
    except:
        return None


# ==============================
# HÀM PHÁT HIỆN SQL INJECTION
# ==============================
def is_sql_injection(value):
    """
    Kiểm tra chuỗi có chứa dấu hiệu SQL Injection không
    """
    if value is None:
        return False

    # Các pattern nguy hiểm thường gặp
    patterns = [
        ";", "--", "/*", "*/",
        "DROP", "SELECT", "INSERT", "DELETE", "UPDATE",
        "' OR", "\" OR"
    ]

    value_lower = str(value).lower()

    for p in patterns:
        if p.lower() in value_lower:
            return True

    return False


# ==============================
# HÀM XỬ LÝ 1 FILE CSV
# ==============================
def process_inventory_file(filepath):
    """
    Đọc file CSV → validate → xử lý lỗi → update DB
    """

    conn = get_mysql_conn()
    cursor = conn.cursor()

    processed = 0
    skipped = 0

    with open(filepath, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                # ==============================
                # 1. DETECT SQL INJECTION
                # ==============================
                if any(is_sql_injection(v) for v in row.values()):
                    skipped += 1
                    continue

                # ==============================
                # 2. VALIDATE DỮ LIỆU
                # ==============================
                pid = safe_int(row.get("product_id"))
                qty = safe_int(row.get("quantity"))

                # Nếu lỗi dữ liệu → bỏ qua
                if pid is None or qty is None:
                    skipped += 1
                    continue

                # quantity không được âm
                if qty < 0:
                    skipped += 1
                    continue

                # ==============================
                # 3. UPDATE DATABASE (AN TOÀN)
                # ==============================
                cursor.execute(
                    "UPDATE products SET stock=%s WHERE id=%s",
                    (qty, pid)
                )

                processed += 1

            except Exception as e:
                # Nếu lỗi bất kỳ → bỏ qua dòng đó
                skipped += 1

    # Lưu DB
    conn.commit()
    conn.close()

    # ==============================
    # 4. DI CHUYỂN FILE SAU KHI XỬ LÝ
    # ==============================
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"inventory_{ts}.csv"

    shutil.move(filepath, os.path.join(PROCESSED_DIR, new_name))

    print(f"[INFO] Processed {processed} records. Skipped {skipped} invalid records.")


# ==============================
# HÀM POLLING
# ==============================
def start_polling():
    """
    Chạy liên tục → quét thư mục input mỗi 5 giây
    """

    print("[INFO] Legacy Adapter started...")

    # Tạo thư mục nếu chưa có
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    while True:
        file_list = os.listdir(INPUT_DIR)

        for file_name in file_list:
            if file_name.endswith(".csv"):
                full_path = os.path.join(INPUT_DIR, file_name)

                print("[INFO] Found file:", file_name)

                process_inventory_file(full_path)

        time.sleep(POLL_INTERVAL)


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    start_polling()