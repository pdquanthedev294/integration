import os
import time
import psycopg2

def get_pg_conn():
  """
    Hàm tạo kết nối tới PostgreSQL database.

    Hàm này đọc thông tin kết nối từ biến môi trường (environment variables)
    được cấu hình trong docker-compose.yml.

    Nếu PostgreSQL chưa sẵn sàng (ví dụ container Postgres khởi động chậm),
    hàm sẽ tự động retry sau mỗi 5 giây cho đến khi kết nối thành công.
  """
  # Vòng lặp retry kết nối database
  # Điều này cần thiết khi chạy Docker vì database thường khởi động chậm hơn các service Python (API, Worker, Adapter)

  while True:
      try:
          # Tạo kết nối tới PostgreSQL
          return psycopg2.connect(

              # Host của PostgreSQL container
              # Trong Docker network ta dùng tên service "postgres"
              host=os.getenv("POSTGRES_HOST", "postgres"),

              # Username đăng nhập PostgreSQL
              user=os.getenv("POSTGRES_USER", "postgres"),

              # Password đăng nhập PostgreSQL
              password=os.getenv("POSTGRES_PASSWORD", "123456"),

              # Tên database cần kết nối
              dbname=os.getenv("POSTGRES_DB", "finance"),

              # Port của PostgreSQL trong container
              port=int(os.getenv("POSTGRES_PORT", "5432")),
          )

      except Exception as e:
          # Nếu kết nối thất bại (ví dụ DB chưa sẵn sàng)
          # in ra cảnh báo để dễ debug
          print(f"[WARN] Postgres not ready ({e}). Retry in 5s...")

          # Chờ 5 giây trước khi thử lại
          time.sleep(5)

