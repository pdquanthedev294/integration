import os
import time
import mysql.connector

def get_mysql_conn():
  """
  Hàm tạo kết nối tới MySQL.
  Hàm này đọc thông tin kết nối từ biến môi trường (environment variables)
  được cấu hình trong docker-compose.yml.

  Nếu MySQL chưa sẵn sàng (ví dụ container MySQL khởi động chậm),
  hàm sẽ retry sau mỗi 5 giây cho đến khi kết nối thành công.
  """
  # Lấy thông tin kết nối từ environment variables
  # Nếu không tồn tại thì dùng giá trị mặc định bên phải
  host = os.getenv("MYSQL_HOST", "mysql") # Tên service MySQL trong docker-compose.yml
  user = os.getenv("MYSQL_USER", "root") # Tên user MySQL
  password = os.getenv("MYSQL_PASSWORD", "123456") # Mật khẩu MySQL
  database = os.getenv("MYSQL_DATABASE", "noah_webstore_db") # Tên database MySQL
  port = int(os.getenv("MYSQL_PORT", "3306")) # Cổng MySQL

  while True:
    try:
      return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
      )

    except Exception as e:
      print(f"[WARN] MySQL not ready ({e}). Retry in 5s...")
      time.sleep(5)