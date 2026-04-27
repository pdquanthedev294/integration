# ---------------------------------------------------------
# Sử dụng image Python 3.9 bản slim (nhẹ hơn bản full)
# Đây là môi trường chạy code Python trong container
# ---------------------------------------------------------
FROM python:3.9-slim


# ---------------------------------------------------------
# Đặt thư mục làm việc trong container là /app
# Mọi lệnh phía dưới sẽ chạy trong thư mục này
# ---------------------------------------------------------
WORKDIR /app


# ---------------------------------------------------------
# PYTHONUNBUFFERED=1
# giúp log Python in ra ngay lập tức (không bị buffer)
# Điều này rất hữu ích khi xem log bằng docker compose logs
# ---------------------------------------------------------
ENV PYTHONUNBUFFERED=1


# ---------------------------------------------------------
# Thêm /app vào PYTHONPATH
# để Python có thể import các module trong project như:
# common.db.mysql
# common.db.postgres
# common.mq.rabbitmq
# ---------------------------------------------------------
ENV PYTHONPATH=/app


# ---------------------------------------------------------
# Copy file requirements.txt của order_worker vào container
# Docker làm bước này trước để tối ưu cache build
# ---------------------------------------------------------
COPY services/order_worker/requirements.txt /app/requirements.txt


# ---------------------------------------------------------
# Cài đặt các thư viện Python cần thiết cho Worker
# --no-cache-dir giúp giảm kích thước image
# ---------------------------------------------------------
RUN pip install --no-cache-dir -r /app/requirements.txt


# ---------------------------------------------------------
# Copy toàn bộ source code của backend vào container
# Bao gồm:
# - common/
# - services/
# - configs...
# ---------------------------------------------------------
COPY . /app


# ---------------------------------------------------------
# Lệnh chạy khi container khởi động
# Worker sẽ chạy file main.py
#
# File này sẽ:
# - Lắng nghe RabbitMQ queue
# - Consume message (đơn hàng)
# - Insert dữ liệu vào PostgreSQL
# - Update trạng thái đơn hàng trong MySQL
# ---------------------------------------------------------
CMD ["python", "services/order_worker/app/main.py"]