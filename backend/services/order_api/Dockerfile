# ---------------------------------------------------------
# Sử dụng image Python 3.9 bản slim (nhẹ hơn bản full)
# Đây sẽ là môi trường chạy code Python trong container
# ---------------------------------------------------------
FROM python:3.9-slim


# ---------------------------------------------------------
# Đặt thư mục làm việc chính trong container là /app
# Tất cả lệnh tiếp theo sẽ chạy trong thư mục này
# ---------------------------------------------------------
WORKDIR /app


# ---------------------------------------------------------
# PYTHONUNBUFFERED=1
# giúp log Python in ra ngay lập tức (không bị buffer)
# rất quan trọng khi xem logs bằng docker compose logs
# ---------------------------------------------------------
ENV PYTHONUNBUFFERED=1


# ---------------------------------------------------------
# Thêm /app vào PYTHONPATH
# để Python có thể import các module như:
# common.db.mysql
# common.mq.rabbitmq
# ---------------------------------------------------------
ENV PYTHONPATH=/app


# ---------------------------------------------------------
# Copy file requirements.txt từ project vào container
# Docker chỉ copy riêng file này trước để tối ưu cache build
# ---------------------------------------------------------
COPY services/order_api/requirements.txt /app/requirements.txt


# ---------------------------------------------------------
# Cài đặt các thư viện Python cần thiết cho service
# --no-cache-dir giúp image nhẹ hơn
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
# Lệnh chạy container khi container start
# Uvicorn sẽ chạy FastAPI application
#
# services.order_api.app.main:app
# nghĩa là:
#   file: services/order_api/app/main.py
#   object FastAPI: app
#
# host 0.0.0.0 để container có thể nhận request từ bên ngoài
# port 8000 là port bên trong container
# ---------------------------------------------------------
CMD ["uvicorn", "services.order_api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]