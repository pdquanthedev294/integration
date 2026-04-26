# Sử dụng image Python chính thức (bản nhẹ)
FROM python:3.9-slim

# Thư mục làm việc bên trong container
# Mọi lệnh sau sẽ chạy trong /app
WORKDIR /app

# In log ra ngay lập tức (không bị buffer)
# Giúp docker logs hiển thị realtime
ENV PYTHONUNBUFFERED=1

# Cho phép Python import module từ thư mục /app
# Nhờ vậy mới import được common.db.mysql
ENV PYTHONPATH=/app


# ==============================
# CÀI ĐẶT THƯ VIỆN
# ==============================

# Copy file requirements.txt vào container
COPY services/legacy_adapter/requirements.txt /app/requirements.txt

# Cài đặt các thư viện cần thiết
RUN pip install --no-cache-dir -r /app/requirements.txt


# ==============================
# COPY SOURCE CODE
# ==============================

# Copy toàn bộ thư mục backend vào container
# Bao gồm:
# - common/
# - services/
# Nhờ vậy legacy_adapter có thể import common.*
COPY . /app


# ==============================
# LỆNH CHẠY CONTAINER
# ==============================

# Khi container start, chạy file main.py của legacy_adapter
CMD ["python", "services/legacy_adapter/app/main.py"]