# ---------------------------------------------------------
# Import thư viện FastAPI để tạo REST API
# ---------------------------------------------------------
from fastapi import FastAPI

# ---------------------------------------------------------
# Import schema dùng để validate dữ liệu request
# và tạo message gửi vào RabbitMQ
# ---------------------------------------------------------
from common.schemas.order import OrderCreate, OrderMessage

# ---------------------------------------------------------
# Import hàm kết nối MySQL
# ---------------------------------------------------------
from common.db.mysql import get_mysql_conn

# ---------------------------------------------------------
# Import hàm publish message vào RabbitMQ queue
# ---------------------------------------------------------
from common.mq.rabbitmq import publish_message

# ---------------------------------------------------------
# Tạo FastAPI application
# Đây là object chính của API server
# ---------------------------------------------------------
app = FastAPI()

# ---------------------------------------------------------
# Endpoint tạo đơn hàng
# URL: POST /api/orders
# ---------------------------------------------------------
@app.post("/api/orders")
def create_order(order: OrderCreate):
    """
    Flow xử lý khi client gửi request tạo đơn hàng:

    1) Validate dữ liệu đầu vào
       FastAPI + Pydantic sẽ kiểm tra:
       - user_id >= 1
       - product_id >= 1
       - quantity > 0

    2) Insert đơn hàng vào MySQL
       status = PENDING (đang chờ xử lý)

    3) Publish message vào RabbitMQ queue
       Worker sẽ đọc message này để xử lý thanh toán

    4) Trả response ngay cho client
       Không chờ worker xử lý xong (asynchronous)
    """

    # -----------------------------------------------------
    # BƯỚC 2: Insert đơn hàng vào MySQL
    # -----------------------------------------------------

    # Tạo kết nối database
    conn = get_mysql_conn()

    # Tạo cursor để thực thi câu lệnh SQL
    cur = conn.cursor()

    try:
        # Thực hiện câu lệnh INSERT vào bảng orders
        cur.execute(
            "INSERT INTO orders (user_id, product_id, quantity, status) VALUES (%s,%s,%s,%s)",
            (order.user_id, order.product_id, order.quantity, "PENDING")
        )

        # Lấy ID của đơn hàng vừa tạo
        order_id = cur.lastrowid

        # Lưu thay đổi vào database
        conn.commit()

    finally:
        # Đóng cursor và connection để tránh leak tài nguyên
        cur.close()
        conn.close()


    # -----------------------------------------------------
    # BƯỚC 3: Tạo message gửi vào RabbitMQ
    # -----------------------------------------------------

    # Tạo message theo schema OrderMessage
    msg = OrderMessage(
        order_id=order_id,
        user_id=order.user_id,
        product_id=order.product_id,
        quantity=order.quantity
    ).model_dump()

    # Publish message vào RabbitMQ queue
    publish_message(msg)


    # -----------------------------------------------------
    # BƯỚC 4: Trả response ngay cho client
    # Không chờ worker xử lý xong
    # -----------------------------------------------------
    return {
        "message": "Order received",
        "order_id": order_id
    }