# ---------------------------------------------------------
# Thư viện xử lý JSON (dùng để đọc message từ RabbitMQ)
# ---------------------------------------------------------
import json

# ---------------------------------------------------------
# Thư viện time dùng để sleep (giả lập xử lý thanh toán)
# ---------------------------------------------------------
import time

# ---------------------------------------------------------
# Import hàm kết nối RabbitMQ và tên queue
# ---------------------------------------------------------
from common.mq.rabbitmq import connect, QUEUE_NAME

# ---------------------------------------------------------
# Import hàm kết nối MySQL (để update trạng thái đơn hàng)
# ---------------------------------------------------------
from common.db.mysql import get_mysql_conn

# ---------------------------------------------------------
# Import hàm kết nối PostgreSQL (để lưu transaction)
# ---------------------------------------------------------
from common.db.postgres import get_pg_conn


def handle_message(ch, method, properties, body):
    """
    Hàm xử lý message khi Worker nhận được từ RabbitMQ.

    Flow:
    1) Parse JSON message
    2) sleep 2s giả lập xử lý thanh toán
    3) Insert dữ liệu vào PostgreSQL (payments)
    4) Update trạng thái đơn hàng trong MySQL → COMPLETED
    5) Gửi ACK để RabbitMQ xoá message khỏi queue
    """

    try:
        # -------------------------------------------------
        # BƯỚC 1: Decode message từ RabbitMQ
        # body là dạng bytes → chuyển sang string → parse JSON
        # -------------------------------------------------
        msg = json.loads(body.decode("utf-8"))

        # Lấy thông tin từ message
        order_id = msg["order_id"]
        user_id = msg["user_id"]
        product_id = msg["product_id"]
        quantity = msg["quantity"]

        print(f"[INFO] Received order #{order_id}. Processing...")

        # -------------------------------------------------
        # BƯỚC 2: Giả lập xử lý thanh toán (sleep 2 giây)
        # -------------------------------------------------
        time.sleep(2)

        # -------------------------------------------------
        # BƯỚC 3: Insert transaction vào PostgreSQL
        # -------------------------------------------------
        pg = get_pg_conn()
        pg_cur = pg.cursor()

        try:
            pg_cur.execute(
                "INSERT INTO payments (order_id, user_id, product_id, quantity) VALUES (%s,%s,%s,%s)",
                (order_id, user_id, product_id, quantity)
            )

            # Lưu thay đổi vào database
            pg.commit()

        finally:
            # Đóng cursor và connection
            pg_cur.close()
            pg.close()

        # -------------------------------------------------
        # BƯỚC 4: Update trạng thái đơn hàng trong MySQL
        # -------------------------------------------------
        my = get_mysql_conn()
        my_cur = my.cursor()

        try:
            my_cur.execute(
                "UPDATE orders SET status=%s WHERE id=%s",
                ("COMPLETED", order_id)
            )

            my.commit()

        finally:
            my_cur.close()
            my.close()

        # -------------------------------------------------
        # BƯỚC 5: Gửi ACK
        # ACK = xác nhận đã xử lý xong message
        # RabbitMQ sẽ xoá message khỏi queue
        # -------------------------------------------------
        ch.basic_ack(delivery_tag=method.delivery_tag)

        print(f"[INFO] Order #{order_id} synced. ACK sent.")

    except Exception as e:
        # -------------------------------------------------
        # Nếu xảy ra lỗi trong quá trình xử lý
        # -------------------------------------------------
        print(f"[ERROR] Worker failed: {e}")

        # NACK = không xác nhận message
        # requeue=True → message sẽ quay lại queue để xử lý lại
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    """
    Hàm main để khởi động Worker service
    """

    # -------------------------------------------------
    # Kết nối tới RabbitMQ
    # -------------------------------------------------
    conn, ch = connect()

    # -------------------------------------------------
    # QoS: chỉ xử lý 1 message tại 1 thời điểm
    # giúp tránh overload worker
    # -------------------------------------------------
    ch.basic_qos(prefetch_count=1)

    # -------------------------------------------------
    # Đăng ký consumer
    # Khi có message mới trong queue → gọi handle_message
    # -------------------------------------------------
    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)

    print("[INFO] Worker started. Waiting for messages...")

    # -------------------------------------------------
    # Bắt đầu lắng nghe queue liên tục
    # -------------------------------------------------
    ch.start_consuming()


# ---------------------------------------------------------
# Chạy chương trình khi container start
# ---------------------------------------------------------
if __name__ == "__main__":
    main()