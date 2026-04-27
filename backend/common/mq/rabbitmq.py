import os
import time
import json
import pika


# ---------------------------------------------------------
# Lấy tên queue từ biến môi trường
# Nếu không có thì mặc định là "order_queue"
# ---------------------------------------------------------
QUEUE_NAME = os.getenv("RABBIT_QUEUE", "order_queue")

# ---------------------------------------------------------
# Host của RabbitMQ
# Trong docker-compose ta đặt tên service là "rabbitmq"
# nên các container khác kết nối bằng hostname "rabbitmq"
# ---------------------------------------------------------
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")


def connect():
    """
    Hàm tạo kết nối tới RabbitMQ.

    Vì RabbitMQ container có thể khởi động chậm hơn
    các service Python (API, Worker),
    nên ta dùng vòng lặp retry cho đến khi kết nối thành công.

    Hàm trả về:
        connection  : kết nối tới RabbitMQ
        channel     : kênh giao tiếp để publish / consume message
    """

    while True:
        try:
            # Tạo connection tới RabbitMQ server
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST)
            )

            # Tạo channel (giống như "session" để gửi nhận message)
            ch = conn.channel()

            # Khai báo queue
            # durable=True nghĩa là queue sẽ tồn tại lâu dài
            # kể cả khi RabbitMQ restart
            ch.queue_declare(queue=QUEUE_NAME, durable=True)

            return conn, ch

        except Exception as e:
            # Nếu RabbitMQ chưa sẵn sàng thì in log
            print(f"[WARN] RabbitMQ not ready ({e}). Retry in 5s...")

            # Đợi 5 giây rồi thử lại
            time.sleep(5)


def publish_message(message_dict: dict):
    """
    Hàm gửi (publish) một message vào RabbitMQ queue.

    message_dict: dữ liệu dạng dictionary
    Ví dụ:
        {
            "order_id": 1,
            "user_id": 1,
            "product_id": 101,
            "quantity": 2
        }

    Message sẽ được chuyển thành JSON và gửi vào queue.
    """

    # Kết nối tới RabbitMQ
    conn, ch = connect()

    try:
        # Gửi message vào queue
        ch.basic_publish(

            # exchange="" nghĩa là gửi trực tiếp vào queue
            exchange="",

            # routing_key là tên queue
            routing_key=QUEUE_NAME,

            # Chuyển dictionary thành JSON rồi encode thành bytes
            body=json.dumps(message_dict).encode("utf-8"),

            # delivery_mode=2 nghĩa là message durable
            # message sẽ không bị mất nếu RabbitMQ restart
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

    finally:
        # Sau khi publish xong thì đóng connection
        conn.close()