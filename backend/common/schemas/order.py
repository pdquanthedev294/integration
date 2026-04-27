from pydantic import BaseModel, Field


# -----------------------------------------------------------
# Schema dùng cho API khi nhận request tạo đơn hàng
# -----------------------------------------------------------
class OrderCreate(BaseModel):
    """
    Model này dùng để validate dữ liệu đầu vào của API /api/orders.

    Khi client (Postman / Frontend) gửi JSON lên,
    FastAPI sẽ tự động kiểm tra dữ liệu theo schema này.
    Nếu dữ liệu sai định dạng hoặc không hợp lệ
    thì API sẽ trả lỗi ngay lập tức.
    """

    # ID của người dùng đặt hàng
    # ge=1 nghĩa là giá trị phải >= 1
    user_id: int = Field(..., ge=1)

    # ID của sản phẩm
    # ge=1 nghĩa là giá trị phải >= 1
    product_id: int = Field(..., ge=1)

    # Số lượng sản phẩm đặt mua
    # gt=0 nghĩa là phải > 0 (không được âm hoặc bằng 0)
    quantity: int = Field(..., gt=0)


# -----------------------------------------------------------
# Schema dùng cho message gửi vào RabbitMQ
# -----------------------------------------------------------
class OrderMessage(BaseModel):
    """
    Model này định nghĩa cấu trúc dữ liệu của message
    được gửi vào RabbitMQ queue.

    Order API sẽ gửi message theo cấu trúc này
    và Order Worker sẽ đọc message theo cùng format.
    """

    # ID của đơn hàng trong MySQL
    order_id: int

    # ID người dùng
    user_id: int

    # ID sản phẩm
    product_id: int

    # Số lượng sản phẩm đặt
    quantity: int