from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- 1. Định nghĩa hàm thực thi ---
def my_task():
    print("Hello from Airflow DAG!")

# --- 2. Thiết lập default arguments ---
default_args = {
    'owner': 'airflow',                # người sở hữu DAG
    'depends_on_past': False,          # có phụ thuộc vào lần chạy trước không
    'email_on_failure': False,         # gửi email khi lỗi
    'email_on_retry': False,           # gửi email khi retry
    'retries': 1,                      # số lần retry nếu task fail
    'retry_delay': timedelta(minutes=5) # thời gian chờ giữa các lần retry
}

# --- 3. Khởi tạo DAG ---
with DAG(
    dag_id='example_dag',               # tên DAG (duy nhất)
    default_args=default_args,          # truyền cấu hình mặc định
    description='Ví dụ DAG cơ bản',     
    schedule_interval=timedelta(days=1),# tần suất chạy (cron hoặc timedelta)
    start_date=datetime(2025, 1, 1),    # ngày bắt đầu
    catchup=False,                      # không chạy bù các lần bị bỏ lỡ
    tags=['example'],                   # gắn tag cho dễ tìm
) as dag:

    # --- 4. Khai báo các task ---
    task1 = PythonOperator(
        task_id='print_hello',         # tên task
        python_callable=my_task,       # hàm thực thi
    )

    task2 = PythonOperator(
        task_id='print_world',
        python_callable=lambda: print("World!"),
    )

    # --- 5. Thiết lập quan hệ phụ thuộc ---
    task1 >> task2  # nghĩa là task1 chạy trước task2
