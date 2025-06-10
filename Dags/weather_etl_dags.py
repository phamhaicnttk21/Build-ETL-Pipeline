from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# --- Thiết lập đường dẫn động cho thư mục 'scripts' ---
# Điều này đảm bảo Airflow có thể tìm và import các hàm từ các file Python của bạn.
# 'os.path.dirname(__file__)' là đường dẫn của thư mục 'dags'.
# 'os.path.join(..., '../scripts')' sẽ đưa chúng ta đến thư mục 'scripts' ngang cấp.
scripts_path = os.path.join(os.path.dirname(__file__), '..', 'scripts')
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

# --- Import các hàm ETL của bạn ---
# Đảm bảo tên hàm và tên file khớp với cấu trúc bạn đã tạo.
from extract_data import extract_weather_data
from transform_data import transform_weather_data
from load_data import load_weather_data

# --- Cấu hình chung cho DAG và đường dẫn dữ liệu trong container ---
# Đây là nơi dữ liệu thô và đã xử lý sẽ được lưu trữ tạm thời bên trong container Airflow.
# Nhớ rằng thư mục 'data' trên host của bạn phải được mount vào '/opt/airflow/data'
# trong file docker-compose.yaml để các file này được lưu trữ bền vững.
AIRFLOW_DATA_DIR = "/opt/airflow/data"
RAW_DATA_DIR = os.path.join(AIRFLOW_DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(AIRFLOW_DATA_DIR, "processed")

# --- Định nghĩa các tham số mặc định cho DAG và tác vụ ---
default_args = {
    'owner': 'airflow',                # Chủ sở hữu của DAG
    'depends_on_past': False,          # Không phụ thuộc vào các lần chạy trước
    'email_on_failure': False,         # Không gửi email khi tác vụ thất bại
    'email_on_retry': False,           # Không gửi email khi thử lại tác vụ
    'retries': 1,                      # Thử lại tác vụ 1 lần nếu thất bại
    'retry_delay': timedelta(minutes=5), # Chờ 5 phút trước khi thử lại
}

# --- Định nghĩa DAG của bạn ---
with DAG(
    dag_id='openweather_etl_pipeline_structured', # ID duy nhất của DAG
    default_args=default_args,                     # Áp dụng các tham số mặc định
    description='A structured ETL pipeline for OpenWeatherMap data', # Mô tả ngắn gọn
    schedule_interval=timedelta(hours=1),          # Lịch trình: chạy mỗi giờ một lần
    start_date=datetime(2023, 1, 1),               # Ngày bắt đầu cho DAG (không chạy trước ngày này)
    catchup=False,                                 # Không chạy lại các DAG bị bỏ lỡ từ start_date
    tags=['weather', 'etl', 'openweather', 'structured'], # Các tag giúp tìm kiếm và lọc DAG
) as dag:
    # --- Tác vụ 1: Extract dữ liệu từ API ---
    extract_task = PythonOperator(
        task_id='extract_weather_data',      # ID duy nhất cho tác vụ này
        python_callable=extract_weather_data, # Hàm Python sẽ được gọi
        op_kwargs={                          # Các đối số được truyền vào hàm
            'city': 'Hanoi',                 # Có thể thay đổi thành phố hoặc lấy từ Airflow Variable
            'output_dir': RAW_DATA_DIR       # Thư mục lưu dữ liệu thô trong container
        },
        do_xcom_push=True,                   # Quan trọng: Đẩy giá trị trả về (đường dẫn file) vào XCom
                                             # để các tác vụ sau có thể sử dụng.
    )

    # --- Tác vụ 2: Transform (Làm sạch và biến đổi) dữ liệu ---
    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
        op_kwargs={
            # Lấy đường dẫn file thô từ kết quả của 'extract_task' thông qua XCom
            'input_file_path': "{{ task_instance.xcom_pull(task_ids='extract_weather_data') }}",
            'output_dir': PROCESSED_DATA_DIR # Thư mục lưu dữ liệu đã xử lý trong container
        },
        do_xcom_push=True,                   # Đẩy đường dẫn file đã xử lý vào XCom
    )

    # --- Tác vụ 3: Load dữ liệu vào PostgreSQL ---
    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
        op_kwargs={
            # Lấy đường dẫn file đã xử lý từ kết quả của 'transform_task' thông qua XCom
            'input_file_path': "{{ task_instance.xcom_pull(task_ids='transform_weather_data') }}",
            'table_name': 'weather_data'     # Tên bảng trong PostgreSQL để tải dữ liệu vào
        },
    )

    # --- Định nghĩa luồng công việc (Task Dependencies) ---
    # Luồng này chỉ ra rằng:
    # extract_task phải hoàn thành trước khi transform_task bắt đầu,
    # và transform_task phải hoàn thành trước khi load_task bắt đầu.
    extract_task >> transform_task >> load_task