from confluent_kafka import Consumer
import psycopg2
import os
import json
import pandas as pd
import time # Import thêm để dễ debug

BROKER = os.getenv("BROKER_LIST", "kafka:9092")

# --- 1. Khởi tạo Kafka Consumer ---
consumer = Consumer({
    # Sử dụng BROKER từ biến môi trường đã sửa
    'bootstrap.servers': BROKER, 
    'group.id': 'postgres_loader_group_test_003', # Đổi sang Group ID mới để đọc lại data
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    
    # TĂNG THỜI GIAN CHỜ VÀ ĐỘ BỀN KẾT NỐI
    'socket.timeout.ms': 6000,
    'broker.address.family': 'v4',
    'reconnect.backoff.max.ms': 5000,
    'metadata.request.timeout.ms': 15000 
})
consumer.subscribe(['stock_price'])

# --- 2. Khởi tạo kết nối PostgreSQL ---
conn = psycopg2.connect(
    dbname='appdb',
    user='admin',
    password='admin123',
    host='postgres.postgres-namespace.svc.cluster.local',
    port='5432'
)
cursor = conn.cursor()

# --- 3. Hàm tạo bảng ---
def create_table_if_not_exists(symbol, headers, cursor, conn):
    table_name = f'stock_{symbol}'
    # Đảm bảo cột 'Thời gian' được dùng làm khóa chính hoặc có index
    columns = ','.join([f'"{header}" TEXT' for header in headers])
    
    # Loại bỏ id SERIAL PRIMARY KEY nếu đã có cột Khóa chính tự nhiên như Thời gian
    # Tuy nhiên, giữ nguyên cấu trúc cũ nếu bạn muốn id tự tăng
    query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            {columns}
        );
    '''
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")
        conn.rollback()

# --- 4. Hàm làm sạch dữ liệu (Nên đặt ngoài vòng lặp) ---
def clean_number(value):
    if value in ("-", "", None):
        return None
    # Thay thế dấu phẩy và dấu chấm, giả sử dữ liệu số nguyên hoặc float
    # Cần cẩn thận nếu dấu "." là dấu thập phân và dấu "," là dấu ngăn cách hàng nghìn
    # Code này đang xóa cả "." và "," (thường là để xử lý tiền tệ/số lớn không có thập phân)
    if isinstance(value, str):
        return value.replace(",", "").replace(".", "")
    return value

# --- 5. Hàm chèn dữ liệu ---
# Trong file load.py
def insert_data(symbol, headers, data, cursor, conn):
    table_name = f'stock_{symbol}'
    columns = ','.join([f'"{h}"' for h in headers])
    placeholders = ','.join(['%s'] * len(headers))

    query = f'''
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders});
    '''

    records_to_insert = []
    
    for idx, row in enumerate(data):
        
        # --- THAY ĐỔI QUAN TRỌNG: SỬ DỤNG .get() VỚI GIÁ TRỊ MẶC ĐỊNH ---
        # 1. Áp dụng làm sạch dữ liệu cho các cột số
        # Đảm bảo các key này tồn tại trước khi làm sạch
        row["Giá mở cửa"] = clean_number(row.get("Giá mở cửa"))
        row["Giá cao nhất"] = clean_number(row.get("Giá cao nhất"))
        row["Giá thấp nhất"] = clean_number(row.get("Giá thấp nhất"))
        row["Giá đóng cửa"] = clean_number(row.get("Giá đóng cửa"))
        row["Khối lượng"] = clean_number(row.get("Khối lượng"))

        # 2. Xử lý các hàng rỗng sau khi làm sạch
        # Kiểm tra lại các giá trị trong hàng
        if not any(v is not None and v != "" for v in row.values()):
            continue

        # 3. Sử dụng .get() để truy xuất giá trị theo headers
        values = [row.get(h, None) for h in headers] # Nếu header 'h' không tồn tại, trả về None
        
        # Kiểm tra xem có đủ số lượng giá trị không
        if len(values) != len(headers):
             # Điều này chỉ xảy ra nếu bạn can thiệp vào headers sau khi tạo
             print(f"Warning: Value count mismatch for row {idx}")
             continue
             
        records_to_insert.append(values)
        
    try:
        if records_to_insert:
             # Sử dụng executemany
             cursor.executemany(query, records_to_insert)
             conn.commit()
             print(f"Successfully inserted {len(records_to_insert)} records for symbol: {symbol}")
    except Exception as e:
        # Nếu lỗi vẫn xảy ra ở đây, nó là lỗi PostgreSQL (ví dụ: kiểu dữ liệu, ràng buộc)
        print(f"Error inserting data into DB for {symbol}: {e}")
        conn.rollback()

# --- 6. Vòng lặp chính xử lý Kafka (Đã loại bỏ logic xử lý trước vòng lặp) ---
print("Starting Kafka consumer loop...")

try:
    while True:
        # poll(1.0) trả về tin nhắn, lỗi, hoặc None (nếu timeout)
        msg = consumer.poll(1.0) 
        
        if msg is None:
            # print("Waiting for message...")
            continue
        
        # Xử lý lỗi Consumer/Kafka
        if msg.error():
            # -191 là lỗi EOF (End of Partition) - không phải lỗi nghiêm trọng
            if msg.error().code() != -191:
                print(f"Consumer error: {msg.error()}")
            continue

        # --- Bắt đầu xử lý tin nhắn hợp lệ ---
        try:
            # msg.value() có thể là None (tin nhắn tombstone)
            if msg.value() is None:
                # print(f"Received None value (tombstone) message for key: {msg.key()}")
                continue
                
            payload = json.loads(msg.value().decode('utf-8'))
            symbol = payload.get('symbol')
            data = payload.get("data")
            
            if not symbol or not data or not isinstance(data, list) or not data:
                print(f"Skipping message with invalid or missing 'symbol' or 'data'. Payload: {payload}")
                continue
                
            headers = list(data[0].keys())

            # --- 7. Xử lý lưu CSV (Trong vòng lặp) ---
            save_dir = os.path.expanduser("~/stock_predict/Load/data")
            os.makedirs(save_dir, exist_ok=True)
            csv_path = os.path.join(save_dir, f"{symbol}.csv")
            
            df = pd.DataFrame(data)
            
            # Chỉ ghi tiêu đề nếu file chưa tồn tại
            if not os.path.exists(csv_path):
                df.to_csv(csv_path, index=False, mode='w')
                # print(f"Created new CSV file: {csv_path}")
            else:
                # Ghi tiếp (không ghi tiêu đề)
                df.to_csv(csv_path, index=False, mode='a', header=False)
                # print(f"Appended to CSV file: {csv_path}")
            
            # --- 8. Tạo bảng và chèn dữ liệu vào PostgreSQL ---
            create_table_if_not_exists(symbol, headers, cursor, conn)
            insert_data(symbol, headers, data, cursor, conn)

        except json.JSONDecodeError:
            print("Error: Could not decode JSON from message value.")
        except Exception as e:
            print(f"Unhandled error during message processing: {e}")

except KeyboardInterrupt:
    print("\nStopping consumer.")
except Exception as e:
    print(f"Fatal error: {e}")

finally:
    # --- 9. Dọn dẹp tài nguyên ---
    if 'consumer' in locals() and consumer:
        consumer.close()
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'conn' in locals() and conn:
        conn.close()
    print("Resources closed.")