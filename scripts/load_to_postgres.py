import pandas as pd
from sqlalchemy import create_engine
import os
import sys

# --- CẤU HÌNH KẾT NỐI ---
DB_HOST = os.getenv("DB_HOST", "localhost") 
DB_PORT = os.getenv("DB_PORT", "5432")

DB_USER = os.getenv("POSTGRES_USER", "minhshiens")  
DB_PASS = os.getenv("POSTGRES_PASSWORD", "minhshiens123") 
DB_NAME = os.getenv("POSTGRES_DB", "bigdata_db")

def load_csv_to_postgres(csv_file_path, table_name):
    print(f"🔗 Đang kết nối đến Postgres tại: {DB_HOST}:{DB_PORT}...")
    
    # Tạo chuỗi kết nối
    connection_str = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_str)

    try:
        # Đọc CSV
        print(f"📖 Đang đọc file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        # Đẩy vào DB
        print(f"⚙️ Đang insert {len(df)} dòng vào bảng '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"✅ Thành công! Bảng '{table_name}' đã được cập nhật.")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Sử dụng: python load_to_postgres.py <đường_dẫn_csv> <tên_bảng>")
        csv_path = "data/processed/segment_summary.csv"
        if os.path.exists(csv_path):
            load_csv_to_postgres(csv_path, "customer_segments")
    else:
        load_csv_to_postgres(sys.argv[1], sys.argv[2])
