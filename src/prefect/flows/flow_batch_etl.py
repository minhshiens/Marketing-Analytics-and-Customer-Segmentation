from prefect import flow, task
import pandas as pd
import os


INPUT_DIR = "/app/data/ads/raw_data"
OUTPUT_FILE = "/app/data/ads/processed_ads_stats.csv"

@task(name="Read CSV Data", log_prints=True)
def read_data():
    """Đọc dữ liệu từ file csv thô"""
    clicks_path = os.path.join(INPUT_DIR, "clicks_train.csv")
    
    if not os.path.exists(clicks_path):
        raise FileNotFoundError(f"❌ Không tìm thấy file tại: {clicks_path}")
        
    print(f"📥 Đang đọc file: {clicks_path}")
    # Đọc thử 100.000 dòng đầu nếu file quá nặng
    df = pd.read_csv(clicks_path, nrows=100000) 
    return df

@task(name="Clean & Aggregate", log_prints=True)
def process_data(df):
    """Xử lý dữ liệu: Tính toán thống kê cơ bản"""
    print("⚙️ Đang xử lý dữ liệu...")
    
    # Ví dụ: Group by theo ad_id hoặc platform (nếu có cột đó)
    # Giả sử file clicks_train có cột 'ad_id' và 'clicked' (0 hoặc 1)
    
    if 'clicked' in df.columns and 'ad_id' in df.columns:
        stats = df.groupby('ad_id').agg(
            total_views=('clicked', 'count'),
            total_clicks=('clicked', 'sum')
        ).reset_index()
        
        # Tính CTR
        stats['ctr'] = (stats['total_clicks'] / stats['total_views']) * 100
        return stats
    else:
        print("⚠️ Không tìm thấy cột 'clicked' hoặc 'ad_id', trả về dữ liệu gốc")
        return df

@task(name="Save Processed Data", log_prints=True)
def save_data(df):
    """Lưu kết quả đã xử lý để Streamlit đọc"""
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Đã lưu kết quả tại: {OUTPUT_FILE}")

@flow(name="Batch Ads Processing")
def batch_ads_etl():
    raw_df = read_data()
    clean_df = process_data(raw_df)
    save_data(clean_df)

if __name__ == "__main__":
    batch_ads_etl.deploy(
        name="deploy-batch-ads",
        work_pool_name="process-pool", 
        cron="0 9 * * *", # (Tùy chọn) Chạy định kỳ 9h sáng
        job_variables={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}}
        storage=LocalFileSystem(storage_path="local-storage")
    )
