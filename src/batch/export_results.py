
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# --- CẤU HÌNH ---
FEATURES_PATH = "data/processed/user_features.parquet"
SEGMENTS_PATH = "data/processed/customer_segments.parquet"
OUTPUT_CSV_PATH = "data/processed/final_report.csv" # File này để Streamlit đọc
SUMMARY_CSV_PATH = "data/processed/segment_summary.csv"

def get_spark_session():
    return SparkSession.builder \
        .appName("Export_Results") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def export_results():
    print("--- BẮT ĐẦU XUẤT KHẨU KẾT QUẢ ---")
    spark = get_spark_session()

    # 1. Đọc dữ liệu
    if not os.path.exists(FEATURES_PATH) or not os.path.exists(SEGMENTS_PATH):
        print("LỖI: Thiếu file đầu vào. Hãy chạy preprocess và train trước!")
        return

    print("Đang đọc dữ liệu đặc trưng và kết quả phân cụm...")
    df_features = spark.read.parquet(FEATURES_PATH)
    df_segments = spark.read.parquet(SEGMENTS_PATH)

    # 2. Ghép dữ liệu (Join)
    # Ghép để biết: User A (Nhóm 1) có bao nhiêu click, ctr bao nhiêu?
    print("Đang ghép dữ liệu...")
    final_df = df_features.join(df_segments, on="uuid", how="inner")

    # 3. Tạo bảng thống kê tóm tắt (Quan trọng cho Dashboard)
    print("Đang tính toán thống kê từng nhóm...")
    summary_df = final_df.groupBy("prediction").agg(
        count("uuid").alias("num_users"),
        avg("total_ads_seen").alias("avg_ads_seen"),
        avg("total_clicks").alias("avg_clicks"),
        avg("click_through_rate").alias("avg_ctr")
    ).orderBy("prediction")

    
    summary_df.show()

    # 4. Xuất ra CSV 
    print(f"Đang lưu báo cáo vào {SUMMARY_CSV_PATH}...")
    summary_df.toPandas().to_csv(SUMMARY_CSV_PATH, index=False)
    

    print("--- XUẤT KHẨU HOÀN TẤT ---")

if __name__ == "__main__":
    export_results()