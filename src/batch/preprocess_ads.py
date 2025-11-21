import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, sum

# --- CẤU HÌNH ĐƯỜNG DẪN ---
EVENTS_PATH = "data/ads/raw_data/events.csv"
CLICKS_PATH = "data/ads/raw_data/clicks_train.csv"
OUTPUT_PATH = "data/processed/user_features.parquet"

def get_spark_session():
    return SparkSession.builder \
        .appName("Batch_Preprocessing_Ads") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def preprocess_ads():
    print("--- BẮT ĐẦU XỬ LÝ DỮ LIỆU BATCH (FIXED JOIN) ---")
    
    if not os.path.exists(EVENTS_PATH) or not os.path.exists(CLICKS_PATH):
        print(f"LỖI: Không tìm thấy file dữ liệu.")
        return

    spark = get_spark_session()

    # 1. ĐỌC DỮ LIỆU
    print("Đang đọc dữ liệu...")
    # Events chứa thông tin: display_id -> uuid
    df_events = spark.read.csv(EVENTS_PATH, header=True, inferSchema=True)
    # Clicks chứa thông tin: display_id -> clicked (0 hoặc 1)
    df_clicks = spark.read.csv(CLICKS_PATH, header=True, inferSchema=True)
    
    USER_COL = "uuid"
    
    # 2. TÍNH TOÁN SỐ LẦN XEM (Total Ads Seen)
    # Mỗi dòng trong events là một lần hiển thị quảng cáo cho user
    print("Đang tính số lần xem quảng cáo...")
    ads_seen_df = df_events.groupBy("uuid").agg(count("*").alias("total_ads_seen"))

    # 3. TÍNH TOÁN SỐ LẦN CLICK (QUAN TRỌNG: SỬA LỖI TẠI ĐÂY)
    print("Đang tính số lần click (Join Events + Clicks)...")
    
    # Bước 3a: Join clicks với events dựa trên 'display_id' để biết ai (uuid) đã click
    actual_clicks = df_clicks.filter(col("clicked") == 1)
    
    # Join với events để lấy uuid
    joined_clicks = actual_clicks.join(df_events, on="display_id", how="inner")
    
    # Bước 3b: Group by uuid và đếm
    clicks_count_df = joined_clicks.groupBy("uuid").agg(count("*").alias("total_clicks"))

    # 4. TỔNG HỢP CUỐI CÙNG (Merge 2 kết quả)
    print("Đang gộp dữ liệu User Features...")
    # Left join từ danh sách người xem sang danh sách người click
    user_features = ads_seen_df.join(clicks_count_df, on="uuid", how="left")
    
    # Những ai xem mà chưa bao giờ click (null) thì điền 0
    user_features = user_features.fillna(0, subset=["total_clicks"])

    # 5. TÍNH CTR
    user_features = user_features.withColumn(
        "click_through_rate", 
        col("total_clicks") / col("total_ads_seen")
    )

    # 6. VECTOR ASSEMBLER
    from pyspark.ml.feature import VectorAssembler
    print("Đang tạo vector đặc trưng...")
    assembler = VectorAssembler(
        inputCols=["total_ads_seen", "total_clicks", "click_through_rate"],
        outputCol="features"
    )
    final_data = assembler.transform(user_features)

    # 7. LƯU FILE
    print(f"Đang lưu kết quả vào: {OUTPUT_PATH}")
    final_data.select(
        USER_COL, 
        "total_ads_seen", 
        "total_clicks", 
        "click_through_rate", 
        "features"
    ).write.mode("overwrite").parquet(OUTPUT_PATH)
    
    print("--- THÀNH CÔNG! ---")

if __name__ == "__main__":
    preprocess_ads()