import sys
import os

# Thêm thư mục gốc vào đường dẫn hệ thống để có thể import các module khác
sys.path.append(os.getcwd())

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# --- CẤU HÌNH ---
# Đường dẫn này tính từ thư mục gốc của dự án (nơi chứa folder src, data, models...)
INPUT_DATA_PATH = "data/processed/user_features.parquet"
MODEL_SAVE_PATH = "models/kmeans_model"
FINAL_K = 4  

def get_spark_session():
    return SparkSession.builder \
        .appName("KMeans_Training_Job") \
        .config("spark.diriver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def train_kmeans():
    print("--- BẮT ĐẦU QUÁ TRÌNH HUẤN LUYỆN (BATCH) ---")
    
    spark = get_spark_session()

    if not os.path.exists(INPUT_DATA_PATH):
        print(f"LỖI: Không tìm thấy file dữ liệu tại {INPUT_DATA_PATH}")
        return

    print(f"Đang đọc dữ liệu từ: {INPUT_DATA_PATH}")
    data = spark.read.parquet(INPUT_DATA_PATH)

    # 3. Thiết lập K-Means
    # Lưu ý: Cột chứa vector đặc trưng phải tên là 'features' (do preprocess tạo ra)
    print(f"Đang cấu hình KMeans với K = {FINAL_K}...")
    kmeans = KMeans(featuresCol='features', k=FINAL_K, seed=42)

    # 4. Train Model
    print("Đang training... (Vui lòng đợi)")
    model = kmeans.fit(data)
    
    cost = model.summary.trainingCost
    print(f"Training hoàn tất! Chi phí (WSSE): {cost}")

    # 6. LƯU MODEL (QUAN TRỌNG NHẤT)
    print(f"Đang lưu model vào: {MODEL_SAVE_PATH}")
    # write().overwrite() giúp ghi đè nếu model cũ đã tồn tại
    model.write().overwrite().save(MODEL_SAVE_PATH)

    print("--- LƯU MODEL THÀNH CÔNG ---")

if __name__ == "__main__":
    train_kmeans()