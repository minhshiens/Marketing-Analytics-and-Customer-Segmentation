import pandas as pd
import os

# --- CẤU HÌNH ĐƯỜNG DẪN ---
RAW_DATA_PATH = "data/ads/raw_data/clicks_train.csv"
OUTPUT_PATH = "data/processed/processed_ads_stats.csv" 

def process_real_data():
    print(f"🔄 Đang đọc dữ liệu từ: {RAW_DATA_PATH}")
    
    if not os.path.exists(RAW_DATA_PATH):
        print(f"❌ Lỗi: Không tìm thấy file tại {RAW_DATA_PATH}")
        return

    # 1. Đọc dữ liệu 
    try:
        df = pd.read_csv(RAW_DATA_PATH)
        print(f"✅ Đã tải xong {len(df):,} dòng dữ liệu thô.")
    except Exception as e:
        print(f"❌ Lỗi đọc file CSV: {e}")
        return

    print("⚙️ Đang tổng hợp số liệu theo từng Quảng cáo (ad_id)...")

    # 2. Group by ad_id để tính toán

    ads_stats = df.groupby('ad_id')['clicked'].agg(['count', 'sum']).reset_index()
    
    # 3. Đổi tên cột cho dễ hiểu
    ads_stats.columns = ['ad_id', 'total_views', 'total_clicks']

    # 4. Tính CTR
    ads_stats['ctr'] = (ads_stats['total_clicks'] / ads_stats['total_views']) * 100

    # 5. Sắp xếp giảm dần theo lượt click để dễ nhìn
    ads_stats = ads_stats.sort_values(by='total_clicks', ascending=False)

    # 6. Lưu file kết quả
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    ads_stats.to_csv(OUTPUT_PATH, index=False)
    
    print("-" * 30)
    print(f"🎉 XỬ LÝ HOÀN TẤT!")
    print(f"📂 File kết quả: {OUTPUT_PATH}")
    print(f"📊 Tổng số Ads tìm thấy: {len(ads_stats)}")
    print(ads_stats.head())

if __name__ == "__main__":
    process_real_data()