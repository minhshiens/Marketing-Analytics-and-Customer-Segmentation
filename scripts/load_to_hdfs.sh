#!/bin/bash


DATA_DIR="./data/ads/raw_data"
REQUIRED_FILES=("clicks_train.csv" "events.csv")

echo "🔍 Kiểm tra dữ liệu đầu vào (Giả lập HDFS Check)..."

if [ ! -d "$DATA_DIR" ]; then
  echo "❌ Lỗi: Thư mục dữ liệu không tồn tại: $DATA_DIR"
  echo "👉 Hãy tạo thư mục và copy file csv vào đó."
  exit 1
fi

for file in "${REQUIRED_FILES[@]}"
do
  if [ -f "$DATA_DIR/$file" ]; then
    echo "✅ Đã tìm thấy file: $file"
  else
    echo "⚠️ Cảnh báo: Thiếu file $file trong $DATA_DIR"
  fi
done

echo "------------------------------------------------"
echo "ℹ️  Lưu ý: Hệ thống hiện tại dùng Shared Volume."
echo "   Spark container sẽ tự động nhìn thấy dữ liệu tại /app/data/ads/raw_data"
echo "   Không cần upload thủ công."
