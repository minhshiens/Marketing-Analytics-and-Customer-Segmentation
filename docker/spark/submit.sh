#!/bin/bash

# Dừng script ngay lập tức nếu có lỗi
set -e

# Kiểm tra xem người dùng có cung cấp đường dẫn đến script Python không
if [ -z "$1" ]; then
  echo "Lỗi: Vui lòng cung cấp đường dẫn đến file Python cần chạy."
  echo "Cách dùng: $0 /path/to/your/script.py"
  exit 1
fi

echo "======================================================================="
echo "Bắt đầu gửi Spark Job..."
echo "Master: spark://spark-master:7077"
echo "Script: $1"
echo "======================================================================="

# Đường dẫn đến thư mục cài đặt Spark bên trong container bitnami
SPARK_HOME=/opt/spark

# Đây là lệnh chính để gửi job đến Spark Master
$SPARK_HOME/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.postgresql:postgresql:42.6.0 \
  "$1" # Tham số đầu tiên truyền vào script, chính là đường dẫn file .py

echo "======================================================================="
echo "Spark Job đã hoàn thành."
echo "======================================================================="
