#!/bin/bash

# Cách dùng: ./scripts/run_spark_job.sh src/batch/process_ads_data.py

JOB_PATH=$1
SPARK_MASTER_CONTAINER="spark-master"

# Kiểm tra xem người dùng có nhập tên file không
if [ -z "$JOB_PATH" ]; then
  echo "❌ Lỗi: Vui lòng nhập đường dẫn file script (Ví dụ: src/batch/process_ads_data.py)"
  exit 1
fi

echo "🚀 Đang submit job: $JOB_PATH lên Spark Cluster..."


docker exec -it $SPARK_MASTER_CONTAINER /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.2.18 \
  --driver-memory 1G \
  --executor-memory 1G \
  /app/$JOB_PATH
