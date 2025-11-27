#!/bin/bash

# Tên container Kafka trong Docker Compose
KAFKA_CONTAINER="kafka"

# Danh sách các topic cần tạo
TOPICS=("ad_clicks" "user_logs" "processed_data")

echo "⏳ Đang chờ Kafka khởi động..."
sleep 5 

echo "🚀 Bắt đầu tạo Kafka Topics..."

for topic in "${TOPICS[@]}"
do
  # Chạy lệnh kafka-topics bên trong container
  docker exec $KAFKA_CONTAINER kafka-topics \
    --create \
    --if-not-exists \
    --topic $topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
    
  echo "✅ Đã tạo (hoặc đã có) topic: $topic"
done

echo "🎉 Hoàn tất thiết lập Kafka!"
