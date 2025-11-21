FROM python:3.10-slim
WORKDIR /app

# 1. Copy folder ui (Nơi chứa file chạy chính)
COPY ui /app/ui

# 2. Copy folder src (Nơi chứa logic xử lý data, nếu cần import)
COPY src /app/src

RUN pip install streamlit psycopg2-binary pandas plotly

EXPOSE 8501

# 3. Sửa đường dẫn CMD cho đúng với hình bạn gửi
CMD ["streamlit", "run", "ui/streamlit_app/app.py"]