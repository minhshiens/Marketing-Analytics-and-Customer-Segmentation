import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Cấu hình trang
st.set_page_config(page_title="Customer Segmentation", layout="wide")

st.title("📊 Báo cáo Phân khúc Khách hàng (K-Means)")

# --- 1. LOAD DỮ LIỆU ---
# Đường dẫn tính từ thư mục gốc dự án
DATA_PATH = "data/processed/segment_summary.csv"

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    
    # Hiển thị bảng dữ liệu thô
    with st.expander("Xem dữ liệu gốc"):
        st.dataframe(df)

    # --- 2. VẼ BIỂU ĐỒ ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Số lượng khách hàng mỗi nhóm")
        fig_pie = px.pie(df, values='num_users', names='prediction', 
                         title='Tỷ lệ phân bổ khách hàng')
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader("Hành vi trung bình theo nhóm")
        # Vẽ biểu đồ cột so sánh số click
        fig_bar = px.bar(df, x='prediction', y='avg_clicks',
                         color='prediction',
                         title='Trung bình số lượt Click',
                         labels={'avg_clicks': 'Số Click', 'prediction': 'Nhóm (Cluster)'})
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # --- 3. PHÂN TÍCH CHI TIẾT ---
    st.subheader("Đặc điểm từng nhóm")
    for index, row in df.iterrows():
        st.info(f"**Nhóm {int(row['prediction'])}**: "
                f"Có {int(row['num_users'])} người dùng. "
                f"Trung bình xem {row['avg_ads_seen']:.1f} quảng cáo "
                f"và click {row['avg_clicks']:.1f} lần "
                f"(CTR: {row['avg_ctr']:.4f})")

else:
    st.error(f"Không tìm thấy file dữ liệu tại: {DATA_PATH}")
    st.warning("Hãy chạy 'src/batch/export_results.py' trước!")
