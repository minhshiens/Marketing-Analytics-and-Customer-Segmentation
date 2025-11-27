import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="Ads Statistics (Batch)", layout="wide")

PROCESSED_FILE = "/app/data/ads/processed_ads_stats.csv"

st.title("📈 Báo Cáo Hiệu Quả Quảng Cáo (Batch Data)")
st.markdown("Phân tích dựa trên dữ liệu lịch sử `clicks_train.csv`")

if os.path.exists(PROCESSED_FILE):
    df = pd.read_csv(PROCESSED_FILE)
    
    # KPI Tổng quan
    col1, col2 = st.columns(2)
    col1.metric("Tổng số Ads đã phân tích", f"{len(df):,}")
    avg_ctr = df['ctr'].mean() if 'ctr' in df.columns else 0
    col2.metric("CTR Trung bình", f"{avg_ctr:.2f}%")
    
    st.divider()
    
    # Biểu đồ phân bố CTR
    if 'ctr' in df.columns:
        st.subheader("Phân bố tỉ lệ Click (CTR)")
        fig = px.histogram(df, x="ctr", nbins=20, title="Histogram of CTR")
        st.plotly_chart(fig, use_container_width=True)
        
    # Top 10 Ads hiệu quả nhất
    st.subheader("Top 10 Ads có nhiều Click nhất")
    top_ads = df.sort_values(by='total_clicks', ascending=False).head(10)
    st.dataframe(top_ads)
    
else:
    st.info("⚠️ Chưa có dữ liệu đã xử lý.")
    st.write("👉 Hãy vào Prefect UI và chạy Flow **'Batch Ads Processing'** trước!")
