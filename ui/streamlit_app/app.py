import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --- CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Customer Segmentation Dashboard",
    page_icon="👥",
    layout="wide"
)

st.title("📊 Báo cáo Phân khúc Khách hàng (K-Means)")
st.markdown("---")

# --- 1. LOAD DỮ LIỆU ---
DATA_PATH = "data/processed/segment_summary.csv"

if os.path.exists(DATA_PATH):
    df = pd.read_csv(DATA_PATH)
    
    # Đảm bảo cột prediction là string để phân loại màu sắc
    df['prediction'] = df['prediction'].astype(str)

    # Sidebar: Bộ lọc (nếu cần mở rộng sau này)
    st.sidebar.header("Cấu hình hiển thị")
    show_raw = st.sidebar.checkbox("Hiển thị dữ liệu thô", value=False)

    if show_raw:
        st.subheader("📋 Dữ liệu tổng hợp")
        st.dataframe(df, use_container_width=True)

    # --- 2. KPI TỔNG QUAN (METRICS) ---
    # Tính toán tổng quan toàn bộ tập khách hàng
    total_users = df['num_users'].sum()
    avg_global_ctr = (df['avg_clicks'] * df['num_users']).sum() / (df['avg_ads_seen'] * df['num_users']).sum()
    
    col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
    col_kpi1.metric("Tổng Khách Hàng", f"{total_users:,}")
    col_kpi2.metric("Số Nhóm (Cluster)", df['prediction'].nunique())
    col_kpi3.metric("CTR Trung Bình Toàn Sàn", f"{avg_global_ctr:.2%}")

    st.markdown("---")

    # --- 3. TRỰC QUAN HÓA CHI TIẾT ---
    
    # TẠO TABS ĐỂ GIAO DIỆN GỌN GÀNG
    tab1, tab2, tab3 = st.tabs(["📈 Phân Bố & Hành Vi", "🕸️ Chân Dung (Radar)", "🎯 Hiệu Suất (CTR)"])

    # --- TAB 1: PHÂN BỐ & HÀNH VI ---
    with tab1:
        c1, c2 = st.columns([1, 2])
        
        with c1:
            st.subheader("Tỷ lệ quy mô nhóm")
            fig_pie = px.pie(df, values='num_users', names='prediction', 
                             color='prediction',
                             hole=0.4, 
                             title='Phần trăm người dùng theo nhóm')
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_pie, use_container_width=True)

        with c2:
            st.subheader("So sánh Hành vi: Xem vs Click")
            # Chuyển đổi dữ liệu sang dạng dài (long format) để vẽ grouped bar chart
            df_melted = df.melt(id_vars=['prediction'], 
                                value_vars=['avg_ads_seen', 'avg_clicks'],
                                var_name='Metric', value_name='Value')
            
            fig_group = px.bar(df_melted, x='prediction', y='Value', 
                               color='Metric', barmode='group',
                               title='So sánh số Quảng cáo đã xem và số Click',
                               labels={'Value': 'Số lượng trung bình', 'prediction': 'Nhóm'},
                               color_discrete_map={'avg_ads_seen': '#83c9ff', 'avg_clicks': '#0068c9'})
            st.plotly_chart(fig_group, use_container_width=True)
        
        # Biểu đồ Bong bóng (Bubble Chart) thể hiện mối quan hệ giữa Xem và Click
        st.subheader("Bản đồ Định vị Nhóm (Bubble Chart)")
        st.caption("Trục X: Số ads xem | Trục Y: Số click | Kích thước bóng: Số lượng khách hàng")
        fig_bubble = px.scatter(df, x="avg_ads_seen", y="avg_clicks",
                                size="num_users", color="prediction",
                                hover_name="prediction",
                                size_max=60,
                                title="Tương quan Xem - Click và Quy mô nhóm")
        st.plotly_chart(fig_bubble, use_container_width=True)

    # --- TAB 2: RADAR CHART (CHÂN DUNG KHÁCH HÀNG) ---
    with tab2:
        st.subheader("Đặc điểm nổi bật của từng nhóm (Chuẩn hóa)")
        st.caption("Biểu đồ này giúp bạn nhận diện nhanh 'tính cách' của nhóm (Ví dụ: Nhóm chỉ xem nhiều nhưng không click).")
        
        # Chuẩn hóa dữ liệu về thang 0-1 để vẽ Radar Chart
        categories = ['avg_ads_seen', 'avg_clicks', 'avg_ctr', 'num_users']
        df_norm = df.copy()
        for col in categories:
            df_norm[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

        fig_radar = go.Figure()

        for index, row in df_norm.iterrows():
            fig_radar.add_trace(go.Scatterpolar(
                r=[row['avg_ads_seen'], row['avg_clicks'], row['avg_ctr'], row['num_users']],
                theta=['Ads Seen', 'Clicks', 'CTR', 'User Count'],
                fill='toself',
                name=f'Nhóm {row["prediction"]}'
            ))

        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            showlegend=True
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    # --- TAB 3: HIỆU SUẤT (CTR) ---
    with tab3:
        st.subheader("Xếp hạng Hiệu quả (CTR)")
        
        # Sắp xếp để nhìn rõ nhóm nào hiệu quả nhất
        df_sorted = df.sort_values(by='avg_ctr', ascending=False)
        
        fig_ctr = px.bar(df_sorted, x='prediction', y='avg_ctr',
                         color='avg_ctr',
                         color_continuous_scale='Viridis',
                         title='Tỷ lệ Click (CTR) theo nhóm',
                         labels={'avg_ctr': 'CTR', 'prediction': 'Nhóm'})
        fig_ctr.update_layout(yaxis_tickformat=".2%") 
        st.plotly_chart(fig_ctr, use_container_width=True)

        # Phân tích text
        st.markdown("### 📝 Tóm tắt Insight")
        best_group = df_sorted.iloc[0]
        st.success(f"🏆 **Nhóm hiệu quả nhất:** Nhóm {best_group['prediction']} với CTR đạt {best_group['avg_ctr']:.2%}")
        
        worst_group = df_sorted.iloc[-1]
        st.warning(f"⚠️ **Cần cải thiện:** Nhóm {worst_group['prediction']} có CTR thấp nhất ({worst_group['avg_ctr']:.2%}), mặc dù trung bình họ xem {worst_group['avg_ads_seen']:.1f} quảng cáo.")

else:
    st.error(f"⚠️ Không tìm thấy file dữ liệu tại: `{DATA_PATH}`")
    st.info("Vui lòng chạy script xử lý dữ liệu trước: `python src/batch/export_results.py`")