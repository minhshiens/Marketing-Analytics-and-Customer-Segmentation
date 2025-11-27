import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Ads Performance Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title(" Báo Cáo Hiệu Quả Quảng Cáo")
st.markdown("---")

# --- 2. HÀM LOAD & XỬ LÝ DỮ LIỆU ---
RAW_DATA_PATH = "data/ads/raw_data/clicks_train.csv"

@st.cache_data(show_spinner="Đang đọc và xử lý hàng triệu dòng dữ liệu...")
def load_and_process_data():
    if not os.path.exists(RAW_DATA_PATH):
        return None
    

    df_raw = pd.read_csv(RAW_DATA_PATH, nrows=1000000) 
    
    # Tổng hợp số liệu theo ad_id
    df_agg = df_raw.groupby('ad_id')['clicked'].agg(
        total_views='count', 
        total_clicks='sum'
    ).reset_index()
    
    # Tính CTR
    df_agg['ctr'] = (df_agg['total_clicks'] / df_agg['total_views'])
    
    return df_agg

# Gọi hàm load data
df = load_and_process_data()

if df is not None:
    # --- 3. SIDEBAR: BỘ LỌC ---
    st.sidebar.header("🎛️ Bộ Lọc Dữ Liệu")
    
    # Slider lọc nhiễu: Chỉ xem các Ads có số View nhất định
    min_views = st.sidebar.slider("Lọc Ads có số lượt xem tối thiểu:", 
                                  min_value=0, 
                                  max_value=int(df['total_views'].mean()), 
                                  value=10, step=10)
    
    # Áp dụng bộ lọc
    df_filtered = df[df['total_views'] >= min_views].copy()
    
    # Hiển thị thông tin filter
    st.sidebar.info(f"Đang hiển thị: **{len(df_filtered):,}** / {len(df):,} quảng cáo")
    st.sidebar.markdown("---")

    # --- 4. DASHBOARD CHÍNH ---

    # --- A. KPI CARDS ---
    total_views = df_filtered['total_views'].sum()
    total_clicks = df_filtered['total_clicks'].sum()
    avg_ctr = (total_clicks / total_views) * 100 if total_views > 0 else 0
    best_ad = df_filtered.loc[df_filtered['ctr'].idxmax()] if not df_filtered.empty else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(" Tổng Lượt Xem", f"{total_views:,.0f}")
    c2.metric(" Tổng Lượt Click", f"{total_clicks:,.0f}")
    c3.metric(" CTR Trung Bình", f"{avg_ctr:.2f}%")
    if best_ad is not None:
        c4.metric(" Ad Hiệu Quả Nhất (ID)", f"{best_ad['ad_id']}", f"CTR: {best_ad['ctr']:.1%}")

    # --- B. PHÂN TÍCH CHI TIẾT (TABS) ---
    tab1, tab2, tab3 = st.tabs(["📊 Tổng Quan & Phân Bố", "🎯 Ma Trận Hiệu Quả", "🏆 Top Xếp Hạng"])

    # === TAB 1: TỔNG QUAN ===
    with tab1:
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.subheader("Phân bố Tỷ lệ Click (CTR Distribution)")
            # Histogram giúp xem đa số Ads nằm ở mức CTR nào
            fig_hist = px.histogram(df_filtered, x="ctr", nbins=50, 
                                    title="Đa số quảng cáo có CTR bao nhiêu?",
                                    color_discrete_sequence=['#636EFA'],
                                    labels={'ctr': 'Tỷ lệ Click (0.0 - 1.0)'})
            fig_hist.update_layout(xaxis_tickformat=".1%")
            st.plotly_chart(fig_hist, use_container_width=True)
            
        with col_right:
            st.subheader("Tỷ lệ Chuyển đổi")
            labels = ['Không Click', 'Click']
            values = [total_views - total_clicks, total_clicks]
            fig_pie = px.pie(values=values, names=labels, 
                             hole=0.4, color_discrete_sequence=['#EF553B', '#00CC96'])
            st.plotly_chart(fig_pie, use_container_width=True)

    # === TAB 2: MA TRẬN HIỆU QUẢ (SCATTER PLOT) ===
    with tab2:
        st.subheader("Biểu đồ Tương quan: Views vs. Clicks")
        # st.markdown("""
        # * **Góc trên bên phải:** Ads "Ngôi sao" (Nhiều view, nhiều click).
        # * **Góc dưới bên phải:** Ads "Lãng phí" (Nhiều view, ít click).
        # * **Màu sắc:** Thể hiện CTR (Đỏ/Vàng là cao, Xanh/Tím là thấp).
        # """)
        
        # Chuyển ad_id sang string để tooltip hiển thị đẹp hơn
        df_filtered['ad_id_str'] = df_filtered['ad_id'].astype(str)
        
        fig_scatter = px.scatter(
            df_filtered, 
            x="total_views", 
            y="total_clicks",
            color="ctr",
            size="total_clicks", # Bong bóng càng to nếu càng nhiều click
            hover_name="ad_id_str",
            hover_data={"ctr": ":.2%"},
            log_x=True, # Dùng log scale vì chênh lệch view thường rất lớn
            log_y=True,
            title="Ma trận Hiệu suất Quảng cáo (Log Scale)",
            color_continuous_scale="Spectral_r" 
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    # === TAB 3: TOP XẾP HẠNG ===
    with tab3:
        st.subheader("Top 10 Ads Xuất Sắc Nhất")
        
        col_table, col_chart = st.columns([1, 1])
        
        # Lấy top 10 theo Click
        top_ads = df_filtered.sort_values(by='total_clicks', ascending=False).head(10)
        
        with col_table:
            st.caption("Chi tiết số liệu")
            st.dataframe(
                top_ads[['ad_id', 'total_views', 'total_clicks', 'ctr']].style.format({
                    "ctr": "{:.2%}",
                    "total_views": "{:,}",
                    "total_clicks": "{:,}"
                }), 
                use_container_width=True
            )
            
        with col_chart:
            st.caption("So sánh trực quan")
            fig_bar = px.bar(top_ads, x='total_clicks', y=top_ads['ad_id'].astype(str),
                             orientation='h',
                             color='ctr',
                             text='total_clicks',
                             labels={'y': 'Ad ID', 'total_clicks': 'Số Click'},
                             title="Top 10 Ads theo số Click",
                             color_continuous_scale='Viridis')
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)

else:
    st.error(f"⚠️ Không tìm thấy file dữ liệu tại: `{RAW_DATA_PATH}`")