"""
E-Commerce Data Pipeline Dashboard
====================================
Reads processed Parquet data from HDFS and displays
interactive visualizations using Streamlit + Plotly.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from hdfs import InsecureClient
import pyarrow.parquet as pq
import io
import os
import tempfile


# ============================================
# PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================
# CUSTOM CSS FOR BETTER STYLING
# ============================================
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0f0c29 0%, #1a1a2e 50%, #16213e 100%);
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5986 100%);
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #4fc3f7;
        margin: 8px 0;
    }
    .kpi-label {
        font-size: 0.95rem;
        color: #b0bec5;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    /* Title styling */
    .dashboard-title {
        text-align: center;
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(90deg, #4fc3f7, #81d4fa, #b3e5fc);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 8px;
    }
    .dashboard-subtitle {
        text-align: center;
        color: #78909c;
        font-size: 1.1rem;
        margin-bottom: 32px;
    }

    /* Section headers */
    .section-header {
        color: #81d4fa;
        font-size: 1.3rem;
        font-weight: 600;
        margin: 16px 0 8px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(79, 195, 247, 0.3);
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1b2a 0%, #1b2838 100%);
    }

    /* Hide default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# ============================================
# LOAD DATA FROM HDFS
# ============================================
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data_from_hdfs(hdfs_path):
    """
    Download Parquet files from HDFS and load into pandas DataFrame.
    Uses WebHDFS REST API via the hdfs Python library.
    """
    try:
        client = InsecureClient('http://namenode:9870', user='root')

        # List all parquet part files in the directory
        files = client.list(hdfs_path)
        parquet_files = [f for f in files if f.endswith('.parquet')]

        if not parquet_files:
            st.error(f"No parquet files found in {hdfs_path}")
            return pd.DataFrame()

        # Read each parquet part file and combine
        dfs = []
        for pfile in parquet_files:
            file_path = f"{hdfs_path}/{pfile}"
            with client.read(file_path) as reader:
                data = reader.read()
                df = pd.read_parquet(io.BytesIO(data))
                dfs.append(df)

        return pd.concat(dfs, ignore_index=True)

    except Exception as e:
        st.error(f"Error loading data from HDFS: {e}")
        return pd.DataFrame()


def load_all_data():
    """Load all processed datasets from HDFS."""
    data = {}
    datasets = {
        'monthly_sales': '/data/processed/monthly_sales',
        'category_sales': '/data/processed/category_sales',
        'top_products': '/data/processed/top_products',
        'customer_summary': '/data/processed/customer_summary',
        'order_status': '/data/processed/order_status',
        'event_funnel': '/data/processed/event_funnel',
        'city_sales': '/data/processed/city_sales',
    }

    progress = st.progress(0, text="Loading data from HDFS...")
    for i, (name, path) in enumerate(datasets.items()):
        data[name] = load_data_from_hdfs(path)
        progress.progress((i + 1) / len(datasets), text=f"Loading {name}...")

    progress.empty()
    return data


# ============================================
# PLOTLY CHART THEME (Dark + Modern)
# ============================================
CHART_COLORS = ['#4fc3f7', '#81d4fa', '#29b6f6', '#03a9f4',
                '#039be5', '#0288d1', '#0277bd', '#01579b',
                '#b3e5fc', '#e1f5fe']

CHART_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(color='#b0bec5', family='Inter'),
    margin=dict(l=40, r=40, t=50, b=40),
    xaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.05)'),
)


# ============================================
# MAIN DASHBOARD
# ============================================
def main():
    # --- Title ---
    st.markdown('<div class="dashboard-title">📊 E-Commerce Analytics Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="dashboard-subtitle">Real-time insights from the data pipeline • Spark + HDFS + Airflow</div>', unsafe_allow_html=True)

    # --- Load Data ---
    data = load_all_data()

    # Check if data loaded successfully
    if all(df.empty for df in data.values()):
        st.error("❌ Could not load data from HDFS. Make sure the ETL pipeline has run successfully.")
        st.info("Run the Airflow DAG first, then refresh this page.")
        return

    # --- Sidebar ---
    with st.sidebar:
        st.markdown("## 🔧 Dashboard Controls")
        st.markdown("---")
        st.markdown("### 📁 Data Source")
        st.markdown("**HDFS** `/data/processed/`")
        st.markdown("---")
        st.markdown("### 📊 Datasets Loaded")
        for name, df in data.items():
            status = "✅" if not df.empty else "❌"
            st.markdown(f"{status} **{name}**: {len(df)} rows")
        st.markdown("---")
        st.markdown("### ⚙️ Pipeline Info")
        st.markdown("- **Spark** 3.5.1")
        st.markdown("- **Airflow** 2.9.1")
        st.markdown("- **HDFS** 3.2.1")

        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

    # ==========================================
    # KPI CARDS
    # ==========================================
    ms = data['monthly_sales']
    cs = data['customer_summary']
    os_df = data['order_status']

    total_revenue = ms['total_revenue'].sum() if not ms.empty else 0
    total_orders = ms['total_orders'].sum() if not ms.empty else 0
    total_customers = len(cs) if not cs.empty else 0
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">💰 Total Revenue</div>
            <div class="kpi-value">${total_revenue:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">📦 Total Orders</div>
            <div class="kpi-value">{total_orders:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">👥 Total Customers</div>
            <div class="kpi-value">{total_customers:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">🛒 Avg Order Value</div>
            <div class="kpi-value">${avg_order_value:,.2f}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # MONTHLY SALES TREND
    # ==========================================
    if not ms.empty:
        st.markdown('<div class="section-header">📈 Monthly Sales Trend</div>', unsafe_allow_html=True)

        ms_sorted = ms.sort_values(['order_year', 'order_month'])
        ms_sorted['month_label'] = ms_sorted.apply(
            lambda x: f"{int(x['order_year'])}-{int(x['order_month']):02d}", axis=1
        )

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=ms_sorted['month_label'],
            y=ms_sorted['total_revenue'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color='#4fc3f7', width=3),
            marker=dict(size=8, color='#81d4fa'),
            fill='tozeroy',
            fillcolor='rgba(79, 195, 247, 0.1)',
        ))
        fig.update_layout(
            **CHART_LAYOUT,
            title=dict(text='Monthly Revenue Over Time', font=dict(size=16, color='#81d4fa')),
            xaxis_title='Month',
            yaxis_title='Revenue ($)',
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ==========================================
    # CATEGORY SALES + ORDER STATUS (side by side)
    # ==========================================
    col_left, col_right = st.columns(2)

    with col_left:
        cat = data['category_sales']
        if not cat.empty:
            st.markdown('<div class="section-header">📊 Sales by Category</div>', unsafe_allow_html=True)

            fig = px.bar(
                cat.sort_values('total_revenue', ascending=True),
                x='total_revenue',
                y='category',
                orientation='h',
                color='total_revenue',
                color_continuous_scale=['#01579b', '#0288d1', '#03a9f4', '#4fc3f7', '#81d4fa'],
            )
            fig.update_layout(
                **CHART_LAYOUT,
                height=400,
                showlegend=False,
                coloraxis_showscale=False,
                xaxis_title='Revenue ($)',
                yaxis_title='',
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        if not os_df.empty:
            st.markdown('<div class="section-header">🥧 Order Status Breakdown</div>', unsafe_allow_html=True)

            fig = px.pie(
                os_df,
                values='order_count',
                names='order_status',
                color_discrete_sequence=CHART_COLORS,
                hole=0.45,
            )
            fig.update_layout(
                **CHART_LAYOUT,
                height=400,
                legend=dict(font=dict(color='#b0bec5')),
            )
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label',
                textfont_size=13,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ==========================================
    # TOP 10 PRODUCTS
    # ==========================================
    tp = data['top_products']
    if not tp.empty:
        st.markdown('<div class="section-header">🏆 Top 10 Best-Selling Products</div>', unsafe_allow_html=True)

        top10 = tp.nlargest(10, 'total_revenue')
        fig = px.bar(
            top10.sort_values('total_revenue', ascending=True),
            x='total_revenue',
            y='product_name',
            orientation='h',
            color='category',
            color_discrete_sequence=CHART_COLORS,
        )
        fig.update_layout(
            **CHART_LAYOUT,
            height=450,
            xaxis_title='Revenue ($)',
            yaxis_title='',
            legend=dict(
                title='Category',
                font=dict(color='#b0bec5'),
                bgcolor='rgba(0,0,0,0.3)',
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ==========================================
    # FUNNEL + CITY SALES (side by side)
    # ==========================================
    col_left2, col_right2 = st.columns(2)

    with col_left2:
        ef = data['event_funnel']
        if not ef.empty:
            st.markdown('<div class="section-header">🔄 Conversion Funnel</div>', unsafe_allow_html=True)

            # Order funnel stages logically
            funnel_order = {'view': 1, 'cart': 2, 'wishlist': 3, 'purchase': 4}
            ef['sort_order'] = ef['event_type'].map(funnel_order).fillna(5)
            ef_sorted = ef.sort_values('sort_order')

            fig = go.Figure(go.Funnel(
                y=ef_sorted['event_type'].str.capitalize(),
                x=ef_sorted['event_count'],
                textinfo="value+percent initial",
                marker=dict(
                    color=['#4fc3f7', '#29b6f6', '#0288d1', '#01579b'],
                ),
                connector=dict(line=dict(color='rgba(79,195,247,0.3)', width=2)),
            ))
            fig.update_layout(
                **CHART_LAYOUT,
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_right2:
        city = data['city_sales']
        if not city.empty:
            st.markdown('<div class="section-header">🌆 Top 10 Cities by Revenue</div>', unsafe_allow_html=True)

            top_cities = city.nlargest(10, 'total_revenue')
            fig = px.bar(
                top_cities.sort_values('total_revenue', ascending=True),
                x='total_revenue',
                y='city',
                orientation='h',
                color='total_revenue',
                color_continuous_scale=['#01579b', '#0288d1', '#03a9f4', '#4fc3f7', '#81d4fa'],
            )
            fig.update_layout(
                **CHART_LAYOUT,
                height=400,
                showlegend=False,
                coloraxis_showscale=False,
                xaxis_title='Revenue ($)',
                yaxis_title='',
            )
            st.plotly_chart(fig, use_container_width=True)

    # ==========================================
    # TOP CUSTOMERS TABLE
    # ==========================================
    if not cs.empty:
        st.markdown('<div class="section-header">👥 Top 20 Customers</div>', unsafe_allow_html=True)

        top_customers = cs.nlargest(20, 'total_spent')[
            ['name', 'email', 'gender', 'city', 'total_spent', 'total_orders', 'avg_order_value']
        ].reset_index(drop=True)

        top_customers.columns = ['Name', 'Email', 'Gender', 'City', 'Total Spent ($)', 'Orders', 'Avg Order ($)']
        top_customers.index = top_customers.index + 1

        st.dataframe(
            top_customers,
            use_container_width=True,
            height=500,
        )

    # --- Footer ---
    st.markdown("---")
    st.markdown(
        '<div style="text-align:center; color:#546e7a; padding:16px;">'
        '📊 E-Commerce Data Pipeline Dashboard | Built with Spark + HDFS + Airflow + Streamlit'
        '</div>',
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
