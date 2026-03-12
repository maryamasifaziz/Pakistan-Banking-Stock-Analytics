import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="Pakistan Banking Sector Analytics", page_icon="🏦", layout="wide")
st.title("🏦 Pakistan Banking Sector Analytics")
st.markdown("*Powered by PySpark + GitHub Actions | 20 Pakistani Banks*")

# Load Local Data 
@st.cache_data(ttl=3600)
def load_data():
    processed_path = 'data/processed/psx_processed.csv'
    monthly_path = 'data/processed/psx_monthly_agg.csv'

    if not os.path.exists(processed_path):
        st.error("No data found. Run ingest.py and transform.py first.")
        st.stop()

    processed = pd.read_csv(processed_path)
    monthly = pd.read_csv(monthly_path)
    return processed, monthly

processed_df, monthly_df = load_data()

# Sidebar Filters 
st.sidebar.header("Filters")
tickers = st.sidebar.multiselect(
    "Select Stocks",
    processed_df['Ticker'].unique(),
    default=list(processed_df['Ticker'].unique())[:3]
)
year_range = st.sidebar.slider(
    "Year Range",
    int(processed_df['Year'].min()),
    int(processed_df['Year'].max()),
    (int(processed_df['Year'].min()), int(processed_df['Year'].max()))
)

filtered = processed_df[
    (processed_df['Ticker'].isin(tickers)) &
    (processed_df['Year'].between(*year_range))
]

# KPI Metrics 
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Records", f"{len(filtered):,}")
col2.metric("Avg Daily Return", f"{filtered['Daily_Return'].mean():.2f}%")
col3.metric("Avg Volatility", f"{filtered['Volatility_30d'].mean():.2f}")
col4.metric("Stocks Tracked", len(tickers))

st.divider()

# Chart 1: Price Trend 
col1, col2 = st.columns(2)
with col1:
    fig1 = px.line(
        filtered, x='Date', y='Close',
        color='Ticker', title='📊 Stock Price Trend Over Time'
    )
    st.plotly_chart(fig1, use_container_width=True)

# Chart 2: Daily Returns Distribution 
with col2:
    fig2 = px.histogram(
        filtered, x='Daily_Return', color='Ticker',
        title='📉 Daily Returns Distribution',
        nbins=50, opacity=0.7
    )
    st.plotly_chart(fig2, use_container_width=True)

# Chart 3: Monthly Average Price 
col3, col4 = st.columns(2)
monthly_filtered = monthly_df[monthly_df['Ticker'].isin(tickers)]
with col3:
    fig3 = px.bar(
        monthly_filtered, x='Month', y='Avg_Close',
        color='Ticker', barmode='group',
        title='📅 Monthly Average Closing Price'
    )
    st.plotly_chart(fig3, use_container_width=True)

# Chart 4: Trend Distribution 
with col4:
    trend_counts = filtered['Trend'].value_counts().reset_index()
    trend_counts.columns = ['Trend', 'Count']
    fig4 = px.pie(
        trend_counts, names='Trend', values='Count',
        title='📈 Up vs Down vs Flat Days',
        color='Trend',
        color_discrete_map={'Up': 'green', 'Down': 'red', 'Flat': 'gray'}
    )
    st.plotly_chart(fig4, use_container_width=True)

# Chart 5: Volatility Over Time 
fig5 = px.line(
    filtered, x='Date', y='Volatility_30d',
    color='Ticker', title='🌊 30-Day Rolling Volatility'
)
st.plotly_chart(fig5, use_container_width=True)

# Raw Data Table
with st.expander("View Raw Data"):
    st.dataframe(filtered.tail(100), use_container_width=True)