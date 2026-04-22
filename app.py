import os
import streamlit as st
import asyncio
import pandas as pd
import numpy as np
import joblib
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import timedelta, datetime
import pytz
import traceback

from processor import DataProcessor
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type
from context import get_earnings_dates, get_news_for_range, get_sector_etf
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="MoneyMaker", layout="wide", page_icon="📈")

st.markdown("""
<style>
/* ── Base ── */
.stApp {
    background-color: #12161f;
    color: #e0e6f0;
}

/* ── Header ── */
.app-header {
    padding: 1.5rem 0 1rem 0;
    border-bottom: 1px solid #2a3a55;
    margin-bottom: 1.5rem;
}
.app-header h1 {
    font-size: 2rem;
    font-weight: 800;
    letter-spacing: -0.5px;
    background: linear-gradient(90deg, #ff5c5c, #ff8c5c);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin: 0;
}
.app-header p {
    color: #8a9bbf;
    font-size: 0.85rem;
    margin: 0.25rem 0 0 0;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background-color: #1a1f2e;
    border-radius: 10px;
    padding: 4px;
    gap: 2px;
    border: 1px solid #2a3a55;
}
.stTabs [data-baseweb="tab"] {
    background-color: transparent;
    border-radius: 8px;
    color: #8a9bbf;
    font-weight: 500;
    font-size: 0.82rem;
    padding: 6px 14px;
    border: none;
}
.stTabs [aria-selected="true"] {
    background-color: #ff5c5c18 !important;
    color: #ff5c5c !important;
    border-bottom: 2px solid #ff5c5c !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color: #c0cfe8 !important;
    background-color: #2a3a5530 !important;
}

/* ── Buttons ── */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #ff5c5c, #ff8c5c);
    color: #000;
    font-weight: 700;
    border: none;
    border-radius: 8px;
    padding: 0.5rem 1.5rem;
    transition: opacity 0.2s;
}
.stButton > button[kind="primary"]:hover {
    opacity: 0.85;
    color: #000;
}
.stButton > button[kind="secondary"] {
    border: 1px solid #2a3a55;
    background-color: #1a1f2e;
    color: #c0cfe8;
    border-radius: 8px;
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background-color: #1a2235;
    border: 1px solid #2a3a55;
    border-radius: 10px;
    padding: 1rem;
}
[data-testid="metric-container"] label {
    color: #8a9bbf !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #e0e6f0 !important;
    font-size: 1.4rem !important;
    font-weight: 700;
}
[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-size: 0.85rem !important;
}

/* ── Inputs ── */
.stTextInput > div > div > input,
.stSelectbox > div > div,
.stDateInput > div > div > input {
    background-color: #1a1f2e !important;
    border: 1px solid #2a3a55 !important;
    border-radius: 8px !important;
    color: #e0e6f0 !important;
}
.stTextInput > div > div > input:focus {
    border-color: #ff5c5c !important;
    box-shadow: 0 0 0 2px #ff5c5c22 !important;
}

/* ── Sliders ── */
.stSlider [data-baseweb="slider"] div[role="slider"] {
    background-color: #ff5c5c !important;
}

/* ── Dataframes ── */
.stDataFrame {
    border: 1px solid #2a3a55 !important;
    border-radius: 10px !important;
    overflow: hidden;
}

/* ── Expanders ── */
.streamlit-expanderHeader {
    background-color: #1a1f2e !important;
    border: 1px solid #2a3a55 !important;
    border-radius: 8px !important;
    color: #c0cfe8 !important;
    font-weight: 600;
}
.streamlit-expanderContent {
    background-color: #12161f !important;
    border: 1px solid #2a3a55 !important;
    border-top: none !important;
    border-radius: 0 0 8px 8px !important;
}

/* ── Info / Success / Warning boxes ── */
.stInfo, [data-testid="stInfoMessage"] {
    background-color: #0f1f38 !important;
    border-left: 3px solid #ff8c5c !important;
    border-radius: 8px !important;
}
.stSuccess, [data-testid="stSuccessMessage"] {
    background-color: #1f0a0a !important;
    border-left: 3px solid #ff5c5c !important;
    border-radius: 8px !important;
}
.stWarning, [data-testid="stWarningMessage"] {
    background-color: #1f1a0a !important;
    border-left: 3px solid #FF9800 !important;
    border-radius: 8px !important;
}
.stError, [data-testid="stErrorMessage"] {
    background-color: #1f0a0a !important;
    border-left: 3px solid #ef5350 !important;
    border-radius: 8px !important;
}

/* ── Section dividers ── */
hr {
    border-color: #2a3a55 !important;
}

/* ── Subheaders ── */
h2, h3 {
    color: #c0cfe8 !important;
    font-weight: 700;
}

/* ── Spinner ── */
.stSpinner > div {
    border-top-color: #ff5c5c !important;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="app-header">
    <h1>📈 MoneyMaker</h1>
    <p>CEO tweet sentiment analysis & stock market correlation</p>
</div>
""", unsafe_allow_html=True)

@st.cache_resource
def get_processor():
    return DataProcessor()

@st.cache_resource
def get_db_engine():
    url = os.getenv("DATABASE_URL")
    if not url:
        return None
    return create_engine(url)

@st.cache_data(ttl=300)
def load_dashboard_data():
    engine = get_db_engine()
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM merged_data"), conn)
        return df if not df.empty else None
    except Exception:
        return None

proc = get_processor()

# Shared inputs — visible on every tab
st.subheader("Query Parameters")
col1, col2 = st.columns(2)
with col1:
    ceo_handle = st.text_input("CEO Twitter Handle", value="", placeholder="e.g. elonmusk")
with col2:
    stock_ticker = st.text_input("Stock Ticker", value="", placeholder="e.g. TSLA")

col3, col4 = st.columns(2)
with col3:
    query_start_date = st.date_input("Start Date", value=datetime.now(pytz.utc).date() - timedelta(days=7))
with col4:
    query_end_date = st.date_input("End Date (Optional)", value=None)

if query_end_date:
    date_display = f"{query_start_date.strftime('%Y-%m-%d')} to {query_end_date.strftime('%Y-%m-%d')}"
else:
    date_display = f"From {query_start_date.strftime('%Y-%m-%d')}"

def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

def filter_tweets_by_date(tweets_df):
    if tweets_df['date'].dt.tz is None:
        tweets_df['date'] = tweets_df['date'].dt.tz_localize('UTC')
    start_date = pd.to_datetime(query_start_date).tz_localize('UTC')
    mask = tweets_df['date'] >= start_date
    if query_end_date:
        end_date = pd.to_datetime(query_end_date).tz_localize('UTC') + timedelta(days=1)
        mask = mask & (tweets_df['date'] < end_date)
    return tweets_df.loc[mask].copy()

def weekend_shift(dt):
    if dt.weekday() == 5:
        return dt + timedelta(days=2)
    elif dt.weekday() == 6:
        return dt + timedelta(days=1)
    return dt

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_home, tab_data, tab_impact, tab_stock, tab_drill, tab_ctx, tab_predict, tab_backtest = st.tabs(["Home", "Data", "Tweet Analysis", "Stock Analysis", "Tweet Explorer", "Market Context", "Predict", "Backtest"])

# ── HOME TAB ──────────────────────────────────────────────────────────────────
with tab_home:
    db_data = load_dashboard_data()

    if db_data is None:
        # ── Getting started ───────────────────────────────────────────────────
        st.markdown("### Welcome to MoneyMaker")
        st.markdown("Your database is empty. Run `/process/all` from the FastAPI backend to populate it, or use the tabs below to start exploring manually.")
        st.markdown("---")
        features = [
            ("Data", "Pull raw tweets, stock prices, and merged data for any CEO and ticker."),
            ("ATR Analysis", "See whether high-sentiment tweet days coincide with unusual stock volatility."),
            ("Tweet Impact", "Measure same-day stock reaction on days a CEO posted a high-sentiment tweet."),
            ("Post-Tweet Trend", "Track how a stock drifts over N trading days after a tweet on a radar chart."),
            ("Stock Analysis", "Full candlestick chart with RSI, MACD, Bollinger Bands, and moving averages."),
            ("Tweet Explorer", "Select any tweet and see the stock chart around that exact date."),
            ("Market Context", "Compare stock performance vs SPY and sector ETFs, with earnings and news overlaid."),
        ]
        cols = st.columns(2)
        for i, (name, desc) in enumerate(features):
            with cols[i % 2]:
                st.markdown(f"""
                <div style="background:#1a1f2e;border:1px solid #2a3a55;border-radius:10px;padding:1rem;margin-bottom:0.75rem;">
                    <div style="color:#ff5c5c;font-weight:700;font-size:0.9rem;margin-bottom:0.25rem;">{name}</div>
                    <div style="color:#c0cfe8;font-size:0.82rem;">{desc}</div>
                </div>
                """, unsafe_allow_html=True)
    else:
        # ── Stats ─────────────────────────────────────────────────────────────
        total_records = len(db_data)
        unique_ceos = db_data['ceo'].nunique()
        avg_sentiment = db_data['sentiment_score'].mean()
        top_ceo = db_data['ceo'].value_counts().idxmax()
        top_ceo_count = db_data['ceo'].value_counts().max()

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Tweets Analyzed", f"{total_records:,}")
        m2.metric("CEOs Tracked", str(unique_ceos))
        m3.metric("Avg Sentiment Score", f"{avg_sentiment:.3f}")
        m4.metric("Most Analyzed CEO", f"@{top_ceo}", f"{top_ceo_count} tweets")

        st.markdown("---")

        # ── Charts ────────────────────────────────────────────────────────────
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            st.subheader("Tweets by CEO")
            tweet_counts = db_data['ceo'].value_counts().reset_index()
            tweet_counts.columns = ['ceo', 'count']
            fig_counts = go.Figure(go.Bar(
                x=tweet_counts['ceo'], y=tweet_counts['count'],
                marker_color='#ff5c5c', text=tweet_counts['count'],
                textposition='outside',
            ))
            fig_counts.update_layout(
                template='plotly_dark', height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor='#1a1f2e', plot_bgcolor='#1a1f2e',
                yaxis=dict(gridcolor='#2a3a55'),
            )
            st.plotly_chart(fig_counts, use_container_width=True)

        with chart_col2:
            st.subheader("Avg Sentiment by CEO")
            avg_by_ceo = db_data.groupby('ceo')['sentiment_score'].mean().reset_index()
            avg_by_ceo.columns = ['ceo', 'avg_sentiment']
            avg_by_ceo = avg_by_ceo.sort_values('avg_sentiment', ascending=False)
            bar_colors = ['#ff5c5c' if v >= 0 else '#ef5350' for v in avg_by_ceo['avg_sentiment']]
            fig_sent = go.Figure(go.Bar(
                x=avg_by_ceo['ceo'], y=avg_by_ceo['avg_sentiment'],
                marker_color=bar_colors,
                text=avg_by_ceo['avg_sentiment'].round(3),
                textposition='outside',
            ))
            fig_sent.update_layout(
                template='plotly_dark', height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor='#1a1f2e', plot_bgcolor='#1a1f2e',
                yaxis=dict(gridcolor='#2a3a55'),
            )
            st.plotly_chart(fig_sent, use_container_width=True)

        st.markdown("---")

        # ── Top tweets ────────────────────────────────────────────────────────
        st.subheader("Top 5 Most Positive Tweets")
        top_positive = db_data.nlargest(5, 'sentiment_score')[['date', 'ceo', 'tweet_text', 'sentiment_score', 'stock_ticker']].reset_index(drop=True)
        st.dataframe(top_positive, use_container_width=True)

        st.subheader("Top 5 Most Negative Tweets")
        top_negative = db_data.nsmallest(5, 'sentiment_score')[['date', 'ceo', 'tweet_text', 'sentiment_score', 'stock_ticker']].reset_index(drop=True)
        st.dataframe(top_negative, use_container_width=True)

# ── DATA TAB ──────────────────────────────────────────────────────────────────
with tab_data:
    btn_col1, btn_col2, btn_col3 = st.columns(3)

    with btn_col1:
        pull_tweets = st.button("Pull Tweets", type="primary", use_container_width=True)
    with btn_col2:
        pull_stocks = st.button("Pull Stock Data", type="primary", use_container_width=True)
    with btn_col3:
        pull_merged = st.button("Pull Merged Data", type="primary", use_container_width=True)

    data_results = st.container()

    if pull_tweets:
        if not ceo_handle:
            st.error("Please enter a CEO Twitter Handle.")
        else:
            with st.spinner(f"Pulling tweets for @{ceo_handle}..."):
                try:
                    tweets_df = run_async(proc.get_tweets(ceo_handle))
                    if not tweets_df.empty and 'date' in tweets_df.columns:
                        filtered = filter_tweets_by_date(tweets_df).sort_values('date', ascending=False)
                        filtered['date'] = filtered['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        with data_results:
                            st.subheader(f"Tweets for @{ceo_handle} ({date_display})")
                            if filtered.empty:
                                st.info("No tweets found for this date range.")
                            else:
                                enriched = filtered.copy()
                            enriched['sentiment_label'] = enriched['sentiment'].apply(get_refined_sentiment)
                            enriched['tone'] = enriched.apply(
                                lambda r: get_tone_category(str(r['text']), float(r['sentiment'])), axis=1
                            )
                            enriched['tweet_type'] = enriched['text'].apply(get_tweet_type)

                            fa, fb, fc = st.columns(3)
                            with fa:
                                sel_labels = st.multiselect(
                                    "Sentiment", sorted(enriched['sentiment_label'].unique()),
                                    default=sorted(enriched['sentiment_label'].unique()),
                                    key="data_filter_sentiment",
                                )
                            with fb:
                                sel_tones = st.multiselect(
                                    "Tone", sorted(enriched['tone'].unique()),
                                    default=sorted(enriched['tone'].unique()),
                                    key="data_filter_tone",
                                )
                            with fc:
                                sel_types = st.multiselect(
                                    "Tweet Type", sorted(enriched['tweet_type'].unique()),
                                    default=sorted(enriched['tweet_type'].unique()),
                                    key="data_filter_type",
                                )

                            fmask = (
                                enriched['sentiment_label'].isin(sel_labels) &
                                enriched['tone'].isin(sel_tones) &
                                enriched['tweet_type'].isin(sel_types)
                            )
                            st.caption(f"{fmask.sum()} of {len(enriched)} tweets shown")
                            st.dataframe(
                                enriched[fmask][['date', 'text', 'sentiment', 'sentiment_label', 'tone', 'tweet_type']],
                                use_container_width=True,
                            )
                    else:
                        with data_results:
                            st.info("No tweets found.")
                except Exception as e:
                    st.error(f"Failed to fetch tweets: {str(e)}")

    if pull_stocks:
        if not stock_ticker:
            st.error("Please enter a Stock Ticker.")
        else:
            with st.spinner(f"Pulling stock data for {stock_ticker.upper()}..."):
                try:
                    start_dt = datetime.combine(query_start_date, datetime.min.time()).replace(tzinfo=pytz.utc) - timedelta(days=2)
                    end_dt = datetime.combine(query_end_date, datetime.max.time()).replace(tzinfo=pytz.utc) + timedelta(days=2) if query_end_date else None
                    stocks_df = proc.get_stocks(stock_ticker, start_date=start_dt, end_date=end_dt)
                    if not stocks_df.empty:
                        stocks_df = stocks_df.reset_index()
                        if 'timestamp' in stocks_df.columns:
                            stocks_df['timestamp'] = stocks_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                        with data_results:
                            st.subheader(f"Stock Data for {stock_ticker.upper()} ({date_display})")
                            st.dataframe(stocks_df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap']], use_container_width=True)
                    else:
                        with data_results:
                            st.info("No stock data found for this date range.")
                except Exception as e:
                    st.error(f"Failed to fetch stock data: {str(e)}\n{traceback.format_exc()}")

    if pull_merged:
        if not ceo_handle or not stock_ticker:
            st.error("Please enter a CEO Twitter Handle and a Stock Ticker.")
        else:
            with st.spinner("Pulling and merging data..."):
                try:
                    tweets_df = run_async(proc.get_tweets(ceo_handle))
                    if not tweets_df.empty and 'date' in tweets_df.columns:
                        tweets_df = filter_tweets_by_date(tweets_df)

                    if tweets_df.empty:
                        with data_results:
                            st.info("No tweets found for this date range to merge.")
                    else:
                        min_date = tweets_df['date'].min() - timedelta(days=5)
                        max_date = tweets_df['date'].max() + timedelta(days=5)
                        stocks_df = proc.get_stocks(stock_ticker, start_date=min_date, end_date=max_date)

                        if not stocks_df.empty:
                            if isinstance(stocks_df.index, pd.MultiIndex):
                                stocks_df['date_only'] = stocks_df.index.get_level_values('timestamp').date
                            else:
                                stocks_df['date_only'] = stocks_df.index.date

                        merged_data = []
                        for _, row in tweets_df.iterrows():
                            sentiment = float(row['sentiment'])
                            text = str(row['text'])
                            tweet_date = row['date']
                            target_date_only = weekend_shift(tweet_date).date()

                            stock_close = stock_volume = stock_open_close_diff = None
                            if not stocks_df.empty:
                                valid = stocks_df[stocks_df['date_only'] >= target_date_only]
                                if not valid.empty:
                                    stock_close = float(valid['close'].iloc[0])
                                    stock_volume = float(valid['volume'].iloc[0])
                                    stock_open = float(valid['open'].iloc[0])
                                    stock_open_close_diff = float(stock_open - stock_close)

                            merged_data.append({
                                "date": tweet_date.strftime('%Y-%m-%d %H:%M:%S'),
                                "ceo": ceo_handle,
                                "tweet_text": text,
                                "sentiment_score": sentiment,
                                "refined_sentiment": get_refined_sentiment(sentiment),
                                "tone_category": get_tone_category(text, sentiment),
                                "tweet_type": get_tweet_type(text),
                                "stock_ticker": stock_ticker,
                                "stock_close": stock_close,
                                "stock_volume": stock_volume,
                                "stock_open_close_diff": stock_open_close_diff,
                            })

                        merged_df = pd.DataFrame(merged_data).sort_values('date', ascending=False)
                        with data_results:
                            st.subheader(f"Merged Data (@{ceo_handle} & {stock_ticker.upper()}) ({date_display})")
                            if merged_df.empty:
                                st.info("No merged data found.")
                            else:
                                st.dataframe(merged_df, use_container_width=True)
                except Exception as e:
                    st.error(f"Failed to fetch merged data: {str(e)}")

# ── STOCK ANALYSIS TAB — ATR section ─────────────────────────────────────────
with tab_stock:
    st.markdown("### ATR Analysis")
    st.markdown("Identifies periods where high-sentiment tweets coincide with unusual stock volatility relative to the 14-day ATR baseline.")
    run_atr = st.button("Run ATR Analysis", type="primary")
    atr_results_container = st.container()

    if run_atr:
        if not ceo_handle or not stock_ticker:
            st.error("Please enter a CEO Twitter Handle and a Stock Ticker.")
        else:
            with st.spinner("Running ATR Analysis..."):
                try:
                    tweets_df = run_async(proc.get_tweets(ceo_handle))
                    if not tweets_df.empty and 'date' in tweets_df.columns:
                        tweets_df = filter_tweets_by_date(tweets_df)

                    if tweets_df.empty:
                        with atr_results_container:
                            st.info("No tweets found for this date range.")
                    else:
                        tweets_df = tweets_df[tweets_df['sentiment'] >= 0.5]
                        if tweets_df.empty:
                            with atr_results_container:
                                st.info("No high-sentiment tweets (>= 0.5) found in this date range.")
                        else:
                            min_date = tweets_df['date'].min() - timedelta(days=40)
                            max_date = tweets_df['date'].max() + timedelta(days=5)
                            stocks_df = proc.get_stocks(stock_ticker, start_date=min_date, end_date=max_date)

                            if stocks_df.empty:
                                with atr_results_container:
                                    st.info("No stock data found to analyze.")
                            else:
                                if isinstance(stocks_df.index, pd.MultiIndex):
                                    stocks_df['date_only'] = stocks_df.index.get_level_values('timestamp').date
                                else:
                                    stocks_df['date_only'] = stocks_df.index.date

                                stocks_df['prev_close'] = stocks_df['close'].shift(1)
                                stocks_df['tr1'] = stocks_df['high'] - stocks_df['low']
                                stocks_df['tr2'] = abs(stocks_df['high'] - stocks_df['prev_close'])
                                stocks_df['tr3'] = abs(stocks_df['low'] - stocks_df['prev_close'])
                                stocks_df['true_range'] = stocks_df[['tr1', 'tr2', 'tr3']].max(axis=1)
                                stocks_df['atr_14'] = stocks_df['true_range'].rolling(window=14).mean()

                                atr_results = []
                                for _, row in tweets_df.iterrows():
                                    sentiment = float(row['sentiment'])
                                    text = str(row['text'])
                                    tweet_date = row['date']
                                    target_date_only = weekend_shift(tweet_date).date()

                                    valid = stocks_df[stocks_df['date_only'] >= target_date_only]
                                    if len(valid) < 2:
                                        continue

                                    target_stock = valid.iloc[0]
                                    next_day_stock = valid.iloc[1]
                                    atr = target_stock['atr_14']
                                    if pd.isna(atr):
                                        continue

                                    close_to_close = float(next_day_stock['close']) - float(target_stock['close'])
                                    next_tr = float(next_day_stock['true_range']) if not pd.isna(next_day_stock['true_range']) else abs(close_to_close)
                                    atr_ratio = next_tr / atr

                                    if sentiment >= 0.9:
                                        bucket = "Very High (0.9+)"
                                    elif sentiment >= 0.7:
                                        bucket = "High (0.7–0.9)"
                                    else:
                                        bucket = "Moderate (0.5–0.7)"

                                    atr_results.append({
                                        "date": tweet_date.strftime('%Y-%m-%d %H:%M:%S'),
                                        "target_date": target_stock['date_only'].strftime('%Y-%m-%d'),
                                        "tweet_text": text,
                                        "sentiment_score": sentiment,
                                        "sentiment_bucket": bucket,
                                        "stock_ticker": stock_ticker,
                                        "close_target": round(float(target_stock['close']), 2),
                                        "close_next": round(float(next_day_stock['close']), 2),
                                        "close_to_close": round(close_to_close, 2),
                                        "next_day_range": round(next_tr, 2),
                                        "atr_14": round(float(atr), 2),
                                        "atr_ratio": round(atr_ratio, 2),
                                        "vs_atr": "Above" if next_tr > atr else "Below",
                                    })

                                results_df = pd.DataFrame(atr_results)
                                with atr_results_container:
                                    st.subheader(f"ATR Analysis (@{ceo_handle} & {stock_ticker.upper()}) ({date_display})")
                                    if results_df.empty:
                                        st.info("No valid market data overlapping with high-sentiment tweets.")
                                    else:
                                        st.dataframe(results_df, use_container_width=True)

                                        viz_df = results_df[['date', 'next_day_range', 'atr_14']].set_index('date')
                                        st.subheader("Next-Day True Range vs ATR (14-day)")
                                        st.bar_chart(viz_df)

                                        ratio_df = results_df[['date', 'atr_ratio']].set_index('date')
                                        st.subheader("ATR Ratio — 1.0 = normal, >1.0 = unusual")
                                        st.bar_chart(ratio_df)

                                        total = len(results_df)
                                        exceeded = len(results_df[results_df['vs_atr'] == 'Above'])
                                        up_moves = len(results_df[results_df['close_to_close'] > 0])
                                        avg_ratio = results_df['atr_ratio'].mean()
                                        st.write(f"**{exceeded}/{total}** tweet days had next-day range above ATR. "
                                                 f"**{up_moves}/{total}** closed up. Avg ATR ratio: **{avg_ratio:.2f}x**.")

                                        st.subheader("ATR Ratio by Sentiment Bucket")
                                        bucket_df = results_df.groupby('sentiment_bucket')['atr_ratio'].agg(['mean', 'count']).rename(columns={'mean': 'avg_atr_ratio', 'count': 'tweets'})
                                        st.dataframe(bucket_df, use_container_width=True)

                except Exception as e:
                    st.error(f"ATR Analysis failed: {str(e)}\n{traceback.format_exc()}")

# ── TWEET ANALYSIS TAB (Tweet Impact + Post-Tweet Trend) ─────────────────────
with tab_impact:
    st.markdown("### Tweet Impact")
    st.markdown("Measures same-day stock reaction on days a high-sentiment tweet was posted. Compares open-to-close move and volume against the 14-day baseline.")
    run_impact = st.button("Run Tweet Impact", type="primary")
    impact_results_container = st.container()

    if run_impact:
        if not ceo_handle or not stock_ticker:
            st.error("Please enter a CEO Twitter Handle and a Stock Ticker.")
        else:
            with st.spinner("Running Tweet Impact Analysis..."):
                try:
                    tweets_df = run_async(proc.get_tweets(ceo_handle))
                    if not tweets_df.empty and 'date' in tweets_df.columns:
                        tweets_df = filter_tweets_by_date(tweets_df)

                    if tweets_df.empty:
                        with impact_results_container:
                            st.info("No tweets found for this date range.")
                    else:
                        tweets_df = tweets_df[tweets_df['sentiment'] >= 0.5]
                        if tweets_df.empty:
                            with impact_results_container:
                                st.info("No high-sentiment tweets (>= 0.5) found in this date range.")
                        else:
                            # Fetch extra history for rolling baselines
                            min_date = tweets_df['date'].min() - timedelta(days=40)
                            max_date = tweets_df['date'].max() + timedelta(days=2)
                            stocks_df = proc.get_stocks(stock_ticker, start_date=min_date, end_date=max_date)

                            if stocks_df.empty:
                                with impact_results_container:
                                    st.info("No stock data found to analyze.")
                            else:
                                if isinstance(stocks_df.index, pd.MultiIndex):
                                    stocks_df['date_only'] = stocks_df.index.get_level_values('timestamp').date
                                else:
                                    stocks_df['date_only'] = stocks_df.index.date

                                # 14-day rolling baselines
                                stocks_df['day_move'] = abs(stocks_df['close'] - stocks_df['open'])
                                stocks_df['avg_day_move_14'] = stocks_df['day_move'].rolling(14).mean()
                                stocks_df['avg_volume_14'] = stocks_df['volume'].rolling(14).mean()

                                impact_results = []
                                for _, row in tweets_df.iterrows():
                                    sentiment = float(row['sentiment'])
                                    text = str(row['text'])
                                    tweet_date = row['date']
                                    target_date_only = weekend_shift(tweet_date).date()

                                    match = stocks_df[stocks_df['date_only'] == target_date_only]
                                    if match.empty:
                                        continue

                                    stock = match.iloc[0]
                                    if pd.isna(stock['avg_day_move_14']) or pd.isna(stock['avg_volume_14']):
                                        continue

                                    open_price = float(stock['open'])
                                    close_price = float(stock['close'])
                                    day_move = close_price - open_price
                                    day_move_pct = (day_move / open_price) * 100
                                    abs_move = abs(day_move)
                                    avg_move = float(stock['avg_day_move_14'])
                                    avg_vol = float(stock['avg_volume_14'])
                                    volume = float(stock['volume'])

                                    move_ratio = abs_move / avg_move if avg_move > 0 else None
                                    volume_ratio = volume / avg_vol if avg_vol > 0 else None
                                    direction_match = (sentiment > 0 and day_move > 0) or (sentiment < 0 and day_move < 0)

                                    if sentiment >= 0.9:
                                        bucket = "Very High (0.9+)"
                                    elif sentiment >= 0.7:
                                        bucket = "High (0.7–0.9)"
                                    else:
                                        bucket = "Moderate (0.5–0.7)"

                                    impact_results.append({
                                        "tweet_date": tweet_date.strftime('%Y-%m-%d %H:%M:%S'),
                                        "stock_date": target_date_only.strftime('%Y-%m-%d'),
                                        "tweet_text": text,
                                        "sentiment_score": round(sentiment, 3),
                                        "sentiment_bucket": bucket,
                                        "open": round(open_price, 2),
                                        "close": round(close_price, 2),
                                        "day_move_$": round(day_move, 2),
                                        "day_move_%": round(day_move_pct, 2),
                                        "move_ratio": round(move_ratio, 2) if move_ratio else None,
                                        "volume": int(volume),
                                        "volume_ratio": round(volume_ratio, 2) if volume_ratio else None,
                                        "direction_match": "Yes" if direction_match else "No",
                                    })

                                impact_df = pd.DataFrame(impact_results)
                                with impact_results_container:
                                    st.subheader(f"Tweet Impact (@{ceo_handle} & {stock_ticker.upper()}) ({date_display})")
                                    if impact_df.empty:
                                        st.info("No tweet days matched to stock trading days.")
                                    else:
                                        st.dataframe(impact_df, use_container_width=True)

                                        # Move ratio chart
                                        mr_df = impact_df[['stock_date', 'move_ratio']].set_index('stock_date')
                                        st.subheader("Day Move Ratio vs Baseline — 1.0 = normal open-to-close move")
                                        st.bar_chart(mr_df)

                                        # Volume ratio chart
                                        vr_df = impact_df[['stock_date', 'volume_ratio']].set_index('stock_date')
                                        st.subheader("Volume Ratio vs 14-day Avg — 1.0 = normal volume")
                                        st.bar_chart(vr_df)

                                        total = len(impact_df)
                                        dir_match = len(impact_df[impact_df['direction_match'] == 'Yes'])
                                        avg_move_ratio = impact_df['move_ratio'].mean()
                                        avg_vol_ratio = impact_df['volume_ratio'].mean()
                                        st.write(
                                            f"**{dir_match}/{total}** tweet days the stock moved in the same direction as sentiment. "
                                            f"Avg move ratio: **{avg_move_ratio:.2f}x** baseline. "
                                            f"Avg volume ratio: **{avg_vol_ratio:.2f}x** baseline."
                                        )

                                        st.subheader("Impact by Sentiment Bucket")
                                        bucket_df = impact_df.groupby('sentiment_bucket').agg(
                                            tweets=('sentiment_score', 'count'),
                                            avg_move_ratio=('move_ratio', 'mean'),
                                            avg_volume_ratio=('volume_ratio', 'mean'),
                                            direction_match_pct=('direction_match', lambda x: round((x == 'Yes').mean() * 100, 1))
                                        )
                                        st.dataframe(bucket_df, use_container_width=True)

                except Exception as e:
                    st.error(f"Tweet Impact analysis failed: {str(e)}\n{traceback.format_exc()}")

    st.markdown("---")
    st.markdown("### Post-Tweet Trend")
    st.markdown("Shows how the stock price evolves over N trading days after a high-sentiment tweet. Each spoke on the radar is a trading day; the radial value is the average cumulative % return across all tweets.")
    max_days = st.slider("Trading days to look ahead", min_value=2, max_value=14, value=7)
    run_trend = st.button("Run Post-Tweet Trend", type="primary")
    trend_results_container = st.container()

    if run_trend:
        if not ceo_handle or not stock_ticker:
            st.error("Please enter a CEO Twitter Handle and a Stock Ticker.")
        else:
            with st.spinner("Running Post-Tweet Trend Analysis..."):
                try:
                    tweets_df = run_async(proc.get_tweets(ceo_handle))
                    if not tweets_df.empty and 'date' in tweets_df.columns:
                        tweets_df = filter_tweets_by_date(tweets_df)

                    if tweets_df.empty:
                        with trend_results_container:
                            st.info("No tweets found for this date range.")
                    else:
                        tweets_df = tweets_df[tweets_df['sentiment'] >= 0.5]
                        if tweets_df.empty:
                            with trend_results_container:
                                st.info("No high-sentiment tweets (>= 0.5) found in this date range.")
                        else:
                            # Fetch enough forward data to cover max_days trading days after last tweet
                            min_date = tweets_df['date'].min() - timedelta(days=5)
                            max_date = tweets_df['date'].max() + timedelta(days=max_days * 2)
                            stocks_df = proc.get_stocks(stock_ticker, start_date=min_date, end_date=max_date)

                            if stocks_df.empty:
                                with trend_results_container:
                                    st.info("No stock data found to analyze.")
                            else:
                                if isinstance(stocks_df.index, pd.MultiIndex):
                                    stocks_df['date_only'] = stocks_df.index.get_level_values('timestamp').date
                                else:
                                    stocks_df['date_only'] = stocks_df.index.date

                                # Ordered list of trading days for indexing
                                trading_days = sorted(stocks_df['date_only'].unique())
                                trading_day_index = {d: i for i, d in enumerate(trading_days)}
                                close_by_date = stocks_df.groupby('date_only')['close'].first()

                                # Build return series per tweet
                                tweet_series = []  # list of lists: each is [day1_ret, day2_ret, ..., dayN_ret]
                                tweet_labels = []

                                for _, row in tweets_df.iterrows():
                                    tweet_date = row['date']
                                    target_date = weekend_shift(tweet_date).date()

                                    # Find nearest trading day on or after tweet date
                                    base_day = next((d for d in trading_days if d >= target_date), None)
                                    if base_day is None:
                                        continue
                                    base_idx = trading_day_index[base_day]
                                    base_close = float(close_by_date[base_day])

                                    returns = []
                                    valid = True
                                    for n in range(1, max_days + 1):
                                        future_idx = base_idx + n
                                        if future_idx >= len(trading_days):
                                            valid = False
                                            break
                                        future_close = float(close_by_date[trading_days[future_idx]])
                                        pct = (future_close - base_close) / base_close * 100
                                        returns.append(round(pct, 3))

                                    if valid:
                                        tweet_series.append(returns)
                                        tweet_labels.append(tweet_date.strftime('%Y-%m-%d'))

                                with trend_results_container:
                                    st.subheader(f"Post-Tweet Trend (@{ceo_handle} & {stock_ticker.upper()}) ({date_display})")

                                    if not tweet_series:
                                        st.info("Not enough forward stock data to compute trends. Try a shorter date range or fewer days ahead.")
                                    else:
                                        categories = [f"Day {n}" for n in range(1, max_days + 1)]
                                        avg_returns = [
                                            round(sum(s[i] for s in tweet_series) / len(tweet_series), 3)
                                            for i in range(max_days)
                                        ]

                                        # Close the radar loop
                                        closed_cats = categories + [categories[0]]
                                        closed_avg = avg_returns + [avg_returns[0]]

                                        fig = go.Figure()

                                        # Individual tweet traces (faint)
                                        for i, (series, label) in enumerate(zip(tweet_series, tweet_labels)):
                                            closed_series = series + [series[0]]
                                            fig.add_trace(go.Scatterpolar(
                                                r=closed_series,
                                                theta=closed_cats,
                                                mode='lines',
                                                name=label,
                                                line=dict(width=1),
                                                opacity=0.3,
                                                showlegend=True,
                                            ))

                                        # Average trace (bold)
                                        fig.add_trace(go.Scatterpolar(
                                            r=closed_avg,
                                            theta=closed_cats,
                                            mode='lines+markers',
                                            name='Average',
                                            line=dict(width=3, color='#00CC96'),
                                            marker=dict(size=6),
                                            showlegend=True,
                                        ))

                                        fig.update_layout(
                                            polar=dict(
                                                radialaxis=dict(
                                                    visible=True,
                                                    ticksuffix='%',
                                                    showline=True,
                                                )
                                            ),
                                            title=f"Cumulative % Return — {max_days} Trading Days After Tweet",
                                            height=550,
                                            legend=dict(orientation='v', x=1.05),
                                        )

                                        st.plotly_chart(fig, use_container_width=True)

                                        # Summary table
                                        summary_df = pd.DataFrame({
                                            'trading_day': categories,
                                            'avg_return_%': avg_returns,
                                        })
                                        st.subheader("Average Cumulative Return by Trading Day")
                                        st.dataframe(summary_df, use_container_width=True)

                                        best_day = avg_returns.index(max(avg_returns)) + 1
                                        worst_day = avg_returns.index(min(avg_returns)) + 1
                                        st.write(
                                            f"Across **{len(tweet_series)}** tweets, avg peak return was on **Day {best_day}** "
                                            f"({max(avg_returns):+.2f}%) and avg trough on **Day {worst_day}** ({min(avg_returns):+.2f}%)."
                                        )

                except Exception as e:
                    st.error(f"Post-Tweet Trend analysis failed: {str(e)}\n{traceback.format_exc()}")

# ── STOCK ANALYSIS TAB — Chart section ───────────────────────────────────────
with tab_stock:
    st.markdown("---")
    st.markdown("### Stock Chart")

    with st.expander("📖 Glossary — click to learn what each term means"):
        g1, g2 = st.columns(2)
        with g1:
            st.markdown("""
**Candlestick Chart**
A visual way to see how a stock moved each day. Each "candle" shows where the price started, where it ended, and how high/low it went in between. 🟢 Green = the stock went up that day. 🔴 Red = it went down.

**SMA — Simple Moving Average**
Imagine averaging a student's last 20 test scores to get a sense of how they're really doing, ignoring one-off good or bad days. SMA does the same for a stock's price. If the price is above its average, things are generally trending up.

**EMA — Exponential Moving Average**
Same idea as SMA, but it pays more attention to what happened recently. Think of it like a coach who cares more about your last few games than games from months ago.

**Bollinger Bands**
Three lines drawn around the price — a middle line (the average) and an upper and lower boundary. When the price touches the top boundary, the stock might be getting too expensive too fast. Near the bottom boundary, it might be getting oversold. When the bands spread wide, the stock is moving a lot; when they're tight, it's calm.

**Volume**
Simply how many shares were bought and sold that day. If a stock jumps in price but barely anyone is trading it, that move might not last. A big price move with lots of trading behind it is more meaningful.
""")
        with g2:
            st.markdown("""
**RSI — Relative Strength Index**
A speedometer for how fast a stock has been moving. It goes from 0 to 100. Above 70 means the stock has been rising very fast and might be due for a cooldown. Below 30 means it's been falling fast and might bounce back. Between 30–70 is the normal zone.

**MACD**
A way to spot when a stock's momentum is shifting. It compares two moving averages and shows when they cross each other — like watching two runners where one overtakes the other. When the MACD line crosses above the signal line, it's often seen as a good sign. Below is a warning sign.

**Support Level**
A price the stock keeps bouncing off of when it drops — like a floor. Buyers tend to step in at this price, so the stock rarely falls through it easily.

**Resistance Level**
The opposite of support — a price the stock keeps hitting but struggling to break through, like a ceiling. Sellers tend to show up here.

**ATR — Average True Range**
Measures how much a stock typically swings up and down in a normal day. A high ATR means the stock is jumpy and moves a lot. A low ATR means it's more stable and predictable.
""")

    st.markdown("---")

    # Controls
    ctrl1, ctrl2, ctrl3 = st.columns(3)
    with ctrl1:
        st.markdown("**Chart & Moving Averages**")
        chart_type = st.radio("Chart type", ["Candlestick", "Line"], horizontal=True)
        show_sma_20 = st.checkbox("SMA 20", value=True)
        show_sma_50 = st.checkbox("SMA 50", value=True)
        show_sma_200 = st.checkbox("SMA 200")
        show_ema_12 = st.checkbox("EMA 12")
        show_ema_26 = st.checkbox("EMA 26")
    with ctrl2:
        st.markdown("**Bollinger Bands**")
        show_bb = st.checkbox("Show Bollinger Bands", value=True)
        bb_period = st.slider("BB Period", 5, 50, 20, disabled=not show_bb)
        bb_std_val = st.slider("BB Std Dev", 1.0, 3.0, 2.0, 0.5, disabled=not show_bb)
    with ctrl3:
        st.markdown("**Indicators**")
        show_rsi = st.checkbox("RSI", value=True)
        rsi_period = st.slider("RSI Period", 5, 30, 14, disabled=not show_rsi)
        show_macd = st.checkbox("MACD", value=True)
        st.caption("MACD uses standard 12/26/9 periods")

    run_stock_analysis = st.button("Load Chart", type="primary")
    stock_chart_container = st.container()

    if run_stock_analysis:
        if not stock_ticker:
            st.error("Please enter a Stock Ticker above.")
        else:
            with st.spinner(f"Loading {stock_ticker.upper()} data..."):
                try:
                    # Fetch with 250-day warmup so SMA 200 is valid from day 1 of the display range
                    warmup_start = datetime.combine(query_start_date, datetime.min.time()).replace(tzinfo=pytz.utc) - timedelta(days=250)
                    fetch_end = datetime.combine(query_end_date, datetime.max.time()).replace(tzinfo=pytz.utc) if query_end_date else None

                    stocks_df = proc.get_stocks(stock_ticker, start_date=warmup_start, end_date=fetch_end)

                    if stocks_df.empty:
                        with stock_chart_container:
                            st.info("No stock data found for this ticker and date range.")
                    else:
                        stocks_df = stocks_df.reset_index()
                        stocks_df['timestamp'] = pd.to_datetime(stocks_df['timestamp'])
                        stocks_df = stocks_df.sort_values('timestamp')

                        # ── Indicators ────────────────────────────────────────
                        stocks_df['sma_20'] = stocks_df['close'].rolling(20).mean()
                        stocks_df['sma_50'] = stocks_df['close'].rolling(50).mean()
                        stocks_df['sma_200'] = stocks_df['close'].rolling(200).mean()
                        stocks_df['ema_12'] = stocks_df['close'].ewm(span=12, adjust=False).mean()
                        stocks_df['ema_26'] = stocks_df['close'].ewm(span=26, adjust=False).mean()

                        stocks_df['bb_mid'] = stocks_df['close'].rolling(bb_period).mean()
                        stocks_df['bb_std'] = stocks_df['close'].rolling(bb_period).std()
                        stocks_df['bb_upper'] = stocks_df['bb_mid'] + bb_std_val * stocks_df['bb_std']
                        stocks_df['bb_lower'] = stocks_df['bb_mid'] - bb_std_val * stocks_df['bb_std']

                        delta = stocks_df['close'].diff()
                        gain = delta.where(delta > 0, 0.0).rolling(rsi_period).mean()
                        loss = (-delta.where(delta < 0, 0.0)).rolling(rsi_period).mean()
                        stocks_df['rsi'] = 100 - (100 / (1 + gain / loss))

                        stocks_df['macd'] = stocks_df['close'].ewm(span=12, adjust=False).mean() - stocks_df['close'].ewm(span=26, adjust=False).mean()
                        stocks_df['macd_signal'] = stocks_df['macd'].ewm(span=9, adjust=False).mean()
                        stocks_df['macd_hist'] = stocks_df['macd'] - stocks_df['macd_signal']

                        # ── Filter to display range ───────────────────────────
                        display_start = pd.to_datetime(query_start_date).tz_localize('UTC')
                        display_df = stocks_df[stocks_df['timestamp'] >= display_start].copy()
                        if query_end_date:
                            display_end = pd.to_datetime(query_end_date).tz_localize('UTC') + timedelta(days=1)
                            display_df = display_df[display_df['timestamp'] < display_end]

                        if display_df.empty:
                            with stock_chart_container:
                                st.info("No data in the selected date range.")
                        else:
                            first_close = float(display_df['close'].iloc[0])
                            last_close = float(display_df['close'].iloc[-1])
                            pct_change = (last_close - first_close) / first_close * 100
                            range_high = float(display_df['high'].max())
                            range_low = float(display_df['low'].min())
                            avg_volume = int(display_df['volume'].mean())
                            last_rsi = display_df['rsi'].dropna().iloc[-1] if show_rsi and not display_df['rsi'].dropna().empty else None

                            with stock_chart_container:
                                st.subheader(f"{stock_ticker.upper()} — {date_display}")

                                m1, m2, m3, m4, m5 = st.columns(5)
                                m1.metric("Last Close", f"${last_close:.2f}")
                                m2.metric("Range Change", f"{pct_change:+.2f}%")
                                m3.metric("Range High", f"${range_high:.2f}")
                                m4.metric("Range Low", f"${range_low:.2f}")
                                if last_rsi is not None:
                                    rsi_label = "Overbought" if last_rsi > 70 else ("Oversold" if last_rsi < 30 else "Neutral")
                                    m5.metric("RSI", f"{last_rsi:.1f} — {rsi_label}")
                                else:
                                    m5.metric("Avg Volume", f"{avg_volume:,}")

                                # ── Build subplot layout ──────────────────────
                                n_rows_chart = 2
                                row_heights_chart = [0.6, 0.15]
                                subplot_titles_chart = [f"{stock_ticker.upper()} Price", "Volume"]
                                rsi_row_num = macd_row_num = None

                                if show_rsi:
                                    rsi_row_num = n_rows_chart + 1
                                    n_rows_chart += 1
                                    row_heights_chart.append(0.125)
                                    subplot_titles_chart.append(f"RSI ({rsi_period})")
                                if show_macd:
                                    macd_row_num = n_rows_chart + 1
                                    n_rows_chart += 1
                                    row_heights_chart.append(0.125)
                                    subplot_titles_chart.append("MACD (12/26/9)")

                                fig = make_subplots(
                                    rows=n_rows_chart, cols=1,
                                    shared_xaxes=True,
                                    row_heights=row_heights_chart,
                                    vertical_spacing=0.04,
                                    subplot_titles=subplot_titles_chart,
                                )

                                ts = display_df['timestamp']

                                # Price
                                if chart_type == "Candlestick":
                                    fig.add_trace(go.Candlestick(
                                        x=ts, open=display_df['open'], high=display_df['high'],
                                        low=display_df['low'], close=display_df['close'],
                                        name="Price",
                                        increasing_line_color='#26a69a',
                                        decreasing_line_color='#ef5350',
                                    ), row=1, col=1)
                                else:
                                    fig.add_trace(go.Scatter(x=ts, y=display_df['close'], name="Close", line=dict(color='#2196F3', width=1.5)), row=1, col=1)

                                # Bollinger Bands
                                if show_bb:
                                    fig.add_trace(go.Scatter(x=ts, y=display_df['bb_upper'], name="BB Upper", line=dict(color='rgba(150,150,150,0.5)', width=1), showlegend=False), row=1, col=1)
                                    fig.add_trace(go.Scatter(x=ts, y=display_df['bb_lower'], name="BB Lower", line=dict(color='rgba(150,150,150,0.5)', width=1), fill='tonexty', fillcolor='rgba(150,150,150,0.06)', showlegend=False), row=1, col=1)
                                    fig.add_trace(go.Scatter(x=ts, y=display_df['bb_mid'], name="BB Mid", line=dict(color='rgba(180,180,180,0.7)', width=1, dash='dot')), row=1, col=1)

                                # Moving averages
                                ma_config = {
                                    'sma_20':  ('SMA 20',  '#FF9800', show_sma_20),
                                    'sma_50':  ('SMA 50',  '#9C27B0', show_sma_50),
                                    'sma_200': ('SMA 200', '#F44336', show_sma_200),
                                    'ema_12':  ('EMA 12',  '#00BCD4', show_ema_12),
                                    'ema_26':  ('EMA 26',  '#8BC34A', show_ema_26),
                                }
                                for col_name, (label, color, show) in ma_config.items():
                                    if show:
                                        fig.add_trace(go.Scatter(x=ts, y=display_df[col_name], name=label, line=dict(color=color, width=1.2)), row=1, col=1)

                                # Volume
                                vol_colors = ['#26a69a' if c >= o else '#ef5350' for c, o in zip(display_df['close'], display_df['open'])]
                                fig.add_trace(go.Bar(x=ts, y=display_df['volume'], name="Volume", marker_color=vol_colors, showlegend=False), row=2, col=1)

                                # RSI
                                if show_rsi and rsi_row_num:
                                    fig.add_trace(go.Scatter(x=ts, y=display_df['rsi'], name="RSI", line=dict(color='#FF9800', width=1.5)), row=rsi_row_num, col=1)
                                    fig.add_hline(y=70, line=dict(color='red', width=1, dash='dash'), row=rsi_row_num, col=1)
                                    fig.add_hline(y=30, line=dict(color='green', width=1, dash='dash'), row=rsi_row_num, col=1)
                                    fig.update_yaxes(range=[0, 100], row=rsi_row_num, col=1)

                                # MACD
                                if show_macd and macd_row_num:
                                    fig.add_trace(go.Scatter(x=ts, y=display_df['macd'], name="MACD", line=dict(color='#2196F3', width=1.5)), row=macd_row_num, col=1)
                                    fig.add_trace(go.Scatter(x=ts, y=display_df['macd_signal'], name="Signal", line=dict(color='#FF9800', width=1.5)), row=macd_row_num, col=1)
                                    hist_colors = ['#26a69a' if v >= 0 else '#ef5350' for v in display_df['macd_hist'].fillna(0)]
                                    fig.add_trace(go.Bar(x=ts, y=display_df['macd_hist'], name="Histogram", marker_color=hist_colors, showlegend=False), row=macd_row_num, col=1)

                                fig.update_layout(
                                    height=750,
                                    xaxis_rangeslider_visible=False,
                                    template='plotly_dark',
                                    legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0),
                                    margin=dict(l=0, r=0, t=60, b=0),
                                )

                                st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Stock analysis failed: {str(e)}\n{traceback.format_exc()}")

# ── TWEET EXPLORER TAB ────────────────────────────────────────────────────────
with tab_drill:
    st.markdown("Fetch tweets, select one, then choose a stock to see how it moved around that tweet's date.")

    CEO_TICKER_MAP = {
        "elonmusk": "TSLA",
        "tim_cook": "AAPL",
        "satyanadella": "MSFT",
        "sundarpichai": "GOOGL",
        "MichaelDell": "DELL",
        "LisaSu": "AMD",
        "ajassy": "AMZN",
        "bchesky": "ABNB",
        "dkhos": "UBER",
        "RobertIger": "DIS",
    }

    if st.button("Fetch Tweets", type="primary", key="drill_fetch"):
        if not ceo_handle:
            st.error("Please enter a CEO Twitter Handle above.")
        else:
            with st.spinner(f"Fetching tweets for @{ceo_handle}..."):
                try:
                    tweets_df = run_async(proc.get_tweets(ceo_handle))
                    if not tweets_df.empty and 'date' in tweets_df.columns:
                        tweets_df = filter_tweets_by_date(tweets_df)
                        tweets_df = tweets_df.sort_values('date', ascending=False).reset_index(drop=True)
                        tweets_df['date_str'] = tweets_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    st.session_state['drill_tweets'] = tweets_df
                    st.session_state['drill_ceo'] = ceo_handle
                except Exception as e:
                    st.error(f"Failed to fetch tweets: {str(e)}")

    if 'drill_tweets' in st.session_state and not st.session_state['drill_tweets'].empty:
        df = st.session_state['drill_tweets']
        st.markdown(f"**Tweets for @{st.session_state.get('drill_ceo', '')}** — click a row to select it")

        event = st.dataframe(
            df[['date_str', 'text', 'sentiment']].rename(columns={'date_str': 'date'}),
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            height=300,
        )

        selected_rows = event.selection.rows
        if selected_rows:
            selected = df.iloc[selected_rows[0]]
            tweet_date_str = selected['date_str']
            tweet_text = selected['text']
            tweet_sentiment = float(selected['sentiment'])

            st.markdown("---")
            st.markdown(f"**Selected tweet** — {tweet_date_str}")
            st.info(f'"{tweet_text}"')

            sentiment_label = "Very Positive" if tweet_sentiment >= 0.6 else \
                              "Positive" if tweet_sentiment >= 0.2 else \
                              "Neutral" if tweet_sentiment >= -0.2 else \
                              "Negative" if tweet_sentiment >= -0.6 else "Very Negative"
            st.caption(f"Sentiment: {tweet_sentiment:.3f} — {sentiment_label}")

            st.markdown("#### Stock to analyze")
            auto_ticker = CEO_TICKER_MAP.get(st.session_state.get('drill_ceo', '').lower(), stock_ticker or "")
            dc1, dc2 = st.columns([1, 2])
            with dc1:
                drill_ticker = st.text_input("Ticker", value=auto_ticker, key="drill_ticker", placeholder="e.g. TSLA")
                window_days = st.slider("Days to show on each side of tweet", 3, 30, 10, key="drill_window")

            if st.button("Load Stock Chart", type="primary", key="drill_load") and drill_ticker:
                tweet_dt = pd.to_datetime(tweet_date_str)
                if tweet_dt.tzinfo is None:
                    tweet_dt = tweet_dt.tz_localize('UTC')

                fetch_start = tweet_dt - timedelta(days=window_days + 5)
                fetch_end = tweet_dt + timedelta(days=window_days + 5)

                with st.spinner(f"Loading {drill_ticker.upper()} around {tweet_date_str[:10]}..."):
                    try:
                        stocks_df = proc.get_stocks(drill_ticker, start_date=fetch_start, end_date=fetch_end)

                        if stocks_df.empty:
                            st.info("No stock data found for this ticker in this window.")
                        else:
                            stocks_df = stocks_df.reset_index()
                            stocks_df['timestamp'] = pd.to_datetime(stocks_df['timestamp'])
                            stocks_df = stocks_df.sort_values('timestamp')

                            window_start = tweet_dt - timedelta(days=window_days)
                            window_end = tweet_dt + timedelta(days=window_days)
                            display_df = stocks_df[
                                (stocks_df['timestamp'] >= window_start) &
                                (stocks_df['timestamp'] <= window_end)
                            ].copy()

                            if display_df.empty:
                                st.info("No stock data in this window.")
                            else:
                                fig = make_subplots(
                                    rows=2, cols=1,
                                    shared_xaxes=True,
                                    row_heights=[0.72, 0.28],
                                    vertical_spacing=0.05,
                                    subplot_titles=[f"{drill_ticker.upper()} Price", "Volume"],
                                )

                                ts = display_df['timestamp']

                                fig.add_trace(go.Candlestick(
                                    x=ts,
                                    open=display_df['open'], high=display_df['high'],
                                    low=display_df['low'], close=display_df['close'],
                                    name="Price",
                                    increasing_line_color='#26a69a',
                                    decreasing_line_color='#ef5350',
                                ), row=1, col=1)

                                vol_colors = ['#26a69a' if c >= o else '#ef5350'
                                              for c, o in zip(display_df['close'], display_df['open'])]
                                fig.add_trace(go.Bar(
                                    x=ts, y=display_df['volume'],
                                    name="Volume", marker_color=vol_colors, showlegend=False
                                ), row=2, col=1)

                                # Tweet marker line
                                fig.add_vline(
                                    x=tweet_dt,
                                    line=dict(color='yellow', width=2, dash='dash'),
                                )
                                fig.add_annotation(
                                    x=tweet_dt, y=1.05, yref='paper',
                                    text="📝 Tweet posted",
                                    showarrow=False,
                                    font=dict(color='yellow', size=12),
                                    bgcolor='rgba(0,0,0,0.6)',
                                )

                                fig.update_layout(
                                    height=520,
                                    xaxis_rangeslider_visible=False,
                                    template='plotly_dark',
                                    margin=dict(l=0, r=0, t=50, b=0),
                                    showlegend=False,
                                )

                                st.plotly_chart(fig, use_container_width=True)

                                # Before / after stats
                                before = display_df[display_df['timestamp'] < tweet_dt]
                                after = display_df[display_df['timestamp'] > tweet_dt]

                                if not before.empty and not after.empty:
                                    pre_close = float(before['close'].iloc[-1])
                                    next_close = float(after['close'].iloc[0])
                                    day5_close = float(after['close'].iloc[min(4, len(after) - 1)])
                                    days_shown = min(5, len(after))

                                    s1, s2, s3 = st.columns(3)
                                    s1.metric("Close before tweet", f"${pre_close:.2f}")
                                    s2.metric("Next trading day", f"${next_close:.2f}",
                                              f"{((next_close - pre_close) / pre_close * 100):+.2f}%")
                                    s3.metric(f"{days_shown} trading days later", f"${day5_close:.2f}",
                                              f"{((day5_close - pre_close) / pre_close * 100):+.2f}%")

                    except Exception as e:
                        st.error(f"Failed to load stock data: {str(e)}\n{traceback.format_exc()}")

# ── MARKET CONTEXT TAB ────────────────────────────────────────────────────────
with tab_ctx:
    st.markdown("See whether a stock's price move was caused by a tweet — or by something bigger happening in the market.")

    with st.expander("📖 Glossary — click to learn what each term means"):
        gc1, gc2 = st.columns(2)
        with gc1:
            st.markdown("""
**SPY (Market Benchmark)**
SPY is an ETF that tracks the S&P 500 — basically the average performance of the 500 biggest companies in the US. Think of it as a thermometer for the whole stock market. If SPY goes up 2% and your stock also goes up 2%, the market did that — not the tweet.

**Sector ETF**
Every stock belongs to an industry group (tech, retail, energy, etc.). A sector ETF tracks just that group. For example, XLK tracks tech stocks. If Apple rises but so does all of tech, the industry moved — not something Apple-specific.

**Indexed to 100**
To fairly compare stocks with very different prices, we reset all of them to start at 100 on day one. After that, every point above or below 100 shows the percentage change. So a stock at 108 is up 8%, and one at 95 is down 5% — even if their actual prices are wildly different.
""")
        with gc2:
            st.markdown("""
**Alpha**
Alpha is how much better (or worse) a stock did compared to the market. If the market went up 3% and your stock went up 7%, the alpha is +4% — that extra 4% is what might be explained by company-specific events like a CEO tweet.

**Earnings Date**
Four times a year, companies publicly report how much money they made. These announcements almost always cause big stock moves — up or down. If a tweet happened right around an earnings date, the earnings report is likely the real driver, not the tweet.

**News Headlines**
Real news articles published around the same time as the tweets. If a CEO tweeted something positive but there was also a major negative news story that day, the news probably had more impact on the stock price than the tweet.
""")

    CEO_COMPANY_MAP = {
        "TSLA": "Tesla", "AAPL": "Apple", "MSFT": "Microsoft",
        "GOOGL": "Google", "GOOG": "Google", "DELL": "Dell",
        "AMZN": "Amazon", "META": "Meta", "NVDA": "Nvidia",
    }

    run_ctx = st.button("Load Market Context", type="primary")
    ctx_container = st.container()

    if run_ctx:
        if not stock_ticker:
            st.error("Please enter a Stock Ticker above.")
        else:
            with st.spinner("Loading market context..."):
                try:
                    ticker_upper = stock_ticker.upper()
                    company_name = CEO_COMPANY_MAP.get(ticker_upper, ticker_upper)
                    sector_etf = get_sector_etf(ticker_upper)

                    start_dt = datetime.combine(query_start_date, datetime.min.time()).replace(tzinfo=pytz.utc)
                    end_dt = datetime.combine(query_end_date, datetime.max.time()).replace(tzinfo=pytz.utc) if query_end_date else None

                    # Fetch stock, SPY, and sector in parallel-ish
                    stock_df = proc.get_stocks(ticker_upper, start_date=start_dt, end_date=end_dt)
                    spy_df, sector_df, _ = proc.get_market_context(ticker_upper, start_date=start_dt, end_date=end_dt)

                    if stock_df.empty:
                        with ctx_container:
                            st.info("No stock data found.")
                    else:
                        # Normalise all three to 100 at start for comparison
                        def normalise(df):
                            df = df.reset_index()
                            df['timestamp'] = pd.to_datetime(df['timestamp'])
                            df = df.sort_values('timestamp')
                            if 'symbol' in df.columns:
                                df = df[['timestamp', 'close']]
                            first = df['close'].iloc[0]
                            df['indexed'] = (df['close'] / first) * 100
                            return df

                        stock_n = normalise(stock_df.copy())
                        spy_n   = normalise(spy_df.copy()) if not spy_df.empty else None
                        sector_n = normalise(sector_df.copy()) if not sector_df.empty else None

                        # Earnings dates
                        earnings_dates = get_earnings_dates(ticker_upper)

                        # News
                        news_start = query_start_date
                        news_end = query_end_date if query_end_date else query_start_date + timedelta(days=7)
                        news = get_news_for_range(ticker_upper, company_name, news_start, news_end)

                        with ctx_container:
                            st.subheader(f"{ticker_upper} vs SPY vs {sector_etf} — {date_display}")
                            st.caption("All series indexed to 100 at the start of the period — shows relative performance, not raw price.")

                            # Relative performance chart
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(x=stock_n['timestamp'], y=stock_n['indexed'],
                                                     name=ticker_upper, line=dict(color='#2196F3', width=2)))
                            if spy_n is not None:
                                fig.add_trace(go.Scatter(x=spy_n['timestamp'], y=spy_n['indexed'],
                                                         name='SPY (Market)', line=dict(color='#9E9E9E', width=1.5, dash='dot')))
                            if sector_n is not None:
                                fig.add_trace(go.Scatter(x=sector_n['timestamp'], y=sector_n['indexed'],
                                                         name=sector_etf, line=dict(color='#FF9800', width=1.5, dash='dash')))

                            # Earnings markers
                            for ed in earnings_dates:
                                ed_dt = pd.to_datetime(ed).tz_localize('UTC')
                                if stock_n['timestamp'].min() <= ed_dt <= stock_n['timestamp'].max():
                                    fig.add_vline(x=ed_dt, line=dict(color='#E91E63', width=1.5, dash='dash'))
                                    fig.add_annotation(x=ed_dt, y=1.02, yref='paper', text="Earnings",
                                                       showarrow=False, font=dict(color='#E91E63', size=10),
                                                       bgcolor='rgba(0,0,0,0.5)')

                            fig.update_layout(
                                height=450,
                                template='plotly_dark',
                                yaxis_title="Indexed Price (start = 100)",
                                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0),
                                margin=dict(l=0, r=0, t=50, b=0),
                            )
                            st.plotly_chart(fig, use_container_width=True)

                            # Summary metrics
                            stock_ret = stock_n['indexed'].iloc[-1] - 100
                            spy_ret   = (spy_n['indexed'].iloc[-1] - 100) if spy_n is not None else None
                            sec_ret   = (sector_n['indexed'].iloc[-1] - 100) if sector_n is not None else None
                            alpha_vs_spy = (stock_ret - spy_ret) if spy_ret is not None else None
                            alpha_vs_sec = (stock_ret - sec_ret) if sec_ret is not None else None

                            m1, m2, m3, m4 = st.columns(4)
                            m1.metric(f"{ticker_upper} Return", f"{stock_ret:+.2f}%")
                            if spy_ret is not None:
                                m2.metric("SPY Return", f"{spy_ret:+.2f}%")
                            if alpha_vs_spy is not None:
                                m3.metric("Alpha vs Market", f"{alpha_vs_spy:+.2f}%",
                                          help="How much better/worse the stock did vs SPY")
                            if alpha_vs_sec is not None:
                                m4.metric(f"Alpha vs {sector_etf}", f"{alpha_vs_sec:+.2f}%",
                                          help=f"How much better/worse vs the sector ETF {sector_etf}")

                            # Interpretation
                            if alpha_vs_spy is not None:
                                if abs(alpha_vs_spy) < 1.0:
                                    st.info(f"**{ticker_upper}** moved almost identically to the market — price changes this period are likely market-driven, not tweet-driven.")
                                elif alpha_vs_spy > 0:
                                    st.success(f"**{ticker_upper}** outperformed the market by {alpha_vs_spy:+.2f}%. That excess return is worth investigating for tweet/news correlation.")
                                else:
                                    st.warning(f"**{ticker_upper}** underperformed the market by {alpha_vs_spy:.2f}%. Something company-specific may be dragging it down.")

                            # News headlines
                            st.subheader("News Headlines in This Period")
                            if not news:
                                st.info("No news found — either no articles exist for this period or the NewsAPI free tier limit (1 month history) was exceeded.")
                            else:
                                for article in news:
                                    st.markdown(f"**{article['published']}** — [{article['title']}]({article['url']}) _{article['source']}_")

                except Exception as e:
                    st.error(f"Market context failed: {str(e)}\n{traceback.format_exc()}")

# ── PREDICT TAB ───────────────────────────────────────────────────────────────
with tab_predict:
    st.markdown("Runs the trained prediction model on recent tweets and shows whether the stock is expected to go **Up** or **Down** the next trading day.")

    import os as _os
    from model.predict import predict_tweets as _predict_tweets

    model_path = _os.path.join("model", "trained_model.pkl")
    if not _os.path.exists(model_path):
        st.warning("No trained model found. Run `python3 model/baseline.py` first to train and save it.")
    else:
        run_predict = st.button("Run Predictions", type="primary")
        predict_container = st.container()

        if run_predict:
            if not ceo_handle or not stock_ticker:
                st.error("Please enter a CEO Twitter Handle and a Stock Ticker above.")
            else:
                with st.spinner("Fetching tweets and computing predictions..."):
                    try:
                        # Fetch tweets
                        tweets_df = run_async(proc.get_tweets(ceo_handle))
                        if not tweets_df.empty and "date" in tweets_df.columns:
                            tweets_df = filter_tweets_by_date(tweets_df)

                        if tweets_df.empty:
                            with predict_container:
                                st.info("No tweets found for this date range.")
                        else:
                            # 260-day lookback so 52-week rolling windows are valid
                            min_date = tweets_df["date"].min() - timedelta(days=260)
                            max_date = tweets_df["date"].max() + timedelta(days=5)
                            stocks_df = proc.get_stocks(stock_ticker, start_date=min_date, end_date=max_date)

                            if not stocks_df.empty:
                                stocks_df = stocks_df.sort_index()
                                if isinstance(stocks_df.index, pd.MultiIndex):
                                    stocks_df["date_only"] = stocks_df.index.get_level_values("timestamp").date
                                else:
                                    stocks_df["date_only"] = stocks_df.index.date

                                # Compute RSI and ATR (same as ingestion)
                                stocks_df["prev_close"] = stocks_df["close"].shift(1)
                                stocks_df["tr"] = stocks_df[["high", "low", "prev_close"]].apply(
                                    lambda r: max(r["high"] - r["low"],
                                                  abs(r["high"] - r["prev_close"]),
                                                  abs(r["low"] - r["prev_close"])), axis=1
                                )
                                stocks_df["atr_14"] = stocks_df["tr"].rolling(14).mean()
                                delta = stocks_df["close"].diff()
                                gain = delta.where(delta > 0, 0.0).rolling(14).mean()
                                loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
                                stocks_df["rsi_14"] = 100 - (100 / (1 + gain / loss))

                                # Tier 1: returns, volume ratio, 52w extremes
                                prev_close_series = stocks_df["close"].shift(1)
                                stocks_df["return_1d"]  = prev_close_series.pct_change(1)
                                stocks_df["return_5d"]  = prev_close_series.pct_change(5)
                                stocks_df["return_20d"] = prev_close_series.pct_change(20)
                                vol_avg_20 = stocks_df["volume"].shift(1).rolling(20).mean()
                                stocks_df["volume_ratio_20d"] = stocks_df["volume"] / vol_avg_20
                                prior_high_52w = stocks_df["high"].shift(1).rolling(252, min_periods=60).max()
                                prior_low_52w  = stocks_df["low"].shift(1).rolling(252, min_periods=60).min()
                                stocks_df["dist_from_52w_high"] = (stocks_df["close"] - prior_high_52w) / prior_high_52w
                                stocks_df["dist_from_52w_low"]  = (stocks_df["close"] - prior_low_52w) / prior_low_52w

                            # SPY same-day return lookup
                            _spy_lookup = {}
                            try:
                                import yfinance as _yf
                                _spy_df = _yf.download("SPY", start=min_date, end=max_date + timedelta(days=1),
                                                        auto_adjust=True, progress=False)
                                if not _spy_df.empty:
                                    if isinstance(_spy_df.columns, pd.MultiIndex):
                                        _spy_df.columns = _spy_df.columns.get_level_values(0)
                                    _spy_df = _spy_df.sort_index()
                                    _spy_ret = _spy_df["Close"].pct_change(1)
                                    _spy_lookup = {d.date(): float(v)
                                                   for d, v in zip(_spy_ret.index, _spy_ret.values)
                                                   if not pd.isna(v)}
                            except Exception:
                                _spy_lookup = {}

                            # Run predictions
                            result_df = _predict_tweets(tweets_df, stocks_df,
                                                         ticker=stock_ticker,
                                                         spy_lookup=_spy_lookup)

                            # Join actual next-day outcomes from DB (if available)
                            _pred_engine = get_db_engine()
                            if _pred_engine is not None:
                                try:
                                    with _pred_engine.connect() as _pc:
                                        _actual_df = pd.read_sql(
                                            text(
                                                "SELECT date, next_day_direction FROM merged_data "
                                                "WHERE ceo = :ceo AND next_day_direction IS NOT NULL"
                                            ),
                                            _pc, params={"ceo": ceo_handle},
                                        )
                                    if not _actual_df.empty:
                                        _actual_df['date_key'] = _actual_df['date'].str[:10]
                                        result_df['date_key'] = result_df['date'].astype(str).str[:10]
                                        result_df = result_df.merge(
                                            _actual_df[['date_key', 'next_day_direction']].drop_duplicates('date_key'),
                                            on='date_key', how='left',
                                        )
                                        result_df['actual_outcome'] = result_df['next_day_direction'].map({1: 'Up', 0: 'Down'})
                                        result_df['correct'] = result_df.apply(
                                            lambda r: True if pd.notna(r.get('next_day_direction')) and (
                                                (r['predicted_direction'] == 'Up' and r['next_day_direction'] == 1) or
                                                (r['predicted_direction'] == 'Down' and r['next_day_direction'] == 0)
                                            ) else (False if pd.notna(r.get('next_day_direction')) else None),
                                            axis=1,
                                        )
                                except Exception:
                                    pass

                            with predict_container:
                                st.subheader(f"Predictions — @{ceo_handle} & {stock_ticker.upper()} ({date_display})")

                                up_count = (result_df["predicted_direction"] == "Up").sum()
                                down_count = (result_df["predicted_direction"] == "Down").sum()
                                avg_conf = result_df["confidence_pct"].mean()

                                has_actuals = 'correct' in result_df.columns and result_df['correct'].notna().any()
                                if has_actuals:
                                    known = result_df[result_df['correct'].notna()]
                                    accuracy = known['correct'].mean()
                                    m1, m2, m3, m4 = st.columns(4)
                                    m1.metric("Predicted Up", up_count)
                                    m2.metric("Predicted Down", down_count)
                                    m3.metric("Avg Confidence", f"{avg_conf:.1f}%")
                                    m4.metric("Accuracy vs Actual", f"{accuracy:.0%}", f"{len(known)} known outcomes")
                                else:
                                    m1, m2, m3 = st.columns(3)
                                    m1.metric("Predicted Up", up_count)
                                    m2.metric("Predicted Down", down_count)
                                    m3.metric("Avg Confidence", f"{avg_conf:.1f}%")

                                # Build display table
                                base_cols = ["date", "text", "sentiment", "likes", "retweet_count", "predicted_direction", "confidence_pct"]
                                rename_map = {
                                    "date": "Date", "text": "Tweet", "sentiment": "Sentiment",
                                    "likes": "Likes", "retweet_count": "Retweets",
                                    "predicted_direction": "Prediction", "confidence_pct": "Confidence %",
                                }
                                if has_actuals:
                                    base_cols += ["actual_outcome", "correct"]
                                    rename_map["actual_outcome"] = "Actual"
                                    rename_map["correct"] = "Correct?"

                                display_df = result_df[base_cols].copy()
                                display_df["date"] = display_df["date"].astype(str).str[:19]
                                display_df["text"] = display_df["text"].str[:120]
                                display_df = display_df.rename(columns=rename_map)

                                def _color_direction(val):
                                    if val in ("Up", "Down"):
                                        return f"color: {'#26a69a' if val == 'Up' else '#ef5350'}; font-weight: bold"
                                    return ""

                                def _color_correct(val):
                                    if val is True:
                                        return "color: #26a69a; font-weight: bold"
                                    if val is False:
                                        return "color: #ef5350; font-weight: bold"
                                    return "color: #8a9bbf"

                                style_cols = ["Prediction"]
                                if has_actuals:
                                    style_cols += ["Actual", "Correct?"]

                                styled = display_df.style.map(_color_direction, subset=["Prediction"])
                                if has_actuals and "Actual" in display_df.columns:
                                    styled = styled.map(_color_direction, subset=["Actual"])
                                    styled = styled.map(_color_correct, subset=["Correct?"])
                                st.dataframe(styled, use_container_width=True)

                    except FileNotFoundError as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"Prediction failed: {str(e)}\n{traceback.format_exc()}")

        # ── What-If Simulator ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### What-If Simulator")
        st.markdown("Adjust tweet characteristics to explore how the model's prediction changes — no real tweet needed.")

        _WI_TONES = [
            'General Commentary',
            'Emotional (Joyful/Excited)',
            'Emotional (Angry/Frustrated)',
            'Informational (Promotional/Update)',
            'Informational (Mixed)',
        ]
        _WI_TYPES = [
            'Personal/General Commentary',
            'Company Milestone',
            'Discussion Starter',
            'Acknowledgment',
            'Poll/Vote',
        ]

        wi1, wi2, wi3 = st.columns(3)
        with wi1:
            wi_sentiment = st.slider("Sentiment Score", -1.0, 1.0, 0.3, 0.01, key="wi_sentiment")
            wi_rsi = st.slider("RSI at Tweet", 0, 100, 50, key="wi_rsi")
            wi_vix = st.slider("VIX", 10.0, 80.0, 20.0, 0.5, key="wi_vix")
        with wi2:
            wi_tone = st.selectbox("Tone Category", _WI_TONES, key="wi_tone")
            wi_type = st.selectbox("Tweet Type", _WI_TYPES, key="wi_type")
            wi_premarket = st.checkbox("Premarket tweet?", key="wi_premarket")
        with wi3:
            wi_likes = st.number_input("Likes", 0, 10_000_000, 1000, 100, key="wi_likes")
            wi_retweets = st.number_input("Retweets", 0, 1_000_000, 200, 10, key="wi_retweets")
            wi_views = st.number_input("Views", 0, 100_000_000, 50_000, 1000, key="wi_views")

        if st.button("Simulate Prediction", type="primary", key="wi_run"):
            try:
                _wi_model = joblib.load(model_path)
                _wi_refined = get_refined_sentiment(wi_sentiment)
                _wi_rsi_fill = float(wi_rsi)
                _wi_row = {
                    "sentiment_score":      wi_sentiment,
                    "sentiment_magnitude":  abs(wi_sentiment),
                    "tweet_length":         100,
                    "word_count":           15,
                    "tweet_count":          1,
                    "log_likes":            float(np.log1p(wi_likes)),
                    "log_retweets":         float(np.log1p(wi_retweets)),
                    "log_views":            float(np.log1p(wi_views)),
                    "log_replies":          float(np.log1p(50)),
                    "engagement_rate":      (wi_likes + wi_retweets + 50) / max(wi_views, 1),
                    "tweet_hour":           9 if wi_premarket else 14,
                    "is_premarket":         int(wi_premarket),
                    "rsi_at_tweet":         _wi_rsi_fill,
                    "atr_at_tweet":         None,
                    "rsi_overbought":       int(_wi_rsi_fill > 70),
                    "rsi_oversold":         int(_wi_rsi_fill < 30),
                    "vix_at_tweet":         float(wi_vix),
                    "days_to_earnings":     None,
                    "prev_day_direction":   None,
                    "news_sentiment_score": None,
                    "finbert_score":        None,
                    "finbert_positive":     None,
                    "finbert_negative":     None,
                    "finbert_neutral":      None,
                    "return_1d":            None,
                    "return_5d":            None,
                    "return_20d":           None,
                    "volume_ratio_20d":     None,
                    "dist_from_52w_high":   None,
                    "dist_from_52w_low":    None,
                    "spy_return_same_day":  None,
                    "refined_sentiment":    _wi_refined,
                    "tone_category":        wi_tone,
                    "tweet_type":           wi_type,
                }
                _wi_feat = pd.DataFrame([_wi_row])
                _wi_pred = _wi_model.predict(_wi_feat)[0]
                _wi_prob = _wi_model.predict_proba(_wi_feat)[0]
                _wi_direction = "Up" if _wi_pred == 1 else "Down"
                _wi_confidence = max(_wi_prob) * 100
                _wi_color = "#26a69a" if _wi_direction == "Up" else "#ef5350"

                st.markdown(f"""
                <div style="background:#1a2235;border:2px solid {_wi_color};border-radius:12px;
                            padding:1.5rem;text-align:center;margin-top:1rem;max-width:400px;">
                    <div style="font-size:0.8rem;color:#8a9bbf;text-transform:uppercase;
                                letter-spacing:0.1em;margin-bottom:0.5rem;">Predicted Next-Day Direction</div>
                    <div style="font-size:3rem;font-weight:900;color:{_wi_color};
                                line-height:1;">{_wi_direction}</div>
                    <div style="font-size:1rem;color:#c0cfe8;margin-top:0.5rem;">
                        Confidence: <strong>{_wi_confidence:.1f}%</strong>
                    </div>
                    <div style="font-size:0.8rem;color:#8a9bbf;margin-top:0.25rem;">
                        Sentiment label: {_wi_refined}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            except Exception as _wi_e:
                st.error(f"Simulation failed: {_wi_e}")

# ── BACKTEST TAB ──────────────────────────────────────────────────────────────
with tab_backtest:
    st.markdown("Runs the trained model against every stored record in the database that has a known next-day outcome, then reports how accurate it was.")

    _bt_engine = get_db_engine()
    if _bt_engine is None:
        st.warning("No database connection. Run the FastAPI backend and populate the database first.")
    else:
        import os as _bos
        _bt_model_path = _bos.path.join("model", "trained_model.pkl")
        if not _bos.path.exists(_bt_model_path):
            st.warning("No trained model found. Run `python3 model/baseline.py` to train and save it.")
        else:
            # ── Filters ───────────────────────────────────────────────────────
            try:
                with _bt_engine.connect() as _bc:
                    _bt_ceo_list = pd.read_sql(
                        text("SELECT DISTINCT ceo FROM merged_data ORDER BY ceo"), _bc
                    )['ceo'].tolist()
            except Exception:
                _bt_ceo_list = []

            bf1, bf2, bf3 = st.columns(3)
            with bf1:
                bt_ceos = st.multiselect("CEO (blank = all)", _bt_ceo_list, key="bt_ceos")
            with bf2:
                bt_start = st.date_input(
                    "Start Date",
                    value=datetime.now(pytz.utc).date() - timedelta(days=365),
                    key="bt_start",
                )
            with bf3:
                bt_end = st.date_input(
                    "End Date",
                    value=datetime.now(pytz.utc).date(),
                    key="bt_end",
                )

            run_backtest = st.button("Run Backtest", type="primary")
            bt_container = st.container()

            if run_backtest:
                with st.spinner("Loading data and running predictions..."):
                    try:
                        ceo_clause = "AND ceo = ANY(:ceos)" if bt_ceos else ""
                        bt_query = text(f"""
                            SELECT date, ceo, stock_ticker, tweet_text, sentiment_score,
                                   refined_sentiment, tone_category, tweet_type,
                                   likes, retweet_count, view_count, reply_count,
                                   tweet_hour, is_premarket, rsi_at_tweet, atr_at_tweet,
                                   vix_at_tweet, days_to_earnings, news_sentiment_score,
                                   return_1d, return_5d, return_20d,
                                   volume_ratio_20d,
                                   dist_from_52w_high, dist_from_52w_low,
                                   spy_return_same_day,
                                   next_day_direction
                            FROM merged_data
                            WHERE next_day_direction IS NOT NULL
                              AND date >= :start_date
                              AND date < :end_date
                              {ceo_clause}
                            ORDER BY date ASC
                        """)
                        bt_params = {
                            "start_date": bt_start.isoformat(),
                            "end_date": (bt_end + timedelta(days=1)).isoformat(),
                        }
                        if bt_ceos:
                            bt_params["ceos"] = bt_ceos

                        with _bt_engine.connect() as _bc:
                            bt_df = pd.read_sql(bt_query, _bc, params=bt_params)

                        if bt_df.empty:
                            with bt_container:
                                st.info("No records with known next-day outcomes found. Run `/process/all` to populate the database.")
                        else:
                            _bt_model = joblib.load(_bt_model_path)

                            def _bt_build_feature(row):
                                sentiment = float(row.get('sentiment_score') or 0)
                                text_val = str(row.get('tweet_text') or '')
                                likes_v    = int(row.get('likes') or 0)
                                rt_v       = int(row.get('retweet_count') or 0)
                                views_v    = int(row.get('view_count') or 0)
                                replies_v  = int(row.get('reply_count') or 0)
                                rsi_v      = row.get('rsi_at_tweet')
                                rsi_fill   = float(rsi_v) if rsi_v is not None and not pd.isna(rsi_v) else 50.0
                                return {
                                    'sentiment_score':      sentiment,
                                    'sentiment_magnitude':  abs(sentiment),
                                    'tweet_length':         len(text_val),
                                    'word_count':           len(text_val.split()),
                                    'tweet_count':          1,
                                    'log_likes':            np.log1p(likes_v),
                                    'log_retweets':         np.log1p(rt_v),
                                    'log_views':            np.log1p(views_v),
                                    'log_replies':          np.log1p(replies_v),
                                    'engagement_rate':      (likes_v + rt_v + replies_v) / max(views_v, 1),
                                    'tweet_hour':           int(row.get('tweet_hour') or 0),
                                    'is_premarket':         int(row.get('is_premarket') or 0),
                                    'rsi_at_tweet':         rsi_v,
                                    'atr_at_tweet':         row.get('atr_at_tweet'),
                                    'rsi_overbought':       int(rsi_fill > 70),
                                    'rsi_oversold':         int(rsi_fill < 30),
                                    'vix_at_tweet':         row.get('vix_at_tweet'),
                                    'days_to_earnings':     row.get('days_to_earnings'),
                                    'prev_day_direction':   None,
                                    'news_sentiment_score': row.get('news_sentiment_score'),
                                    'finbert_score':        None,
                                    'finbert_positive':     None,
                                    'finbert_negative':     None,
                                    'finbert_neutral':      None,
                                    'return_1d':            row.get('return_1d'),
                                    'return_5d':            row.get('return_5d'),
                                    'return_20d':           row.get('return_20d'),
                                    'volume_ratio_20d':     row.get('volume_ratio_20d'),
                                    'dist_from_52w_high':   row.get('dist_from_52w_high'),
                                    'dist_from_52w_low':    row.get('dist_from_52w_low'),
                                    'spy_return_same_day':  row.get('spy_return_same_day'),
                                    'refined_sentiment':    row.get('refined_sentiment') or get_refined_sentiment(sentiment),
                                    'tone_category':        row.get('tone_category') or get_tone_category(text_val, sentiment),
                                    'tweet_type':           row.get('tweet_type') or get_tweet_type(text_val),
                                }

                            feat_rows = [_bt_build_feature(row) for _, row in bt_df.iterrows()]
                            feat_df = pd.DataFrame(feat_rows)

                            bt_preds = _bt_model.predict(feat_df)
                            bt_probs = _bt_model.predict_proba(feat_df)

                            bt_df = bt_df.copy()
                            bt_df['predicted_direction'] = ['Up' if p == 1 else 'Down' for p in bt_preds]
                            bt_df['actual_direction']    = bt_df['next_day_direction'].map({1: 'Up', 0: 'Down'})
                            bt_df['confidence_pct']      = [round(max(p) * 100, 1) for p in bt_probs]
                            bt_df['correct']             = (bt_preds == bt_df['next_day_direction'].values)

                            from sklearn.metrics import (
                                accuracy_score, precision_score,
                                recall_score, f1_score,
                                confusion_matrix as sk_confusion_matrix,
                            )

                            y_true = bt_df['next_day_direction'].values
                            y_pred = bt_preds
                            acc  = accuracy_score(y_true, y_pred)
                            prec = precision_score(y_true, y_pred, zero_division=0)
                            rec  = recall_score(y_true, y_pred, zero_division=0)
                            f1   = f1_score(y_true, y_pred, zero_division=0)

                            with bt_container:
                                st.subheader(f"Backtest Results — {len(bt_df):,} tweets")

                                bm1, bm2, bm3, bm4, bm5 = st.columns(5)
                                bm1.metric("Accuracy", f"{acc:.1%}")
                                bm2.metric("Precision (Up)", f"{prec:.1%}")
                                bm3.metric("Recall (Up)", f"{rec:.1%}")
                                bm4.metric("F1 Score", f"{f1:.1%}")
                                bm5.metric("Correct / Total", f"{bt_df['correct'].sum()} / {len(bt_df)}")

                                st.markdown("---")

                                # Confusion matrix
                                st.subheader("Confusion Matrix")
                                cm = sk_confusion_matrix(y_true, y_pred)
                                cm_df = pd.DataFrame(
                                    cm,
                                    index=['Actual Down', 'Actual Up'],
                                    columns=['Predicted Down', 'Predicted Up'],
                                )
                                st.dataframe(cm_df, use_container_width=False)
                                st.caption(
                                    f"Diagonal = correct predictions. "
                                    f"Top-right = predicted Up but actually went Down. "
                                    f"Bottom-left = predicted Down but actually went Up."
                                )

                                st.markdown("---")

                                # Per-CEO breakdown
                                st.subheader("Performance by CEO")

                                def _ceo_stats(g):
                                    g_true = g['next_day_direction'].values
                                    g_pred = (g['predicted_direction'] == 'Up').astype(int).values
                                    return pd.Series({
                                        'tweets':    len(g),
                                        'accuracy':  round(accuracy_score(g_true, g_pred), 3),
                                        'up_calls':  (g['predicted_direction'] == 'Up').sum(),
                                        'down_calls': (g['predicted_direction'] == 'Down').sum(),
                                        'pct_up_actual': round((g['next_day_direction'] == 1).mean(), 3),
                                    })

                                ceo_stats_df = bt_df.groupby('ceo').apply(_ceo_stats).reset_index()
                                st.dataframe(ceo_stats_df, use_container_width=True)

                                st.markdown("---")

                                # Full results table
                                st.subheader("All Predictions vs Actual")
                                show_bt = bt_df[[
                                    'date', 'ceo', 'tweet_text',
                                    'sentiment_score', 'predicted_direction',
                                    'actual_direction', 'confidence_pct', 'correct',
                                ]].copy()
                                show_bt['tweet_text'] = show_bt['tweet_text'].str[:100]
                                show_bt['date'] = show_bt['date'].astype(str).str[:10]

                                def _bt_color_dir(val):
                                    if val in ('Up', 'Down'):
                                        return f"color: {'#26a69a' if val == 'Up' else '#ef5350'}; font-weight: bold"
                                    return ""

                                def _bt_color_correct(val):
                                    if val is True:
                                        return "color: #26a69a; font-weight: bold"
                                    if val is False:
                                        return "color: #ef5350; font-weight: bold"
                                    return ""

                                styled_bt = (
                                    show_bt.style
                                    .map(_bt_color_dir, subset=['predicted_direction', 'actual_direction'])
                                    .map(_bt_color_correct, subset=['correct'])
                                )
                                st.dataframe(styled_bt, use_container_width=True)

                    except Exception as _bt_e:
                        st.error(f"Backtest failed: {str(_bt_e)}\n{traceback.format_exc()}")
