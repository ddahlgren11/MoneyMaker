import streamlit as st
import asyncio
import pandas as pd
from datetime import timedelta, datetime
import pytz
import traceback

from processor import DataProcessor
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type

# Set up the UI layout
st.set_page_config(page_title="MoneyMaker Data Fetcher", layout="wide", page_icon="📈")

st.title("MoneyMaker Data Fetcher")

# Initialize DataProcessor
@st.cache_resource
def get_processor():
    return DataProcessor()

proc = get_processor()

# Generate a list of recent weeks (e.g., past 12 weeks)
def get_recent_weeks(num_weeks=12):
    weeks = []
    # Start from today, get the current week's monday
    today = datetime.now(pytz.utc)
    current_monday = today - timedelta(days=today.weekday())
    
    for i in range(num_weeks):
        start_of_week = current_monday - timedelta(weeks=i)
        end_of_week = start_of_week + timedelta(days=6)
        
        # Format for display: "YYYY-MM-DD to YYYY-MM-DD"
        display_str = f"{start_of_week.strftime('%Y-%m-%d')} to {end_of_week.strftime('%Y-%m-%d')}"
        weeks.append({
            "display": display_str,
            "start": start_of_week,
            "end": end_of_week
        })
    return weeks

recent_weeks = get_recent_weeks(12)
week_displays = [w["display"] for w in recent_weeks]

# UI Query Parameters
st.subheader("Data Query Parameters")
col1, col2 = st.columns(2)
with col1:
    ceo_handle = st.text_input("CEO Twitter Handle", value="", placeholder="e.g. elonmusk")
with col2:
    stock_ticker = st.text_input("Stock Ticker", value="", placeholder="e.g. TSLA")

selected_week_display = st.selectbox("Select Week", options=week_displays)
selected_week = next(w for w in recent_weeks if w["display"] == selected_week_display)

# Define async execution helper
def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# Action Buttons
col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 1])

# Container for results
results_container = st.container()

def fetch_tweets():
    if not ceo_handle:
        st.error("Please enter a CEO Twitter Handle.")
        return
        
    with st.spinner(f"Pulling tweets for @{ceo_handle}..."):
        try:
            tweets_df = run_async(proc.get_tweets(ceo_handle))
            
            if not tweets_df.empty and 'created_at' in tweets_df.columns:
                # Filter by selected week
                # Ensure tz-awareness matches
                if tweets_df['created_at'].dt.tz is None:
                    tweets_df['created_at'] = tweets_df['created_at'].dt.tz_localize('UTC')
                    
                start_date = pd.to_datetime(selected_week["start"])
                end_date = pd.to_datetime(selected_week["end"]) + timedelta(days=1) # Include full end day
                
                mask = (tweets_df['created_at'] >= start_date) & (tweets_df['created_at'] < end_date)
                filtered_tweets = tweets_df.loc[mask].copy()
                
                # Format datetime for display
                if 'created_at' in filtered_tweets.columns:
                    filtered_tweets['created_at'] = filtered_tweets['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                with results_container:
                    st.subheader(f"Tweets for @{ceo_handle} ({selected_week['display']})")
                    if filtered_tweets.empty:
                        st.info("No tweets found for this week.")
                    else:
                        st.dataframe(filtered_tweets[['ceo', 'created_at', 'text', 'sentiment']], use_container_width=True)
            else:
                with results_container:
                    st.info("No tweets found.")
        except Exception as e:
            st.error(f"Failed to fetch tweets: {str(e)}")

def fetch_stocks():
    if not stock_ticker:
        st.error("Please enter a Stock Ticker.")
        return
        
    with st.spinner(f"Pulling stock data for {stock_ticker.upper()}..."):
        try:
            # Add padding to start/end dates for API to find data reliably
            start_date = selected_week["start"] - timedelta(days=2)
            end_date = selected_week["end"] + timedelta(days=2)
            
            stocks_df = proc.get_stocks(stock_ticker, start_date=start_date, end_date=end_date)
            
            if not stocks_df.empty:
                stocks_df = stocks_df.reset_index()
                
                # Format datetime for display
                if 'timestamp' in stocks_df.columns:
                    stocks_df['timestamp'] = stocks_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                with results_container:
                    st.subheader(f"Stock Data for {stock_ticker.upper()} ({selected_week['display']})")
                    st.dataframe(stocks_df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap']], use_container_width=True)
            else:
                with results_container:
                    st.info("No stock data found for this week.")
        except Exception as e:
            st.error(f"Failed to fetch stock data: {str(e)}\n{traceback.format_exc()}")

def fetch_merged():
    current_ticker = stock_ticker
    if ceo_handle and not current_ticker:
        current_ticker = proc.ceo_map.get(ceo_handle, "")

    if not ceo_handle or not current_ticker:
        st.error("Please enter a CEO Twitter Handle and a Stock Ticker (or use a mapped CEO like elonmusk).")
        return
        
    with st.spinner("Pulling and merging data..."):
        try:
            # 1. Fetch Tweets
            tweets_df = run_async(proc.get_tweets(ceo_handle))
            
            # Filter tweets by selected week
            start_date = pd.to_datetime(selected_week["start"])
            end_date = pd.to_datetime(selected_week["end"]) + timedelta(days=1)
            
            if not tweets_df.empty and 'created_at' in tweets_df.columns:
                if tweets_df['created_at'].dt.tz is None:
                    tweets_df['created_at'] = tweets_df['created_at'].dt.tz_localize('UTC')
                mask = (tweets_df['created_at'] >= start_date) & (tweets_df['created_at'] < end_date)
                tweets_df = tweets_df.loc[mask].copy()

            if tweets_df.empty:
                with results_container:
                    st.info("No tweets found for this week to merge.")
                return

            # Calculate date range with padding for weekends/holidays for stocks
            min_date = tweets_df['created_at'].min() - timedelta(days=5)
            max_date = tweets_df['created_at'].max() + timedelta(days=5)
            
            # 2. Fetch Stock Data
            stocks_df = proc.get_stocks(current_ticker, start_date=min_date, end_date=max_date)

            if not stocks_df.empty:
                if isinstance(stocks_df.index, pd.MultiIndex):
                    stock_dates = stocks_df.index.get_level_values('timestamp').date
                else:
                    stock_dates = stocks_df.index.date
                stocks_df['date_only'] = stock_dates
            
            merged_data = []
            
            # 3. Process and merge each tweet
            for _, row in tweets_df.iterrows():
                sentiment = float(row['sentiment'])
                text = str(row['text'])
                tweet_date = row['created_at']

                # Match weekend tweets to following Monday
                target_date = tweet_date
                if target_date.weekday() == 5:  # Saturday
                    target_date += timedelta(days=2)
                elif target_date.weekday() == 6:  # Sunday
                    target_date += timedelta(days=1)

                target_date_only = target_date.date()

                stock_close = None
                stock_volume = None
                stock_open_close_diff = None
                if not stocks_df.empty:
                    valid_stocks = stocks_df[stocks_df['date_only'] >= target_date_only]
                    if not valid_stocks.empty:
                        stock_close = float(valid_stocks['close'].iloc[0])
                        stock_volume = float(valid_stocks['volume'].iloc[0])
                        stock_open = float(valid_stocks['open'].iloc[0])
                        stock_open_close_diff = float(stock_open - stock_close)
                
                merged_data.append({
                    "date": tweet_date.strftime('%Y-%m-%d %H:%M:%S'),
                    "ceo": ceo_handle,
                    "tweet_text": text,
                    "sentiment_score": sentiment,
                    "refined_sentiment": get_refined_sentiment(sentiment),
                    "tone_category": get_tone_category(text, sentiment),
                    "tweet_type": get_tweet_type(text),
                    "stock_ticker": current_ticker,
                    "stock_close": stock_close,
                    "stock_volume": stock_volume,
                    "stock_open_close_diff": stock_open_close_diff
                })
            
            merged_df = pd.DataFrame(merged_data)
            
            with results_container:
                st.subheader(f"Merged Data (@{ceo_handle} & {current_ticker.upper()}) ({selected_week['display']})")
                if merged_df.empty:
                    st.info("No merged data found.")
                else:
                    # ensure stock_ticker is visible in the DataFrame output
                    st.dataframe(merged_df, use_container_width=True)
                    
        except Exception as e:
            st.error(f"Failed to fetch merged data: {str(e)}")

with col_btn1:
    if st.button("Pull Tweets", type="primary", use_container_width=True):
        fetch_tweets()
        
with col_btn2:
    if st.button("Pull Stock Data", type="primary", use_container_width=True):
        fetch_stocks()
        
with col_btn3:
    if st.button("Pull Merged Data", type="primary", use_container_width=True):
        fetch_merged()