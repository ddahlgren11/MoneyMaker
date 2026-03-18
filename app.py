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

# UI Query Parameters
st.subheader("Data Query Parameters")
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

# For displaying the selected range in subheaders
if query_end_date:
    date_display = f"{query_start_date.strftime('%Y-%m-%d')} to {query_end_date.strftime('%Y-%m-%d')}"
else:
    date_display = f"From {query_start_date.strftime('%Y-%m-%d')}"

# Define async execution helper
def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

# Action Buttons
col_btn1, col_btn2, col_btn3, col_btn4 = st.columns([1, 1, 1, 1])

# Container for results
tab_data, tab_viz = st.tabs(["Data Viewer", "Visualizations"])
results_container = tab_data.container()
viz_container = tab_viz.container()

def fetch_tweets():
    if not ceo_handle:
        st.error("Please enter a CEO Twitter Handle.")
        return
        
    with st.spinner(f"Pulling tweets for @{ceo_handle}..."):
        try:
            tweets_df = run_async(proc.get_tweets(ceo_handle))
            
            if not tweets_df.empty and 'date' in tweets_df.columns:
                # Filter by selected dates
                # Ensure tz-awareness matches
                if tweets_df['date'].dt.tz is None:
                    tweets_df['date'] = tweets_df['date'].dt.tz_localize('UTC')
                    
                start_date = pd.to_datetime(query_start_date).tz_localize('UTC')
                mask = (tweets_df['date'] >= start_date)

                if query_end_date:
                    end_date = pd.to_datetime(query_end_date).tz_localize('UTC') + timedelta(days=1) # Include full end day
                    mask = mask & (tweets_df['date'] < end_date)
                
                filtered_tweets = tweets_df.loc[mask].copy()
                filtered_tweets = filtered_tweets.sort_values(by='date', ascending=False)
                
                # Format datetime for display
                if 'date' in filtered_tweets.columns:
                    filtered_tweets['date'] = filtered_tweets['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                with results_container:
                    st.subheader(f"Tweets for @{ceo_handle} ({date_display})")
                    if filtered_tweets.empty:
                        st.info("No tweets found for this date range.")
                    else:
                        st.dataframe(filtered_tweets[['ceo', 'date', 'text', 'sentiment']], use_container_width=True)
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
            # Need datetime, not date
            start_dt = datetime.combine(query_start_date, datetime.min.time()).replace(tzinfo=pytz.utc) - timedelta(days=2)
            if query_end_date:
                end_dt = datetime.combine(query_end_date, datetime.max.time()).replace(tzinfo=pytz.utc) + timedelta(days=2)
            else:
                end_dt = None
            
            stocks_df = proc.get_stocks(stock_ticker, start_date=start_dt, end_date=end_dt)
            
            if not stocks_df.empty:
                stocks_df = stocks_df.reset_index()
                
                # Format datetime for display
                if 'timestamp' in stocks_df.columns:
                    stocks_df['timestamp'] = stocks_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                with results_container:
                    st.subheader(f"Stock Data for {stock_ticker.upper()} ({date_display})")
                    st.dataframe(stocks_df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap']], use_container_width=True)

                with viz_container:
                    st.subheader(f"Stock Price for {stock_ticker.upper()} ({date_display})")
                    chart_data = stocks_df[['timestamp', 'close']].copy()
                    chart_data['timestamp'] = pd.to_datetime(chart_data['timestamp'])
                    chart_data.set_index('timestamp', inplace=True)
                    st.line_chart(chart_data)
            else:
                with results_container:
                    st.info("No stock data found for this date range.")
        except Exception as e:
            st.error(f"Failed to fetch stock data: {str(e)}\n{traceback.format_exc()}")

def fetch_atr_analysis():
    current_ticker = stock_ticker
    if ceo_handle and not current_ticker:
        current_ticker = proc.ceo_map.get(ceo_handle, "")

    if not ceo_handle or not current_ticker:
        st.error("Please enter a CEO Twitter Handle and a Stock Ticker (or use a mapped CEO like elonmusk).")
        return

    with st.spinner("Running ATR Analysis..."):
        try:
            # 1. Fetch Tweets
            tweets_df = run_async(proc.get_tweets(ceo_handle))

            # Filter tweets by selected dates
            if not tweets_df.empty and 'date' in tweets_df.columns:
                if tweets_df['date'].dt.tz is None:
                    tweets_df['date'] = tweets_df['date'].dt.tz_localize('UTC')

                start_date = pd.to_datetime(query_start_date).tz_localize('UTC')
                mask = (tweets_df['date'] >= start_date)

                if query_end_date:
                    end_date = pd.to_datetime(query_end_date).tz_localize('UTC') + timedelta(days=1)
                    mask = mask & (tweets_df['date'] < end_date)

                tweets_df = tweets_df.loc[mask].copy()

            if tweets_df.empty:
                with results_container:
                    st.info("No tweets found for this date range to analyze.")
                return

            # Filter for high sentiment tweets
            tweets_df = tweets_df[tweets_df['sentiment'] >= 0.5]

            if tweets_df.empty:
                with results_container:
                    st.info("No high-sentiment tweets (>= 0.5) found in this date range.")
                return

            # Compute earliest date minus 40 days for 14-day rolling ATR calculation
            min_date = tweets_df['date'].min() - timedelta(days=40)
            max_date = tweets_df['date'].max() + timedelta(days=5)

            # Fetch Stock Data
            stocks_df = proc.get_stocks(current_ticker, start_date=min_date, end_date=max_date)

            if stocks_df.empty:
                with results_container:
                    st.info("No stock data found to analyze.")
                return

            if isinstance(stocks_df.index, pd.MultiIndex):
                stock_dates = stocks_df.index.get_level_values('timestamp').date
            else:
                stock_dates = stocks_df.index.date
            stocks_df['date_only'] = stock_dates

            # Calculate 14-day rolling ATR
            stocks_df['prev_close'] = stocks_df['close'].shift(1)
            stocks_df['tr1'] = stocks_df['high'] - stocks_df['low']
            stocks_df['tr2'] = abs(stocks_df['high'] - stocks_df['prev_close'])
            stocks_df['tr3'] = abs(stocks_df['low'] - stocks_df['prev_close'])
            stocks_df['true_range'] = stocks_df[['tr1', 'tr2', 'tr3']].max(axis=1)
            stocks_df['atr_14'] = stocks_df['true_range'].rolling(window=14).mean()

            atr_results = []

            # Process and merge each high sentiment tweet
            for _, row in tweets_df.iterrows():
                sentiment = float(row['sentiment'])
                text = str(row['text'])
                tweet_date = row['date']

                # Match weekend tweets to following Monday
                target_date = tweet_date
                if target_date.weekday() == 5:  # Saturday
                    target_date += timedelta(days=2)
                elif target_date.weekday() == 6:  # Sunday
                    target_date += timedelta(days=1)

                target_date_only = target_date.date()

                # Find valid stocks on or after the target date
                valid_stocks = stocks_df[stocks_df['date_only'] >= target_date_only]
                if not valid_stocks.empty and len(valid_stocks) >= 2:
                    target_stock = valid_stocks.iloc[0]
                    next_day_stock = valid_stocks.iloc[1]

                    stock_close_target = float(target_stock['close'])
                    stock_close_next = float(next_day_stock['close'])

                    # Next-day price change
                    price_change = abs(stock_close_next - stock_close_target)

                    # ATR on target date
                    atr = target_stock['atr_14']
                    if pd.isna(atr):
                        continue

                    exceeded_atr = price_change > atr

                    atr_results.append({
                        "date": tweet_date.strftime('%Y-%m-%d %H:%M:%S'),
                        "target_date": target_stock['date_only'].strftime('%Y-%m-%d'),
                        "tweet_text": text,
                        "sentiment_score": sentiment,
                        "stock_ticker": current_ticker,
                        "close_target": stock_close_target,
                        "close_next": stock_close_next,
                        "price_change": price_change,
                        "atr_14": atr,
                        "exceeded_atr": "Yes" if exceeded_atr else "No"
                    })

            results_df = pd.DataFrame(atr_results)

            with results_container:
                st.subheader(f"ATR Analysis (@{ceo_handle} & {current_ticker.upper()}) ({date_display})")

                if results_df.empty:
                    st.info("No valid market data overlapping with high sentiment tweets could be analyzed.")
                    return

                # Display Results Table
                st.dataframe(results_df, use_container_width=True)

                # Prepare Visualization
                viz_df = results_df[['date', 'price_change', 'atr_14']].copy()
                viz_df.set_index('date', inplace=True)

                with viz_container:
                    st.subheader(f"Price Change vs ATR (14-day) (@{ceo_handle} & {current_ticker.upper()})")
                    st.bar_chart(viz_df)

                # Summary Statistics
                total = len(results_df)
                exceeded_count = len(results_df[results_df['exceeded_atr'] == 'Yes'])
                st.write(f"**Summary**: Out of {total} high-sentiment tweets, the subsequent next-day stock price change exceeded the rolling 14-day ATR {exceeded_count} times.")

        except Exception as e:
            st.error(f"Failed to fetch and analyze ATR data: {str(e)}\n{traceback.format_exc()}")

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
            
            # Filter tweets by selected dates
            if not tweets_df.empty and 'date' in tweets_df.columns:
                if tweets_df['date'].dt.tz is None:
                    tweets_df['date'] = tweets_df['date'].dt.tz_localize('UTC')

                start_date = pd.to_datetime(query_start_date).tz_localize('UTC')
                mask = (tweets_df['date'] >= start_date)

                if query_end_date:
                    end_date = pd.to_datetime(query_end_date).tz_localize('UTC') + timedelta(days=1)
                    mask = mask & (tweets_df['date'] < end_date)

                tweets_df = tweets_df.loc[mask].copy()

            if tweets_df.empty:
                with results_container:
                    st.info("No tweets found for this date range to merge.")
                return

            # Calculate date range with padding for weekends/holidays for stocks
            min_date = tweets_df['date'].min() - timedelta(days=5)
            max_date = tweets_df['date'].max() + timedelta(days=5)
            
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
                tweet_date = row['date']

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
            if not merged_df.empty:
                merged_df = merged_df.sort_values(by='date', ascending=False)
            
            with results_container:
                st.subheader(f"Merged Data (@{ceo_handle} & {current_ticker.upper()}) ({date_display})")
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

with col_btn4:
    if st.button("Run ATR Analysis", type="primary", use_container_width=True):
        fetch_atr_analysis()