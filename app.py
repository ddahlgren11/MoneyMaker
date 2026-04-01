import streamlit as st
import asyncio
import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta, datetime
import pytz
import traceback

from processor import DataProcessor
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type

st.set_page_config(page_title="MoneyMaker", layout="wide", page_icon="📈")
st.title("MoneyMaker")

@st.cache_resource
def get_processor():
    return DataProcessor()

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
tab_data, tab_atr, tab_impact, tab_trend = st.tabs(["Data", "ATR Analysis", "Tweet Impact", "Post-Tweet Trend"])

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
                                st.dataframe(filtered[['ceo', 'date', 'text', 'sentiment']], use_container_width=True)
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

# ── ATR ANALYSIS TAB ──────────────────────────────────────────────────────────
with tab_atr:
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

# ── TWEET IMPACT TAB ──────────────────────────────────────────────────────────
with tab_impact:
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

# ── POST-TWEET TREND TAB ──────────────────────────────────────────────────────
with tab_trend:
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
