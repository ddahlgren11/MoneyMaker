import streamlit as st
import requests

# Base URL of your FastAPI server
API_URL = "http://localhost:8000"

st.title("MoneyMaker Active Controller")

# Example: Triggering the process/all endpoint
st.header("Process Data")
ceo_name = st.text_input("CEO Name", "Elon Musk")
ticker = st.text_input("Stock Ticker", "TSLA")

if st.button("Run Analysis"):
    with st.spinner("Fetching tweets and stock data..."):
        # Making a request to your existing FastAPI endpoint
        response = requests.post(f"{API_URL}/process/all")
        if response.status_code == 200:
            st.success("Processing complete!")
            st.json(response.json())
        else:
            st.error("Error processing data.")