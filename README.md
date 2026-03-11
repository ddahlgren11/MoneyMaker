# MoneyMaker Active Controller API

## Description
This project is an API for fetching CEO tweets, analyzing their sentiment, correlating them with stock data, and saving the merged records to a database (PostgreSQL). It leverages FastAPI for the backend, Alpaca for stock data, and Tweety to fetch tweets.

*(Note: The file `actualcapstone.ipynb` is a scratchpad/exploratory notebook and is not part of the active API structure.)*

## Project Structure
- `main.py`: The FastAPI backend containing API endpoints and database setup.
- `processor.py`: Contains data processing logic, fetching tweets and stock data.
- `classifier.py`: Text classification functions for sentiment and tweet categorization.
- `requirements.txt`: Python dependencies.

## API Endpoints
- `GET /`: Root endpoint, returns a welcome message.
- `POST /process/all`: Fetches recent tweets (e.g., Elon Musk), fetches related stock data (e.g., TSLA), processes sentiment, merges the data, and saves it to the database.
- `POST /ingest/tweets`: Manually ingest a list of tweets into the database.
- `POST /ingest/stocks`: Manually ingest a list of stock records into the database.
- `POST /ingest/merged`: Manually ingest merged data into the database.

## Required Environment Variables
To run the API, you need to configure the following environment variables in a `.env` file:
- `DATABASE_URL`: Your database connection URL (e.g., PostgreSQL/Neon).
- `ALPACA_API_KEY`: Your Alpaca API key for fetching stock data.
- `ALPACA_SECRET_KEY`: Your Alpaca API secret key.

## How to Start It Up

Follow these steps to start the API server locally:

1. **Install Dependencies**:
   Ensure you have Python installed. Install the required packages using pip:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment Variables**:
   Create a `.env` file in the root directory and add your credentials:
   ```env
   DATABASE_URL=your_database_url
   ALPACA_API_KEY=your_alpaca_api_key
   ALPACA_SECRET_KEY=your_alpaca_secret_key
   ```

3. **Run the FastAPI Server**:
   Start the application using `uvicorn`:
   ```bash
   uvicorn main:app --reload
   ```
   The server will be running at `http://localhost:8000`. You can access the automatic interactive API documentation at `http://localhost:8000/docs`.

   uvicorn main:app --reload
   streamlit run app.py