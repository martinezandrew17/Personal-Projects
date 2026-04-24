"""
ingestion/transform.py

Cleans and validates raw yfinance DataFrames before they reach the database.
Input:  raw DataFrame from yfinance.download()
Output: clean DataFrame with columns matching the prices table schema
"""
import pandas as pd
from loguru import logger


# yfinance returns Title Case columns — map them to your lowercase schema
COLUMN_MAP = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}

def clean(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Transform a raw yfinance DataFrame into a clean DataFrame
    ready to be inserted into the prices table.

    Returns an empty DataFrame if the input is invalid.
    """
    if df is None or df.empty:
        logger.warning(f"[{ticker}] Received empty DataFrame - skipping")
        return pd.DataFrame()
    
    try:
        # ── Step 1: flatten MultiIndex columns yfinance sometimes returns ────
        # yfinance can return columns like ("Close", "AAPL") in some versions
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # ── Step 2: keep only the OHLCV columns we care about ─────────────
        df =df[[col for col in COLUMN_MAP if col in df.columns]].copy()

        # ── Step 3: rename to match the database schema ────────────────────
        df = df.rename(columns = COLUMN_MAP)

        # ── Step 4: move the index (timestamps) into a regular column ──────
        # yfinance sets the DateTime as the index — we need it as a column
        df = df.reset_index().rename(columns = {"Datetime": "timestamp", 
                                                "Date": "timestamp"})
        
        # ── Step 5: add the ticker column ──────────────────────────────────
        df["ticker"] = ticker

        # ── Step 6: enforce correct types ──────────────────────────────────
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors = "coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors = "coerce").astype("Int64")

        # ── Step 7: drop bad rows ───────────────────────────────────────────
        # A row with a null or zero close price is unusable
        before = len(df)
        df = df.dropna(subset = ["timestamp", "open", "high", "low", "close"])
        df = df[df["close"] > 0]
        dropped = before - len(df)
        if dropped:
            logger.warning(f"[{ticker}] Dropped {dropped} invalid rows")

        # ── Step 8: final column order to match INSERT statement ────────────
        df = df[["ticker", "timestamp", "open", "low", "close", "volume"]]

        logger.info(f"[{ticker}] Transformed {len(df)} clean rows")
        return df
    
    except Exception as e:
        logger.error(f"[{ticker}] Transform failed: {e}")
        return pd.DataFrame()
    
    