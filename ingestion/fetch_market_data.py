import yfinance as yf

def fetch_stock_data(symbol="AAPL"):
    ticker = yf.Ticker(symbol)
    history = ticker.history(period="1d", interval="1m")

    if history.empty:
        print(f"No data found for {symbol}")
        return None

    latest = history.iloc[-1]

    data = {
        "symbol": symbol,
        "price": float(latest["Close"]),
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "volume": float(latest["Volume"]),
        "source": "yfinance"
    }

    return data

if __name__ == "__main__":
    result = fetch_stock_data("AAPL")
    print(result)