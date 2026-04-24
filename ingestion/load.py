from sqlalchemy import text
from app.db import engine
from ingestion.fetch_market_data import fetch_stock_data

def get_or_create_asset(symbol, asset_type="stock", name=None):
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT id, asset_type FROM assets WHERE symbol = :symbol"),
            {"symbol": symbol}
        ).fetchone()

        if result:
            asset_id, existing_type = result
            if existing_type != asset_type:
                conn.execute(
                    text("""
                        UPDATE assets
                        SET asset_type = :asset_type
                        WHERE id = :asset_id
                    """),
                    {"asset_type": asset_type, "asset_id": asset_id}
                )
            return asset_id

        inserted = conn.execute(
            text("""
                INSERT INTO assets (symbol, asset_type, name)
                VALUES (:symbol, :asset_type, :name)
                RETURNING id
            """),
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "name": name or symbol
            }
        ).fetchone()

        return inserted[0]

def insert_price_snapshot(data, asset_type="stock"):
    asset_id = get_or_create_asset(data["symbol"], asset_type=asset_type)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO price_snapshots
                (asset_id, price, open, high, low, volume, source)
                VALUES
                (:asset_id, :price, :open, :high, :low, :volume, :source)
            """),
            {
                "asset_id": asset_id,
                "price": data["price"],
                "open": data["open"],
                "high": data["high"],
                "low": data["low"],
                "volume": data["volume"],
                "source": data["source"]
            }
        )

def main():
    symbols = [
        ("AAPL", "stock"),
        ("MSFT", "stock"),
        ("TSLA", "stock"),
        ("NVDA", "stock"),
        ("AMZN", "stock"),
        ("META", "stock"),
        ("GOOGL", "stock"),
        ("BTC-USD", "crypto"),
        ("ETH-USD", "crypto"),
        ("SOL-USD", "crypto")
    ]

    for symbol, asset_type in symbols:
        data = fetch_stock_data(symbol)

        if data:
            insert_price_snapshot(data, asset_type=asset_type)
            print(f"Inserted {symbol} into price_snapshots table.")
        else:
            print(f"No data to insert for {symbol}.")

if __name__ == "__main__":
    main()