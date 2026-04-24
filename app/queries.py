from sqlalchemy import text
from app.db import engine

def get_symbols():
    query = text("""
        SELECT symbol
        FROM assets
        ORDER BY symbol
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result.fetchall()]

def get_price_history(symbol, limit=200):
    query = text("""
        SELECT 
            a.symbol,
            p.price,
            p.open,
            p.high,
            p.low,
            p.volume,
            p.source,
            p.captured_at
        FROM price_snapshots p
        JOIN assets a ON p.asset_id = a.id
        WHERE a.symbol = :symbol
        ORDER BY p.captured_at ASC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"symbol": symbol, "limit": limit})
        return result.fetchall()

def get_latest_per_symbol():
    query = text("""
        SELECT DISTINCT ON (a.symbol)
            a.symbol,
            a.asset_type,
            p.price,
            p.open,
            p.high,
            p.low,
            p.volume,
            p.source,
            p.captured_at
        FROM price_snapshots p
        JOIN assets a ON p.asset_id = a.id
        ORDER BY a.symbol, p.captured_at DESC
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        return result.fetchall()

def get_multi_symbol_history(symbols, limit_per_symbol=100):
    if not symbols:
        return []

    query = text("""
        SELECT *
        FROM (
            SELECT
                a.symbol,
                p.price,
                p.captured_at,
                ROW_NUMBER() OVER (
                    PARTITION BY a.symbol
                    ORDER BY p.captured_at DESC
                ) AS rn
            FROM price_snapshots p
            JOIN assets a ON p.asset_id = a.id
            WHERE a.symbol = ANY(:symbols)
        ) ranked
        WHERE rn <= :limit_per_symbol
        ORDER BY captured_at ASC
    """)

    with engine.connect() as conn:
        result = conn.execute(
            query,
            {"symbols": symbols, "limit_per_symbol": limit_per_symbol}
        )
        return result.fetchall()