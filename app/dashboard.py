import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from app.queries import (
    get_symbols,
    get_price_history,
    get_latest_per_symbol,
    get_multi_symbol_history
)

ACCENT = "#38bdf8"
ACCENT_2 = "#22c55e"
NEGATIVE = "#ef4444"
CARD_BG = "#111827"
PAGE_BG = "#0b1220"
TEXT_MAIN = "#f8fafc"
TEXT_MUTED = "#94a3b8"
BORDER = "rgba(255,255,255,0.08)"

st.set_page_config(
    page_title="MarketPulse Dashboard",
    layout="wide"
)

st_autorefresh(interval=10000, key="market_refresh_main")

st.markdown(f"""
<style>
    .main {{
        background-color: {PAGE_BG};
    }}

    .block-container {{
        padding-top: 1.1rem;
        padding-bottom: 2rem;
        max-width: 96rem;
    }}

    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
        border-right: 1px solid {BORDER};
    }}

    h1, h2, h3 {{
        color: {TEXT_MAIN};
        font-weight: 700;
        letter-spacing: -0.02em;
    }}

    p, label, div {{
        color: #cbd5e1;
    }}

    .dashboard-title {{
        font-size: 2.15rem;
        font-weight: 800;
        color: {TEXT_MAIN};
        margin-bottom: 0.2rem;
    }}

    .dashboard-subtitle {{
        color: {TEXT_MUTED};
        font-size: 0.98rem;
        margin-bottom: 1rem;
    }}

    .section-title {{
        font-size: 1.12rem;
        font-weight: 700;
        color: {TEXT_MAIN};
        margin-bottom: 0.8rem;
        margin-top: 0.1rem;
    }}

    .card {{
        background: linear-gradient(180deg, #111827 0%, #0f172a 100%);
        border: 1px solid {BORDER};
        border-radius: 18px;
        padding: 1rem 1rem 0.85rem 1rem;
        box-shadow: 0 10px 24px rgba(0,0,0,0.24);
        margin-bottom: 1rem;
    }}

    .mini-card {{
        background: linear-gradient(180deg, #111827 0%, #0f172a 100%);
        border: 1px solid rgba(56,189,248,0.18);
        border-radius: 16px;
        padding: 0.95rem 1rem;
        box-shadow: 0 10px 20px rgba(0,0,0,0.20);
        margin-bottom: 1rem;
    }}

    .stat-label {{
        color: {TEXT_MUTED};
        font-size: 0.78rem;
        margin-bottom: 0.35rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }}

    .stat-value {{
        color: {TEXT_MAIN};
        font-size: 1.5rem;
        font-weight: 800;
        line-height: 1.1;
    }}

    .stat-delta-pos {{
        color: {ACCENT_2};
        font-size: 0.9rem;
        margin-top: 0.25rem;
        font-weight: 700;
    }}

    .stat-delta-neg {{
        color: {NEGATIVE};
        font-size: 0.9rem;
        margin-top: 0.25rem;
        font-weight: 700;
    }}

    .stat-delta-neutral {{
        color: {TEXT_MUTED};
        font-size: 0.9rem;
        margin-top: 0.25rem;
        font-weight: 700;
    }}

    .watchlist-title {{
        color: {TEXT_MAIN};
        font-size: 0.95rem;
        font-weight: 700;
        margin-top: 0.8rem;
        margin-bottom: 0.4rem;
    }}

    div[data-testid="stDataFrame"] {{
        border-radius: 14px;
        overflow: hidden;
        border: 1px solid {BORDER};
    }}
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="dashboard-title">MarketPulse Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="dashboard-subtitle">Near real-time stock and crypto monitoring powered by PostgreSQL, Streamlit, and Plotly</div>',
    unsafe_allow_html=True
)

def clean_history_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["captured_at"] = pd.to_datetime(df["captured_at"])
    df = df.sort_values("captured_at")

    numeric_cols = ["price", "open", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["captured_at", "price"])
    df = df.drop_duplicates(subset=["captured_at"], keep="last")

    return df.reset_index(drop=True)

def compute_volume_delta(df: pd.DataFrame) -> pd.DataFrame:
    volume_df = df[["captured_at", "volume"]].copy()
    volume_df["volume"] = pd.to_numeric(volume_df["volume"], errors="coerce").fillna(0)
    volume_df["volume_delta"] = volume_df["volume"].diff()
    volume_df["volume_delta"] = volume_df["volume_delta"].where(volume_df["volume_delta"] >= 0, volume_df["volume"])
    volume_df["volume_delta"] = volume_df["volume_delta"].fillna(0)
    return volume_df

def safe_pct_change(price, open_price):
    try:
        price = float(price)
        open_price = float(open_price)
        if open_price == 0:
            return 0.0
        return ((price - open_price) / open_price) * 100
    except Exception:
        return 0.0

symbols = get_symbols()
latest_rows = get_latest_per_symbol()

if not symbols or not latest_rows:
    st.warning("No data found in the database yet.")
    st.stop()

latest_df = pd.DataFrame(latest_rows, columns=[
    "symbol",
    "asset_type",
    "price",
    "open",
    "high",
    "low",
    "volume",
    "source",
    "captured_at"
])

latest_df["captured_at"] = pd.to_datetime(latest_df["captured_at"])
for col in ["price", "open", "high", "low", "volume"]:
    latest_df[col] = pd.to_numeric(latest_df[col], errors="coerce")

latest_df = latest_df.sort_values("captured_at", ascending=False)
latest_df["pct_change"] = latest_df.apply(lambda row: safe_pct_change(row["price"], row["open"]), axis=1)

st.sidebar.markdown("## Controls")

asset_filter = st.sidebar.radio(
    "Asset Type",
    ["All", "Stocks", "Crypto"],
    index=0
)

if asset_filter == "Stocks":
    filtered_latest_df = latest_df[latest_df["asset_type"] == "stock"].copy()
elif asset_filter == "Crypto":
    filtered_latest_df = latest_df[latest_df["asset_type"] == "crypto"].copy()
else:
    filtered_latest_df = latest_df.copy()

filtered_symbols = filtered_latest_df["symbol"].sort_values().tolist()

if not filtered_symbols:
    st.warning("No symbols match the selected asset type.")
    st.stop()

selected_symbol = st.sidebar.selectbox("Primary Symbol", filtered_symbols, index=0)

watchlist_defaults = [s for s in ["AAPL", "MSFT", "NVDA", "BTC-USD", "ETH-USD"] if s in filtered_symbols]
watchlist_symbols = st.sidebar.multiselect(
    "Watchlist",
    filtered_symbols,
    default=watchlist_defaults if watchlist_defaults else [selected_symbol]
)

comparison_defaults = [s for s in [selected_symbol, "MSFT", "NVDA", "BTC-USD"] if s in filtered_symbols]
comparison_symbols = st.sidebar.multiselect(
    "Compare Symbols",
    filtered_symbols,
    default=comparison_defaults if comparison_defaults else [selected_symbol]
)

show_recent_all = st.sidebar.checkbox("Show All Recent Records", value=True)
history_limit = st.sidebar.slider("History Points", min_value=20, max_value=300, value=100, step=20)

st.sidebar.markdown("---")
st.sidebar.markdown('<div class="watchlist-title">Live Watchlist</div>', unsafe_allow_html=True)

sidebar_watch_df = filtered_latest_df.copy()
if watchlist_symbols:
    sidebar_watch_df = sidebar_watch_df[sidebar_watch_df["symbol"].isin(watchlist_symbols)]

sidebar_watch_df = sidebar_watch_df[["symbol", "price", "pct_change"]].sort_values("pct_change", ascending=False)
sidebar_watch_display = sidebar_watch_df.copy()
sidebar_watch_display["price"] = sidebar_watch_display["price"].map(lambda x: f"${x:,.2f}")
sidebar_watch_display["pct_change"] = sidebar_watch_display["pct_change"].map(lambda x: f"{x:.2f}%")

st.sidebar.dataframe(sidebar_watch_display, use_container_width=True, hide_index=True)

history_rows = get_price_history(selected_symbol, limit=history_limit)

if not history_rows:
    st.warning("No history found for the selected symbol.")
    st.stop()

history_df = pd.DataFrame(history_rows, columns=[
    "symbol",
    "price",
    "open",
    "high",
    "low",
    "volume",
    "source",
    "captured_at"
])

history_df = clean_history_df(history_df)
volume_df = compute_volume_delta(history_df)

selected_latest = latest_df[latest_df["symbol"] == selected_symbol].iloc[0]

latest_price = float(selected_latest["price"])
day_open = float(selected_latest["open"])
day_high = float(selected_latest["high"])
day_low = float(selected_latest["low"])
volume = float(selected_latest["volume"])

previous_price = None
if len(history_df) > 1:
    previous_price = float(history_df.iloc[-2]["price"])

price_delta = latest_price - previous_price if previous_price is not None else None
pct_change_from_open = safe_pct_change(latest_price, day_open)

top_gainer = filtered_latest_df.sort_values("pct_change", ascending=False).iloc[0]
top_loser = filtered_latest_df.sort_values("pct_change", ascending=True).iloc[0]

stocks_count = int((latest_df["asset_type"] == "stock").sum())
crypto_count = int((latest_df["asset_type"] == "crypto").sum())
tracked_assets = int(len(latest_df))

st.markdown(
    f"""
    <div style="color:{TEXT_MUTED}; font-size:0.95rem; margin-bottom:0.8rem;">
        <strong style="color:{TEXT_MAIN};">{selected_symbol}</strong>
        <span style="margin:0 8px;">•</span>
        Updated {selected_latest['captured_at'].strftime('%b %d, %Y %I:%M:%S %p')}
    </div>
    """,
    unsafe_allow_html=True
)

def delta_class(value):
    if value is None:
        return "stat-delta-neutral"
    if value > 0:
        return "stat-delta-pos"
    if value < 0:
        return "stat-delta-neg"
    return "stat-delta-neutral"

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    delta_text = f"{price_delta:,.2f}" if price_delta is not None else "No change data"
    st.markdown(f"""
    <div class="mini-card">
        <div class="stat-label">Latest Price</div>
        <div class="stat-value">${latest_price:,.2f}</div>
        <div class="{delta_class(price_delta)}">{delta_text}</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="mini-card">
        <div class="stat-label">Day High</div>
        <div class="stat-value">${day_high:,.2f}</div>
        <div class="stat-delta-neutral">Intraday high</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="mini-card">
        <div class="stat-label">Day Low</div>
        <div class="stat-value">${day_low:,.2f}</div>
        <div class="stat-delta-neutral">Intraday low</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    pct_class = "stat-delta-pos" if pct_change_from_open >= 0 else "stat-delta-neg"
    st.markdown(f"""
    <div class="mini-card">
        <div class="stat-label">% From Open</div>
        <div class="stat-value">{pct_change_from_open:.2f}%</div>
        <div class="{pct_class}">Open-to-current move</div>
    </div>
    """, unsafe_allow_html=True)

with k5:
    st.markdown(f"""
    <div class="mini-card">
        <div class="stat-label">Volume</div>
        <div class="stat-value">{volume:,.0f}</div>
        <div class="stat-delta-neutral">Latest available volume</div>
    </div>
    """, unsafe_allow_html=True)

top_left, top_right = st.columns([1.35, 1])

with top_left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Watchlist Snapshot</div>', unsafe_allow_html=True)

    snapshot_df = filtered_latest_df.copy()
    if watchlist_symbols:
        snapshot_df = snapshot_df[snapshot_df["symbol"].isin(watchlist_symbols)]

    snapshot_df = snapshot_df[["symbol", "asset_type", "price", "pct_change", "volume"]].sort_values("pct_change", ascending=False)

    snapshot_display = snapshot_df.copy()
    snapshot_display["price"] = snapshot_display["price"].map(lambda x: f"${x:,.2f}")
    snapshot_display["pct_change"] = snapshot_display["pct_change"].map(lambda x: f"{x:.2f}%")
    snapshot_display["volume"] = snapshot_display["volume"].map(lambda x: f"{x:,.0f}")

    st.dataframe(snapshot_display, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

with top_right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Market Summary</div>', unsafe_allow_html=True)

    s1, s2, s3 = st.columns(3)
    s1.metric("Tracked Assets", f"{tracked_assets}")
    s2.metric("Stocks", f"{stocks_count}")
    s3.metric("Crypto", f"{crypto_count}")

    st.info(f"Top gainer: {top_gainer['symbol']} ({top_gainer['pct_change']:.2f}%)")
    st.info(f"Top mover down: {top_loser['symbol']} ({top_loser['pct_change']:.2f}%)")
    st.markdown('</div>', unsafe_allow_html=True)

mid_left, mid_right = st.columns([1.65, 1])

with mid_left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{selected_symbol} Price Trend</div>', unsafe_allow_html=True)

    price_chart_df = history_df[["captured_at", "price"]].copy()
    price_chart_df["ma_3"] = price_chart_df["price"].rolling(3, min_periods=1).mean()
    price_chart_df["ma_5"] = price_chart_df["price"].rolling(5, min_periods=1).mean()

    fig_price = go.Figure()
    fig_price.add_trace(go.Scatter(
        x=price_chart_df["captured_at"],
        y=price_chart_df["price"],
        mode="lines",
        name="Price",
        line=dict(width=3, color=ACCENT)
    ))
    fig_price.add_trace(go.Scatter(
        x=price_chart_df["captured_at"],
        y=price_chart_df["ma_3"],
        mode="lines",
        name="MA 3",
        line=dict(width=2, dash="dot", color="#a78bfa")
    ))
    fig_price.add_trace(go.Scatter(
        x=price_chart_df["captured_at"],
        y=price_chart_df["ma_5"],
        mode="lines",
        name="MA 5",
        line=dict(width=2, dash="dash", color=ACCENT_2)
    ))

    fig_price.update_layout(
        template="plotly_dark",
        height=430,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        xaxis_title="Time",
        yaxis_title="Price",
        legend=dict(orientation="h", y=1.08, x=0),
        font=dict(color=TEXT_MAIN)
    )
    fig_price.update_xaxes(gridcolor="rgba(255,255,255,0.05)")
    fig_price.update_yaxes(gridcolor="rgba(255,255,255,0.05)")

    st.plotly_chart(fig_price, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with mid_right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Asset Mix</div>', unsafe_allow_html=True)

    mix_source = filtered_latest_df.copy()
    asset_mix = mix_source["asset_type"].value_counts().reset_index()
    asset_mix.columns = ["asset_type", "count"]

    fig_pie = px.pie(
        asset_mix,
        names="asset_type",
        values="count",
        hole=0.6,
        color="asset_type",
        color_discrete_map={"stock": ACCENT, "crypto": ACCENT_2},
        template="plotly_dark"
    )

    fig_pie.update_layout(
        height=430,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font=dict(color=TEXT_MAIN),
        showlegend=True
    )

    st.plotly_chart(fig_pie, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

bottom_left, bottom_right = st.columns(2)

with bottom_left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Top Movers</div>', unsafe_allow_html=True)

    movers_chart_df = filtered_latest_df[["symbol", "pct_change"]].sort_values("pct_change", ascending=False)
    movers_chart_df["bar_color"] = movers_chart_df["pct_change"].apply(lambda x: ACCENT_2 if x >= 0 else NEGATIVE)

    fig_movers = go.Figure(go.Bar(
        x=movers_chart_df["symbol"],
        y=movers_chart_df["pct_change"],
        marker_color=movers_chart_df["bar_color"]
    ))

    fig_movers.update_layout(
        template="plotly_dark",
        height=380,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        xaxis_title="Symbol",
        yaxis_title="% Change",
        font=dict(color=TEXT_MAIN)
    )
    fig_movers.update_xaxes(gridcolor="rgba(255,255,255,0.03)")
    fig_movers.update_yaxes(gridcolor="rgba(255,255,255,0.05)")

    st.plotly_chart(fig_movers, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with bottom_right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{selected_symbol} Volume Trend</div>', unsafe_allow_html=True)

    fig_volume = px.bar(
        volume_df,
        x="captured_at",
        y="volume_delta",
        template="plotly_dark"
    )
    fig_volume.update_traces(marker_color=ACCENT)

    fig_volume.update_layout(
        height=380,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        xaxis_title="Time",
        yaxis_title="Volume Change",
        font=dict(color=TEXT_MAIN)
    )
    fig_volume.update_xaxes(gridcolor="rgba(255,255,255,0.03)")
    fig_volume.update_yaxes(gridcolor="rgba(255,255,255,0.05)")

    st.plotly_chart(fig_volume, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Asset Comparison</div>', unsafe_allow_html=True)

compare_mode = st.radio(
    "Comparison Mode",
    ["Normalized Performance", "Raw Price"],
    horizontal=True
)

compare_rows = get_multi_symbol_history(comparison_symbols, limit_per_symbol=history_limit)

if compare_rows and len(comparison_symbols) > 0:
    compare_df = pd.DataFrame(compare_rows, columns=["symbol", "price", "captured_at", "rn"])
    compare_df["captured_at"] = pd.to_datetime(compare_df["captured_at"])
    compare_df["price"] = pd.to_numeric(compare_df["price"], errors="coerce")
    compare_df = compare_df.dropna(subset=["captured_at", "price"])
    compare_df = compare_df.sort_values(["symbol", "captured_at"])
    compare_df = compare_df.drop_duplicates(subset=["symbol", "captured_at"], keep="last")

    pivot_df = compare_df.pivot_table(
        index="captured_at",
        columns="symbol",
        values="price",
        aggfunc="last"
    ).sort_index()

    if compare_mode == "Normalized Performance":
        chart_df = pivot_df.copy()

        for col in chart_df.columns:
            first_valid = chart_df[col].dropna()
            if not first_valid.empty and first_valid.iloc[0] != 0:
                chart_df[col] = (chart_df[col] / first_valid.iloc[0]) * 100

        y_title = "Normalized Performance (Base = 100)"
        chart_note = "Each asset is rebased to 100 so you can compare performance across very different price ranges."
    else:
        chart_df = pivot_df.copy()
        y_title = "Raw Price"
        chart_note = "Raw price mode can make lower-priced assets look flat when BTC or ETH are included."

    color_cycle = [ACCENT, ACCENT_2, "#a78bfa", "#f59e0b", "#f43f5e", "#14b8a6", "#f97316", "#06b6d4"]

    fig_compare = go.Figure()

    for i, col in enumerate(chart_df.columns):
        series = chart_df[col].dropna()
        if not series.empty:
            fig_compare.add_trace(go.Scatter(
                x=series.index,
                y=series.values,
                mode="lines+markers",
                name=col,
                line=dict(width=2.5, color=color_cycle[i % len(color_cycle)]),
                marker=dict(size=5)
            ))

    fig_compare.update_layout(
        template="plotly_dark",
        height=500,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        xaxis_title="Time",
        yaxis_title=y_title,
        font=dict(color=TEXT_MAIN),
        hovermode="x unified"
    )
    fig_compare.update_xaxes(gridcolor="rgba(255,255,255,0.03)")
    fig_compare.update_yaxes(gridcolor="rgba(255,255,255,0.05)")

    st.plotly_chart(fig_compare, use_container_width=True)
    st.caption(chart_note)

    compare_snapshot = filtered_latest_df[filtered_latest_df["symbol"].isin(comparison_symbols)].copy()
    compare_snapshot = compare_snapshot[["symbol", "asset_type", "price", "pct_change", "volume", "captured_at"]]
    compare_snapshot["price"] = compare_snapshot["price"].map(lambda x: f"${x:,.2f}")
    compare_snapshot["pct_change"] = compare_snapshot["pct_change"].map(lambda x: f"{x:.2f}%")
    compare_snapshot["captured_at"] = compare_snapshot["captured_at"].dt.strftime("%Y-%m-%d %I:%M:%S %p")

    st.dataframe(compare_snapshot, use_container_width=True, hide_index=True)
else:
    st.info("Select one or more symbols in the sidebar to compare assets.")

st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Recent Records</div>', unsafe_allow_html=True)

records_df = filtered_latest_df.copy()

if not show_recent_all:
    records_df = records_df[records_df["symbol"] == selected_symbol]

records_df = records_df[[
    "symbol", "asset_type", "price", "open", "high", "low", "volume", "source", "captured_at"
]].copy()

records_df["price"] = records_df["price"].map(lambda x: f"${x:,.2f}")
records_df["open"] = records_df["open"].map(lambda x: f"${x:,.2f}")
records_df["high"] = records_df["high"].map(lambda x: f"${x:,.2f}")
records_df["low"] = records_df["low"].map(lambda x: f"${x:,.2f}")
records_df["captured_at"] = pd.to_datetime(records_df["captured_at"]).dt.strftime("%Y-%m-%d %I:%M:%S %p")

st.dataframe(records_df, use_container_width=True, hide_index=True)
st.markdown('</div>', unsafe_allow_html=True)