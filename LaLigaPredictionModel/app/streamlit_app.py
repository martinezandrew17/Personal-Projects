import streamlit as st
st.set_page_config(page_title="La Liga Predictor", layout="wide")

import pandas as pd
from pathlib import Path
import sys
import os
import altair as alt
import base64

def add_bg_from_local(image_file):
    with open(image_file, "rb") as file:
        encoded_string = base64.b64encode(file.read())
    
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url(data:image/jpg;base64,{encoded_string.decode()});
            background-attachment: fixed;
            background-size: cover;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

st.markdown("""
    <style>
    .stApp {
        background-color: white;
    }
    .stMarkdown, .stDataFrame, .stSelectbox {
        color: black;
    }
    div[data-testid="stVerticalBlock"] {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="main">', unsafe_allow_html=True)

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.utils.helpers import load_model

st.title("⚽ La Liga Predictor Using Machine Learning!")

champ_model = load_model("models/champion_model.pkl")
scorer_model = load_model("models/top_scorer_model.pkl")

tdf = pd.read_csv("data/synthetic/future_teams.csv")
pdf = pd.read_csv("data/synthetic/future_players.csv")

seasons = sorted(tdf["season"].unique())
selected_season = st.selectbox("Select Season", seasons, index=0)

st.subheader(f"🏆 Predicted Champion - {selected_season}")
group = tdf[tdf["season"] == selected_season].copy()
X = group[["wins", "draws", "losses", "goals_scored", "goals_against", "goal_diff", "points"]]
try:
    group["champion_pred"] = champ_model.predict(X)
    winner = group[group["champion_pred"] == 1]
    if not winner.empty:
        st.markdown(f"### 🥇 {winner.iloc[0]['team']}")
    else:
        top_team = group.sort_values("points", ascending=False).iloc[0]["team"]
        st.markdown(f"### 🥇 {top_team}")
except Exception as e:
    st.error(f"Prediction error: {e}")
st.dataframe(group[["team", "wins", "draws", "losses", "points"]].sort_values("points", ascending=False), use_container_width=True)

st.subheader(f"🎯 Predicted Top Scorers - {selected_season}")
scorer_group = pdf[pdf["season"] == selected_season].copy()
Xp = scorer_group[["matches", "minutes", "goals_per_match", "minutes_per_goal"]]
scorer_group["predicted_goals"] = scorer_model.predict(Xp)
top_scorers = scorer_group.sort_values("predicted_goals", ascending=False).head(10)
top_scorers["player"] = top_scorers["player"].str.replace("_", " ").str.replace("Player", "Player ")
st.dataframe(top_scorers[["player", "team", "predicted_goals"]], use_container_width=True)

st.subheader(f"📊 Goals by Number - {selected_season}")
chart = alt.Chart(top_scorers).mark_bar().encode(
    x=alt.X("predicted_goals:Q", title="Goals"),
    y=alt.Y("player:N", sort="-x", title="Player"),
    color=alt.Color("team:N", title="Team")
).properties(
    width=800,
    height=400
)

st.altair_chart(chart, use_container_width=True)

st.divider()
st.caption("La Liga predictions powered by machine learning (XGBoost) -- Visca El Barça!")

