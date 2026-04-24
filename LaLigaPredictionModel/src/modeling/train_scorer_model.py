import pandas as pd
import xgboost as xgb
import os
import pickle

def train():
    df = pd.read_csv("data/synthetic/future_players.csv")

    X = df[["matches", "minutes", "goals_per_match", "minutes_per_goal"]]
    y = df["goals"]

    model = xgb.XGBRegressor(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1
    )
    model.fit(X, y)

    os.makedirs("models", exist_ok=True)
    with open("models/top_scorer_model.pkl", "wb") as f:
        pickle.dump(model, f)

    print("✅ Top scorer model trained and saved using synthetic data.")

if __name__ == "__main__":
    train()