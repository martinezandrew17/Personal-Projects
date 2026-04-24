import pickle
import pandas as pd

def load_model(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

def predict_champion(model, X):
    return model.predict(X)

def predict_scorers(model, X):
    preds = model.predict(X)
    return pd.Series(preds, name="predicted_goals")
