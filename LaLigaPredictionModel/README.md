# La Liga Predictor Using Machine Learning

## Overview
This project uses machine learning to predict La Liga outcomes, including the league champion and top goal scorers. The application is built using Streamlit and provides an interactive interface for users to explore predictions for different seasons.

## Features
- 🏆 League Champion Prediction
- 🎯 Top Scorer Prediction
- 📊 Interactive Visualizations
- 📈 Historical Data Analysis
- 🔄 Season Selection

## Technologies Used
- Python
- Streamlit
- XGBoost
- Pandas
- Altair (for visualizations)

## Installation

1. Clone the repository:
```bash
git clone [your-repository-url]
cd LaLigaPredictionModel
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

## Usage

1. Navigate to the app directory:
```bash
cd app
```

2. Run the Streamlit app:
```bash
streamlit run streamlit_app.py
```

3. Open your web browser and go to the URL shown in the terminal (typically http://localhost:8501)

## How It Works

### League Champion Prediction
- Uses historical team performance data
- Considers factors such as:
  - Wins, draws, and losses
  - Goals scored and conceded
  - Goal difference
  - Points

### Top Scorer Prediction
- Analyzes player performance metrics
- Takes into account:
  - Matches played
  - Minutes played
  - Goals per match
  - Minutes per goal

## Project Structure
```
LaLigaPredictionModel/
├── app/
│   └── streamlit_app.py
├── data/
│   └── synthetic/
│       ├── future_teams.csv
│       └── future_players.csv
├── models/
│   ├── champion_model.pkl
│   └── top_scorer_model.pkl
├── src/
│   └── utils/
│       └── helpers.py
└── README.md
```

## Model Performance
The models are trained using XGBoost, a powerful machine learning algorithm that provides:
- High accuracy in predictions
- Ability to handle complex relationships in the data
- Robust performance with limited data

## Future Improvements
- Add more prediction features (e.g., relegation, European qualification)
- Include player transfer predictions
- Implement real-time data updates
- Add historical prediction accuracy metrics
- Include team form analysis

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
- La Liga for the inspiration
- Streamlit for the amazing framework
- The open-source community for their valuable tools and libraries

## Contact
For any questions or suggestions, please open an issue in the repository. 

