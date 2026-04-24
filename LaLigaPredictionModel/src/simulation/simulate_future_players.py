import os
import pandas as pd
import numpy as np

def generate_synthetic_player_data(start_year=2025, seasons=5):
    np.random.seed(42)
    teams = [
        "Real Madrid", "Barcelona", "Atlético Madrid", "Girona", "Athletic Club",
        "Real Sociedad", "Real Betis", "Villarreal", "Valencia", "Sevilla",
        "Getafe", "Osasuna", "Las Palmas", "Almería", "Granada",
        "Mallorca", "Rayo Vallecano", "Cádiz", "Alavés", "Celta Vigo"
    ]

    player_pool = {
        "Real Madrid": ["Jude Bellingham", "Vinícius Jr.", "Rodrygo", "Federico Valverde"],
        "Barcelona": ["Robert Lewandowski", "Pedri", "Gavi", "Ferran Torres"],
        "Atlético Madrid": ["Antoine Griezmann", "Álvaro Morata", "Saúl Ñíguez", "Rodrigo De Paul"],
        "Girona": ["Artem Dovbyk", "Savinho", "Aleix García"],
        "Athletic Club": ["Iñaki Williams", "Nico Williams", "Oihan Sancet"],
        "Real Sociedad": ["Takefusa Kubo", "Mikel Oyarzabal", "Brais Méndez"],
        "Real Betis": ["Isco", "Borja Iglesias", "Ayoze Pérez"],
        "Villarreal": ["Gerard Moreno", "Álex Baena", "Yéremy Pino"],
        "Valencia": ["Hugo Duro", "Javi Guerra", "Pepelu"],
        "Sevilla": ["Youssef En-Nesyri", "Lucas Ocampos", "Suso"],
        "Getafe": ["Borja Mayoral", "Mason Greenwood", "Carles Aleñá"],
        "Osasuna": ["Chimy Ávila", "Ante Budimir", "Moi Gómez"],
        "Las Palmas": ["Jonathan Viera", "Munir", "Kirian Rodríguez"],
        "Almería": ["Luis Suárez", "Léo Baptistão", "Melero"],
        "Granada": ["Lucas Boyé", "Bryan Zaragoza", "Antonio Puertas"],
        "Mallorca": ["Vedat Muriqi", "Dani Rodríguez", "Antonio Sánchez"],
        "Rayo Vallecano": ["Isi Palazón", "Raúl de Tomás", "Óscar Trejo"],
        "Cádiz": ["Roger Martí", "Rubén Alcaraz", "Chris Ramos"],
        "Alavés": ["Luis Rioja", "Jon Guridi", "Abde Rebbach"],
        "Celta Vigo": ["Iago Aspas", "Jørgen Strand Larsen", "Franco Cervi"]
    }

    data = []
    for year in range(start_year, start_year + seasons):
        for team in teams:
            players = player_pool.get(team, [])
            for player in players:
                matches = np.random.randint(15, 38)
                minutes = matches * np.random.randint(60, 95)
                goals = np.random.poisson(lam=matches * 0.3)
                gpm = goals / matches if matches else 0
                mpg = minutes / goals if goals else minutes
                data.append({
                    "season": year,
                    "player": player,
                    "team": team,
                    "matches": matches,
                    "minutes": minutes,
                    "goals": goals,
                    "goals_per_match": gpm,
                    "minutes_per_goal": mpg
                })

    os.makedirs("data/synthetic", exist_ok=True)
    df = pd.DataFrame(data)
    df.to_csv("data/synthetic/future_players.csv", index=False)
    print("✅ Synthetic player data saved using 2024–25 rosters.")
    return df

if __name__ == "__main__":
    generate_synthetic_player_data()