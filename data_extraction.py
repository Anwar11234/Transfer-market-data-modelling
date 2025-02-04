import pandas as pd

def load_data():
    df_games = pd.read_csv('data/raw/games.csv')
    df_clubs = pd.read_csv('data/raw/clubs.csv')
    df_player = pd.read_csv('data/raw/players.csv')
    df_competitions = pd.read_csv('data/raw/competitions.csv')
    fact_apperance = pd.read_csv('data/raw/appearances.csv')
    df_fact_transfer = pd.read_csv('data/raw/transfers.csv')
    
    return df_games, df_clubs, df_player, df_competitions, fact_apperance, df_fact_transfer