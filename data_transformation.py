import pandas as pd

def add_surrogate_keys(df_games, df_clubs, df_player, df_competitions):
    df_games['game_key'] = range(1, len(df_games) + 1)
    df_clubs['club_key'] = range(1, len(df_clubs) + 1)
    df_player['player_key'] = range(1, len(df_player) + 1)
    df_competitions['competition_key'] = range(1, len(df_competitions) + 1)
    return df_games, df_clubs, df_player, df_competitions

def transform_games(df_games, df_competitions):
    df_games = pd.merge(df_games, df_competitions[['competition_id', 'name']], on='competition_id')
    df_games = df_games.drop(columns=['competition_id'])
    df_games.rename(columns={'name': 'competition_name'}, inplace=True)
    df_games.drop(columns=['home_club_id', 'away_club_id'], inplace=True)
    return df_games

def transform_clubs(df_clubs, df_competitions):
    df_clubs = pd.merge(df_clubs, df_competitions[['competition_id', 'name']], 
                        left_on='domestic_competition_id', right_on='competition_id')
    df_clubs = df_clubs.drop(columns=['domestic_competition_id', 'competition_id'])
    df_clubs.rename(columns={'name_x': 'club_name', 'name_y': 'competition_name'}, inplace=True)
    df_clubs.drop(columns=['club_code'], inplace=True)
    
    unknown_club_record = {
        'club_id': -1,  
        'club_name': 'Unknown',
        'total_market_value': 0,
        'squad_size': 0,
        'average_age': 0,
        'foreigners_number': 0,
        'foreigners_percentage': 0,
        'national_team_players': 0,
        'stadium_name': 'Unknown',
        'stadium_seats': 0,
        'net_transfer_record': 0,
        'coach_name': 'Unknown',
        'last_season': 'Unknown',
        'filename': 'Unknown',
        'url': 'Unknown',
        'club_key': -1,  
        'competition_name': 'Unknown'
    }
    df_clubs.loc[len(df_clubs)] = unknown_club_record

    return df_clubs

def transform_players(df_player):
    df_player = df_player.drop(columns=['first_name', 'last_name', 'player_code', 
                                          'current_club_id', 'current_club_domestic_competition_id'])
    df_player.rename(columns={'name': 'player_name'}, inplace=True)
    return df_player

def transform_competitions(df_competitions):
    df_competitions = df_competitions.drop(columns=['competition_code', 'country_id', 'domestic_league_code'])
    return df_competitions

def transform_fact_appearance(fact_appearance, df_games, df_clubs, df_competitions, df_player):
    fact_appearance = pd.merge(fact_appearance, df_games[['game_id', 'game_key']], on='game_id').drop(columns=['game_id'])
    
    fact_appearance = pd.merge(fact_appearance, df_clubs[['club_id', 'club_key']], 
                               left_on='player_current_club_id', right_on='club_id').drop(columns=['club_id', 'player_current_club_id'])
    
    fact_appearance = pd.merge(fact_appearance, df_competitions[['competition_id', 'competition_key']], 
                               on='competition_id', how='inner').drop(columns=['competition_id'])
    
    fact_appearance = pd.merge(fact_appearance, df_player[['player_id', 'player_key']], 
                               on='player_id', how='inner').drop(columns=['player_id'])
    
    fact_appearance.drop(columns=['player_club_id', 'player_name'], inplace=True)
    fact_appearance = fact_appearance[['appearance_id', 'player_key', 'game_key', 'club_key', 
                                       'competition_key', 'date', 'yellow_cards', 'red_cards', 
                                       'goals', 'assists', 'minutes_played']]
    return fact_appearance

def transform_fact_transfer(df_fact_transfer, df_player, df_clubs):
    df_fact_transfer.fillna(0, inplace=True)
    df_fact_transfer = df_fact_transfer.merge(df_player[['player_id', 'player_key']], on="player_id", how="left").drop(columns=['player_id'])
    
    df_fact_transfer = df_fact_transfer.merge(df_clubs[['club_id', 'club_key']], 
                                               left_on="from_club_id", right_on="club_id", how="left") \
                                       .rename(columns={"club_key": "from_club_key"}).drop(columns=['from_club_id'])
    
    df_fact_transfer = df_fact_transfer.merge(df_clubs[['club_id', 'club_key']], 
                                               left_on="to_club_id", right_on="club_id", how="left") \
                                       .rename(columns={"club_key": "to_club_key"}).drop(columns=['to_club_id'])
    
    valid_club_keys = df_clubs['club_key']
    df_fact_transfer['from_club_key'] = df_fact_transfer['from_club_key'].apply(
        lambda x: x if x in valid_club_keys.values else -1)
    df_fact_transfer['to_club_key'] = df_fact_transfer['to_club_key'].apply(
        lambda x: x if x in valid_club_keys.values else -1)
    
    df_fact_transfer.drop(columns=['from_club_name', 'to_club_name', 'player_name', 'club_id_x', 'club_id_y'], inplace=True)
    return df_fact_transfer

def write_data(df_games, df_clubs, df_player, df_competitions, fact_apperance, df_fact_transfer):
    fact_apperance.to_csv('data/transformed/csv/fact_apperance.csv', index=False)
    df_player.to_csv('data/transformed/csv/Dim_Players.csv', index=False)
    df_clubs.to_csv('data/transformed/csv/Dim_clubs.csv', index=False)
    df_competitions.to_csv('data/transformed/csv/Dim_competitions.csv', index=False)
    df_games.to_csv('data/transformed/csv/Dim_games.csv', index=False)
    df_fact_transfer.to_csv('data/transformed/csv/fact_transfer.csv', index=False)