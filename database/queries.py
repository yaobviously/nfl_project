import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

from dotenv import load_dotenv
load_dotenv()


def get_game_stats(min_year=2000) -> int:
    """
    This function takes a minimum year and return a dataframe with team stats for each game.
        """

    # loading up the postgres credentials
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')
    port = os.getenv('DB_PORT')

    # # creating the URI for the database
    URI = f'postgresql://{user}:{password}@{host}:{port}/{database}'

    # # establishing a connection to the database
    engine = create_engine(URI)    
    
    if min_year < 2000:
        return "Please enter a year greater than 2000"
    

    game_stat_query = """
        select 
            game_id,
            home_team,
            away_team,
            sum(case when posteam = home_team and play_type_nfl = 'RUSH' then yards_gained else 0 end) as home_rush_yards,
            sum(case when posteam = home_team and play_type_nfl = 'RUSH' then epa else 0 end) as home_rush_epa,
            sum(case when posteam = home_team and play_type_nfl = 'PASS' then yards_gained else 0 end) as home_pass_yards,
            sum(case when posteam = home_team and play_type_nfl = 'PASS' then epa else 0 end) as home_pass_epa,
            sum(case when posteam = away_team and play_type_nfl = 'SACK' then 1 else 0 end) as home_team_sacks,
            sum(case when posteam = away_team and play_type_nfl = 'RUSH' then yards_gained else 0 end) as away_rush_yards,
            sum(case when posteam = away_team and play_type_nfl = 'RUSH' then epa else 0 end) as away_rush_epa,
            sum(case when posteam = away_team and play_type_nfl = 'PASS' then yards_gained else 0 end) as away_pass_yards,
            sum(case when posteam = away_team and play_type_nfl = 'PASS' then epa else 0 end) as away_pass_epa,
            sum(case when posteam = home_team and play_type_nfl = 'SACK' then 1 else 0 end) as away_team_sacks
        from pbp
        where season >=2016
        group by game_id, home_team, away_team
        order by game_id asc
    """

    game_stats = pd.read_sql(game_stat_query, con=engine)
    
    return game_stats