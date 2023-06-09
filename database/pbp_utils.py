
import pandas as pd
import numpy as np


def get_qb_pass(df: pd.DataFrame = None):
    """
    Get QB passing stats from raw play-by-play data

    Args:
        df (pd.DataFrame): Must be a pandas dataframe with play by play data

    Returns:
        df: Pandas dataframe with QB passing statistics
    """
    qb_df = (
        df.loc[((df['pass_attempt'] == 1) & (
            ~df['play_type'].isin(['two_point_att']) & (df['sack'] == 0)))]
        .groupby(['game_id', 'passer_player_name', 'posteam'], as_index=False)
        .agg({
            'passer_player_id': lambda x: x.unique()[0],
            'season_type': lambda x: x.unique()[0],
            'pass_attempt': 'sum',
            'complete_pass': 'sum',
            'yards_gained': 'sum',
            'air_yards': 'sum',
            'yards_after_catch': 'sum',
            'air_yards_to_sticks': 'sum',
            'interception': 'sum',
            'total_line': 'max',
            'home_team': lambda x: x.unique()[0],
            'away_team': lambda x: x.unique()[0],
            'success': 'sum',
            'temp': 'max',
            'wind': 'max',
            'epa': 'sum',
            'cpoe': 'sum'
        })
    )

    qb_df.rename(
        columns={
            'passer_player_name': 'player',
            'passer_player_id': 'player_id',
            'pass_attempt': 'att',
            'complete_pass': 'com',
            'yards_gained': 'pass_yards',
            'posteam': 'team',
            'air_yards_to_sticks': 'AYTS'},
        inplace=True)

    sacks = (
        df.groupby(['game_id', 'posteam', 'passer_player_name'],
                   as_index=False)['sack']
        .sum()
        .rename(columns={'passer_player_name': 'player',
                         'posteam': 'team'})
    )

    qb_df = qb_df.merge(sacks, how='left', on=['game_id', 'team', 'player'])
    qb_df['att'] = qb_df['att'].add(qb_df['sack'])
    qb_df['comp_perc'] = qb_df['com'].div(qb_df['att']).round(3) * 100

    qb_td = (
        df.loc[((df['pass_attempt'] == 1) & (~df['play_type'].isin(
            ['two_point_att']) & (df['sack'] == 0) & (df['interception'] == 0)))]
        .groupby(['game_id', 'passer_player_id'], as_index=False)['touchdown']
        .sum()
        .rename(columns={'passer_player_id': 'player_id'})
    )

    qb_df = qb_df.merge(qb_td, how='left', on=['game_id', 'player_id'])

    ay_complete = (
        df[df['complete_pass'] == 1]
        .groupby(['game_id', 'posteam', 'passer_player_name'], as_index=False)['air_yards']
        .sum()
        .rename(columns={
            'air_yards': 'ay_completions',
            'passer_player_name': 'player',
            'posteam': 'team'})
    )

    ay_incomplete = (
        df[((df['complete_pass'] == 0) & (df['play_type'] != 'two_point_att'))]
        .groupby(['game_id', 'posteam', 'passer_player_name', 'complete_pass'], as_index=False)['air_yards']
        .sum()
        .rename(columns={
            'passer_player_name': 'player',
            'air_yards': 'ay_incompletions',
            'posteam': 'team'
        })
    )

    qb_df = qb_df.merge(ay_complete, how='left', on=[
                        'game_id', 'team', 'player'])
    qb_df['avg_ay_comp'] = qb_df['ay_completions'].div(qb_df['com']).round(1)
    qb_df = qb_df.merge(ay_incomplete, how='left', on=[
                        'game_id', 'team', 'player'])
    qb_df['avg_ay_incomp'] = qb_df['ay_incompletions'].div(
        qb_df['att'] - qb_df['com']).round(1)
    qb_df['att'] = qb_df['att'].sub(qb_df['sack'])
    qb_df['cpoe'] = qb_df['cpoe'].div(100)
    qb_df['epa_per_dropback'] = qb_df['epa'].div(
        qb_df['att'] + qb_df['sack']).round(3)

    # fixing qb names

    qb_df['player'].replace('Ty.Taylor', 'T.Taylor', inplace=True)
    qb_df['player'].replace('Aa.Rodgers', 'A.Rodgers', inplace=True)
    qb_df['player'].replace('Alex Smith', 'A.Smith', inplace=True)
    qb_df['player'].replace('Jos.Smith', 'J.Smith', inplace=True)

    qb_dict = {x: x.split('.') for x in qb_df['player'].unique()}

    new_values = []

    for values in qb_dict.values():
        new_last = values[1].lstrip()
        new_values.append('.'.join([values[0], new_last]))

    new_qb_dict = {k: v for k, v in zip(qb_dict.keys(), new_values)}
    qb_df['player'] = qb_df['player'].map(new_qb_dict).fillna(qb_df['player'])

    return qb_df


def get_rushing(df: pd.DataFrame = None):
    """
    A function that returns a dataframe of rushing stats

    Args:
        df (pd.DataFrame): Must be a dataframe of play-by-play data

    Returns:
        df (pd.DataFrame): A dataframe of each player's rushing stats
    """

    run_df = (
        df[df['rush_attempt'] == 1]
        .groupby(['game_id', 'rusher_player_id', 'posteam'])
        .agg({
            'rush_attempt': 'sum',
            'rusher_player_name': lambda x: x.unique()[0],
            'yards_gained': 'sum',
            'success': 'sum',
            'touchdown': 'sum',
            'total_line': 'mean',
            'epa': 'sum',
            'fumble_lost': 'sum',
            'home_team': lambda x: x.unique()[0],
            'away_team': lambda x: x.unique()[0]
        })
        .sort_index()
        .reset_index()
    )

    run_df.rename(
        columns={
            'rusher_player_id': 'player_id',
            'rusher_player_name': 'player',
            'rush_attempt': 'rush_att',
            'posteam': 'team',
            'fumble_lost': 'fumbles',
            'touchdown': 'rush_td',
            'yards_gained': 'rush_yds',
            'epa': 'rush_epa'}, inplace=True)

    run_df['rush_yds_per_att'] = run_df['rush_yds'].div(
        run_df['rush_att']).round(1)
    run_df['success_perc'] = run_df['success'].div(run_df['rush_att']).round(3)
    run_df['team_rush_atts'] = run_df.groupby(['game_id', 'team'])[
        'rush_att'].transform('sum')
    run_df['rush_att_share'] = run_df['rush_att'].div(
        run_df['team_rush_atts']).round(2)

    return run_df


def get_opp_pass(df: pd.DataFrame = None):
    """
    A function that returns a dataframe of opponent passing stats

    Args:
        df (pd.DataFrame,): A dataframe of play-by-play data

    Returns:
        df: A dataframe of each team's opponent's passing stats
    """

    opp_pass = (
        df[df['pass_attempt'] == 1]
        .groupby(['game_id', 'defteam'], as_index=False)['yards_gained']
        .sum()
        .rename(columns={'defteam': 'team',
                         'yards_gained': 'opp_pass_yds'})
    )

    return opp_pass


def get_opp_rush(df: pd.DataFrame = None):
    """
    A function that returns a dataframe of opponent rushing stats

    Args:
        df (pd.DataFrame): A dataframe of play-by-play data

    Returns:
        df: A dataframe of each team's opponent's rushing stats
    """

    opp_rush = (
        df[df['rush_attempt'] == 1]
        .groupby(['game_id', 'defteam'], as_index=False)['yards_gained']
        .sum()
        .rename(columns={'defteam': 'team',
                         'yards_gained': 'opp_rush_yds'})
    )

    return opp_rush


def get_def_stats(df: pd.DataFrame = None):
    """
    A function that returns a dataframe of defensive stats

    Args:
        df: A dataframe of play-by-play data

    Returns:
        df: A dataframe of each team's defensive stats
    """

    def_cols = ['interception', 'season', 'return_touchdown', 'fumble',
                'sack', 'epa']

    def_stats = (
        df[~df['desc'].str.contains('Aborted')].copy()
        .groupby(['game_id', 'defteam'], as_index=False)
        .agg({
            'interception': 'sum',
            'season': lambda x: x.unique()[0],
            'return_touchdown': 'sum',
            'fumble_lost': 'sum',
            'sack': 'sum',
            'safety': 'sum',
            'blocked_player_name': 'sum'
        })
        .rename(columns={
            'defteam': 'team',
            'interception': 'def_int',
            'return_touchdown': 'def_td',
            'sack': 'def_sack',
            'fumble': 'def_fumble',
            'blocked_player_name': 'kick_blocked'})
    )

    return def_stats


def get_kicker_stats(df: pd.DataFrame = None):
    """
    A function that returns a dataframe of kicker stats

    Args:
        df (pd.DataFrame): A dataframe of play-by-play data

    Returns:
        df (pd.DataFrame): A dataframe of each kicker's stats
    """

    kicks = ['field_goal', 'extra_point']

    df = df[df['desc'].str.contains('GOOD')].copy()

    kick_df = (

        df[df['play_type'].isin(kicks)]
        .groupby(['game_id', 'kicker_player_id'], as_index=False)
        .agg({
            'kicker_player_name': lambda x: x.unique()[0],
            'posteam': lambda x: x.unique()[0],
            'field_goal_result': 'sum',
            'extra_point_result': 'sum',
            'fg_0_39': 'sum',
            'fg_40_49': 'sum',
            'fg_50_on': 'sum'

        })
        .rename(columns={
            'kicker_player_name': 'player',
            'field_goal_result': 'fgs',
            'extra_point_result': 'pats',
            'posteam': 'team'
        })
    )

    return kick_df


def get_team_adjusted_epa(df: pd.DataFrame = None):
    """
    Processes play-by-play data to calculate team adjusted Defensive EPA.

    Args:
        df (pandas dataframe): Must be a play-by-play dataframe with game_id, season, posteam, \
            defteam, epa, play_type columns.

    Raises:
        ValueError: If dataframe does not contain required columns.

    Returns:
        pd.DataFrame: A dataframe with team adjusted Defensive EPA.
    """

    def_epa_cols = ['game_id', 'season', 'posteam', 'defteam', 'epa',
                    'play_type']

    if not all([x in df.columns for x in def_epa_cols]):

        raise ValueError('Dataframe must contain columns: game_id, season, posteam, defteam, \
                         epa, play_type')

    season_epa_def = (
        df[df['play_type'].isin(['pass', 'run'])][def_epa_cols]
    )

    season_epa_def['season_epa_play'] = season_epa_def.groupby(
        ['season', 'play_type'])['epa'].transform(lambda x: x.shift().expanding().mean())
    season_epa_def['season_epa_def'] = season_epa_def.groupby(['season', 'defteam', 'play_type'])[
        'epa'].transform(lambda x: x.shift().expanding().mean())
    season_epa_def['season_epa_off'] = season_epa_def.groupby(['season', 'posteam', 'play_type'])[
        'epa'].transform(lambda x: x.shift().expanding().mean())

    season_epa_def['team_adjusted_off_epa'] = season_epa_def['season_epa_off'].sub(
        season_epa_def['season_epa_play']).round(3)
    season_epa_def['team_adjusted_def_epa'] = season_epa_def['season_epa_def'].sub(
        season_epa_def['season_epa_play']).round(3)

    return season_epa_def


def get_team_pass_yds(df: pd.DataFrame = None):
    """
    A function that returns a dataframe of each team's passing yards
    for each game

    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of each team's passing yards
    """

    team_pass_yds = (
        df.loc[df['pass_attempt'] == 1]
        .groupby(['game_id', 'posteam'], as_index=False)['yards_gained']
        .sum()
        .rename(columns={'posteam': 'team',
                         'yards_gained': 'pass_yards'})
    )

    return team_pass_yds


def get_team_rush_yds(df: pd.DataFrame = None):
    """
    A function that returns a dataframe of each team's rushing yards
    for each game

    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of each team's rushing yards
    """

    team_rush_yds = (
        df.loc[df['rush_attempt'] == 1]
        .groupby(['game_id', 'posteam'], as_index=False)['yards_gained']
        .sum()
        .rename(columns={'posteam': 'team',
                         'yards_gained': 'rush_yards'})
    )

    return team_rush_yds


def get_team_scores(df: pd.DataFrame = None):
    """
    A function that returns each team's offensive scores (TD, FG, etc.)

    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of each team's offensive scores
    """

    condition = df['td_team'] == df['posteam']

    team_scores = (
        df[condition]
        .groupby(['game_id', 'posteam'], as_index=False)[['touchdown', 'field_goal_result', 'two_point_conv_result']]
        .sum()
        .rename(columns={
            'touchdown': 'off_td',
            'posteam': 'team',
            'two_point_conv_result': 'two_pts_conv',
            'field_goal_result': 'fgs'
        })
        .fillna(0)
    )

    return team_scores


def get_game_results(df: pd.DataFrame = None, team_rush_yds: pd.DataFrame = None,
                     team_pass_yds: pd.DataFrame = None, team_scores: pd.DataFrame = None,
                     opp_rush: pd.DataFrame = None, opp_pass: pd.DataFrame = None,
                     ls: pd.DataFrame = None):
    """
    A dataframe returning each team's aggregate stats for each game

    Args:
        df (pd.DataFrame, optional): _description_. Defaults to None.
        team_rush_yds (pd.DataFrame, optional): _description_. Defaults to None.
        team_pass_yds (pd.DataFrame, optional): _description_. Defaults to None.
        team_scores (pd.DataFrame, optional): _description_. Defaults to None.
        opp_rush (pd.DataFrame, optional): _description_. Defaults to None.
        opp_pass (pd.DataFrame, optional): _description_. Defaults to None.
        ls (pd.DataFrame, optional): _description_. Defaults to None.

    Returns:
        df (pd.DataFrame): A dataframe of each team's aggregate stats for each game
    """

    game_results_cols = ['year', 'week', 'season_type', 'home_team', 'away_team',
                         'home_score', 'away_score', 'spread_line', 'total_line']
    game_results = (
        df
        .groupby(['game_id', 'posteam'], as_index=False)[game_results_cols]
        .max()
    )

    game_results['home'] = (game_results['posteam'] ==
                            game_results['home_team']).astype(int)
    game_results['spread_line'] = [x if y == 1 else (
        x * -1) for x, y in zip(game_results['spread_line'], game_results['home'])]
    game_results['actual_spread'] = (
        game_results['home_score'] - game_results['away_score']) * -1
    game_results['points'] = np.where(
        game_results['home'] == 1, game_results['home_score'], game_results['away_score'])
    game_results['opp_points'] = np.where(
        game_results['home'] == 0, game_results['home_score'], game_results['away_score'])

    game_results.rename(columns={'posteam': 'team'}, inplace=True)
    # game_results.drop(columns={'home_score', 'away_score', 'home_team', 'away_team'}, inplace=True)

    game_results = game_results.merge(
        team_rush_yds, how='left', on=['game_id', 'team'])
    game_results = game_results.merge(
        team_pass_yds, how='left', on=['game_id', 'team'])
    game_results = game_results.merge(
        opp_rush, how='left', on=['game_id', 'team'])
    game_results = game_results.merge(
        opp_pass, how='left', on=['game_id', 'team'])
    game_results = game_results.merge(
        team_scores, how='left', on=['game_id', 'team'])
    game_results['actual_total'] = game_results['points'].add(
        game_results['opp_points'])
    game_results['over'] = (game_results['actual_total']
                            > game_results['total_line']).astype(int)

    game_results = game_results.merge(ls, how='left', on=['game_id'])
    game_results['rest'] = np.where(
        game_results['home'] == 1, game_results['home_rest'], game_results['away_rest'])
    game_results['opp_rest'] = np.where(
        game_results['home'] == 1, game_results['away_rest'], game_results['home_rest'])
    game_results['coach'] = np.where(
        game_results['home'] == 1, game_results['home_coach'], game_results['away_coach'])
    game_results['opp_coach'] = np.where(
        game_results['home'] == 1, game_results['away_coach'], game_results['home_coach'])
    # game_results.drop(columns=['home_rest', 'away_rest', 'home_coach', 'away_coach'], inplace=True)

    game_results['season'] = [int(x.split('_')[0])
                              for x in game_results.game_id]
    game_results['season'] = game_results['season'].astype('category')
    game_results = game_results[game_results['team'] != ''].copy()

    return game_results


def get_drive_stats(df: pd.DataFrame = None):
    """
    A function that returns the results of each drive for each team
    for each game

    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of each team's drive results for each game
    """

    drive_cols = ['time_between', 'score_differential', 'score_differential_post', 'rush_attempt',
                  'pass_attempt', 'yards_gained', 'interception', 'fumble', 'sack', 'epa',
                  'success']

    drive_details = (
        df
        .groupby(['game_id', 'posteam', 'drive'], as_index=False)[drive_cols]
        .agg({
            'time_between': 'sum',
            'score_differential': 'first',
            'score_differential_post': 'last',
            'rush_attempt': 'sum',
            'pass_attempt': 'sum',
            'yards_gained': 'sum',
            'interception': 'sum',
            'fumble': 'sum',
            'sack': 'sum',
            'success': 'sum',
            'epa': 'sum'
        }
        )
        .rename(
            columns={'time_between': 'poss_time',
                     'score_differential': 'score_diff_start',
                     'score_differential_post': 'score_diff_end',
                     'interception': 'int',
                     'fumble': 'fumble',
                     'posteam': 'team'}
        )
    )

    drive_details['score_gain'] = drive_details['score_diff_end'].sub(
        drive_details['score_diff_start'])
    drive_details['td'] = (drive_details['score_gain'] >= 6).astype(int)
    drive_details['fg'] = (drive_details['score_gain'] == 3).astype(int)
    drive_details['total_plays'] = drive_details['rush_attempt'].add(
        drive_details['pass_attempt']).astype(int)
    drive_details.drop(columns=['score_gain'], inplace=True)

    return drive_details


def get_receiving(df: pd.DataFrame = None):
    """
    A function returning each player's receiving stats for each game.

    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of each player's receiving stats for each game
    """

    rec_df = (
        df.loc[df['qb_dropback'] == 1]
        .groupby(['game_id', 'receiver_player_id', 'posteam'])
        .agg({
            'receiver_player_name': lambda x: x.unique()[0],
            'pass_attempt': 'count',
            'complete_pass': 'sum',
            'air_yards': 'sum',
            'yards_after_catch': 'sum',
            'yards_gained': 'sum',
            'touchdown': 'sum'
        })
        .sort_index()
        .reset_index()
    )

    rec_df.rename(
        columns={
            'receiver_player_id': 'player_id',
            'receiver_player_name': 'player',
            'pass_attempt': 'targets',
            'complete_pass': 'rec',
            'posteam': 'team',
            'yards_after_catch': 'yac',
            'yards_gained': 'rec_yards',
            'touchdown': 'td'},
        inplace=True)

    rec_df['team_targets'] = rec_df.groupby(['game_id', 'team'])[
        'targets'].transform('sum')
    rec_df['target_share'] = rec_df['targets'].div(
        rec_df['team_targets']).round(3) * 100
    rec_df['aDOT'] = rec_df['air_yards'].div(rec_df['targets']).fillna(np.nan)
    rec_df['yrds_per_rec'] = rec_df['rec_yards'].div(rec_df['rec']).round(1)

    rec_df['team_air_yards'] = rec_df.groupby(['game_id', 'team'])[
        'air_yards'].transform('sum')
    rec_df['air_yards_share'] = rec_df['air_yards'].div(
        rec_df['team_air_yards']).round(3) * 100
    rec_df['yac_per_rec'] = rec_df['yac'].div(rec_df['rec']).round(1)

    rec_df.drop(columns='team_targets', inplace=True)

    return rec_df


def get_opp_pass(df: pd.DataFrame = None):
    """
    A function returning each opponent's passing stats for each game.

    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of each opponent's passing stats for each game
    """

    opp_pass = (
      df[df['pass_attempt'] == 1]
      .groupby(['game_id', 'defteam'], as_index=False)['yards_gained']
      .sum()
      .rename(columns={'defteam' : 'team',
                      'yards_gained' : 'opp_pass_yds'})
  )
    
    return opp_pass   



def get_def_stats(df: pd.DataFrame = None):
    """
    A function returning each team's defensive stats for each game.
    
    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of each team's defensive stats for each game
    """    

    def_cols = ['interception', 'season', 'return_touchdown', 'fumble', 
                'sack', 'epa']

    def_stats = (
        df[~df['desc'].str.contains('Aborted')].copy()
        .groupby(['game_id', 'defteam'], as_index=False)
        .agg({
            'interception' : 'sum',
            'season' : lambda x: x.unique()[0],
            'return_touchdown' : 'sum',
            'fumble_lost' : 'sum',
            'sack' : 'sum',
            'safety' : 'sum',
            'blocked_player_name' : 'sum'
        })
        .rename(columns={
            'defteam' : 'team',
            'interception' : 'def_int',
            'return_touchdown' : 'def_td',
            'sack' : 'def_sack',
            'fumble' : 'def_fumble',
            'blocked_player_name' : 'kick_blocked'
        })
    )
  
    return def_stats

def process_pbp(df: pd.DataFrame = None):
    """
    A function that processes pbp data to add useful features.
    
    Args:
        df (pd.DataFrame, optional): A dataframe of play-by-play data.

    Returns:
        df (pd.DataFrame): A dataframe of play-by-play data with added features
    """    
    df['year'] = pd.to_datetime(df['game_date']).dt.year
    df['two_point_conv_result'] = (
        df['two_point_conv_result']
        .map(
            {'success' : 1,
            'failure' : 0,
            }
        )
        .fillna('None')
    )

    df['game_date'] = pd.to_datetime(df['game_date'])
    df['spread_line'] = df['spread_line'] * -1
    df['field_goal_result'] = np.where(df['field_goal_result'] == 'made', 1, 0)
    df['time_between'] = df.groupby(['game_id'])['game_seconds_remaining'].transform(lambda x: x.sub(x.shift(-1)).fillna(0))
    df['play_type'] = np.where(df['two_point_attempt'] > 0.5, 'two_point_att', df['play_type'])
    df['air_yards_to_sticks'] = df['air_yards'].sub(df['ydstogo'])
    df['season'] = [int(x.split('_')[0]) for x in df.game_id]
    df['blocked_player_name'] = np.where(df['blocked_player_name'].notnull(), 1, 0)
    df['fg_0_39'] = np.where(((df['play_type'] == 'field_goal') & (df['kick_distance'].between(0,39))), 1, 0)
    df['fg_40_49'] = np.where(((df['play_type'] == 'field_goal') & (df['kick_distance'].between(40,49))), 1, 0)
    df['fg_50_on'] = np.where(((df['play_type'] == 'field_goal') & (df['kick_distance'].between(50,100))), 1, 0)
    df['extra_point_result'] = np.where(df['extra_point_result'] == 'good', 1, 0)
    
    return df