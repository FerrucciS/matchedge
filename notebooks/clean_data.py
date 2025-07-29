import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from rapidfuzz import process, fuzz


## --- Global Vars ---
tournament_surfaces = {
    'Brisbane International presented by Evie': 'Hard',
    'Bank of China Hong Kong Tennis Open': 'Hard',
    'Adelaide International': 'Hard',
    'ASB Classic': 'Hard',
    'Australian Open': 'Hard',
    'Open Occitanie': 'Hard (Indoor)',
    'Dallas Open': 'Hard (Indoor)',
    'ABN AMRO Open': 'Hard (Indoor)',
    'Open 13 Provence': 'Hard (Indoor)',
    'Delray Beach Open': 'Hard',
    'IEB+ Argentina Open': 'Clay',
    'Qatar ExxonMobil Open': 'Hard',
    'Rio Open presented by Claro': 'Clay',
    'Dubai Duty Free Tennis Championships': 'Hard',
    'Abierto Mexicano Telcel presentado por HSBC': 'Hard',
    'Movistar Chile Open': 'Clay',
    'BNP Paribas Open': 'Hard',
    'Miami Open presented by Itau': 'Hard',
    "Fayez Sarofim & Co. U.S. Men's Clay Court Championship": 'Clay',
    'Grand Prix Hassan II': 'Clay',
    'Tiriac Open presented by UniCredit Bank': 'Clay',
    'Rolex Monte-Carlo Masters': 'Clay',
    'Barcelona Open Banc Sabadell': 'Clay',
    'BMW Open by Bitpanda': 'Clay',
    'Mutua Madrid Open': 'Clay',
    "Internazionali BNL d'Italia": 'Clay',
    'Bitpanda Hamburg Open': 'Clay',
    'Gonet Geneva Open': 'Clay',
    'Roland Garros': 'Clay',
    'BOSS OPEN': 'Grass',
    'Libema Open': 'Grass',
    'HSBC Championships': 'Grass',
    'Terra Wortmann Open': 'Grass',
    'Wimbledon': 'Grass',
    'Hamburg': 'Clay',
    'Newport': 'Grass',
    'Bastad': 'Clay',
    'Gstaad': 'Clay',
    'Umag': 'Clay',
    'Atlanta': 'Hard',
    'Kitzbuhel': 'Clay',
    'Washington': 'Hard',
    'ATP Masters 1000 Canada': 'Hard',
    'ATP Masters 1000 Cincinnati': 'Hard',
    'Winston-Salem': 'Hard',
    'US Open': 'Hard',
    'Chengdu': 'Hard',
    'Hangzhou': 'Hard',
    'Tokyo': 'Hard',
    'Beijing': 'Hard',
    'ATP Masters 1000 Shanghai': 'Hard',
    'Almaty': 'Hard',
    'Antwerp': 'Hard',
    'Stockholm': 'Hard (Indoor)',
    'Vienna': 'Hard (Indoor)',
    'Basel': 'Hard (Indoor)',
    'ATP Masters 1000 Paris': 'Hard (Indoor)',
    'Belgrade': 'Clay',
    'Metz': 'Hard (Indoor)',
    'Nitto ATP Finals': 'Hard (Indoor)',
    'Next Gen ATP Finals': 'Hard (Indoor)'
}
# Load player rankings data and create mappings between player ID and Name
df_live_rankings = pd.read_csv("s3://matchedge-pipeline/data/clean/top_500_players.csv")
id_to_name = df_live_rankings.set_index("id")["Name"].to_dict()     # Mapping: player ID -> player Name
name_to_id = df_live_rankings.set_index("Name")["id"].to_dict()     # Mapping: player Name -> player ID

# Load tournament data and create mapping between tournament ID and end date
df_tournaments = pd.read_csv("s3://matchedge-pipeline/data/raw/All_Tournaments.csv")
id_to_end_date = df_tournaments.set_index("id")["end_date"].to_dict()  # Mapping: tournament ID -> end date


cols = ["p1_first_serve", "p1_1st_serve_points_won", "p1_2nd_serve_points_won",
        "p1_1st_serve_return_points_won", "p1_service_points_won", "p1_2nd_serve_return_points_won", "p1_return_points_won", "p1_total_points_won",
        "p2_first_serve", "p2_1st_serve_points_won", "p2_2nd_serve_points_won",
        "p2_1st_serve_return_points_won", "p2_service_points_won", "p2_2nd_serve_return_points_won", "p2_return_points_won", "p2_total_points_won"]
cols2 = ["p1_break_points_saved", "p1_break_points_converted", "p1_net_points_won",
        "p2_break_points_saved", "p2_break_points_converted", "p2_net_points_won"]
new_cols = ["p2_break_point_opportunities", "p1_break_point_opportunities", "p1_net_points_played",
            "p1_break_point_opportunities", "p2_break_point_opportunities", "p2_net_points_played"]




def changing_date(row, col="match_date"):
    """
    Adjusts match_date for rows where the date is a placeholder (e.g., 'final', 'round').
    Uses fuzzy matching to find the real end date from `id_to_end_date`.
    Ensures returned value is always a datetime object.
    """
    match_date = str(row[col])
    tourn_id = list(map(str, id_to_end_date.keys()))

    if isinstance(match_date, str) and ("round" in match_date.lower() or "final" in match_date.lower()):
        results_tourn_id = str(row["tournament_id"])
        matched_name, score, ind = process.extractOne(results_tourn_id, tourn_id)

        if score > 95:
            raw_date = id_to_end_date[int(matched_name)]

            # Try to parse raw_date regardless of format
            for fmt in ["%d-%m-%Y", "%Y-%m-%d", "%a, %d %B, %Y"]:
                try:
                    return datetime.strptime(raw_date, fmt)
                except ValueError:
                    continue
            try:
                return pd.to_datetime(raw_date)
            except:
                print(f"Failed to parse date from dict: {raw_date}")
                return pd.NaT
        else:
            print(f"Unmatched: {match_date} (Best guess: {matched_name}, Score: {score})")
            return pd.NaT
    else:
        try:
            return pd.to_datetime(match_date)
        except:
            return pd.NaT


def fix_match_date(df, col="match_date"):
    df[col] = df.apply(lambda row: changing_date(row, col=col), axis=1)
    df[col] = df[col].dt.strftime("%d-%m-%Y")  # Format everything
    return df


def get_scores(p1_score, p2_score):
    """
    Splits player 1 and player 2 score strings into lists per set,
    padding with NaN up to 5 sets, then combines into a single pandas Series.

    Args:
        p1_score (str): Player 1 scores as a space-separated string.
        p2_score (str): Player 2 scores as a space-separated string.

    Returns:
        pd.Series: Combined list of scores for p1 and p2 (length 10, with NaNs padded).
                   Format: [p1_set1, ..., p1_set5, p2_set1, ..., p2_set5]
    """
    p1_score_list = p1_score.split() if isinstance(p1_score, str) else []
    p2_score_list = p2_score.split() if isinstance(p2_score, str) else []
    
    while len(p1_score_list) < 5:
        p1_score_list.append(np.nan)
    while len(p2_score_list) < 5:
        p2_score_list.append(np.nan)

    return pd.Series(p1_score_list + p2_score_list)


def change_scores(df, col1="player_1_scores", col2="player_2_scores"):
    """
    Applies get_scores row-wise to split player score columns into individual set score columns.
    Adds columns p1_set1 to p1_set5 and p2_set1 to p2_set5 to the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing the player score columns.
        col1 (str): Column name for player 1 scores (default "player_1_scores").
        col2 (str): Column name for player 2 scores (default "player_2_scores").

    Returns:
        pd.DataFrame: DataFrame with added columns for individual set scores.
    """
    scores_df = df[[col1, col2]].apply(lambda row: get_scores(row[col1], row[col2]), axis=1)
    scores_df.columns = [f'p1_set{i+1}' for i in range(5)] + [f'p2_set{i+1}' for i in range(5)]
    df = pd.concat([df, scores_df], axis=1)
    return df


def best_of_col(df, col="tournament_id"):
    """
    Adds a 'best_of' column indicating if the match is best-of-3 or best-of-5 sets,
    based on tournament ID matching Grand Slam or Next Gen tournament IDs.

    Args:
        df (pd.DataFrame): DataFrame containing the tournament_id column.
        col (str): Name of the tournament_id column (default "tournament_id").

    Returns:
        pd.DataFrame: DataFrame with added 'best_of' column (3 or 5).
    """
    GS = ['580', '520', '540', '560', '7696']                        # Known best-of-5 tournament IDs
    df['best_of'] = df[col].apply(lambda x: 5 if str(x) in GS else 3)
    return df


def add_winner_id(df):
    """
    Adds a 'winner_id' column by fuzzy-matching winner names to player names
    from the player rankings dictionary, appending NaN for unmatched names.

    Args:
        df (pd.DataFrame): DataFrame containing a 'winner' column with player names.

    Returns:
        pd.DataFrame: DataFrame with added 'winner_id' column containing player IDs.
    """
    winner_id = []
    name_keys = list(name_to_id.keys())

    for name in df['winner']:
        matched_name, score, ind = process.extractOne(name, name_keys)
        if score > 50:
            winner_id.append(name_to_id[matched_name])
        else:
            print(f"Unmatched: {name} (Best guess: {matched_name}, Score: {score})")
            winner_id.append(np.nan)

    df["winner_id"] = winner_id
    return df


def clean_results_df(df):
    """
    Runs a full cleaning pipeline on match results DataFrame:
    - Fixes and formats match_date values
    - Parses player set scores into individual columns
    - Adds 'best_of' column for number of sets played
    - Adds 'winner_id' column by fuzzy matching winner names to IDs

    Args:
        df (pd.DataFrame): Raw match results DataFrame.

    Returns:
        pd.DataFrame: Cleaned match results DataFrame with added columns.
    """
    df = fix_match_date(df)           # Correct match_date values with fuzzy tournament end date lookup
    df = change_scores(df)            # Split player scores into p1_set1..5 and p2_set1..5 columns
    df = best_of_col(df)              # Add best_of column (3 or 5) based on tournament_id
    df = add_winner_id(df)            # Add winner_id column by fuzzy matching winner names
    return df





## For Tournamnets

def insert_surface(df_tournaments):
    
    surfaces = []
    for tournament in df_tournaments['name'].to_list():
        name, score, ind = process.extractOne(tournament, list(tournament_surfaces.keys()))
        if score > 75:
            surfaces.append(tournament_surfaces[name])
        else:
            print(f"Unmatched: {tournament} (Best guess: {name}, Score: {score})")
            surfaces.append(np.nan)
        
    df_tournaments.insert(loc=4, column='surface', value=surfaces)
    
    return df_tournaments    





## For Stats
############################################################################################################################################################
## Not sure if will use this
# Function to use this mapping
def abbreviate_name(row):
    player_id = row["id"]
    fallback_name = row["name"]
    
    # Try to get name from rankings; if not, use scraped one
    correct_name = id_to_name.get(player_id, fallback_name)
    
    if correct_name == None:
        return fallback_name
    else:
        return correct_name


# Apply to a DataFrame
def apply_abbreviation(df, col1="player_1", col1_id="p1_id", col2="player_2", col2_id="p2_id"):
    df[col1] = df[[col1, col1_id]].rename(columns={col1: "name", col1_id: "id"}).apply(abbreviate_name, axis=1)
    df[col2] = df[[col2, col2_id]].rename(columns={col2: "name", col2_id: "id"}).apply(abbreviate_name, axis=1)
    return df
############################################################################################################################################################

# def frac_to_decimal(df, cols=cols):
#     for col in cols:
        
#         df[col] = df[col].apply(
#         lambda x: round(eval(x.strip()), 2) if isinstance(x, str) and "/" in x else np.nan
#         )
#     return df



def frac_to_decimal(df, cols=cols):
    for col in cols:
        def safe_eval(x):
            if isinstance(x, str) and '/' in x:
                try:
                    num, denom = map(float, x.strip().split('/'))
                    if denom == 0:
                        return np.nan
                    return round(num / denom, 2)
                except:
                    return np.nan
            return np.nan

        df[col] = df[col].apply(safe_eval)
    return df


def split_ratio_stat(x):
    if isinstance(x, str) and '/' in x:
        try:
            num, denom = map(float, x.strip().split('/'))
            if denom == 0:
                return np.nan, 0
            return round(num / denom, 2), denom
        except:
            return np.nan, 0
    else:
        return np.nan, 0


def split_small_frac(df, cols=cols2, new_cols=new_cols):
    for col, new_col in zip(cols, new_cols):
        
        df[[col, new_col]] = df[col].apply(lambda x: pd.Series(split_ratio_stat(x)))
    
    return df


def clean_stats_df(df):
    df = frac_to_decimal(df)
    df = split_small_frac(df)
    return df