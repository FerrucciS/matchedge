import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from rapidfuzz import process, fuzz
import os
import re


# -------------------------------- Global Vars --------------------------------

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

tournament_order = [
    'Next Gen ATP Finals',
    'ATP 250',
    'ATP 500',
    'ATP 1000',
    'Nitto ATP Finals',
    'Grand Slam'
]

round_order = [
    'Round Robin -',
    '1st Round Qualifying', 
    '2nd Round Qualifying',
    '3rd Round Qualifying',
    'Round of 128',
    'Round of 64',
    'Round of 32',
    'Round of 16',
    'Quarter-Finals',
    'Semi-Finals',
    'Final']


name_corrections = {                                                                                        # Define all mismatched names and corrections
    'F. Agustin Gomez': ('F. Gomez', 'gj16'),
    'J. Manuel Cerundolo': ('J. Cerundolo', 'c0c8'),
    'T. Martin Etcheverry': ('T. Etcheverry', 'ea24'),
    'T. Agustin Tirante': ('T. Tirante', 't0a1'),
    'J. Bautista Torres': ('J. Torres', 't0dm'),
    'J. Pablo Ficovich': ('J. Ficovich', 'fa43'),
    'R. Andres Burruchaga': ('R. Burruchaga', 'b0fv'),
    'J. Pablo Varillas': ('J. Varillas', 'v836'),
    'D. Elahi Galan': ('D. Galan', 'ge33'),
    'F. Cristian Jianu': ('F. Jianu', 'j09x'),
    'Y. Hsiou Hsu': ('Y. Hsu', 'h09f'),
    'G. Arnaud Bailly': ('G. Bailly', 'b0qc')
    # Add more entries as needed...
}

# df_live_rankings = pd.read_csv("s3://matchedge-pipeline/data/clean/top_500_players.csv")                    # Load player rankings data

df_top_players = pd.read_csv("s3://matchedge-pipeline/data/clean/top_players.csv")                    # Load player archive

# df_player_archive = pd.read_csv("s3://matchedge-pipeline/data/clean/player_archive.csv")                    # Load player archive
name_to_id = df_top_players.set_index("name")["id"].to_dict()                              # Mapping: player Name -> player ID
id_to_name = df_top_players.set_index("id")["name"].to_dict()                              # Mapping: player ID -> player Name

df_tournaments = pd.read_csv("s3://matchedge-pipeline/data/clean/all_tournaments.csv")                      # Load tournament data
id_to_end_date = df_tournaments.set_index("id")["end_date"].to_dict()                                       # Mapping: tournament ID -> end date

cols = ["p1_first_serve", "p1_1st_serve_points_won", "p1_2nd_serve_points_won",                             # Defining columns for stats
        "p1_1st_serve_return_points_won", "p1_service_points_won", "p1_2nd_serve_return_points_won", "p1_return_points_won", "p1_total_points_won",
        "p2_first_serve", "p2_1st_serve_points_won", "p2_2nd_serve_points_won",
        "p2_1st_serve_return_points_won", "p2_service_points_won", "p2_2nd_serve_return_points_won", "p2_return_points_won", "p2_total_points_won"]
cols2 = ["p1_break_points_saved", "p1_break_points_converted", "p1_net_points_won",
        "p2_break_points_saved", "p2_break_points_converted", "p2_net_points_won"]
new_cols = ["p2_break_point_opportunities", "p1_break_point_opportunities", "p1_net_points_played",
            "p1_break_point_opportunities", "p2_break_point_opportunities", "p2_net_points_played"]

df_results = pd.read_csv("s3://matchedge-pipeline/data/clean/all_results.csv", sep=',')                     # Load results
df_stats = pd.read_csv("s3://matchedge-pipeline/data/clean/all_stats.csv", sep=',')                         # Load stats


# -------------------------------- For Rankings DF --------------------------------

def lower_column_names(df):
    """
    Converts all column names in the DataFrame to lowercase and strips any leading/trailing whitespace.

    Parameters:
    df (pd.DataFrame): Input DataFrame with column names to normalize.

    Returns:
    pd.DataFrame: DataFrame with normalized (lowercase, stripped) column names.
    """
    df.columns = df.columns.map(lambda x: x.lower().strip())
    return df


def format_ranking_cols(df):
    """
    Formats specific columns in the rankings DataFrame to appropriate data types.

    - Converts 'name' and 'id' columns to string type.
    - You may uncomment 'rank' conversion to integer if rank is always numeric.

    Parameters:
    df (pd.DataFrame): Rankings DataFrame.

    Returns:
    pd.DataFrame: DataFrame with formatted columns.
    """
    df['name'] = df['name'].astype('string')
    # df['rank'] = df['rank'].astype('int')                                                                 # Uncomment if 'rank' column is clean and numeric
    df['id'] = df['id'].astype('string')
    return df


def clean_rankings(df):
    """
    Cleans a rankings DataFrame by:
    1. Normalizing column names (lowercase, stripped).
    2. Formatting key columns to proper data types.

    Parameters:
    df (pd.DataFrame): Raw rankings DataFrame.

    Returns:
    pd.DataFrame: Cleaned rankings DataFrame.
    """
    df = lower_column_names(df)
    df = format_ranking_cols(df)
    return df


# -------------------------------- For Results --------------------------------

def changing_date(row, col="match_date"):
    """
    Cleans and standardizes the match date for a given row in a DataFrame.

    This function is designed to handle cases where the 'match_date' field is not a real date,
    but instead contains placeholder text (e.g., 'final', 'round'). In such cases, it uses
    fuzzy matching to find the corresponding tournament ID in a predefined dictionary
    (`id_to_end_date`) and extracts the correct date.

    Parameters:
    row (pd.Series): A row from a DataFrame.
    col (str): Name of the column containing the match date. Defaults to 'match_date'.

    Returns:
    datetime.datetime or pd.NaT: A datetime object if successful, otherwise pandas NaT.
    """
    match_date = str(row[col])                                                                              # Ensure match_date is a string
    tourn_id = list(map(str, id_to_end_date.keys()))                                                        # Convert keys to strings for matching

    if isinstance(match_date, str) and ("round" in match_date.lower() or "final" in match_date.lower()):    # Handle cases where match_date contains text like "round", "final", etc.
        results_tourn_id = str(row["tournament_id"])
        
        matched_name, score, ind = process.extractOne(results_tourn_id, tourn_id)                           # Use fuzzy matching to find the best match for the tournament ID

        if score > 95:
            raw_date = id_to_end_date[int(matched_name)]                                                    # High-confidence match: attempt to parse the corresponding end date

            for fmt in ["%d-%m-%Y", "%Y-%m-%d", "%a, %d %B, %Y"]:                                           # Try different known date formats
                try:
                    return datetime.strptime(raw_date, fmt)
                except ValueError:
                    continue
            try:                                                                                            # Fallback to pandas date parser
                return pd.to_datetime(raw_date)
            except:
                print(f"Failed to parse date from dict: {raw_date}")
                return pd.NaT
        else:                                                                                               # Fuzzy match score too low — return NaT and print diagnostic
            print(f"Unmatched: {match_date} (Best guess: {matched_name}, Score: {score})")
            return pd.NaT
    else:           
        try:                                                                                                # Regular case — try to convert to datetime
            return pd.to_datetime(match_date)
        except:
            return pd.NaT


def fix_match_date(df, col="match_date"):
    """
    Applies a cleaning and formatting function to standardize match date values in a DataFrame.

    This function uses `changing_date()` to fix non-date values (e.g., 'Final', 'Round') 
    by replacing them with actual dates. Afterward, it formats all dates in 'DD-MM-YYYY' string format.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the column to clean.
    col (str): The name of the column containing the match date. Defaults to 'match_date'.

    Returns:
    pd.DataFrame: The updated DataFrame with cleaned and formatted date strings.
    """
    df[col] = df.apply(lambda row: changing_date(row, col=col), axis=1)                                     # Apply the custom date-fixing function row-wise
    df[col] = df[col].dt.strftime("%d-%m-%Y")                                                               # Format all datetime objects into 'DD-MM-YYYY' string format

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
    p1_score_list = p1_score.split() if isinstance(p1_score, str) else []                                   # Converts "6 4 7" --> [6, 4, 7] 
    p2_score_list = p2_score.split() if isinstance(p2_score, str) else []
    
    while len(p1_score_list) < 5:                                                                           # Appends Nan to remaining sets up to 5th set
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
    scores_df = df[[col1, col2]].apply(lambda row: get_scores(row[col1], row[col2]), axis=1)                # Applies get_scores
    scores_df.columns = [f'p1_set{i+1}' for i in range(5)] + [f'p2_set{i+1}' for i in range(5)]             # Names new columns by set        
    df = pd.concat([df, scores_df], axis=1)                                                                 # Concats them to df
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
    GS = ['580', '520', '540', '560', '7696']                                                               # Known best-of-5 tournament IDs
    df['best_of'] = df[col].apply(lambda x: 5 if str(x) in GS else 3)
    return df


def add_winner_id(df):
    """
    Matches winner names to IDs using last name + first initial match,
    with fuzzy fallback for edge cases.

    Parameters:
        df (pd.DataFrame): DataFrame with a 'winner' column containing full names.
    
    Returns:
        pd.DataFrame: DataFrame with a new 'winner_id' column.
    """
    winner_id = []
    name_keys = list(name_to_id.keys())

    for winner_full in df['winner']:                                                                        # Split winner's name into words
        normalized_winner = winner_full.replace('-', ' ').replace('.', '').lower()
        winner_parts = normalized_winner.split()
        winner_last = winner_parts[-1]
        winner_first_initial = winner_parts[0][0]

        match = None                                                                                        # Try direct match based on last name + first initial
        for key in name_keys:
            key_parts = key.replace('.', '').split()
            key_last = key_parts[-1]
            key_first_initial = key_parts[0][0]

            if key_last.lower() == winner_last.lower() and key_first_initial.lower() == winner_first_initial.lower():
                match = key
                break
        
        if not match:                                                                                       # If no direct match, fallback to fuzzy matching
            matched_name, score, _ = process.extractOne(
                winner_full, name_keys, scorer=fuzz.token_sort_ratio
            )
            if score > 70:                                                                                  # slightly stricter threshold
                match = matched_name

        if match:
            winner_id.append(name_to_id[match])
        else:
            print(f"Unmatched: {winner_full}")
            winner_id.append(np.nan)

    df['winner_id'] = winner_id
    return df

    
def clean_incorrect_results(df):
    """
    Cleans up incorrect or missing match results in a tennis match DataFrame.

    This function standardizes match results by applying the following logic:
    1. If either player is listed as "Bye", the match result is set to "Bye".
    2. If the match has no duration and no score data for player 1, it's considered a "Walkover".
    3. If player 1 has no scores and neither player is "Bye", it's also marked as a "Walkover".

    Parameters:
        df (pd.DataFrame): The DataFrame containing tennis match data. 
                           Must contain the columns: 'player_1', 'player_2', 'result', 
                           'duration', and 'player_1_scores'.

    Returns:
        pd.DataFrame: The updated DataFrame with corrected 'result' values.
    """
    df.loc[                                                                                                 # Case 1: If either player is 'Bye', mark the result as 'Bye'
        (df['player_1'] == 'Bye') | (df['player_2'] == 'Bye'),
        'result'
    ] = 'Bye'
    
    df.loc[                                                                                                 # Case 2: If match has no duration and no score, it's a 'Walkover'
        (df['duration'].isna()) & (df['player_1_scores'].isna()), 
        'result'
    ] = 'Walkover'
    
    mask = (                                                                                                # Case 3: If only player_1_scores is missing and no one is 'Bye', also mark as 'Walkover'
        df['player_1_scores'].isna() &
        ~((df['player_1'] == 'Bye') | (df['player_2'] == 'Bye'))
    )
    df.loc[mask, 'result'] = 'Walkover'
    
    return df


def normalize_duration(dur):
    """
    Normalises match duration strings to the format 'hh:mm:ss':
    
    Handles durations in the following ways:
    - If the input is NaN, returns np.nan.
    - If the duration is in 'hh:mm' format, appends ':00' to standardize it to 'hh:mm:ss'.
    - If already in 'hh:mm:ss' format, returns it unchanged.
    - For any other unexpected format, returns np.nan.

    Parameters:
        dur (str or float): The match duration as a string or NaN

    Returns:
        str or np.nan: Normalised duration in 'hh:mm:ss' format or NaN.
    """
    if pd.isna(dur):
        return np.nan
    dur = str(dur).strip()                                                                                  # Ensure it's a clean string
    parts = dur.split(':')
    if len(parts) == 2:                                                                                     # hh:mm format, add seconds as 00
        return dur + ':00'
    elif len(parts) == 3:                                                                                   # hh:mm:ss format, keep as is
        return dur
    else:
        return np.nan                                                                                       # Unknown format


def format_results_cols(df):
    """
    Cleans and formats the columns of a tennis match results DataFrame to ensure consistency
    and proper data types for analysis.

    Parameters 
        df (pd.DataFrame):
        The DataFrame containing match results with columns like player names, IDs,
        scores, durations, match rounds, dates, etc.

    Returns (pd.DataFrame):
        The input DataFrame with formatted columns:
        - Player names as strings
        - Player IDs as lowercase strings
        - Duration normalized and converted to timedelta
        - Match rounds standardized and set as an ordered categorical
        - Scores extracted as integers (removing tie-break notation)
        - Dates converted to datetime objects
        - Year extracted from match dates
    """
    df['player_1'] = df['player_1'].astype('string')                                                        # Ensure player names are strings
    df['player_2'] = df['player_2'].astype('string')
    
    df = df.rename(columns={"player_1_id": "p1_id", "player_2_id": "p2_id"})                                # Rename player ID columns
    
    df['p1_id'] = df['p1_id'].astype('string').str.lower()                                                  # Convert to lowercase string
    df['p2_id'] = df['p2_id'].astype('string').str.lower()
    df['duration'] = df['duration'].astype(str).str.strip()                                                 # Strip whitespace
    df['duration'] = df['duration'].apply(normalize_duration)                                               # Normalize format (hh:mm:ss)
    df['duration'] = pd.to_timedelta(df['duration'], errors='coerce')                                       # Convert to timedelta
    
    df['match_round'] = df['match_round'].replace({                                                         # Standardize round names
        'Quarterfinals': 'Quarter-Finals',
        'Semifinals': 'Semi-Finals',
        'Finals': 'Final'})
    df['match_round'] = pd.Categorical(                                                                     # Apply categorical type with order
        df['match_round'], categories=round_order, ordered=True)
    df['player_1_scores'] = df['player_1_scores'].astype('string')                                          # Standardize to string
    df['player_2_scores'] = df['player_2_scores'].astype('string')
    df['winner'] = df['winner'].astype('string')
    df['result'] = df['result'].astype('category')
    df['match_id'] = df['match_id'].astype('string')
    df['tournament_id'] = df['tournament_id'].astype('Int64')                                               # Nullable integer
    df['stats_link'] = df['stats_link'].astype('string')
    df['winner_id'] = df['winner_id'].astype('string')

    cols = [                                                                                                # Set score columns to clean
        'p1_set1', 'p1_set2', 'p1_set3', 'p1_set4', 'p1_set5',
        'p2_set1', 'p2_set2', 'p2_set3', 'p2_set4', 'p2_set5', 'best_of']
    for col in cols:
        df[col] = df[col].astype(str).str.extract(r'(\d+)')                                                 # Extract only the score (drop tie-break notation)
        df[col] = df[col].astype('Int64')                                                                   # Convert to nullable int

    df["match_date"] = pd.to_datetime(df["match_date"], format="%Y-%m-%d")                                  # Convert match date to datetime
    df["year"] = pd.to_datetime(df["match_date"], dayfirst=True).dt.year                                    # Extract year from match date

    return df






    
                    # def fill_proper_id_from_archive(df, name_to_id=name_to_id):
                    #     """
                    #     Maps player names to their IDs using a given name-to-ID dictionary.

                    #     Parameters:
                    #     df (pd.DataFrame): DataFrame containing 'player_1' and 'player_2' name columns.
                    #     name_to_id (dict): Dictionary mapping player names to player IDs.

                    #     Returns:
                    #     pd.DataFrame: DataFrame with 'player_1_id' and 'player_2_id' columns updated.
                    #     """
                    #     df['player_1_id'] = df['player_1'].map(name_to_id)                                                      # Map player_1 names to IDs
                    #     df['player_2_id'] = df['player_2'].map(name_to_id)                                                      # Map player_2 names to IDs
                    #     return df


def fill_proper_id_from_archive(df, name_to_id=name_to_id):
    """
    Maps player names to IDs by matching first initial and last name.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'player_1' and 'player_2'.
        name_to_id (dict): Mapping from full names to player IDs.

    Returns:
        pd.DataFrame: DataFrame with updated 'player_1_id' and 'player_2_id'.
    """
    def match_id(name):
        if pd.isna(name):
            return None
        parts = name.split()
        if len(parts) < 2:
            return None
        first_initial = parts[0][0].upper() + "."                              # e.g., "J."
        last_name = parts[-1].title()                                          # e.g., "Struff"

        # Search name_to_id keys for same initial + last name
        for key in name_to_id:
            k_parts = key.split()
            if len(k_parts) >= 2:
                if k_parts[0] == first_initial and k_parts[-1].title() == last_name:
                    return name_to_id[key]
        return None

    df['player_1_id'] = df['player_1'].apply(match_id)
    df['player_2_id'] = df['player_2'].apply(match_id)
    return df






def remove_unwanted_values(df):
    """
    Removes rows with missing 'match_id' or missing player scores (likely walkovers or byes).

    Parameters:
    df (pd.DataFrame): DataFrame containing match data.

    Returns:
    pd.DataFrame: Filtered DataFrame with unwanted rows removed.
    """
    df = df.dropna(subset=['match_id'])                                                                     # Drop rows with missing match_id
    df = df[~(df['player_1_scores'].isna() & df['player_2_scores'].isna())]                                 # Remove rows missing both players' scores
    return df


def concat_name(row):
    """
    Formats player names by adding first initial and period if not already present.
    
    Parameters:
        row (pd.Series): A row of the DataFrame with 'player_1' and 'player_2' columns.
    
    Returns:
        pd.Series: The modified row with formatted player names.
    """
    for col in ['player_1', 'player_2', 'winner']:
        name = row[col]
        if isinstance(name, str) and '.' not in name:
            parts = name.strip().replace('-',' ').split()
            if len(parts) >= 2:
                row[col] = parts[0][0].upper() + '. ' + ' '.join(parts[1:]).title()
        else: row[col] = name.strip().replace('-',' ').title()
    return row


def clean_names(df):
    """
    Applies name formatting to all rows in the DataFrame.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing 'player_1' and 'player_2' columns.
    
    Returns:
        pd.DataFrame: Modified DataFrame with cleaned names.
    """
    return df.apply(concat_name, axis=1)



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
    df = fix_match_date(df)														                            # Correct match_date values for 2024 with fuzzy tournament end_date lookup
    df = change_scores(df)														                            # Split player scores into p1_set1..5 and p2_set1..5 columns
    df = best_of_col(df)														                            # Add best_of column (3 or 5) based on tournament_id
    df = clean_names(df)
    df = add_winner_id(df)														                            # Add winner_id column by fuzzy matching winner names
    # df = fix_mismatch_names(df)													                            # Fixes F. Augustin Gomez --> F. Gomez and adds his id
    df = clean_incorrect_results(df)											                            # Fix incorrect 'result' entries like Bye and Walkover
    df = fill_proper_id_from_archive(df)										                            # Map player names to IDs using archive dictionary
    df = remove_unwanted_values(df)												                            # Remove rows with missing match_id or player scores
    df = format_results_cols(df)												                            # Format and normalize results DataFrame columns

    return df


# -------------------------------- For Tournamnets --------------------------------

def insert_surface(df_tournaments):
    """
    Inserts a 'surface' column into the tournament DataFrame using fuzzy matching on tournament names.

    Parameters:
    df_tournaments (pd.DataFrame): DataFrame containing tournament data with at least a 'name' column.

    Returns:
    pd.DataFrame: DataFrame with a new 'surface' column inserted at position 4.
    """
    if 'surface' in df_tournaments.columns:
        return df_tournaments

    surfaces = []
    for tournament in df_tournaments['name'].to_list():
        name, score, ind = process.extractOne(tournament, list(tournament_surfaces.keys()))
        if score > 75:
            surfaces.append(tournament_surfaces[name].lower())
        else:
            print(f"Unmatched: {tournament} (Best guess: {name}, Score: {score})")
            surfaces.append(np.nan)

    df_tournaments.insert(loc=4, column='surface', value=surfaces)
    return df_tournaments


def format_tourn_cols(df):
    """
    Formats tournament DataFrame columns to appropriate types for consistency and analysis.

    Parameters:
    df (pd.DataFrame): DataFrame with tournament data, including 'id', 'name', 'level', 'location',
                       'surface', 'end_date', and 'url'.

    Returns:
    pd.DataFrame: DataFrame with cleaned and typed columns.
    """
    df['id'] = df['id'].astype('int')
    df['name'] = df['name'].astype('string')
    df['level'] = pd.Categorical(df['level'], categories=tournament_order, ordered=True)
    df['location'] = df['location'].astype('string')
    df['surface'] = df['surface'].astype('category')
    df['end_date'] = pd.to_datetime(df['end_date'], format='%Y-%m-%d', errors='coerce')
    df['url'] = df['url'].astype('string')
    df["year"] = pd.to_datetime(df["end_date"]).dt.year
    return df


def clean_tournaments(df):
    """
    Cleans and enriches the tournament DataFrame by inserting surface types and formatting columns.

    Parameters:
    df (pd.DataFrame): Raw tournament DataFrame.

    Returns:
    pd.DataFrame: Cleaned and enriched tournament DataFrame.
    """
    df = insert_surface(df)
    df = format_tourn_cols(df)
    return df


# -------------------------------- For Stats --------------------------------

def fill_proper_name_from_archive(df, id_to_name=id_to_name):
    """
    Parameters:
        df (pd.DataFrame): Input DataFrame containing 'p1_id' and 'p2_id' columns.
        id_to_name (dict): Dictionary mapping player IDs to player names.

    Returns:
        pd.DataFrame: DataFrame with 'player_1' and 'player_2' names filled from ID mapping.
    """
    df['player_1'] = df['p1_id'].map(id_to_name).fillna(df['player_1'])         	                        # Replace missing player_1 names using ID mapping
    df['player_2'] = df['p2_id'].map(id_to_name).fillna(df['player_2'])         	                        # Replace missing player_2 names using ID mapping
    return df


def frac_to_decimal(df, cols=cols):
    """
    Parameters:
        df (pd.DataFrame): Input DataFrame with string fractions to convert.
        cols (list): List of column names to convert from fraction to decimal.

    Returns:
        pd.DataFrame: DataFrame with specified columns converted to decimals.
    """
    for col in cols:
        def safe_eval(x):
            if isinstance(x, str) and '/' in x:                                 	                        # Ensure input is a fraction string
                try:
                    num, denom = map(float, x.strip().split('/'))              	                            # Split and convert numerator/denominator to float
                    if denom == 0:                                              	                        # Avoid division by zero
                        return np.nan
                    return round(num / denom, 2)                               	                            # Convert to decimal with 2 decimal places
                except:
                    return np.nan                                              	                            # Return NaN for any parsing errors
            return np.nan                                                      	                            # Return NaN if not a fraction string

        df[col] = df[col].apply(safe_eval)                                     	                            # Apply conversion to each specified column
    return df


def split_ratio_stat(x):
    """
    Converts a string fraction (e.g. "12/20") into a decimal and returns the denominator.

    Parameters:
    x (str): A string representing a fraction.

    Returns:
    tuple: A tuple containing the decimal value (rounded to 2 decimal places) and the denominator.
    """
    if isinstance(x, str) and '/' in x:																		# Check if input is a string containing a "/"
        try:
            num, denom = map(float, x.strip().split('/'))													# Split string and convert to floats
            if denom == 0:																					# Avoid division by zero
                return 0, 0
            return round(num / denom, 2), denom																# Return decimal and denominator
        except:																								# Catch conversion errors
            return np.nan, np.nan                                                                           # Invalid fraction
    else:
        return np.nan, np.nan																				# Return fallback if not a fraction


def split_small_frac(df, cols=cols2, new_cols=new_cols):
    """
    Splits string fractions in specified columns into two: one with decimal values, another with denominators.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    cols (list): List of column names containing fraction strings.
    new_cols (list): List of new column names to store the denominator values.

    Returns:
    pd.DataFrame: The DataFrame with the original columns replaced and new denominator columns added.
    """
    for col, new_col in zip(cols, new_cols):																# Loop through paired column names
        df[[col, new_col]] = df[col].apply(lambda x: pd.Series(split_ratio_stat(x)))						# Apply split_ratio_stat and expand into two columns

    return df																								# Return modified DataFrame


def format_stat_cols(df):
    """
    Formats key columns in the match statistics DataFrame by setting appropriate data types.

    Parameters:
    df (pd.DataFrame): The input DataFrame with raw statistical data.

    Returns:
    pd.DataFrame: The DataFrame with properly typed and formatted columns.
    """
    df['match_id'] = df['match_id'].astype('string')														# convert match_id to string type
    df['tournament_id'] = df['tournament_id'].astype('Int64')												# convert tournament_id to nullable integer
    df['player_1'] = df['player_1'].astype('string')														# ensure player_1 is string
    df['player_2'] = df['player_2'].astype('string')														# ensure player_2 is string
    df['p1_id'] = df['p1_id'].astype('string').str.lower()													# convert p1_id to lowercase string
    df['p2_id'] = df['p2_id'].astype('string').str.lower()													# convert p2_id to lowercase string

    float_cols = ['p1_serve_rating', 'p1_aces', 'p1_double_faults', 'p1_first_serve',
       'p1_1st_serve_points_won', 'p1_2nd_serve_points_won',
       'p1_break_points_saved', 'p1_service_games_played', 'p1_return_rating',
       'p1_1st_serve_return_points_won', 'p1_2nd_serve_return_points_won',
       'p1_break_points_converted', 'p1_return_games_played',
       'p1_net_points_won', 'p1_winners', 'p1_unforced_errors',
       'p1_service_points_won', 'p1_return_points_won', 'p1_total_points_won',
       'p1_max_speed', 'p1_1st_serve_average_speed',
       'p1_2nd_serve_average_speed', 'p2_serve_rating', 'p2_aces',
       'p2_double_faults', 'p2_first_serve', 'p2_1st_serve_points_won',
       'p2_2nd_serve_points_won', 'p2_break_points_saved',
       'p2_service_games_played', 'p2_return_rating',
       'p2_1st_serve_return_points_won', 'p2_2nd_serve_return_points_won',
       'p2_break_points_converted', 'p2_return_games_played',
       'p2_net_points_won', 'p2_winners', 'p2_unforced_errors',
       'p2_service_points_won', 'p2_return_points_won', 'p2_total_points_won',
       'p2_max_speed', 'p2_1st_serve_average_speed',
       'p2_2nd_serve_average_speed', 'p2_break_point_opportunities',
       'p1_break_point_opportunities', 'p1_net_points_played',
       'p2_net_points_played']																				# list of columns to convert to float

    for col in float_cols:																					# iterate through each float column
        df[col] = pd.to_numeric(df[col], errors='coerce')												    # convert to numeric, coercing errors to NaN

    return df																								# return the formatted DataFrame

    
def clean_stats_df(df):
    """
    Clean and standardize the input stats DataFrame using a series of transformation steps.

    1. Parameters:
        df (pd.DataFrame): The input DataFrame containing raw match statistics.

    2. Returns:
        pd.DataFrame: A cleaned and standardized DataFrame with numeric values, split ratios,
                      properly formatted column types, and player names mapped from IDs.
    """
    df = frac_to_decimal(df)									                                            # convert fraction strings (e.g. '3/4') to decimals in place
    df = split_small_frac(df)								                                                # split certain fractional stats into (decimal_value, denominator)
    df = format_stat_cols(df)									                                            # convert key columns to appropriate data types (e.g., float, string)
    df = fill_proper_name_from_archive(df)				                                            		# map player IDs to names using historical archive
    
    return df										                                            			# return cleaned DataFrame


# -------------------------------- Join DataFrames And Drop Columns --------------------------------

def combine_results_and_tourn(df_results, df_tourn):
    """
    Combine cleaned results DataFrame with tournaments DataFrame to add tournament attributes.

    Parameters:
    df_results (pd.DataFrame): Cleaned match results DataFrame containing 'tournament_id' column.
    df_tourn (pd.DataFrame): Tournament DataFrame containing 'id', 'level', 'location', and 'surface' columns.

    Returns:
    pd.DataFrame: Merged DataFrame containing results enriched with tournament level, location, and surface.
    """
    merged_results = pd.merge(
                df_results,												                                    # Left DataFrame: match results
                df_tourn[['id', 'level', 'location', 'surface', 'year']],		                                    # Right DataFrame: selected tournament columns      #ADDED YEAR 12/8
                left_on=["tournament_id", 'year'],							                                    	# Join key from results
                right_on=["id", "year"],								                                			# Join key from tournaments
                how="left"									                                    			# Left join to keep all results
    ).drop(columns=["id"])                                                                                  # Drop the redundant 'id' column
    return merged_results									                                				# Return enriched results DataFrame


def reorder_players(df_stats: pd.DataFrame, df_results: pd.DataFrame) -> pd.DataFrame:
    """
    Reorders players in `df_results` to match the player ordering in `df_stats`,
    based on combinations of tournament_id, match_id, and player_id values.

    Parameters:
    ----------
    df_stats : pd.DataFrame
                DataFrame containing statistics with consistent player order.
    
    df_results : pd.DataFrame
                DataFrame containing match results, potentially with inconsistent player order.

    Returns:
    -------
    pd.DataFrame
                The `df_results` DataFrame with player positions reordered to match `df_stats`.
    """
    
    p1_cols = [
                'player_1', 'player_1_scores', 'p1_id',
                'p1_set1', 'p1_set2', 'p1_set3', 'p1_set4', 'p1_set5'
    ]																					                    # Columns related to player 1
    
    p2_cols = [
                'player_2', 'player_2_scores', 'p2_id',
                'p2_set1', 'p2_set2', 'p2_set3', 'p2_set4', 'p2_set5'
    ]																					                    # Columns related to player 2

    valid_pairs1 = set(zip(df_stats['tournament_id'], df_stats['match_id']))			                    # Valid tournament & match id pairs
    valid_pairs2 = set(zip(df_stats['tournament_id'], df_stats['match_id'], df_stats['p1_id']))	            # Valid p1 id included
    valid_pairs3 = set(zip(df_stats['tournament_id'], df_stats['match_id'], df_stats['p2_id']))         	# Valid p2 id included
    valid_pairs4 = set(zip(df_stats['tournament_id'], df_stats['p1_id'], df_stats['p2_id']))		    	# Valid player pairs without match_id

    def _reorder_row(row: pd.Series) -> pd.Series:
        tid, mid = row['tournament_id'], row['match_id']									                # Extract identifiers
        p1_id, p2_id = row['p1_id'], row['p2_id']
        
        match_pair = (tid, mid)															                    # For quick checks
        match_p1 = (tid, mid, p1_id)
        match_p2 = (tid, mid, p2_id)
        player_pair = (tid, p1_id, p2_id)
        swapped_pair = (tid, p2_id, p1_id)

        if pd.notna(mid) and match_pair in valid_pairs1:							                    	# If match_id is valid
            if match_p1 in valid_pairs2 or match_p2 in valid_pairs3:				                    	# Player order matches stats order
                return row															                    	# No swap needed
            elif match_p1 in valid_pairs3 or match_p2 in valid_pairs2:				                    	# Player order reversed compared to stats
                for c1, c2 in zip(p1_cols, p2_cols):									
                    row[c1], row[c2] = row[c2], row[c1]							                       		# Swap all corresponding player 1 and 2 columns
                return row
            else:
                return row																                    # IDs don’t match stats, return as is

        elif pd.isna(mid):																                    # No match_id provided
            if player_pair in valid_pairs4:											                        # Player pair matches stats order
                return row															                    	# No swap needed
            elif swapped_pair in valid_pairs4:											                    # Player pair swapped compared to stats
                for c1, c2 in zip(p1_cols, p2_cols):
                    row[c1], row[c2] = row[c2], row[c1]								                    	# Swap player columns
                return row
            else:
                return row																                    # Pair not found, return as is
        else:
            return row																	                    # match_id not in stats, return as is

    return df_results.apply(_reorder_row, axis=1)									                    	# Apply row-wise reordering


def combine_results_and_stats(df_stats: pd.DataFrame, df_results: pd.DataFrame) -> pd.DataFrame:
    """
    Reorders players in the results DataFrame and merges it with match statistics.

    Parameters:
    ----------
    df_stats : pd.DataFrame
                A DataFrame containing detailed match statistics for players.

    df_results : pd.DataFrame
                 A DataFrame containing match-level results and player info.

    Returns:
    -------
    pd.DataFrame
                A merged DataFrame combining results and statistics, with consistent player ordering.
                Rows with no player stat data are dropped.
    """

    df_results = reorder_players(df_stats, df_results)									                    # Align player order in results to match stats

    merge_keys = ['tournament_id', 'match_id', 'p1_id', 'p2_id']											                    # Keys to merge on
    merged_df = pd.merge(df_results, df_stats, on=merge_keys, how='inner')				                	# Merge stats into results

    merged_df = merged_df.loc[:, ~merged_df.columns.str.endswith('_y')]				                    	# Drop duplicated '_y' columns from merge
    merged_df.columns = merged_df.columns.str.replace('_x$', '', regex=True)		                		# Remove '_x' suffix from merged columns

    stat_cols = [																	                    	# Columns to check for stat presence
                "p1_first_serve", "p1_1st_serve_points_won", "p1_2nd_serve_points_won",
                "p1_1st_serve_return_points_won", "p1_service_points_won", "p1_2nd_serve_return_points_won",
                "p1_return_points_won", "p1_total_points_won",
                "p2_first_serve", "p2_1st_serve_points_won", "p2_2nd_serve_points_won",
                "p2_1st_serve_return_points_won", "p2_service_points_won", "p2_2nd_serve_return_points_won",
                "p2_return_points_won", "p2_total_points_won"]
    merged_df = merged_df.dropna(subset=stat_cols, how='all')							                	# Drop rows missing all stats columns

    return merged_df																                    	# Return combined and cleaned DataFrame

