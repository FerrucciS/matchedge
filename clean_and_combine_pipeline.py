import pandas as pd
from rapidfuzz import process, fuzz
import numpy as np
import s3fs
from datetime import datetime
from scripts.clean_data import clean_results_df, clean_stats_df, clean_rankings, clean_tournaments, combine_results_and_tourn, combine_results_and_stats


if __name__ == "__main__":
    # --- 1.0. Get last scraped date ---
    fs = s3fs.S3FileSystem()
    date_file = "s3://matchedge-pipeline/logs/last_scraped_date.csv"

    # Step 1: Check if file exists and read last scrape date
    if fs.exists(date_file):
        with fs.open(date_file, 'r') as f:
            try:
                last_scraped_df = pd.read_csv(f)
                start_date = pd.to_datetime(last_scraped_df.iloc[0, 0])
                file_date =last_scraped_df.iloc[0,0]                        # Format for opening files
            except Exception as e:
                raise FileNotFoundError(f"Failed to read {date_file}: {e}")
    else:
        raise FileNotFoundError(f"{date_file} does not exist in S3.")
    

    # --- 2.0. Load recently raw scraped data ---
    df_top = pd.read_csv(f"s3://matchedge-pipeline/data/raw/top_players_{file_date}.csv", sep=",")
    df_all_tournament = pd.read_csv(f"s3://matchedge-pipeline/data/raw/All_Tournaments_{file_date}.csv", sep=",")
    df_results = pd.read_csv(f"s3://matchedge-pipeline/data/raw/all_results_{file_date}.csv", sep=",")
    df_all_stats = pd.read_csv(f's3://matchedge-pipeline/data/raw/all_stats_GS_{file_date}.csv', sep=",")


    # --- 3.0. Update player archive OUTDATED--- 
    # update_player_listings(df_live_rankings=df_top, df_results=df_results, df_stats=df_all_stats)


    # --- 4.0. Clean DataFrames ---
    df_top = clean_rankings(df_top)
    df_all_tournament = clean_tournaments(df_all_tournament)
    df_results = clean_results_df(df_results)
    df_all_stats = clean_stats_df(df_all_stats)


    # --- 5.0. Save Clean DataFrames ---
    df_top.to_csv('/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/top_players.csv', sep=",", columns=df_top.columns, index=False)
    df_top.to_csv("s3://matchedge-pipeline/data/clean/top_players.csv", sep=",", columns=df_top.columns, index=False)
    df_top.to_parquet("s3://matchedge-pipeline/data/clean/top_players.parquet", index=False)

    df_all_tournament.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/all_tournaments_{file_date}.csv', sep=',', columns=df_all_tournament.columns, index=False)
    df_all_tournament.to_csv(f's3://matchedge-pipeline/data/clean/all_tournaments_{file_date}.csv', sep=',', columns=df_all_tournament.columns, index=False)
    df_all_tournament.to_parquet(f's3://matchedge-pipeline/data/clean/all_tournaments_{file_date}.parquet', index=False)

    df_results.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/all_results_{file_date}.csv', sep=',', columns=df_results.columns, index=False)
    df_results.to_csv(f's3://matchedge-pipeline/data/clean/all_results_{file_date}.csv', sep=',', columns=df_results.columns, index=False)
    df_results.to_parquet(f's3://matchedge-pipeline/data/clean/all_results_{file_date}.parquet', index=False)

    df_all_stats.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/all_stats_{file_date}.csv', sep=',', columns=df_all_stats.columns, index=False)
    df_all_stats.to_csv(f"s3://matchedge-pipeline/data/clean/all_stats_{file_date}.csv", sep=',', columns=df_all_stats.columns, index=False)
    df_all_stats.to_parquet(f"s3://matchedge-pipeline/data/clean/all_stats_{file_date}.parquet", index=False)


    # --- 6.0. Combine Results and Tournament
    merged_tourn_results = combine_results_and_tourn(df_results, df_all_tournament)


    # --- 7.0. Combine With Stats and Save
    merged_matches = combine_results_and_stats(df_all_stats, merged_tourn_results)

    merged_matches.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/merged_matches_{file_date}.csv', sep=',', columns=merged_matches.columns, index=False)
    merged_matches.to_csv(f's3://matchedge-pipeline/data/clean/merged_matches_{file_date}.csv', sep=',', columns=merged_matches.columns, index=False)
    merged_matches.to_parquet(f's3://matchedge-pipeline/data/clean/merged_matches_{file_date}.parquet', index=False)


    # --- 8.0. Merge with Old DataFrame
    all_merged_matches = pd.read_parquet("s3://matchedge-pipeline/data/clean/merged_matches.parquet")  
    
    # Create backup
    all_merged_matches.to_csv(f"s3://matchedge-pipeline/data/clean/merged_matches_{file_date}_backup.csv", sep=',', columns=all_merged_matches.columns, index=False)
    all_merged_matches.to_parquet(f"s3://matchedge-pipeline/data/clean/merged_matches_{file_date}_backup.parquet", index=False)
    

    all_merged_matches = pd.concat([all_merged_matches, merged_matches], ignore_index=True)         #SAVE TO PARQUET
    all_merged_matches.to_csv("s3://matchedge-pipeline/data/clean/merged_matches.csv", sep=',', columns=all_merged_matches.columns, index=False)
    all_merged_matches.to_csv("/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/merged_matches.csv", sep=',', columns=all_merged_matches.columns, index=False)
    all_merged_matches.to_parquet("s3://matchedge-pipeline/data/clean/merged_matches.parquet", index=False)
    