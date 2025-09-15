import pandas as pd
from rapidfuzz import process, fuzz
import numpy as np
import s3fs
from datetime import datetime
from scripts.clean_data import clean_results_df, clean_stats_df, clean_rankings, clean_tournaments, combine_results_and_tourn, combine_results_and_stats


if __name__ == "__main__":
    
        # --- 2.0. Load recently raw scraped data ---
    df_top = pd.read_csv(f"s3://matchedge-pipeline/data/raw/top_players_2025-08-06.csv", sep=",")
    df_all_tournament = pd.read_csv(f"s3://matchedge-pipeline/data/raw/All_Tournaments.csv", sep=",")
    df_results = pd.read_csv(f"s3://matchedge-pipeline/data/raw/all_results.csv", sep=",")
    df_all_stats = pd.read_csv(f's3://matchedge-pipeline/data/raw/all_stats_GS.csv', sep=",")
    
    
        # --- 4.0. Clean DataFrames ---
    df_top = clean_rankings(df_top)
    df_all_tournament = clean_tournaments(df_all_tournament)
    df_results = clean_results_df(df_results)
    df_all_stats = clean_stats_df(df_all_stats)
    
    
    df_top.to_csv('/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/trial/top_players.csv', sep=",", columns=df_top.columns, index=False)
    df_all_tournament.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/trial/all_tournaments.csv', sep=',', columns=df_all_tournament.columns, index=False)
    df_results.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/trial/all_results.csv', sep=',', columns=df_results.columns, index=False)
    df_all_stats.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/trial/all_stats.csv', sep=',', columns=df_all_stats.columns, index=False)
    
    
    
    
    
    
    # file_date = '2025-08-06'
    
    #     # --- 6.0. Combine Results and Tournament
    # merged_tourn_results = combine_results_and_tourn(df_results, df_all_tournament)


    # # --- 7.0. Combine With Stats and Save
    # merged_matches = combine_results_and_stats(df_all_stats, merged_tourn_results)

    #     # merged_matches.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/merged_matches_{file_date}.csv', sep=',', columns=merged_matches.columns, index=False)
    #     # merged_matches.to_csv(f's3://matchedge-pipeline/data/clean/merged_matches_{file_date}.csv', sep=',', columns=merged_matches.columns, index=False)
    
    # merged_matches.to_csv(f'/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/trial/merged_matches_{file_date}.csv', sep=',', columns=merged_matches.columns, index=False)


    # # --- 8.0. Merge with Old DataFrame
    # all_merged_matches = pd.read_csv("s3://matchedge-pipeline/data/clean/merged_matches.csv")
    
    # # Create backup
    #     # all_merged_matches.to_csv(f"s3://matchedge-pipeline/data/clean/merged_matches_{file_date}.csv", sep=',', columns=all_merged_matches.columns, index=False)
    #     # 
    
    # all_merged_matches.to_csv(f"/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/trial/merged_matches_{file_date}.csv", sep=',', columns=all_merged_matches.columns, index=False)

    # all_merged_matches = pd.concat([all_merged_matches, merged_matches], ignore_index=True)
    #     # all_merged_matches.to_csv("s3://matchedge-pipeline/data/clean/merged_matches.csv", sep=',', columns=all_merged_matches.columns, index=False)
    #     # all_merged_matches.to_csv("/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/clean/merged_matches.csv", sep=',', columns=all_merged_matches.columns, index=False)
        
    # all_merged_matches.to_csv("/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/trial/merged_matches.csv", sep=',', columns=all_merged_matches.columns, index=False)