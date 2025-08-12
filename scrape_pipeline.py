# --- 1.0. Libraries ---

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import random
import time
from datetime import datetime, timedelta
import pandas as pd
import json
import requests
import re
import os
import sys
import numpy as np
import s3fs
from scripts.scraping_utils import (
    get_tournament, create_driver, get_live_player_rankings, 
    extract_results_url, get_tournament_results, get_stat_names, get_stats
    )


if __name__ == "__main__":
    # --- 2.0. Get Tournaments ---
    tourn_archive_url = ["https://www.atptour.com/en/scores/results-archive?year=2025"]     # ATP tournamnet archive
    df_tournaments = get_tournament(tourn_archive_url = tourn_archive_url, save_cache=False)


    # --- 3.0. Get Live Rankings ---
    df_top_players = get_live_player_rankings()


    # --- 4.0. Extract results URL's From Last Scraped Date Onwards ---
    fs = s3fs.S3FileSystem()
    date_file = "s3://matchedge-pipeline/logs/last_scraped_date.csv"

    # Step 1: Check if file exists and read last scrape date
    if fs.exists(date_file):
        with fs.open(date_file, 'r') as f:
            try:
                last_scraped_df = pd.read_csv(f)
                start_date = pd.to_datetime(last_scraped_df.iloc[0, 0])
            except Exception as e:
                raise FileNotFoundError(f"Failed to read {date_file}: {e}")
    else:
        raise FileNotFoundError(f"{date_file} does not exist in S3.")

        # --- Step 2: Use start_date in your scraping function ---
    results_url = extract_results_url(df_tournament=df_tournaments, start_date=start_date)

        # --- Step 3: Save today's date to file for next run ---
    today = datetime.today().date()
    with fs.open(date_file, 'w') as f:
        pd.DataFrame([today]).to_csv(f, index=False, header=True)


    # --- 5.0. Get Results ---
    df_tournament_results = get_tournament_results(results_url)


    # --- 6.0. Get Stats ---
    stat_url = df_tournament_results['stats_link'].to_list()
    stats = get_stats(stat_url)


    # --- 7.0. Save ---
    df_tournaments.to_csv(f"s3://matchedge-pipeline/data/raw/All_Tournaments_{today}.csv", index=False)
    df_top_players.to_csv(f"s3://matchedge-pipeline/data/raw/top_players_{today}.csv", index=False)
    df_tournament_results.to_csv(f"s3://matchedge-pipeline/data/raw/all_results_{today}.csv", index=False)
    stats.to_csv(f"s3://matchedge-pipeline/data/raw/all_stats_GS_{today}.csv", index=False)

    df_tournaments.to_csv(f"/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/raw//All_Tournaments_{today}.csv", index=False)
    df_top_players.to_csv(f"/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/raw/top_players_{today}.csv", index=False)
    df_tournament_results.to_csv(f"/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/raw/all_results_{today}.csv", index=False)
    stats.to_csv(f"/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/raw/all_stats_GS_{today}.csv", index=False)