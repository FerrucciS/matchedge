## --- Libraries --- 
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
import numpy as np


## --- Global Vars ---
master_stat_columns = ['match_id', 'tournament_id', 'player_1', 'player_2', 'p1_id', 'p2_id',
       'p1_serve_rating', 'p1_aces', 'p1_double_faults', 'p1_first_serve',
       'p1_1st_serve_points_won', 'p1_2nd_serve_points_won',
       'p1_break_points_saved', 'p1_service_games_played', 'p1_return_rating',
       'p1_1st_serve_return_points_won', 'p1_2nd_serve_return_points_won',
       'p1_break_points_converted', 'p1_return_games_played',
       'p1_net_points_won', 'p1_winners', 'p1_unforced_errors',
       'p1_service_points_won', 'p1_return_points_won', 'p1_total_points_won',
       'p1_max_speed', 'p1_1st_serve_average_speed', 'p1_2nd_serve_average_speed',
       'p2_serve_rating', 'p2_aces', 'p2_double_faults', 'p2_first_serve',
       'p2_1st_serve_points_won', 'p2_2nd_serve_points_won',
       'p2_break_points_saved', 'p2_service_games_played', 'p2_return_rating',
       'p2_1st_serve_return_points_won', 'p2_2nd_serve_return_points_won',
       'p2_break_points_converted', 'p2_return_games_played',
       'p2_net_points_won', 'p2_winners', 'p2_unforced_errors',
       'p2_service_points_won', 'p2_return_points_won', 'p2_total_points_won',
       'p2_max_speed', 'p2_1st_serve_average_speed', 'p2_2nd_serve_average_speed']

## --- Functions ---


def create_driver():
    """
    Create and configure a headless Chrome WebDriver instance with optimized options
    for scraping ATP tournament data.
    
    Returns:
        webdriver.Chrome: Configured Chrome WebDriver instance.
    """
    options = Options()
    options.add_argument("--headless=new")  # Use new headless mode (for recent Chrome versions)
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Set a realistic user-agent to avoid detection as a bot
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    # Disable loading images, stylesheets, and cookies for faster loading; keep JavaScript enabled
    options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 2,
        "profile.managed_default_content_settings.javascript": 1,  # keep JS if needed
    })
    # Initialize the Chrome driver using the ChromeDriverManager for auto installation
    
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def extract_tournament_names(soup):
    """
    Extract tournament names from a BeautifulSoup-parsed HTML page.

    Args:
        soup (BeautifulSoup): Parsed HTML content of the tournament archive page.

    Returns:
        list[str]: List of tournament names as strings.
    """
    
    return [tag.get_text(strip=True) for tag in soup.select("span.name")]


def extract_tournament_loc(soup):
    """
    Extract tournament locations from the soup and clean the text by removing extra characters.

    Args:
        soup (BeautifulSoup): Parsed HTML content.

    Returns:
        list[str]: List of cleaned tournament locations.
    """
    tourn_loc = [tag.get_text(strip=True) for tag in soup.select("span.venue")]
    for i in range(len(tourn_loc)):                                 # Remove the '|' and trim whitespace
            tourn_loc[i] = tourn_loc[i].replace("|","")
            tourn_loc[i] = tourn_loc[i].strip()
            
    return tourn_loc


def extract_tournament_levels(soup):
    """
    Extract tournament levels (e.g., ATP 250, Grand Slam) by inspecting badge image sources.

    Args:
        soup (BeautifulSoup): Parsed HTML content.

    Returns:
        list[str]: List of tournament level strings.
    """
    tourn_levels = []
    for img in soup.select("div.event-badge_container img"):
        src = img.get("src", "")
        if "250" in src:
            tourn_levels.append("ATP 250")
        elif "500" in src:
            tourn_levels.append("ATP 500")
        elif "1000" in src:
            tourn_levels.append("ATP 1000")
        elif "gs" in src or "grand" in src:
            tourn_levels.append("Grand Slam")
        elif "final" in src:
            tourn_levels.append("Nitto ATP Finals")
        elif "nextgen" in src:
            tourn_levels.append("Next Gen ATP Finals")
        else:
            tourn_levels.append("Other")                            # e.g., United Cup
            
    return tourn_levels
    

def extract_date_ranges(soup):
    """
    Extract raw tournament date ranges as strings.

    Args:
        soup (BeautifulSoup): Parsed HTML content.

    Returns:
        list[str]: List of date range strings, e.g. "10 - 16 June 2025".
    """
    
    return [tag.get_text(strip=True) for tag in soup.select("span.Date")]


def extract_end_dates(date_ranges):
    """
    Convert raw date range strings into datetime.date objects representing the end date.

    Args:
        date_ranges (list[str]): List of date range strings.

    Returns:
        list[datetime.date or None]: Parsed end dates or None if parsing failed.
    """
    finish_dates = []
    for date_str in date_ranges:
            try:
                # Extract end date substring (e.g. "16 June 2025")
                end_date_str = date_str.split("-")[-1].strip()                      
                end_date = datetime.strptime(end_date_str, "%d %B, %Y").date()      # Parse date
                finish_dates.append(end_date)
            except:
                finish_dates.append(None)                                           # Fallback if parsing fails
                
    return finish_dates


def extract_tournament_links(soup):
    """
    Extract full URLs to individual tournament pages.

    Args:
        soup (BeautifulSoup): Parsed HTML content.

    Returns:
        list[str]: List of full tournament URLs.
    """
    links = []
    for a_tag in soup.select("ul.events a.tournament__profile"):
        href = a_tag.get("href")
        if href:
            full_url = "https://www.atptour.com" + href
            links.append(full_url)
            
    return links


def get_tournament_id(tournament_links):
    """
    Extract tournament IDs from tournament URLs.

    Args:
        tournament_links (list[str]): List of tournament URLs.

    Returns:
        list[str]: List of tournament IDs extracted from URLs.
    """
    tourn_id = []
    for link in tournament_links:
        parts = link.split('/')
        tourn_id.append(parts[-2])                                  # Second last part is ID
        
    return tourn_id


def get_tournament(tourn_archive_url, save_cache=False):
    """
    Scrape ATP tournament data for given archive URLs and return structured tournament information.

    Args:
        driver (webdriver.Chrome): Selenium WebDriver instance for browser automation.
        tourn_archive_url (list[str]): List of URLs for tournament archive years.
        save_cache (bool, optional): If True, saves HTML content of pages locally for caching.

    Returns:
        pd.DataFrame: Pandas DataFrame containing id, name, level, location, end_date, url.
    """
    all_tournaments = []
    driver = create_driver()                                        # Call create_driver()
    for url in tourn_archive_url:                                   # Loop over all tournament archive years
        
        # --- 0.0 Access link ---
        driver.get(url)                                             # Open the tournament results archive page in a Selenium-controlled browser
        time.sleep(random.uniform(5,7))                             # Let the page load
        soup = BeautifulSoup(driver.page_source, "html.parser")     # Get HTML and parse with BeautifulSoup
        
        # --- 1.0 Extract Data ---
        tournament_names = extract_tournament_names(soup)
        tournament_locations = extract_tournament_loc(soup)
        tournament_levels = extract_tournament_levels(soup)
        date_strings = extract_date_ranges(soup)                    
        end_dates = extract_end_dates(date_strings)
        tournament_links = extract_tournament_links(soup)
        tournament_id = get_tournament_id(tournament_links)

        # --- 2.0 Create Tournament List ---
        tournaments = []
        today = datetime.today().date()                             # Get todays date
        year = url.split("=")[-1]                                   # Get year of tournaments 

        if int(year) == today.year:
            # If the tournament is current year, include tournaments already finished                                
            for name, loc, level, end_date, link, id in zip(
                tournament_names, tournament_locations, 
                tournament_levels, end_dates, tournament_links, 
                tournament_id
                ):
                if end_date and end_date < today and level != "Other":
                    tournaments.append({"id": f"{id}", 
                                    "name": f"{name}",
                                    "level": f"{level}",
                                    "location": f"{loc}",
                                    "end_date": f"{end_date}",
                                    "url": f"{link}"
                                    })      
        elif int(year) < today.year:
            # For past years, only include tournaments after current month                             
            for name, loc, level, end_date, link, id in zip(
                tournament_names, tournament_locations, 
                tournament_levels, end_dates, tournament_links, 
                tournament_id
                ):
                if end_date and end_date.month > today.month and level != "Other":
                    tournaments.append({"id": f"{id}",
                                    "name": f"{name}",
                                    "level": f"{level}",
                                    "location": f"{loc}",
                                    "end_date": f"{end_date}",
                                    "url": f"{link}"
                                    })
        else: pass

        ## --- 3.0 Cahce (Optional) ---
        if save_cache:
            with open(f"/Users/samueleferrucci/Library/CloudStorage/GoogleDrive-samueleferrucci94@gmail.com/Other computers/My MacBook Air/Coding/Projects/Tennis ML/cache/tournaments_{year}_cached_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
                
        all_tournaments.extend(tournaments)
    
    driver.quit()                                                   # Quit driver
    df_tournaments = pd.DataFrame(all_tournaments)
        
    return df_tournaments


def get_live_player_rankings():
    """
    Call ultimate-tennis API to get and return live ATP n-rankings
    
    Args:
        num: Highest player rank (max is num=500)
    
    Returns:
        DataFrame: Pandas DataFrame with columns name, rank, id, ....
    """
    links = []
    names = []
    id = []
    corrected_names = []
    
    driver = create_driver()
    driver.get("https://www.atptour.com/en/rankings/singles?rankRange=0-5000")                                                                      
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")   
    
    # Find all <li> with class 'name center' and get the <a> href
    for li in soup.find_all("li", class_="name center"):
        a_tag = li.find("a")
        if a_tag and "href" in a_tag.attrs:
            links.append(a_tag["href"])
    driver.quit() 

    
    for link in links:
        parts = link.split("/")
        names.append(parts[-3])
        id.append(parts[-2])   
    
    for name in names:
        name_parts = name.split("-")
        first_name = name_parts[0][0].upper()
        last_name = " ".join(name_parts[1:]).title()
        name = first_name + ". " + last_name
        corrected_names.append(name)
        
    df_top_players = pd.DataFrame({"name": corrected_names, "id": id})
    return df_top_players
    

def extract_results_url(df_tournament: pd.DataFrame, url_col="url", date_col="end_date", start_date=datetime.today()):
    """
    Generate a list of ATP results URLs from a tournament DataFrame.

    Args:
        df_tournaments (pd.DataFrame): DataFrame with at least URL and end_date columns.
        url_col (str): Name of the column containing tournament profile URLs.
        date_col (str): Name of the column containing tournament end dates.

    Returns:
        list[str]: List of full tournament results page URLs.
    """
    base_url = "https://www.atptour.com/en/scores/archive/"
                # todays_date = datetime.today().date()
    results_url = []
    year = start_date.year
    
    if not pd.api.types.is_datetime64_any_dtype(df_tournament[date_col]):                       # Convert col to datetime if not already
        df_tournament[date_col] = pd.to_datetime(df_tournament[date_col], errors='coerce')

    for i, row in df_tournament.iterrows():                                                     # Iterate through each row
        end_date = row[date_col]
        profile_url = row[url_col]

                    # year = end_date.year
        tourn_info = profile_url.split("/")[-3:-1]
        
            # # --- 1.0 Make Results URLs for 1 YTD --- FOR 1 years worth
            # if (year == todays_date.year and end_date.date() < todays_date) or \
            #     (year < todays_date.year and end_date.date() > (todays_date - timedelta(days=365))):
            #     full_url = base_url + f"{tourn_info[0]}/{tourn_info[1]}/{year}/results"
            #     results_url.append(full_url)
            # --- 1.0 Make Results URLs from start_date ---
        if end_date > start_date:
            full_url = base_url + f"{tourn_info[0]}/{tourn_info[1]}/{year}/results"
            results_url.append(full_url)

    return results_url









def cache_html(computer: str, filename: str, driver=None):
    """
    Save current HTML page source to cache depending on the machine type.

    Args:
        computer (str): Must be "imac" or "macbook".
        filename (str): Filename for the cached HTML (without extension).
        driver (webdriver.Chrome): Selenium driver to get page source.

    Raises:
        ValueError: If 'computer' is not "imac" or "macbook".
    """
    if driver is None:
        raise ValueError("A Selenium driver instance must be provided.")
    
    computer = computer.lower()
    
    if computer == "imac":                                  # iMac
        path = f"/Users/samueleferrucci/Library/CloudStorage/GoogleDrive-samueleferrucci94@gmail.com/Other computers/My MacBook Air/Coding/Projects/Tennis ML/cache/{filename}.html"
    elif computer == "macbook":                             # Macbook
        path = f"/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/cache/{filename}.html"
    else: 
        raise ValueError("Invalid computer name. Must be 'imac' or 'macbook'.") 
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    
    return None


def extract_match_dates(sub):
    """
    Extracts the match date text from an ATP tournament page section.

    Args:
        sub (bs4.element.Tag): A BeautifulSoup tag corresponding to an accordion item section for a single day of matches.

    Returns:
        str: Cleaned date text string (e.g. "Wednesday, July 10"), with nested <span> elements removed.
    """
    h4_tag = sub.select_one(".tournament-day h4")
    
    if h4_tag:
        span = h4_tag.find('span')                          # Get only the date part by removing the <span>
        
        if span:
            span.extract()                                  # remove <span> from the tag
        date_text = h4_tag.get_text(strip=True)
        
    return date_text


def extract_match_round(match):
    """
    Extracts and formats the match round name (e.g., "Quarter Final", "Round 1") from a match element.

    Args:
        match (bs4.element.Tag): A BeautifulSoup tag representing a single match block.

    Returns:
        str: Formatted match round name. Returns an empty string if the round is not found.
    """
    try:
        first_span = match.select_one('.match-header strong')
        match_rounds = first_span.get_text(strip=True) if first_span else ''
    except:
        match_rounds = ''
    if 'round' in match_rounds.lower():
        match_rounds = match_rounds.split(' ')
        match_rounds = ' '.join(match_rounds[0:3])
    elif 'final' in match_rounds.lower():
        match_rounds = match_rounds.split(' ')
        match_rounds = match_rounds[0]
    else: pass
    
    return match_rounds


def extract_match_duration(match):
    """
    Extracts the match duration (e.g., "1:45:00") from the match header section.

    Args:
        match (bs4.element.Tag): A BeautifulSoup tag representing a single match block.

    Returns:
        str: Duration string if found; otherwise, returns an empty string.
    """
    try:
        durations = match.select_one('.match-header span:nth-child(2)').text
    except:
        durations = ''
        
    return durations


def extract_match_notes(match):
    """
    Extracts textual notes attached to a match, such as retirements or walkovers.

    Args:
        match (bs4.element.Tag): A BeautifulSoup tag representing a single match block.

    Returns:
        str: Cleaned match note text, or an empty string if no note exists.
    """
    notes = match.select_one('.match-notes')
    note_text = notes.text.strip() if notes else ''
    
    return note_text


def extract_match_player_names(match):
    """
    Extracts the names of both players involved in a match.

    Args:
        match (bs4.element.Tag): A BeautifulSoup tag representing a single match block.

    Returns:
        list[str]: A list of player names (usually 2). If names are missing, the list may be shorter.
    """
    players = match.select('.stats-item .player-info')
    player_names = [p.select_one('.name a').text.strip() for p in players]
    
    return player_names


def extract_match_winner(match, notes: str):
    """
    Extracts the name of the winning player from a match block. 
    Falls back to parsing the notes if the standard winner tag is missing.

    Args:
        match (bs4.element.Tag): A BeautifulSoup tag representing a single match block.
        notes (str): Optional match note text to use as a fallback (e.g., "Game Set and Match John Doe.").

    Returns:
        str or None: The name of the winner if found, otherwise None.
    """
    # --- 0.0 Get winner ---
    winner_name = None
    players = match.select('.stats-item .player-info')
    for p in players:
        if p.select_one('.winner'):
            winner_name = p.select_one('.name a').text.strip()
    
    # --- 1.0 Fallback: Try to extract from match-notes ---
    if not winner_name and notes:
        winner_match = re.search(r"Game Set and Match (.+?)\.", notes)
        if not winner_match:
            winner_match = re.search(r"Winner:?\s+([A-Z]\.\s*[A-Z]+)", notes, re.IGNORECASE)
        if winner_match:
            winner_name = winner_match.group(1).strip()
                        
    return winner_name


def extract_match_score(match, notes: str):
    """
    Extracts set-level scores for both players from a match block.
    Each player's scores are parsed from nested span tags within the score block.

    Args:
        match (bs4.element.Tag): A BeautifulSoup tag representing a single match block.
        notes (str): Unused fallback or metadata string (included for future compatibility).

    Returns:
        tuple[str, str]: Two strings representing the set scores for player 1 and player 2 respectively.
                         Returns empty strings if the structure is invalid.
    """
    # --- 0.0 Get scores for both players ---
    score_blocks = match.select('.stats-item .scores')
    player_scores = []
    for score in score_blocks:
        sets = []
        for score_item in score.select('.score-item'):
            spans = score_item.select('span')
            if spans:
                text = [s.text.strip() for s in spans if s.text.strip()]
                if len(text) == 2:
                    combined = f"{text[0]}({text[1]})" # e.g. 6(2)
                elif len(text) == 1:
                    combined = text[0]
                else:
                    comined = ''
                if combined:
                    sets.append(combined)
        player_scores.append(sets)
        
    if len(player_scores) == 2:
        p_1_score = ' '.join(player_scores[0])
        p_2_score = ' '.join(player_scores[1])
    else:
        p_1_score = ''
        p_2_score = ''
            
    return p_1_score, p_2_score


def extract_match_result(note: str):
    """
    Determines the result type of a match based on the note string.

    Args:
        note (str): Textual note from the match (e.g., "RET", "Walkover").

    Returns:
        str: One of the following result types — "Completed", "RET", "Walkover", or "Default".
    """
    result = "Completed"
    if 'RET' in note:
        result = 'RET'
    elif 'Walkover' in note or 'W/O' in note:
        result = 'Walkover'
    elif 'Default' in note:
        result = 'Default'
    
    return result


def extract_match_id_and_statlink(match, atp_url: str):
    """
    Extracts the unique match ID and constructs a full stat link URL for a given match.

    Args:
        match (bs4.element.Tag): A BeautifulSoup tag representing a single match block.
        atp_url (str): Base ATP website URL to prepend to relative stat link paths.

    Returns:
        tuple[str, str]: A tuple containing (match_id, full_stat_link).
                         If no stat link is found, both values will be empty strings.
    """
    stat_link = match.select("div.match-group.match-group--active div.match-cta a:nth-child(2)", href=True)
    if stat_link:
        match_id = stat_link[0]["href"].split("/")[-1]
        stat_link = atp_url + stat_link[0]["href"]
    else:
        match_id = ''
        stat_link = ''
    
    return match_id, stat_link


def get_tournament_results(tourn_urls: list, cache=False, computer_cache: str=None, store_individual_tourn_data: bool=False, computer: str='macbook'):
    """
    Scrapes ATP tournament results from a list of tournament result URLs.

    For each tournament URL provided, this function uses Selenium and BeautifulSoup to extract match-level data 
    including player names, scores, duration, match round, and match result. Optionally, it can cache the HTML 
    pages and save each tournament's results to a CSV file based on the user's computer setup.

    Args:
        tourn_urls (list[str]): 
            A list of URLs pointing to ATP tournament result pages.
        
        cache (bool): default False
            If True, caches the raw HTML of each tournament result page.
        
        computer_cache (str): optional
            Required if `cache=True`. Indicates which computer is being used ('imac' or 'macbook') to determine where 
            to store cached HTML files.

        store_individual_tourn_data (bool): default False
            If True, stores each tournament's extracted results as a separate CSV file.

        computer (str): default 'macbook'
            Used when `store_individual_tourn_data=True`. Specifies which local file path to use when saving tournament CSVs.
            Must be either 'imac' or 'macbook'.

    Returns
        pd.DataFrame
            A concatenated DataFrame containing match-level data for all tournaments processed.
            Columns include:
            ['match_date', 'player_1', 'player_2', 'duration', 'match_round',
            'player_1_scores', 'player_2_scores', 'winner', 'result', 
            'match_id', 'tournament_id', 'stats_link']

    Raises
        TypeError
            If `tourn_urls` is not a list.
        
        ValueError
            If `cache` is True but `computer_cache` is not provided, or if an invalid `computer` is passed.
    
    Notes
        - The function uses Selenium WebDriver to automate browser interaction. Make sure the correct driver is installed.
        - Helper functions like `extract_match_dates`, `extract_match_player_names`, etc., are assumed to be defined elsewhere.
        - Tournament IDs and years are extracted from the tournament URL.
    """
    if not isinstance(tourn_urls, list):
        raise TypeError("The argument must be a list.")
    if cache and not computer_cache:
        raise ValueError("You must specify 'computer_cache' if caching is enabled.")
    
    atp_url = "https://www.atptour.com/"
    driver = create_driver()
    df = pd.DataFrame(columns=['match_date', 'player_1', 'player_2', 'duration',
                               'match_round', 'player_1_scores', 'player_2_scores',
                               'winner', 'result', 'match_id', 'tournament_id', 'stats_link'])
    
    # --- 1.0 ---
    for tourn_url in tourn_urls:
        
        # --- 0.0 Declare List to Append Data ---
        player_1_list = []
        player_2_list = []
        player_1_scores_list = []
        player_2_scores_list = []
        winner_list = []
        result_list =[]
        match_id_list = []
        stats_link_list = []
        match_date = []
        duration = []
        tournament_id = [tourn_url.split("/")[-3]]
        match_round = []
        tournament = tourn_url.split("/")[-4]
        year = tourn_url.split("/")[-2] 
    
        # --- 1.0 Access link and cache ---
        driver.get(tourn_url)                                                   # Open the tournament results archive page in a Selenium-controlled browser
        time.sleep(random.uniform(5,7))                                         # Let the page load
        soup = BeautifulSoup(driver.page_source, "html.parser")                 # Get HTML and parse with BeautifulSoup
        if cache:
            cache_html(computer_cache, f"{tournament}_{year}_cached_page", driver=driver)
        
        # --- 2.0 Loop Over Match Dates ---
        for sub in soup.select(".atp_accordion-item"):
            date_text = extract_match_dates(sub=sub)
            
            # --- 0.0 Loop Over Individual Matches ---
            for match in sub.select('.match'):
                match_rounds = extract_match_round(match=match)
                durations = extract_match_duration(match=match)
                note_text = extract_match_notes(match=match)
                player_names = extract_match_player_names(match=match)
                winner_name = extract_match_winner(match=match, notes=note_text)
                p_1_scores, p_2_scores = extract_match_score(match=match, notes=note_text)
                result = extract_match_result(note=note_text)
                match_id, stat_link = extract_match_id_and_statlink(match=match, atp_url=atp_url)
                
                # --- 1.0 Store the result ---
                winner_list.append(winner_name)
                result_list.append(result)
                match_id_list.append(match_id)
                stats_link_list.append(stat_link)
                match_date.append(date_text)
                duration.append(durations)
                match_round.append(match_rounds)
                player_1_scores_list.append(p_1_scores)
                player_2_scores_list.append(p_2_scores)
                player_1_list.append(player_names[0] if len(player_names) > 0 else '')
                player_2_list.append(player_names[1] if len(player_names) > 1 else '')
                
        
        # --- 3.0 Create DataFrame ---    
        tournament_id = tournament_id * len(match_id_list)                          # Tournament ID same length array
        df_tournament_result = pd.DataFrame({'match_date': match_date, 'player_1': player_1_list, 'player_2': player_2_list, 'duration': duration,
                                        'match_round': match_round, 'player_1_scores': player_1_scores_list, 'player_2_scores': player_2_scores_list,
                    'winner': winner_list, 'result': result_list, 'match_id': match_id_list, 'tournament_id': tournament_id, 'stats_link': stats_link_list})
        
        # --- 4.0 Store Individual Tournament Data ---
        if store_individual_tourn_data:
                computer = computer.lower()
                if computer == "imac":                                              # iMac
                    path = f"/Users/samueleferrucci/Library/CloudStorage/GoogleDrive-samueleferrucci94@gmail.com/Other computers/My MacBook Air/Coding/Projects/Tennis ML/data/raw/{tournament}_{year}_results.csv"
                elif computer == "macbook":                                         # Macbook
                    path = f"/Users/samueleferrucci/Documents/Coding/Projects/Tennis ML/data/raw/{tournament}_{year}_results.csv"
                else: 
                    raise ValueError("Invalid computer name. Must be 'imac' or 'macbook'.") 
                df_tournament_result.to_csv(path, sep=',', columns=df_tournament_result.columns)
            
        df = pd.concat([df, df_tournament_result])
    
    driver.quit()
    
    return df


















def get_stat_names(soup):

    stat_names = []

    stat_legends = soup.select('div.stats-item-legend')                         # Check for Grand Slam structure first
    if stat_legends:
        for stat in stat_legends:
            try:
                name = stat.text.strip()
                if name:
                    stat_names.append(name)
            except Exception as e:
                print(f"Error extracting stat (GS layout): {e}")
                continue
    else:                                                                       # Fallback to normal ATP layout
        for tile in soup.select("div.topStatsWrapper div.labelWrappper"):
            try:
                stat_name = tile.find("div", class_="labelBold").text.strip()
                if stat_name:
                    stat_names.append(stat_name)
            except Exception as e:
                print(f"Error extracting stat (ATP layout): {e}")
                continue

    p1_stat_names = ["p1_" + s.replace(' ', '_').lower() for s in stat_names]   # Normalize and prefix
    p2_stat_names = ["p2_" + s.replace(' ', '_').lower() for s in stat_names]

    return ["match_id", "tournament_id", "player_1", "player_2", "p1_id", "p2_id"] + p1_stat_names + p2_stat_names    


def extract_stat_players(soup):
    #Find Player names, Ids, URLs
    # --- 1.0 Find Player 1 (For ATP)---
    team1 = soup.find("div", class_="team team1")                                           # Player 1 is inside div with class "team team1" and inside that div class "player"
    if team1:
        
        player1 = team1.find("div", class_="player")

        # --- 2.0 Extract player 1 name, URL and id ---
        ### OUTDTED - GIVES WRONG NAMES ###
                    # player1_name = player1.find("span", class_="name").get_text(strip=True)
                    # player1_name = player1_name.split("  ")                                             # Get name in format "S. Ferrucci" 
                    # player1_name = " ".join([t[0].upper() + t[1:] for t in player1_name])
                    # player1_url = player1.find("a", class_="player-details-anchor")["href"]
                    # player1_id = player1_url.split('/')[-2].lower()
        ### OUTDTED - GIVES WRONG NAMES ###
        
        player1_url = player1.find("a", class_="player-details-anchor")["href"]
        # Extract from href
        link_parts = player1_url.strip("/").split("/")
        name_slug = link_parts[-3]  # 'nishesh-basavareddy'
        player1_id = link_parts[-2].lower()  # 'B0NN'
        # Convert to "N. Basavareddy"
        name_parts = name_slug.split("-")
        if len(name_parts) >= 2:
            first_initial = name_parts[0][0].upper()
            last_name_parts = name_parts[1:]  # Everything after the first word
            last_name = " ".join([part.capitalize() for part in last_name_parts])
            player1_name = f"{first_initial}. {last_name}"
        else:
            # fallback if there's only one part in the name
            player1_name = name_parts[0].capitalize()


        # --- 3.0 Find Player 2 ---
        team2 = soup.find("div", class_="team team2")                                       # Player 2 is inside div with class "team team2" and inside that div class "player player-r"
        player2 = team2.find("div", class_="player player-r")                               # or just class_="player player-r"

        # --- 4.0 Extract player 2 name, URL and id ---
        ### OUTDTED - GIVES WRONG NAMES ###
                    # player2_name = player2.find("span", class_="name").get_text(strip=True)             # Extract player 2 name and URL
                    # player2_name = player2_name.split("  ")                                             # Get name in format "S. Ferrucci" 
                    # player2_name = " ".join([t[0].upper() + t[1:] for t in player2_name])
                    # player2_url = player2.find("a", class_="player-details-anchor")["href"]
                    # player2_id = player2_url.split('/')[-2].lower()
        ### OUTDTED - GIVES WRONG NAMES ###
        
        player2_url = player2.find("a", class_="player-details-anchor")["href"]
        # Extract from href
        link_parts = player2_url.strip("/").split("/")
        name_slug = link_parts[-3]  # 'nishesh-basavareddy'
        player2_id = link_parts[-2].lower()  # 'B0NN'
        # Convert to "N. Basavareddy"
        name_parts = name_slug.split("-")
        if len(name_parts) >= 2:
            first_initial = name_parts[0][0].upper()
            last_name_parts = name_parts[1:]  # Everything after the first word
            last_name = " ".join([part.capitalize() for part in last_name_parts])
            player2_name = f"{first_initial}. {last_name}"
        else:
            # fallback if there's only one part in the name
            player2_name = name_parts[0].capitalize()
        
    # --- 5.0 Find Players (GrandSlams) ---
    else:
        player_links = soup.select("div.atp_match-stats a")
        player1_name = player_links[1].get_text(strip=True) if len(player_links) > 0 else ''
        player2_name = player_links[2].get_text(strip=True) if len(player_links) > 1 else ''

        # Normalize name like in ATP section
        player1_name = " ".join([t[0].upper() + t[1:] for t in player1_name.split(" ")])
        player2_name = " ".join([t[0].upper() + t[1:] for t in player2_name.split(" ")])

        images = soup.select("div.atp_match-stats img.player-image")
        player1_id = images[0].get("src").split("/")[-1] if len(images) > 0 else ''
        player2_id = images[3].get("src").split("/")[-1] if len(images) > 1 else ''
        
        
    
    return player1_name, player1_id, player2_name, player2_id


def extract_player_stats(soup):
    
    p1_values = []
    p2_values = []
    
    tag = soup.select("div.atp_match-stats")
    stat_items_p1 = tag[0].select("div.player-stats-item")
    stat_items_p2 = tag[0].select("div.opponent-stats-item")

    if stat_items_p1 and stat_items_p2:                                      # For Grand Slams

        for item_p1, item_p2 in zip(stat_items_p1, stat_items_p2):
                value_div_p1 = item_p1.find("div", class_="value")
                span_1 = value_div_p1.find("span")
                value_div_p2 = item_p2.find("div", class_="value")
                span_2 = value_div_p2.find("span")
                
                if span_1:
                    # Extract x/y inside ( )
                    ratio = span_1.get_text(strip=True).strip("()")
                    p1_values.append(ratio)
                else:
                    # Extract just the number or percentage
                    text = value_div_p1.get_text(strip=True)
                    p1_values.append(text)
                
                if span_2:
                    # Extract x/y inside ( )
                    ratio = span_2.get_text(strip=True).strip("()")
                    p2_values.append(ratio)
                else:
                    # Extract just the number or percentage
                    text = value_div_p2.get_text(strip=True)
                    p2_values.append(text)
    
    else:                                                                      # For ATP
        # Loop through each stat tile
        for tile in soup.find_all("div", class_="statTileWrapper"):
            if tile.find("div", class_="label player1 non-speed"):
                player1_stat = tile.find("div", class_="label player1 non-speed").text.strip()
                player1_stat = player1_stat.split(" ")
                player1_stat = player1_stat[0]
            elif tile.find("div", class_="labelBold player1 non-speed"):
                player1_stat = tile.find("div", class_="labelBold player1 non-speed").text.strip()
                player1_stat = player1_stat.split(" ")
                player1_stat = player1_stat[0]
            ########## NEW TRIAL ########
            elif tile.find("div", class_="speedInMPH player1"):
                player1_stat =tile.find("div", class_="speedInMPH player1").text.strip()
            ########## NEW TRIAL ########    
            else: player1_stat = ''
            
            p1_values.append(player1_stat) 
            
            if tile.find("div", class_="label player2 non-speed"):
                player2_stat = tile.find("div", class_="label player2 non-speed").text.strip()
                player2_stat = player2_stat.split(" ")
                player2_stat = player2_stat[0]
            elif tile.find("div", class_="labelBold player2 non-speed"):
                player2_stat = tile.find("div", class_="labelBold player2 non-speed").text.strip()
                player2_stat = player2_stat.split(" ")
                player2_stat = player2_stat[0]
            ########## NEW TRIAL ########
            elif tile.find("div", class_="speedInMPH player2"):
                player2_stat =tile.find("div", class_="speedInMPH player2").text.strip()
            ########## NEW TRIAL ########  
            else: player2_stat = ''  
            
            p2_values.append(player2_stat)
    
    return p1_values, p2_values
                    

def get_stats(stat_urls):
    """
    Scrapes player and match statistics from a list of stat URLs.

    Parameters:
    
    stat_urls (list): 
        A list of URLs (strings) pointing to individual tennis match stat pages.

    Returns:
    
    df_stats (pd.DataFrame): 
        A Pandas FataFrame where each column is a stat name (from `master_stat_columns`),
        Missing data is filled with np.nan.
    """

    # --- 1.0 Input validation --- 
    if not isinstance(stat_urls, list):
        raise TypeError("The argument must be a list.")

    # --- 2.0 Initialize column-wise data container ---
    column_data = {col: [] for col in master_stat_columns}

    # --- 3.0 Begin looping through URLs ---
    for match_stat_url in stat_urls:

        # --- 0.1 Handle valid URLs only ---
        if isinstance(match_stat_url, str):
            driver = None                                                # Ensure it's defined in case of early errors

            try:
                # --- 0.2 Extract match and tournament ID ---
                tournId = match_stat_url.split("/")[-2]
                match_id = match_stat_url.split("/")[-1]

                # --- 0.3 Load and parse page ---
                driver = create_driver()
                driver.get(match_stat_url)
                time.sleep(15)                                          # Let the page load completely
                soup = BeautifulSoup(driver.page_source, "html.parser")

                # --- 0.4 Extract stat names and make lowercase ---
                stat_names = [s.lower() for s in get_stat_names(soup)]

                # --- 0.5 Extract players and stats ---
                player1_name, player1_id, player2_name, player2_id = extract_stat_players(soup)
                p1, p2 = extract_player_stats(soup)

                # --- 0.6 Combine into a full match dictionary ---
                stats_dict = dict(zip(
                    stat_names,
                    [match_id, tournId, player1_name, player2_name, player1_id, player2_id] + p1 + p2
                ))

                # --- 0.7 Align data to master column list ---
                aligned_row = {col: stats_dict.get(col, np.nan) for col in master_stat_columns}

                # --- 0.8 Append values column-wise ---
                for col in master_stat_columns:
                    column_data[col].append(aligned_row[col])

            except Exception as e:
                # --- Handle error during scraping/parsing ---
                print(f"Skipping broken or invalid stat page: {match_stat_url} — Error: {e}")
                for col in master_stat_columns:
                    column_data[col].append(np.nan)

            finally:
                # --- Always close browser session ---
                if driver:
                    driver.quit()

        else:
            # --- Handle case where stat URL is not a valid string ---
            print(f"Invalid match_stat_url: {match_stat_url}")
            for col in master_stat_columns:
                column_data[col].append(np.nan)

    df_stats = pd.DataFrame.from_dict(column_data)
    
    return df_stats
