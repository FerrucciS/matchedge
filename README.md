# 🎾 MatchEdge: Tennis Match Win Prediction

## Project Overview

MatchEdge is a data-driven project designed to predict outcomes of professional tennis matches using historical player statistics, match results, and tournament data. The project pipeline automates data scraping, cleaning, and feature engineering to produce machine learning-ready datasets and provides a foundation for a deployable prediction system.

Key aspects of the project include:

- **Automated Data Collection & Preprocessing:** Scrapes ATP match results, player rankings, and match statistics, storing raw and cleaned data in AWS S3.  
- **Feature Engineering:** Computes rolling statistics (e.g., last 5 matches), derived metrics such as recent match-play, surface-specific win rates, and match ratios, ensuring no data leakage.  
- **Machine Learning Modeling:** Uses Random Forest classifiers to prioritize predictive power, handle non-linear relationships, and accommodate multicollinear features that would violate logistic regression assumptions.  
- **Insights & Improvements:** Identifies the importance of recent rankings, head-to-head statistics, and other derived features, highlighting opportunities for model refinement and higher predictive performance.


  > **Note:** Data paths in the project (local and S3) are specific to my environment. The project is primarily intended as a **portfolio showcase**, and running the code as-is may require adjustments to paths and data sources.


---

## 📁 Project Structure

```text
Tennis ML/
├── airflow/                          # Airflow DAGs and Docker setup (WIP)
│   └── docker-compose.yml            # Docker configuration file for Airflow
├── cache/                            # Cached raw HTML files from scraping
│   └── *.html                        # Individual cached match pages
├── data/                             # Local datasets (excluded from GitHub)
│   ├── raw/                          # Raw scraped data in CSV format
│   └── clean/                        # Cleaned and structured data
├── dags/                             # Airflow DAGs (currently empty)
├── notebooks/                        # Jupyter notebooks for development & trials
│   ├── 01_scrape_historic_data.ipynb
│   └── 02_clean_and_combine_data.ipynb
│   └── 03_feature_engineer.ipynb
    └── scrape_pipeline_trial.ipynb
│   └── clean_combine_pipeline_trial.ipynb
├── s3_schemas/                       # JSON schemas for validating S3 uploads
│   └── match_schema.json             # Example: schema for match-level data
├── scripts/                          # Python scripts with reusable functions
│   ├── scraping_utils.py             # Scraping helpers
│   └── clean_data.py                 # Data cleaning utilities
├── scrape_pipeline.py                # Main script to scrape and upload data
├── clean_and_combine_pipeline.py     # Main script to clean and prepare data
├── requirements_scrape.txt           # pip requirements for scraping env
├── requirements_cleaning.txt         # pip requirements for cleaning env
├── environment_scrape.yml            # Conda environment file for scraping
├── environment_cleaning.yml          # Conda environment file for cleaning
├── README.md                         # Project documentation (this file)
```



## Setup / Environments

#### 3. **Installation & Setup**


### ⚙️ Setup

1. Clone the repo:

```bash
git clone https://github.com/FerrucciS/matchedge.git
cd tennis-ml
```
2. Install requirements:
3. 
**Using pip**
```bash
# Create a virtual environment
python -m venv <name_of_venv>
# Activate it
# macOS/Linux:
source <name_of_venv>/bin/activate

# Install packages
pip install -r requirements_scrape.txt #requirements_cleaning.txt for cleaning
```

***Using conda***
```bash
# Create and activate the conda environment
# This will install all necessary packages including those in requirements.txt
conda env create -f environment_scrape.yml # environment_cleaning.yml for cleaning
conda activate scraping_env # cleaning_env for cleaning environment
```

⚠️ Note: File paths in the scripts point to my local directories and S3 buckets. To run pipelines successfully, users will need to adjust paths and provide their own datasets.



## Running Pipelines

### 🖥 Running the Scraper

The scraper collects tournament, player rankings, match results, and match stats, then saves them both locally and to S3.

1. **Dependencies**  
   - Requires `selenium`, `beautifulsoup4`, and other packages listed in `requirements_scrape.txt` or `environment_scrape.yml`.
   - Uses `last_scraped_date.csv` on S3 to determine the start date for scraping. If this file does not exist, the script will raise a `FileNotFoundError`.

2. **How it works**  
   - Scrapes the ATP Tour website for tournaments, live rankings, results, and stats.
   - Uses the last scraped date (`last_scraped_date.csv`) to avoid duplicate scraping.
   - Saves today's data both locally and to S3 in the `data/raw/` folder:
     - `top_players_<date>.csv`
     - `All_Tournaments_<date>.csv`
     - `all_results_<date>.csv`
     - `all_stats_GS_<date>.csv`
   - Updates `last_scraped_date.csv` with today's date for the next run.

3. **Running the scraper**

```bash
# Activate your environment
# pip virtualenv
source <name_of_venv>/bin/activate
# or conda
conda activate scraping_env

# Run scraper
python scrape_pipeline.py
```
---

⚠️ Ensure last_scraped_date.csv exists on S3 if running for the first time, or modify the script to provide an initial start_date.

---

### 🧹 Running the Cleaning & Combining Pipeline

The cleaning pipeline processes raw scraped data and produces structured, ML-ready datasets. It combines player stats, match results, and tournament information, and saves cleaned outputs locally and on S3.

1. **Dependencies**  
   - Requires `pandas`, `numpy`, `s3fs`, and other packages listed in `requirements_cleaning.txt` or `environment_cleaning.yml`.
   - Relies on raw CSVs generated by the scraper:
     - `top_players_<date>.csv`
     - `All_Tournaments_<date>.csv`
     - `all_results_<date>.csv`
     - `all_stats_GS_<date>.csv`
   - Uses `last_scraped_date.csv` on S3 to determine the most recent data.

2. **How it works**  
   1. **Read last scraped date** – Ensures only new matches are processed.  
   2. **Load raw data** – Reads top players, tournaments, match results, and stats.  
   3. **Clean data** – Uses utility functions to standardize formats, remove inconsistencies, and normalize stats.  
   4. **Save cleaned datasets** – Writes cleaned CSVs and Parquet files to both local directories and S3:
      - `top_players.csv`
      - `all_tournaments_<date>.csv`
      - `all_results_<date>.csv`
      - `all_stats_<date>.csv`  
   5. **Combine datasets** – Merges results with tournaments and player stats to produce a complete match-level dataset:  
      - `merged_matches_<date>.csv` / `.parquet`  
   6. **Update master dataset** – Concatenates new matches with existing `merged_matches.parquet` on S3, creating a backup for safety.

3. **Running the cleaning pipeline**

```bash
# Activate your environment
# pip virtualenv
source <name_of_venv>/bin/activate
# or conda
conda activate cleaning_env

# Run cleaning pipeline
python clean_and_combine_pipeline.py
```


---
### ⚠️ Dependencies on Previously Scraped Data

The cleaning and combination pipeline assumes that raw match data has already been scraped and uploaded to S3. Specifically:

1. **`last_scraped_date.csv`**  
   - Location: `s3://matchedge-pipeline/logs/last_scraped_date.csv`  
   - Purpose: Tracks the last date the scraper ran. The pipeline uses this to determine which new files to process.
   - If this file does not exist, the pipeline will fail.

2. **Raw scraped files**  
   - Expected files in S3 (with date suffixes from `last_scraped_date.csv`):
     - `top_players_<date>.csv`
     - `All_Tournaments_<date>.csv`
     - `all_results_<date>.csv`
     - `all_stats_GS_<date>.csv`  
   - The pipeline loads these, cleans them, and saves processed outputs both locally and to S3.

3. **Merged datasets**  
   - After scraping, the pipeline combines tournament, results, and stats data into `merged_matches_<date>.csv/parquet`.
   - It also updates the full `merged_matches.parquet` by appending the new matches.

---

## Running Pipelines (Apache Airflow)




## 📝 Final Data Preparation & Feature Engineering


The `notebooks/` folder contains Jupyter notebooks used for experimenting, cleaning, and preparing data for modeling. In particular:

- **`03_feature_engineer.ipynb`**  
  This notebook performs the final steps to get the match data ready for machine learning:
  - Handles missing values and imputations.
  - Creates rolling features (e.g., last 5 matches statistics) for both players, ensuring no data leaks.
  - Generates opponent-specific features by merging player stats.
  - Feature engineers advanced match statistics such as:
    - Straight sets won/lost
    - Win rate ratios per surface (clay, hard, grass, indoor)
    - Recent match-play and other derived metrics
  - Normalizes numeric stats where necessary.
  - Encodes categorical variables (e.g., surface, match round).
  - Splits data into training and test sets in a time-aware manner to prevent leakage.
  - Performs feature selection for the Random Forest model based on importance scores.
  - Conducts grid search for hyperparameter tuning of the Random Forest model (`n_estimators`, `max_depth`, `min_samples_leaf`, `max_features`).
  - Fits the best Random Forest model and evaluates it using accuracy and ROC AUC metrics.


**Why these choices:**

- **Random Forest** was selected because:
  - Predictive power is prioritized over transparency.
  - Not all variables are linearly related to match outcome.
  - Some features are multicollinear, which logistic regression assumptions cannot handle well.

**Improvements:**
- **Date-specific player rankings** capture recent form, providing a strong signal of performance leading into each match.  
- **Head-to-head statistics** help model player-specific rivalries and historical outcomes between opponents.


- **Purpose:**  
  This notebook allows for experimentation and iterative feature engineering without affecting the main pipeline. Once the final features are ready, they are saved and used in the modeling scripts for prediction.