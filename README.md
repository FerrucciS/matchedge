# 🎾 MatchEdge: Tennis Match Win Prediction

## Project Overview

MatchEdge is a data-driven project aimed at predicting the outcomes of professional tennis matches using historical player statistics and match data. The pipeline scrapes, cleans, and processes tennis data into a machine learning-ready format and serves as a foundation for a future deployment-ready prediction system.

The project focuses on:

- Automating data collection and preprocessing.
- Storing and managing data in AWS S3.
- Preparing clean data for ML models and dashboards.

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
│   ├── 02_clean_and_combine_data.ipynb
│   ├── scrape_pipeline_trial.ipynb
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

Give steps to recreate your environment.

### ⚙️ Setup

1. Clone the repo:

```bash
git clone https://github.com/FerrucciS/matchedge.git
cd tennis-ml
```

## Running Pipelines

## Running Pipelines (Apache Airflow)

## S3 Structure

## Data Streamer

## Roadmap
