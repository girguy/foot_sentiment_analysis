# Soccer Fan Sentiment Analysis Dashboard

## Project Overview
The **Soccer Fan Sentiment Analysis Dashboard** is designed to analyze the sentiments of soccer fans the English Premier League teams. The project extracts data from various social media platforms and sports news websites, applies sentiment analysis, and visualizes the results in an interactive dashboard.

## Objectives
- **Data Extraction**: Scrape and collect fan opinions and reactions from:
  - **Twitter**: Fan tweets using hashtags related to teams, leagues, or matches.
  - **Reddit**: Discussions from soccer-related subreddits.
  - **Sports News Websites**: Articles and comments from BBC Sports, Marca, Gazzetta dello Sport, Kicker, and L'Equipe.
- **Sentiment Analysis**: Classify sentiments as positive, negative, or neutral using NLP techniques.
- **Dashboard**: Build a dynamic, user-friendly dashboard using **Streamlit** or **Taipy** to visualize the sentiment analysis results.
- **Automated Pipeline**: Orchestrate the entire data pipeline using **Dagster** for automation and scheduling.

## Data Sources
- **Twitter**: Collect tweets mentioning teams, leagues, and matches using relevant hashtags.
- **Reddit**: Extract posts and comments from soccer-related subreddits (e.g., `/r/soccer`, `/r/epl`, `/r/LaLiga`).
- **BBC Sports**, **Marca**, **Gazzetta dello Sport**, **Kicker**, **L'Equipe**: Scrape articles and user comments related to soccer matches and fan discussions.

## Tools & Technologies
- **Dagster**: Task orchestration and pipeline management.
- **Python**: Main programming language for data extraction, transformation, and analysis.
- **Polars**: Efficient data manipulation and processing of large datasets.
- **PostgreSQL**: Database to store cleaned and processed data.
- **Sentiment Analysis**: Implemented using NLP libraries such as:
  - **TextBlob**
- **Streamlit** or **Taipy**: Framework for building the interactive dashboard.
- **UV**: Dependency management and virtual environment handling.
- **Git**: Version control and collaboration.

## TO DO :
- scrapper_config.json should be at the root of the project **only**
- In table articles, delete 'no title found line'
- utils should be loaded as a package ! **important** (and be put at the root of the project)
- ".env" should be at the root of the project
- one time a week -> purge raw -> create new job
- Start Taipy tutorial

## Project Structure
```bash
├── data/                   # Raw and processed data
├── dags/                   # Dagster pipelines
├── scripts/                # Python scripts for data extraction and sentiment analysis
├── dashboard/              # Streamlit or Taipy dashboard code
├── pyproject.toml          # Poetry dependency file
├── poetry.lock             # Locked dependencies
└── README.md               # Project documentation

PS : This is not the right project structure. I need a tool extracting the right one