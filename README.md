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
- **Polars** or **FireDucks**: Efficient data manipulation and processing of large datasets.
- **PostgreSQL**: Database to store cleaned and processed data.
- **Sentiment Analysis**: Implemented using NLP libraries such as:
  - **TextBlob**
  - **VADER**
  - **Transformers** (for more advanced sentiment classification models).
- **Streamlit** or **Taipy**: Framework for building the interactive dashboard.
- **Poetry**: Dependency management and virtual environment handling.
- **Git**: Version control and collaboration.

## TO DO :
- filtering out rows having strings such as : "Catch up on the Premier League action" **DONE**
- filtering out rows having strings such as : "Follow [Monday|Tuesday|...|Sunday]'s Premier League games" **DONE**
- filtering out rows having strings such as : "Follow [Monday|Tuesday|...|Sunday]'s Carabao Cup" **DONE**
- filtering out rows having strings such as : "Follow teamA v teamB" **DONE**
- filtering out rows having strings such as : "check "who is your team facing?" **DONE**
- filtering out rows having strings such as : "send us your thoughts" **DONE**
- the cleaning needs to be done in process_raw_epl_news.py **DONE**
- look in the title ->  "the fans' verdict"
  - look in content -> "Here are some of your comments" then extract the fan reactions
- scrap 10 pages instead of two
- one time a week -> purge raw -> create new job

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



