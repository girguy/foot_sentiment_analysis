import pandas as pd


class DataTransformer:
    """
    Class to handle data transformations related to articles, reactions, and sentiment analysis.
    """

    def __init__(self, dataframes):
        self.df_date = dataframes['df_date']
        self.df_article = dataframes['df_article']
        self.df_team = dataframes['df_team']
        self.df_sentiment = dataframes['df_sentiment']
        self.df_fact_reaction = dataframes['df_fact_reaction']
        self.df_fact_reaction = self._prepare_fact_reaction(dataframes['df_fact_reaction'])
        self.df_fact_title = self._prepare_fact_title(dataframes['df_article'])

    def _prepare_fact_title(self, df_fact_title):
        """Filters and deduplicates the article dataset."""
        return df_fact_title.dropna(subset=["article_id"]).drop_duplicates()

    def _prepare_fact_reaction(self, df_fact_reaction):
        """Filters and deduplicates the article dataset."""
        return df_fact_reaction.dropna(subset=["reaction_id"]).drop_duplicates()

    def count_article_reaction(self):
        """Counts the number of articles and reactions per team."""
        df_nb_articles = (
            self.df_fact_title.groupby("fk_team_id")["article_id"]
            .nunique()
            .reset_index()
            .rename(columns={"article_id": "count_article"})
            .merge(self.df_team, left_on="fk_team_id", right_on="team_id", how="inner")
            .drop(columns=["fk_team_id"])
            .sort_values(by="count_article")
        )

        df_nb_reaction = (
            self.df_fact_reaction.groupby("fk_team_id")["reaction_id"]
            .nunique()
            .reset_index()
            .rename(columns={"reaction_id": "count_reaction"})
            .merge(self.df_team, left_on="fk_team_id", right_on="team_id", how="inner")
            .drop(columns=["fk_team_id"])
            .sort_values(by="count_reaction")
        )

        df_count = (
            df_nb_articles
            .merge(df_nb_reaction, on=["team_id", "team_name"], how="left")
        )
        df_count["count_total"] = df_count[["count_article", "count_reaction"]].sum(axis=1, skipna=True)
        df_count = df_count[['team_name', 'count_article', 'count_reaction', 'count_total']]
        return df_count

    def compute_subjectivity_per_team(self):
        """Computes the mean subjectivity score per team."""
        return (
            self.df_fact_reaction.groupby("fk_team_id")["subjectivity_score"]
            .mean()
            .reset_index()
            .rename(columns={"subjectivity_score": "subjectivity_mean"})
            .merge(self.df_team, left_on="fk_team_id", right_on="team_id", how="inner")
            .loc[:, ['team_name', 'subjectivity_mean']]
        )

    def compute_sentiment_analysis(self):
        """Performs sentiment analysis per team."""
        df = self.df_fact_reaction \
            .groupby(['fk_team_id', 'fk_sentiment_id']) \
            .agg(count_reaction=('reaction_id', 'nunique')) \
            .reset_index() \
            .merge(self.df_sentiment, left_on="fk_sentiment_id", right_on="sentiment_id", how="left") \
            .merge(self.df_team, left_on="fk_team_id", right_on="team_id", how="inner") \
            .drop(columns=["fk_team_id", "fk_sentiment_id", "sentiment_value"])

        df_pivot = df.pivot_table(index="team_name", columns="sentiment_label", values="count_reaction", fill_value=0) \
            .reset_index() \
            .sort_values(by="team_name")

        df_pivot.columns.name = None

        df_pivot['total_count'] = df_pivot[['positive', 'neutral', 'negative']].sum(axis=1)
        df_pivot['p_positive'] = df_pivot['positive'] / df_pivot['total_count']
        df_pivot['p_neutral'] = df_pivot['neutral'] / df_pivot['total_count']
        df_pivot['p_negative'] = df_pivot['negative'] / df_pivot['total_count']

        return df_pivot[['team_name', 'positive', 'neutral', 'negative', 'total_count', 'p_positive', 'p_neutral', 'p_negative']]

    def compute_overall_sentiment(self):
        """Computes the overall sentiment score per team."""
        return (
            self.df_fact_reaction.groupby("fk_team_id")["sentiment_score"]
            .mean()
            .reset_index()
            .rename(columns={"sentiment_score": "mean_overall_feeling"})
            .merge(self.df_team, left_on="fk_team_id", right_on="team_id", how="inner")
            .drop(columns=["fk_team_id"])
            .loc[:, ['team_name', 'mean_overall_feeling']]
        )

    def compute_cumulative_sentiment(self):
        """Computes cumulative sentiment score per team over time."""
        df_result = (
            self.df_fact_reaction.groupby(["fk_team_id", "fk_date_id"])["sentiment_score"]
            .sum()
            .reset_index()
            .sort_values(by=["fk_team_id", "fk_date_id"])
        )
        df_result["cumulative_value"] = (
            df_result.groupby("fk_team_id")["sentiment_score"].cumsum()
        )
        df_result = df_result.merge(self.df_team, left_on="fk_team_id", right_on="team_id", how="inner")
        df_result = df_result.rename(columns={"fk_date_id": "date"})
        return df_result[['team_name', 'date', 'sentiment_score', 'cumulative_value']]

    def compute_total_sentiments(self):
        """Computes the total number of each sentiment across all teams."""
        df_sentiment_analysis = self.compute_sentiment_analysis()
        total_sentiments = {
            'positive': df_sentiment_analysis['positive'].sum(),
            'neutral': df_sentiment_analysis['neutral'].sum(),
            'negative': df_sentiment_analysis['negative'].sum()
        }
        df_total_sentiments = pd.DataFrame(list(total_sentiments.items()), columns=['sentiment', 'total'])
        return df_total_sentiments

    def get_teams(self):
        return self.df_team['team_name'].unique()
