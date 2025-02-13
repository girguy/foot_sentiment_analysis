import sys
import os
import pandas as pd
from taipy.g ui import Gui, Icon, navigate, Markdown
import taipy.gui.builder as tgb

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from AzureBlobLoader import AzureBlobLoader
from DataTransformer import DataTransformer
from utils.config import CONFIG, CONN_STRING_AZURE_STORAGE


def load_data():
    try:
        loader = AzureBlobLoader(CONFIG, CONN_STRING_AZURE_STORAGE)
        dataframes = loader.load_dataframes()
        print("Data loaded successfully!")
        return dataframes
    except Exception as e:
        print(f"Error loading data: {e}", exc_info=True)


def menu_option_selected(state, action, info):
    page = info["args"][0]
    navigate(state, to=page)


dataframes = load_data()

if dataframes:
    transformer = DataTransformer(dataframes)

    df_count = transformer.count_article_reaction()
    df_subjectivity = transformer.compute_subjectivity_per_team()
    df_sentiment_analysis = transformer.compute_sentiment_analysis()
    df_overall_sentiment = transformer.compute_overall_sentiment()
    df_cumulative_sentiment = transformer.compute_cumulative_sentiment()
    df_total_sentiment = transformer.compute_total_sentiments()

    team_name_choice = 'AFC Bournemouth'
    df_cumulative_sentiment_choice = df_cumulative_sentiment[df_cumulative_sentiment['team_name'] == team_name_choice]
    df_overall_sentiment_choice = df_overall_sentiment[df_overall_sentiment['team_name'] == team_name_choice]


with tgb.Page() as general_page:
    with tgb.part(class_name="container"):
        tgb.text("# General", mode="md")
        with tgb.part(class_name="card"):
            with tgb.layout(columns="2 1"):
                with tgb.part():
                    tgb.chart("{df_count}", type="bar", x="count_total", y__1="team_name", orientation='h')
                with tgb.part():
                    tgb.chart("{df_total_sentiment}", type="pie", values="total", labels="sentiment")

    with tgb.part(class_name="container"):
        tgb.html("br")
        with tgb.part(class_name="card"):
            with tgb.layout(columns="2 1"):
                with tgb.part():
                    layout = {"barmode": "stack"}
                    tgb.chart(
                        "{df_sentiment_analysis}", type="bar", x="team_name", y__1="positive", y__2="negative", y__3="neutral",
                        layout="{layout}", orientation='v'
                        )
                with tgb.part():
                    tgb.chart("{df_total_sentiment}", type="pie", values="total", labels="sentiment")

with tgb.Page() as team_page:
    with tgb.part(class_name="container"):
        tgb.text("# Team", mode="md")
        teams = transformer.get_teams()
        tgb.selector(
            value="{teams}",
            lov=teams,
            #on_change=change_category,
            dropdown=True,
        )
        tgb.html("br")
    with tgb.part(class_name="container"):
        with tgb.layout(columns="1 1 1", gap="25px", columns__mobile=1):
            with tgb.part():
                Markdown("""
                    <|card|
                    **Deaths**{: .color-primary}
                    <|{to_text(df_overall_sentiment_choice.iloc[-1]['mean_overall_feeling'])}|text|class_name=h3|>
                    |>
                """)
            with tgb.part():
                value=10
                tgb.metric("{value}", min=0, max=100)
            with tgb.part():
                tgb.chart("{df_overall_sentiment_choice}", type="pie", values="mean_overall_feeling", labels="team_name")

    
    tgb.html("br")
    with tgb.part(class_name="container"):
        tgb.chart(
            "{df_cumulative_sentiment_choice}",
            mode = "lines",
            x = "date",
            y__1="cumulative_value"
        )



with tgb.Page() as root_page:
    tgb.menu(
        label="Menu",
        lov=[
            ("General"),
            ("Team")
        ],
        on_action=menu_option_selected,
    )

pages = {"/": root_page, "General": general_page, "Team": team_page}

if __name__ == "__main__":

    Gui(pages=pages).run(
        title="EPL News",
        dark_mode=True,
        debug=True,
        use_reloader=True
    )
