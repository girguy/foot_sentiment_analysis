import sys
import os
import polars as pl
from taipy.gui import Gui, Icon, navigate
import taipy.gui.builder as tgb

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from AzureBlobLoader import AzureBlobLoader
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


with tgb.Page() as general_page:
    with tgb.part(class_name="container"):
        tgb.text("# General", mode="md")
        with tgb.part(class_name="card"):
            with tgb.layout(columns="1 1 1"):
                with tgb.part():
                    tgb.text("", mode="md")
                with tgb.part():
                    tgb.text("Filter Product **Category**", mode="md")
                with tgb.part():
                    tgb.text("Filter Product **Category**", mode="md")
        

with tgb.Page() as team_page:
    tgb.text("# Team", mode="md")

with tgb.Page() as compare_page:
    tgb.text("# Compare", mode="md")

with tgb.Page() as root_page:
    tgb.menu(
        label="Menu",
        lov=[
            ("General"),
            ("Team"),
            ("Compare"),
        ],
        on_action=menu_option_selected,
    )

pages = {"/": root_page, "General": general_page, "Team": team_page, "Compare": compare_page}

if __name__ == "__main__":
    dataframes = load_data()

    Gui(pages=pages).run(
        title="EPL News",
        dark_mode=True,
        debug=True,
        use_reloader=True
    )
