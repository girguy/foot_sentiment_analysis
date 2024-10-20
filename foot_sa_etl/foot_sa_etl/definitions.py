import warnings
import dagster

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
    with_source_code_references,
)

# assets is a package module
# (name of the folder containing the assets)
# from . import assets
# it's not needed to load bronze_assets, because they will be
# automatically loaded thanks to silver_assets
from .assets.bronze_assets.scrappe_epl_news import scrappe_epl_news
from .assets.silver_assets.process_raw_epl_news import process_raw_epl_news
from .assets.gold_assets.reaction import reaction
from .assets.gold_assets.article import article
from .assets.gold_assets.dim_assets.dim_article import dim_article
from .assets.gold_assets.dim_assets.dim_team import dim_team
from .assets.gold_assets.dim_assets.dim_date import dim_date
from .assets.gold_assets.dim_assets.dim_sentiment import dim_sentiment
from .assets.gold_assets.fact_assets.fact_reaction import fact_reaction
from .assets.gold_assets.fact_assets.fact_title import fact_title
from .assets.gold_assets.fact_assets.fact_sentiment_trend import fact_sentiment_trend


# from assets import 

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

# To automatically attach code references to Python assets'
# function definitions, you can use the with_source_code_references
# utility. Any asset definitions passed to the utility will have
# their source file attached as metadata.

all_assets = with_source_code_references(
    [
        scrappe_epl_news,
        process_raw_epl_news,
        reaction, article,
        dim_article, dim_team, dim_date, dim_sentiment,
        fact_reaction, fact_title, fact_sentiment_trend
    ]
)

defs = Definitions(
    assets=all_assets,
    schedules=[daily_refresh_schedule],
)
