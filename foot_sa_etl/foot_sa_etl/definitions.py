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
from .assets import bronze_assets, silver_assets, gold_assets, example_assets

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
        *load_assets_from_package_module(example_assets),
    ]
)

defs = Definitions(
    assets=all_assets,
    schedules=[daily_refresh_schedule],
)
