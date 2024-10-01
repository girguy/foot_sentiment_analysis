import warnings
import dagster

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
    with_source_code_references,
)

from . import assets  # assets is a package module

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)


all_assets = with_source_code_references(
    [
        *load_assets_from_package_module(assets),
    ]
)

defs = Definitions(
    assets=all_assets,
    schedules=[daily_refresh_schedule],
)
