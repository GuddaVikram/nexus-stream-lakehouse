import os
from pathlib import Path
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource, dbt_assets

# Fix: Use standard pathlib syntax (.joinpath or / operator)
DBT_PROJECT_DIR = Path(__file__).parent.joinpath("analytics_warehouse").resolve()
MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# 1. Teach Dagster how to read and execute your dbt project
dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

@dbt_assets(manifest=MANIFEST_PATH)
def analytics_warehouse_dbt_assets(context, dbt: DbtCliResource):
    """Yields your DuckDB staging and mart tables as official Dagster tracked assets."""
    yield from dbt.cli(["run", "--profiles-dir", "."], context=context).stream()

# 2. Define an automated Job execution loop for these assets
refresh_warehouse_job = define_asset_job(
    name="refresh_warehouse_job",
    selection=AssetSelection.assets(analytics_warehouse_dbt_assets)
)

# 3. Schedule the pipeline loop to run automatically every minute
warehouse_scheduler = ScheduleDefinition(
    job=refresh_warehouse_job,
    cron_schedule="* * * * *", # Fires every single minute
)

# 4. Compile everything together into the Dagster control definition plane
defs = Definitions(
    assets=[analytics_warehouse_dbt_assets],
    schedules=[warehouse_scheduler],
    resources={
        "dbt": dbt_resource,
    },
)