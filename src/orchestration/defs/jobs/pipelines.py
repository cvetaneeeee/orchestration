import dagster as dg
from dagster import define_asset_job
from ..assets.fixtures_assets import create_fixtures_table, extract_fixtures, upsert_fixtures_data
from ..assets.odds_assets_partitioned import league_partition_def



@dg.job
def fixtures_pipeline_job():
    create_fixtures_table()
    df = extract_fixtures()
    upsert_fixtures_data(df=df)


odds_job = define_asset_job(
    name="odds_job",
    selection=[
        "create_odds_table_asset",
        "extract_links_asset",
        "process_odds_asset",
        "upsert_odds_asset",
    ],
    partitions_def=league_partition_def
)


# staging_job = define_asset_job(
#     name="staging_job",
#     selection=["stage_odds_data"]
# )


# fact_job = define_asset_job(
#     name="fact_job",
#     selection=["fact_odds_data"]
# )
