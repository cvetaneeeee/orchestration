import dagster as dg
from dagster import define_asset_job
from ..assets.odds_assets_partitioned import league_partition_def


fixtures_pipeline_job = define_asset_job(
    name="fixtures_pipeline_job",
    selection=[
        "create_fixtures_table",
        "extract_fixtures",
        "upsert_fixtures_data",
    ],
)


odds_job = define_asset_job(
    name="odds_job",
    selection=[
        "create_odds_table_asset",
        "extract_links_asset",
        "process_odds_asset",
        "upsert_odds_asset",
        "load_odds_to_postgres",
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
