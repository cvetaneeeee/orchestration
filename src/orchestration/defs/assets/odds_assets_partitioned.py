import asyncio
import dagster as dg
from dagster_duckdb import DuckDBResource
from .links_assets import extract_links_async
from .odds_assets import process_odds_async, create_odds_table, upsert_odds_data, upsert_postgres_df
from orchestration.source_code.config import EXT_POSTGRES_URL

league_partition_def = dg.StaticPartitionsDefinition(
    ["epl", "laliga", "seriea", "bundesliga", "ligue1"]
)

@dg.asset(kinds={"duckdb"})
def create_odds_table_asset(duckdb: DuckDBResource):
    return create_odds_table(duckdb=duckdb)

@dg.asset(partitions_def=league_partition_def, required_resource_keys={"config"})
def extract_links_asset(context: dg.AssetExecutionContext):
    league_key = context.partition_key
    config = context.resources.config
    result = asyncio.run(extract_links_async(config, league_key=league_key))
    context.log.info(f'✅ Collected {len(result)} match IDs for {league_key}')
    return result

@dg.asset(partitions_def=league_partition_def, required_resource_keys={"config"})
def process_odds_asset(context: dg.AssetExecutionContext, extract_links_asset):
    league_key = context.partition_key
    config = context.resources.config
    df = process_odds_async(url_list=extract_links_asset, config=config, league_key=league_key)
    return df

@dg.asset(kinds={"duckdb"}, partitions_def=league_partition_def)
def upsert_odds_asset(duckdb: DuckDBResource, process_odds_asset):
    return upsert_odds_data(duckdb=duckdb, df=process_odds_asset)

PARTITION_TO_COMPETITION = {
    "epl": "premier-league",
    "laliga": "laliga",
    "seriea": "serie-a",
    "bundesliga": "bundesliga",
    "ligue1": "ligue-1",
}

@dg.asset(kinds={"python"}, partitions_def=league_partition_def, deps=["upsert_odds_asset"])
def load_odds_to_postgres(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    league_key = context.partition_key
    competition = PARTITION_TO_COMPETITION[league_key]
    with duckdb.get_connection() as con:
        df = con.execute(
            "SELECT * FROM historical_odds WHERE competition = ?", [competition]
        ).fetchdf()
    context.log.info(f"📦 Loaded {len(df)} rows from DuckDB for competition='{competition}'")
    upsert_postgres_df(pg_url=EXT_POSTGRES_URL, df=df, table="historical_odds")
    return f"Postgres load completed: {len(df)} rows for {competition}"

