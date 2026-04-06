import pandas as pd
import dagster as dg
from dagster_duckdb import DuckDBResource
from sqlalchemy import create_engine, text
from orchestration.source_code.rapidapi_fixtures import get_league_response, build_dataframe
from orchestration.source_code.config import EXT_POSTGRES_URL


def create_table(duckdb: DuckDBResource, table_name: str):
    """
    Create the necessary tables in the DuckDB database.
    """
    # Create table for fixtures
    with duckdb.get_connection() as con:
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            weekday TEXT,
            round TEXT,
            date TIMESTAMP,
            home_team TEXT,
            home_goals INTEGER,
            away_goals INTEGER,
            away_team TEXT,
            league TEXT,
            season TEXT,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY (round, home_team, away_team)
        );
        """)
        print("Fixtures table created or already exists.")
        return


def upsert_df(duckdb: DuckDBResource, df: pd.DataFrame, table: str):
    with duckdb.get_connection() as con:
        con.register("fixtures_df", df)
        con.execute(f"""
            INSERT INTO {table}
            SELECT * FROM fixtures_df
            ON CONFLICT(season, round, home_team, away_team) DO UPDATE SET
            weekday = excluded.weekday,
            date = excluded.date,
            home_goals = excluded.home_goals,
            away_goals = excluded.away_goals,
            league = excluded.league,
            updated_at = excluded.updated_at;
        """
        )
        con.commit()
        con.unregister("fixtures_df")
        print(f"Upserted {len(df)} rows into {table} table.")
        return


@dg.asset(required_resource_keys={"fixtures_config"}) #, key=["target", "fixtures", "extract_fixtures"])
def extract_fixtures(context) -> pd.DataFrame:
    """
    Extract fixtures from RapidAPI.
    """
    config_dict = context.resources.fixtures_config
    print(f"Config: {config_dict}")
    league_ids = config_dict.get("league_ids", {})
    api_key = config_dict.get("api_key", "")
    season = config_dict.get("year", "")
    results = config_dict.get("results", None)

    all_leagues = []
    try:
        for key, value in league_ids.items():
            print(f"Processing league {key} with ID {value} for season {season}")
            response = get_league_response(league_id=value, league_str=key, api_key=api_key, season=season, resp_size=results)
            print(f"Response for league {key} in season {season}: {response}")
            if not response:
                context.log.info(f"No data found for league {key} in season {season}.")
                continue
            data = build_dataframe(output=response, league=key, season=int(season))
            context.log.info(f'✅ Extracted {len(data)} fixtures for season {season} in {key}: {data}')
            print(f'✅ Extracted {len(data)} fixtures for season {season} in {key}: {data}')
            all_leagues.append(data)
        df = pd.concat(all_leagues, ignore_index=True)
        if not df.empty:
            df[['inserted_at', 'updated_at']] = pd.Timestamp.now()
        return df
        # df['date'] = pd.to_datetime(df['date'])

    except Exception as e:
        print(f'Exception: {str(e)}')
        return pd.DataFrame()
    

@dg.asset(kinds={"duckdb"}) #, key=["target", "fixtures", "create_fixtures_table"])
def create_fixtures_table(duckdb: DuckDBResource) -> str:
    """
    Create the fixtures table in the DuckDB database.
    """
    create_table(duckdb=duckdb, table_name="fixtures")
    return "fixtures"


@dg.asset(kinds={"duckdb"}, ins={"df": dg.AssetIn("extract_fixtures")}, deps=["create_fixtures_table"])
def upsert_fixtures_data(duckdb: DuckDBResource, df: pd.DataFrame) -> str:
    """
    Upsert the fixtures data into the fixtures table.
    """
    if df.empty:
        return "No data to upsert."

    upsert_df(duckdb=duckdb, df=df, table="fixtures")
    return "Upsert completed successfully."


@dg.asset(kinds={"python"}, deps=["upsert_fixtures_data"], auto_materialize_policy=dg.AutoMaterializePolicy.eager())
def load_fixtures_to_postgres(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """
    Load fixtures from DuckDB into the external Postgres instance.
    """
    with duckdb.get_connection() as con:
        df = con.execute("SELECT * FROM fixtures").fetchdf()
    context.log.info(f"📦 Loaded {len(df)} fixtures rows from DuckDB")

    if not EXT_POSTGRES_URL or df.empty:
        context.log.warning("Missing Postgres URL or empty DataFrame. Skipping.")
        return "Skipped."

    expected_columns = [
        'weekday', 'round', 'date', 'home_team', 'home_goals',
        'away_goals', 'away_team', 'league', 'season', 'updated_at'
    ]
    df_to_insert = df[expected_columns].copy().drop_duplicates(
        subset=['round', 'home_team', 'away_team'], keep='last'
    )

    engine = create_engine(EXT_POSTGRES_URL)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fixtures (
                weekday TEXT,
                round TEXT,
                date TIMESTAMP,
                home_team TEXT,
                home_goals INTEGER,
                away_goals INTEGER,
                away_team TEXT,
                league TEXT,
                season TEXT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (round, home_team, away_team)
            );
        """))

        temp_table = "temp_fixtures"
        df_to_insert.to_sql(temp_table, con=conn, if_exists='replace', index=False)

        set_clause = ",\n".join([f"{col} = EXCLUDED.{col}" for col in expected_columns if col not in ('round', 'home_team', 'away_team')])
        conn.execute(text(f"""
            INSERT INTO fixtures ({', '.join(expected_columns)})
            SELECT * FROM {temp_table}
            ON CONFLICT (round, home_team, away_team) DO UPDATE SET
            {set_clause};
        """))
        conn.execute(text(f"DROP TABLE {temp_table};"))

    context.log.info(f"✅ Upserted {len(df_to_insert)} rows into Postgres fixtures table.")
    return f"Postgres load completed: {len(df_to_insert)} rows"