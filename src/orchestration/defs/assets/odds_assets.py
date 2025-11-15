
import asyncio
import pandas as pd
import dagster as dg
from dagster_duckdb import DuckDBResource
from orchestration.source_code.odds_async import run_odds_async


def create_table(duckdb: DuckDBResource, table_name: str):
    """
    Create the necessary tables in the DuckDB database.
    """
    with duckdb.get_connection() as con:
        if table_name == "historical_odds":
            con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT,
                bookie TEXT,
                competition TEXT,
                season TEXT,
                home_team TEXT,
                away_team TEXT,
                opening_time TIMESTAMP,
                closing_time TIMESTAMP,
                home_win_opening FLOAT,
                draw_opening FLOAT,
                away_win_opening FLOAT,
                home_win_closing FLOAT,
                draw_closing FLOAT,
                away_win_closing FLOAT,
                home_score TEXT,
                away_score TEXT,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (id)
            );
            """)
            print(f"✅ {table_name} table created or already exists.")
            return


def upsert_df(duckdb: DuckDBResource, df: pd.DataFrame, table: str):
    expected_columns = [
        'id', 'bookie', 'competition', 'season',
        'home_team', 'away_team',
        'opening_time', 'closing_time',
        'home_win_opening', 'draw_opening', 'away_win_opening',
        'home_win_closing', 'draw_closing', 'away_win_closing',
        'home_score', 'away_score', 'updated_at'
    ]
    df_to_insert = df[expected_columns].copy()

    with duckdb.get_connection() as con:
        con.register(f"{table}_df", df_to_insert)
        con.execute(f"""
            INSERT INTO {table} ({', '.join(expected_columns)})
            SELECT * FROM {table}_df
            ON CONFLICT(id) DO UPDATE SET
                bookie = excluded.bookie,
                competition = excluded.competition,
                season = excluded.season,
                home_team = excluded.home_team,
                away_team = excluded.away_team,
                opening_time = excluded.opening_time,
                closing_time = excluded.closing_time,
                home_win_opening = excluded.home_win_opening,
                draw_opening = excluded.draw_opening,
                away_win_opening = excluded.away_win_opening,
                home_win_closing = excluded.home_win_closing,
                draw_closing = excluded.draw_closing,
                away_win_closing = excluded.away_win_closing,
                home_score = excluded.home_score,
                away_score = excluded.away_score,
                updated_at = excluded.updated_at;
        """
        )
        con.commit()
        con.unregister(f"{table}_df")
        print(f"✅ Upserted {len(df)} rows into {table} table.")
        print("-----------------------------------------------")
        return


def process_odds_async(url_list: list[str], config, league_key: str) -> pd.DataFrame:
    """
    Prepare odds dataframe for insertion into the DB.
    """
    config_dict = config.get(league_key, {})

    league      = config_dict.get("league", "")
    season      = config_dict.get("season", "")
    bookie_id   = config_dict.get("bookie_id", "")
    bookie_name = config_dict.get("bookie_name", "")

    df = asyncio.run(
        run_odds_async(
            urls=url_list,
            league=league,
            season=season,
            bookie_id=bookie_id,
            bookie_name=bookie_name,
            competition=league
        )
    )

    if not df.empty:
        df[['inserted_at', 'updated_at']] = pd.Timestamp.now()
        return df

    return pd.DataFrame()


def create_odds_table(duckdb: DuckDBResource) -> str:
    """
    Create the historical_odds table in the DuckDB database.
    """
    create_table(duckdb=duckdb, table_name="historical_odds")
    return "historical_odds"


def upsert_odds_data(duckdb: DuckDBResource, df: pd.DataFrame) -> str:
    if df.empty:
        return "No data to upsert."
    upsert_df(duckdb=duckdb, df=df, table="historical_odds")
    return "Upsert completed successfully"
