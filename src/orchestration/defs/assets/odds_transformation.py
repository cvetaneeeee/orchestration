import pandas as pd
import dagster as dg
from itertools import product
from sqlalchemy import create_engine, text
from dagster_duckdb import DuckDBResource
from orchestration.source_code.config import EXT_POSTGRES_URL


@dg.asset(required_resource_keys={"duckdb"}, deps=["upsert_odds_asset"], auto_materialize_policy=dg.AutoMaterializePolicy.eager())
def stage_odds_data(context):
    """
    Executes transformations on historical odds and fixtures data and stores the results in DuckDB tables.
    """
    duckdb = context.resources.duckdb

    # Query 1: Surprises per round
    surprises_query = """
    WITH src AS (
        SELECT
            f.*,
            h.*,
            SUM(
                CASE
                    WHEN 
                        (h.home_win_opening >= 4 AND h.home_score > h.away_score)
                        OR (h.draw_opening >= 4 AND h.home_score = h.away_score)
                        OR (h.away_win_opening >= 4 AND h.home_score < h.away_score)
                    THEN 1
                    ELSE 0
                END
            ) OVER (PARTITION BY f.round, h.season, h.competition) AS surprises
        FROM main.historical_odds h
        LEFT JOIN main.fixtures f 
            ON h.home_team = f.home_team 
            AND h.away_team = f.away_team 
            AND h.season = f.season
    )
    SELECT
        CAST(hash(competition || season || SPLIT(round, ' - ')[2]) % 9223372036854775807 AS BIGINT) AS surprises_id
        , competition
        , season
        , CAST(SPLIT(round, ' - ')[2] AS INTEGER) AS round
        , MIN(surprises) AS surprises
    FROM src
    WHERE CAST(SPLIT(round, ' - ')[2] AS INTEGER) IS NOT NULL
    GROUP BY 1, 2, 3, 4
    ORDER BY competition, season, round;
    """

    surprises_clubs_query = """
    WITH stage AS (    
        SELECT
            CAST( hash(h.competition || h.season || SPLIT(f.round, ' - ')[2] || h.home_team || h.away_team ) % 9223372036854775807 AS BIGINT) AS surprises_id
            , h.competition
            , h.season
            , CASE
                WHEN (home_win_opening >= 4 AND home_score > away_score) OR (draw_opening >= 4 AND home_score = away_score) OR (away_win_opening >= 4 AND home_score < away_score) THEN h.home_team
            END AS home
            , CASE
                WHEN (home_win_opening >= 4 AND home_score > away_score) OR (draw_opening >= 4 AND home_score = away_score) OR (away_win_opening >= 4 AND home_score < away_score) THEN h.away_team
            END AS away
            , CASE
                WHEN (home_win_opening >= 4 AND home_score > away_score) OR (draw_opening >= 4 AND home_score = away_score AND home_win_opening > away_win_opening) THEN h.away_team
                WHEN (away_win_opening >= 4 AND home_score < away_score) OR (draw_opening >= 4 AND home_score = away_score AND home_win_opening < away_win_opening) THEN h.home_team
            END AS fallen_giant
            , CAST(SPLIT(f.round, ' - ')[2] AS INTEGER) AS round
            , 1 AS surprises
        FROM main.historical_odds h
        LEFT JOIN main.fixtures f 
            ON h.home_team = f.home_team 
            AND h.away_team = f.away_team 
            AND h.season = f.season
        WHERE 1=1
            AND CAST(SPLIT(round, ' - ')[2] AS INTEGER) IS NOT NULL
            -- AND home IS NOT NULL AND away IS NOT NULL
        ORDER BY h.competition, h.season, round
    ),

    grouping as (
        SELECT
            competition
            , season
            , home
            , away
            , fallen_giant
            , round
            , CASE WHEN home IS NULL THEN 0 ELSE 1 END AS surprises
        FROM stage
        GROUP BY 1,2,3,4,5,6
        ORDER BY competition, season DESC, round, fallen_giant
    ),

    round_summary AS (
        SELECT
            competition,
            season,
            round,
            SUM(surprises) AS has_surprise
        FROM grouping
        GROUP BY competition, season, round
    )

    SELECT t.*
    FROM grouping t
    JOIN round_summary s
    ON t.competition = s.competition
    AND t.season = s.season
    AND t.round = s.round
    WHERE 
        -- keep only surprise rows when there are surprises
        (s.has_surprise > 0 AND t.surprises = 1)
        -- or, when there are no surprises, keep only NULL rows
        OR (s.has_surprise = 0 AND t.home IS NULL AND t.away IS NULL);
    """

    # Execute the first query and store the result in stg__surprises_per_round
    context.log.info("Executing query for stg__surprises_per_round...")
    with duckdb.get_connection() as con:
        con.execute(f"CREATE OR REPLACE TABLE stg__surprises_per_round AS {surprises_query}")
        context.log.info("Stored results in stg__surprises_per_round.")

        con.execute(f"CREATE OR REPLACE TABLE stg__surprises_per_club AS {surprises_clubs_query}")
    context.log.info("Stored results in stg__surprises_per_club.")

    # Query 2: Favourites success per round
    favourites_query = """
    SELECT
        CAST(
            hash(h.competition || h.season || SPLIT(f.round, ' - ')[2]) % 9223372036854775807 
            AS BIGINT
        ) AS favourites_id,
        h.season,
        h.competition,
        CAST(SPLIT(f.round, ' - ')[2] AS INTEGER) AS round,
        SUM(
            CASE
                WHEN
                    (h.home_win_opening < h.away_win_opening AND h.home_score > h.away_score AND ABS(h.home_win_opening - h.away_win_opening) >= 0.5)
                    OR (h.home_win_opening > h.away_win_opening AND h.home_score < h.away_score AND ABS(h.home_win_opening - h.away_win_opening) >= 0.5)
                THEN 1
                ELSE 0
            END
        ) AS success,
        SUM(
            CASE
                WHEN
                    (h.home_win_opening < h.away_win_opening AND h.home_score > h.away_score AND ABS(h.home_win_opening - h.away_win_opening) >= 0.5)
                    OR (h.home_win_opening > h.away_win_opening AND h.home_score < h.away_score AND ABS(h.home_win_opening - h.away_win_opening) >= 0.5)
                THEN 1
                ELSE 0
            END
        ) / 10.0 AS success_rate
    FROM main.historical_odds h
    LEFT JOIN main.fixtures f 
        ON h.home_team = f.home_team 
        AND h.away_team = f.away_team 
        AND h.season = f.season
    GROUP BY 1, 2, 3, 4
    ORDER BY h.season DESC, h.competition, round;
    """

    # Execute the second query and store the result in stg__favourites_success_per_round
    context.log.info("Executing query for stg__favourites_success_per_round...")
    with duckdb.get_connection() as con:
        con.execute(f"CREATE OR REPLACE TABLE stg__favourites_success_per_round AS {favourites_query}")
    context.log.info("Stored results in stg__favourites_success_per_round.")


@dg.asset(required_resource_keys={"duckdb"}, deps=["stage_odds_data"], auto_materialize_policy=dg.AutoMaterializePolicy.eager())
def fact_odds_data(context):
    """
    Executes transformations on staged data and stores the results in DuckDB tables.
    """

    duckdb = context.resources.duckdb

    context.log.info("Initiating pivot transformation for stg__surprises_per_round...")
    with duckdb.get_connection() as con:

        # Step 1: Aggregate the table to one row per competition, round, season
        # This ensures no duplicates
        agg_table_query = """
        WITH agg AS (
            SELECT
                competition,
                season,
                round,
                SUM(surprises) AS surprises
            FROM stg__surprises_per_round
            GROUP BY competition, season, round
        )
        SELECT * FROM agg
        """
        agg_df = con.execute(agg_table_query).fetchdf()

        # Register the DataFrame as a temporary view in DuckDB
        con.register("agg_df", agg_df)

        # Step 2: Generate the dynamic list of seasons in descending order
        seasons = con.execute("""
            SELECT DISTINCT season 
            FROM stg__surprises_per_round 
            ORDER BY season DESC
        """).fetchall()

        season_list = [f"'{s[0]}'" for s in seasons]  # Quote each season
        season_in_clause = ", ".join(season_list)

        # Step 3: Build the pivot SQL
        pivot_sql = f"""
        SELECT *
        FROM agg_df
        PIVOT (
            SUM(surprises) FOR season IN ({season_in_clause})
        )
        ORDER BY competition, round
        """

        con.execute(f"CREATE OR REPLACE TABLE fct__surprises_per_season AS {pivot_sql}")
    context.log.info("Stored results in fct__surprises_per_season.")


    context.log.info("Initiating pivot transformation for stg__favourites_success_per_round...")
    with duckdb.get_connection() as con:

        # Step 1: Aggregate the table to one row per competition, round, season
        # This ensures no duplicates
        agg_table_query = """
        WITH agg AS (
            SELECT
                competition,
                season,
                round,
                SUM(success) AS success
            FROM stg__favourites_success_per_round
            GROUP BY competition, season, round
        )
        SELECT * FROM agg
        """
        agg_df = con.execute(agg_table_query).fetchdf()

        # Register the DataFrame as a temporary view in DuckDB
        con.register("agg_df", agg_df)

        # Step 2: Generate the dynamic list of seasons in descending order
        seasons = con.execute("""
            SELECT DISTINCT season 
            FROM stg__favourites_success_per_round 
            ORDER BY season DESC
        """).fetchall()

        season_list = [f"'{s[0]}'" for s in seasons]  # Quote each season
        season_in_clause = ", ".join(season_list)

        # Step 3: Build the pivot SQL
        pivot_sql = f"""
        SELECT *
        FROM agg_df
        PIVOT (
            SUM(success) FOR season IN ({season_in_clause})
        )
        ORDER BY competition, round
        """

        con.execute(f"CREATE OR REPLACE TABLE fct__favourites_success_per_season AS {pivot_sql}")
    context.log.info("Stored results in fct__favourites_success_per_season.")


    context.log.info("Initiating pivot transformation for stg__surprises_per_club...")
    with duckdb.get_connection() as con:

        # --- 0.  ---
        con.execute("""
            CREATE OR REPLACE TABLE fct__surprises_per_club AS
            WITH
            -- 1 All teams (home + away)
            all_teams AS (
                SELECT DISTINCT league as competition, season, home_team AS club FROM main.fixtures
                UNION
                SELECT DISTINCT league as competition, season, away_team AS club FROM main.fixtures
            ),

            -- 2 Max round per competition + season (handles 34 vs 38)
            max_rounds AS (
                SELECT
                    league AS competition,
                    season,
                    MAX(CAST(SPLIT(round, ' - ')[2] AS INTEGER)) AS max_round
                FROM main.fixtures
                GROUP BY competition, season
            ),

            -- 3 Build complete (competition, season, club, round) grid
            team_rounds AS (
                SELECT
                    CASE
                        WHEN lower(a.competition) = 'epl' THEN 'premier-league'
                        WHEN lower(a.competition) = 'ligue1' THEN 'ligue-1'
                        WHEN lower(a.competition) = 'seriea' THEN 'serie-a'
                        ELSE lower(a.competition)
                    END AS competition,
                    a.season,
                    a.club,
                    gs.round
                FROM all_teams a
                JOIN max_rounds m USING (competition, season)
                CROSS JOIN generate_series(1, m.max_round) AS gs(round)
            ),

            -- 4 Join surprises (fill with 0 if none)
            joined AS (
                SELECT
                    tr.competition,
                    tr.season,
                    tr.club,
                    tr.round,
                    COALESCE(cs.surprises, 0) AS surprises
                FROM team_rounds tr
                LEFT JOIN stg__surprises_per_club cs
                ON tr.competition = cs.competition
                    AND tr.season = cs.season
                    AND tr.round = cs.round
                    AND tr.club = cs.fallen_giant
            ),

            -- 5 Cumulative sum of surprises per team
            accumulated AS (
                SELECT
                    competition,
                    season,
                    club,
                    round,
                    SUM(surprises) OVER (
                        PARTITION BY competition, season, club
                        ORDER BY round
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_surprises
                FROM joined
            ),

            -- 6 Pivot: one row per team with rounds as columns
            pivoted AS (
                PIVOT accumulated
                ON round
                USING MAX(cumulative_surprises)
            )

            -- 7 Final output
            SELECT *
            FROM pivoted
            ORDER BY competition, season, club
        """)

        # reorder columns numerically
        cols = con.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'fct__surprises_per_club'
            AND column_name ~ '^[0-9]+$'
            ORDER BY CAST(column_name AS INTEGER)
        """).fetchdf()['column_name'].tolist()

        # Build final sorted select
        quoted_cols = [f'"{c}"' for c in cols]
        # Build final SQL
        cols_sql = ", ".join(['competition', 'season', 'club'] + quoted_cols)

        con.execute(f"""
            CREATE OR REPLACE TABLE fct__surprises_per_club AS
            SELECT {cols_sql}
            FROM fct__surprises_per_club
            ORDER BY competition, season, club;
        """)

        # Persist back to DuckDB
        # con.register("pivoted_temp", pivot_df)
        # con.execute("CREATE OR REPLACE TABLE fct__surprises_per_club AS SELECT * FROM pivoted_temp")
    context.log.info("Stored results in fct__surprises_per_club.")


@dg.asset(kinds={"python"}, deps=["fact_odds_data"], auto_materialize_policy=dg.AutoMaterializePolicy.eager())
def load_facts_to_postgres(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    """
    Loads all 3 fact tables from DuckDB into the external Postgres instance.
    Tables are fully replaced since they are pre-aggregated pivot tables with
    dynamic column schemas (one column per season/round).
    """
    if not EXT_POSTGRES_URL:
        context.log.warning("EXT_POSTGRES_URL not set. Skipping Postgres load.")
        return "Skipped."

    fact_tables = [
        "fct__surprises_per_season",
        "fct__favourites_success_per_season",
        "fct__surprises_per_club",
    ]

    engine = create_engine(EXT_POSTGRES_URL)
    with duckdb.get_connection() as con:
        for table in fact_tables:
            df = con.execute(f"SELECT * FROM {table}").fetchdf()
            context.log.info(f"📦 Loaded {len(df)} rows from DuckDB '{table}'")
            if df.empty:
                context.log.warning(f"'{table}' is empty — skipping.")
                continue
            with engine.begin() as pg_conn:
                # Drop and replace — pivot tables have dynamic columns per season
                # so replace is safer than upsert here
                pg_conn.execute(text(f"DROP TABLE IF EXISTS {table};"))
                df.to_sql(table, con=pg_conn, if_exists='replace', index=False)
            context.log.info(f"✅ Loaded {len(df)} rows into Postgres '{table}'")

    return f"Postgres fact load completed: {', '.join(fact_tables)}"
