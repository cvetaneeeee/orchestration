import subprocess
import dagster as dg
from dagster_duckdb import DuckDBResource
from orchestration.source_code import config

database_resource = DuckDBResource(database=config.DB_LOC)


@dg.resource
def config_resource():
    """Provide configuration values from config.py."""
    return config.current_settings


@dg.resource
def fixtures_config_resource():
    """Provide configuration values from config.py."""
    return config.fixtures_settings


@dg.resource
def playwright_resource():
    """Ensure Playwright browsers are installed."""
    try:
        subprocess.run(["playwright", "install"], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to install Playwright browsers: {e}")


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "duckdb": database_resource,
            "config": config_resource,
            "fixtures_config": fixtures_config_resource,
            "playwright": playwright_resource,
        }
    )
