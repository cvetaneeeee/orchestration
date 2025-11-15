from orchestration.source_code.links_async import main

async def extract_links_async(config, league_key: str) -> list[dict]:
    """Async version of extract_links asset."""
    country = config[league_key].get("country")
    league = config[league_key].get("league")
    season = config[league_key].get("season", "")
    collect = config[league_key].get("collect", False)
    
    result = await main(collect=collect, country=country, league=league, season=season)
    return result
