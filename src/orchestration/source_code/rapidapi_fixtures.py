import requests
import datetime
import pandas as pd
from collections.abc import Iterator


def get_league_response(league_id: str, league_str: str, api_key: str, season: str, resp_size: str | None = None) -> dict:
	url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
	querystring = {
		"league": league_id, 
		"season": season, 
		"next": resp_size,
	}
	
	headers = {
		"X-RapidAPI-Key": api_key,
		"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
	}
	response = requests.get(url, headers=headers, params=querystring)
	if response.status_code != 200:
		raise Exception(f"Error fetching data: {response.status_code} - {response.text}")
	if len(response.json()['response']) > 0:
		print(f"Fetched {len(response.json()['response'])} fixtures for league {league_str} in season {season}.")
	return response.json()['response']


def build_iter(obj: dict) -> Iterator:
	for fixture in obj:
		round = fixture['league']['round']
		date = datetime.datetime.fromtimestamp(fixture['fixture']['timestamp'])#.strftime('%m/%d/%Y %H:%M:%S')
		weekday = datetime.datetime.fromtimestamp(fixture['fixture']['timestamp']).strftime('%a')
		home_team = fixture['teams']['home']['name']
		home_goals = fixture['goals']['home']
		away_goals = fixture['goals']['away']
		away_team = fixture['teams']['away']['name']
		yield weekday, round, date, home_team, home_goals, away_goals, away_team


def build_dataframe(output: dict, league: str, season: int) -> pd.DataFrame:
	columns = ['weekday', 'round', 'date', 'home_team', 'home_goals', 'away_goals', 'away_team']
	alldata = list(build_iter(output))
	alldata_frame = pd.DataFrame(alldata, columns=columns)
	replace = {
		"Manchester United": "Manchester Utd",
		"Nottingham Forest": "Nottingham",
		"Hull City": "Hull",
		"Stoke City": "Stoke",
		"Bayern München": "Bayern Munich",
		"SC Freiburg": "Freiburg",
		"VfL Bochum": "Bochum",
		"Vfl Bochum": "Bochum",
		"1899 Hoffenheim": "Hoffenheim",
		"VfB Stuttgart": "Stuttgart",
		"FSV Mainz 05": "Mainz",
		"1.FC Köln": "FC Koln",
		"FC Augsburg": "Augsburg",
		"FC St. Pauli": "St. Pauli",
		"SV Darmstadt 98": "Darmstadt",
		"SC Paderborn 07": "Paderborn",
		"FC Nurnberg": "Nurnberg",
		"Hannover 96": "Hannover",
		"Fortuna Dusseldorf": "Dusseldorf",
		"SpVgg Greuther Furth": "Greuther Furth",
		"Borussia Mönchengladbach": "B. Monchengladbach",
		"Borussia Monchengladbach": "B. Monchengladbach",
		"Borussia Dortmund": "Dortmund",
		"FC Schalke 04": "Schalke",
		"FC Kaiserslautern": "Kaiserslautern",
		"Eintracht Braunschweig": "Braunschweig",
		"FC Ingolstadt 04": "Ingolstadt",
		"VfL Wolfsburg": "Wolfsburg",
		"1. FC Heidenheim": "Heidenheim",
		"FC Heidenheim": "Heidenheim",
		"Real Betis": "Betis",
		"Athletic Club": "Ath Bilbao",
		"Deportivo La Coruna": "Dep. La Coruna",
		"Cadiz": "Cadiz CF",
		"Oviedo": "R. Oviedo",
		"Sporting Gijon": "Gijon",
		"Hércules": "Hercules",
		"Atletico Madrid": "Atl. Madrid",
		"Stade Brestois 29": "Brest",
		"Paris Saint Germain": "PSG",
		"Saint Etienne": "St Etienne",
		"Arles": "Arles-Avignon",
		"Clermont Foot": "Clermont",
		"Ajaccio": "AC Ajaccio",
		"Estac Troyes": "Troyes",
		"Gazelec FC Ajaccio": "GFC Ajaccio",
		"SC Bastia": "Bastia",
		"Evian TG": "Thonon-Evian",
		"Robur Siena": "Siena",
		}
	alldata_frame = alldata_frame.replace({"home_team":replace, "away_team":replace})
	alldata_frame['league'] = league
	alldata_frame['season'] = f'{season}-{season + 1}'
	print(f"Built dataframe with {len(alldata_frame)} rows for league {league} in season {season}.")
	return alldata_frame


def upsert_df(con, df: pd.DataFrame, table: str):
    con.register("fixtures_df", df)
    con.execute(f"""
        INSERT INTO {table}
        SELECT * FROM fixtures_df
        ON CONFLICT(round, home_team, away_team) DO UPDATE SET
		weekday = excluded.weekday,
		date = excluded.date,
		home_goals = excluded.home_goals,
		away_goals = excluded.away_goals,
		league = excluded.league,
		season = excluded.season;
    """
	)
    con.commit()
    con.unregister("fixtures_df")
    print(f"Upserted {len(df)} rows into {table} table.")
    return
    

    