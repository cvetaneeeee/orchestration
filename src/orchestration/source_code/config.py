import os
from dotenv import load_dotenv
load_dotenv()

DB_LOC = os.getenv("DB_LOC")
EXT_POSTGRES_URL = os.getenv("EXT_POSTGRES_URL")

# odds configs
bookie: str = '16'
bookie_name: str = "bet365"
season: str = "2025-2026"
current_season: str = "2025-2026"
spreadsheet_id: str = "11HjTZIeY3yUrMgJQnhUdgxM1ZY0wRDzIC-ABj0K7wgE"
type = 'current' # historic, current
collect = True

bookie_eng = {"bookie_id": bookie, "bookie_name": bookie_name, "season": season, "spreadsheet_id": spreadsheet_id, "sheet_name": "EPL", "country": "england", "league": "premier-league", "type": type, "current_season": current_season, "collect": collect} 
bookie_esp = {"bookie_id": bookie, "bookie_name": bookie_name, "season": season, "spreadsheet_id": spreadsheet_id, "sheet_name": "LALIGA", "country": "spain", "league": "laliga", "type": type, "current_season": current_season, "collect": collect} 
bookie_ita = {"bookie_id": bookie, "bookie_name": bookie_name, "season": season, "spreadsheet_id": spreadsheet_id, "sheet_name": "SERIEA", "country": "italy", "league": "serie-a", "type": type, "current_season": current_season, "collect": collect} 
bookie_fra = {"bookie_id": bookie, "bookie_name": bookie_name, "season": season, "spreadsheet_id": spreadsheet_id, "sheet_name": "LIGUE1", "country": "france", "league": "ligue-1", "type": type, "current_season": current_season, "collect": collect} 
bookie_ger = {"bookie_id": bookie, "bookie_name": bookie_name, "season": season, "spreadsheet_id": spreadsheet_id, "sheet_name": "BUNDESLIGA", "country": "germany", "league": "bundesliga", "type": type, "current_season": current_season, "collect": collect} 

headers = {
    'sec-ch-ua-platform': '"Windows"',
    'Referer': 'https://www.oddsportal.com/',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'DNT': '1',
}

# fixtures configs
league_ids = {"EPL":"39", "LaLiga":"140", "SerieA":"135", "Ligue1":"61", "Bundesliga":"78"}
api_key = os.getenv("API_KEY")
year = "2025"
results = None # "50"

# oddsportal credentials
ODDSPORTAL_USERNAME = os.getenv("ODDSPORTAL_USERNAME")
ODDSPORTAL_PASSWORD = os.getenv("ODDSPORTAL_PASSWORD")

# combined settings
current_settings = { "epl": bookie_eng, "laliga": bookie_esp, "seriea": bookie_ita, "ligue1": bookie_fra, "bundesliga": bookie_ger }
fixtures_settings = { "league_ids": league_ids, "api_key": api_key, "year": year, "results": results }
