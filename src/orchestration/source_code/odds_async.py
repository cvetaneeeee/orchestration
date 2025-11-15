import json
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime
from tqdm.asyncio import tqdm
from orchestration.source_code import config, utils
from ..source_code.decoder import decrypt_data
from ..source_code.decoder_v2 import decrypt_oddsportal
from ..source_code.decrypt_keys import extract_encryption_keys
from playwright.async_api import async_playwright, Page, Response
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential, retry_if_exception_type, RetryError

# Limit concurrent browser sessions (tune based on system/resources)
concurrency_limit = 20
semaphore = asyncio.Semaphore(concurrency_limit)
config_dict = config.current_settings


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def capture_api_data(url: str, playwright) -> dict:
    results = {}

    async with semaphore:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=config.headers['User-Agent'])
        page = await context.new_page()

        async def handle_response(response):
            if '.dat' in response.url:
                # asyncio.create_task(process_dat_url(response.url, results))
                await process_dat_url(response, results, page)

        page.on("response", handle_response)

        try:
            await page.goto(url, wait_until="networkidle")
            await utils.handle_cookies(page)
            await asyncio.sleep(3)  # give responses time to resolve

        finally:
            await page.close()
            await context.close()
            await browser.close()

    return results


# @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
# async def capture_api_data(url: str, playwright) -> dict:
#     results = {}
#     tasks = []

#     async with semaphore:
#         browser = await playwright.chromium.launch(headless=True)
#         context = await browser.new_context(user_agent=config.headers['User-Agent'])
#         page = await context.new_page()

#         # call key extraction function
#         # keys = await extract_encryption_keys(url=url) # page.url)
        
#         # print("Extracted keys:", keys)
#         # setattr(page, "runtime_keys", keys)

#         async def handle_response(response):
#             if ".dat" in response.url:
#                 task = asyncio.create_task(process_dat_url(response, results, page))
#                 tasks.append(task)

#         # Listen for .dat responses and process them
#         page.on("response", handle_response)

#         try:
#             await page.goto(url, wait_until="networkidle")
#             await utils.handle_cookies(page)
#             await asyncio.sleep(3)  # give responses time to resolve
#             if tasks:
#                 await asyncio.gather(*tasks)
#         except Exception as e:
#             print(f"Error capturing API data from {url}: {type(e).__name__} - {e}")        
#         finally:
#             await page.close()
#             await context.close()
#             await browser.close()

#     return results


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=(
        retry_if_exception_type(KeyError) |
        retry_if_exception_type(ValueError) |
        retry_if_exception_type(RetryError)
    )
)
async def safe_capture_api_data(url, playwright):
    data = await capture_api_data(url, playwright)
    odds_data = json.loads(data['data'])
    score_data = json.loads(data['score'])
    return odds_data, score_data


# @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
# async def process_dat_url(url: str, results: dict, page: Page) -> None:
#     from curl_cffi import requests

#     r = requests.get(url, headers=config.headers, impersonate="chrome")
#     # Pull Uwt/Qwt from page if available, fallback to empty string
#     password = getattr(page, "runtime_keys", {}).get("Uwt", "")
#     salt = getattr(page, "runtime_keys", {}).get("Qwt", "")
#     # print(f"Fetched {url} with status {r.status_code}")
#     # print(f"Response content (first 200 chars): {r.text[:200]}...")
#     # decrypted = decrypt_data(encrypted_input=r.text)
#     decrypted = decrypt_oddsportal(encrypted_input=r.text.strip(), password=password, salt=salt)

#     if "match-event" in url:
#         results["data"] = decrypted
#         results["url"] = url
#     elif "postmatch-score" in url:
#         results["score"] = decrypted


@retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
async def process_dat_url(response: Response, results: dict, page: Page):
    """
    Decrypts the response content using the page's JS decryption function `xNt`.
    """
    try:
        encrypted_text = await response.text()

        # retrieve
        # Uwt = getattr(page, "runtime_keys")["Uwt"]
        # Qwt = getattr(page, "runtime_keys")["Qwt"]
        
        decrypted = decrypt_data(encrypted_input=encrypted_text.strip()) # , password=Uwt, salt=Qwt)

        url = response.url
        if "match-event" in url:
            results["data"] = decrypted
            results["url"] = url
        elif "postmatch-score" in url:
            results["score"] = decrypted
            # print(f"Postmatch score from {response.url}: {results["score"]}...")
    except Exception as e:
        print(f"Error processing .dat URL {response.url}: {type(e).__name__} - {e}")


async def process_url(url_data: dict, playwright, bookie_id: str, bookie_name: str, competition: str) -> pd.DataFrame | None:
    home_team = url_data.get('home_team')
    away_team = url_data.get('away_team')
    url = url_data.get('url', '')
    print(f"Processing URL: {url}...")

    try:
        odds_data, score_data = await safe_capture_api_data(url, playwright)
    except (RetryError, ValueError, KeyError) as e:
        print(f"Error processing {url}: {type(e).__name__} - {e}")
        return None

    match_id = url.split('/', 6)[-1].split('-')[-1].split('/')[0]
    if not match_id:
        print(f"Warning: URL will be skipped due to missing match ID - {url}")
    # bookie_id = config_dict['bookie_id']
    # bookie_name = config_dict['bookie_name']
    # competition = config_dict['league']

    try:
        closing_odds = odds_data['d']['oddsdata']['back']['E-1-2-0-0-0']['odds'][bookie_id]
        opening_odds = odds_data['d']['oddsdata']['back']['E-1-2-0-0-0']['openingOdd'][bookie_id]
        closing_time = odds_data['d']['oddsdata']['back']['E-1-2-0-0-0']['changeTime'][bookie_id]["0"]
        opening_time = odds_data['d']['oddsdata']['back']['E-1-2-0-0-0']['openingChangeTime'][bookie_id]["0"]

        home_score = score_data['d']['homeResult']
        away_score = score_data['d']['awayResult']

        odds_movement = {
            'id': match_id,
            'bookie': bookie_name,
            'competition': competition,
            'season': config.season,
            'home_team': home_team,
            'away_team': away_team,
            'opening_time': datetime.fromtimestamp(opening_time),
            'closing_time': datetime.fromtimestamp(closing_time),
            'home_win_opening': opening_odds["0"],
            'draw_opening': opening_odds["1"],
            'away_win_opening': opening_odds["2"],
            'home_win_closing': closing_odds["0"],
            'draw_closing': closing_odds["1"],
            'away_win_closing': closing_odds["2"],
            'home_score': home_score,
            'away_score': away_score
        }
    except KeyError as err:
        if err.args[0] in ['homeResult', 'awayResult']:
            print(f"Key Error processing odds for {home_team} vs {away_team}: {type(err).__name__} - {err}. Result will be stored as None.")
            odds_movement = {
                'id': match_id,
                'bookie': bookie_name,
                'competition': competition,
                'season': config.season,
                'home_team': home_team,
                'away_team': away_team,
                'opening_time': datetime.fromtimestamp(opening_time),
                'closing_time': datetime.fromtimestamp(closing_time),
                'home_win_opening': opening_odds["0"],
                'draw_opening': opening_odds["1"],
                'away_win_opening': opening_odds["2"],
                'home_win_closing': closing_odds["0"],
                'draw_closing': closing_odds["1"],
                'away_win_closing': closing_odds["2"],
                'home_score': None,
                'away_score': None
            }
        else:
            raise
    except Exception as e:
        print(f"Error processing odds for {home_team} vs {away_team}: {type(e).__name__} - {e}")
        odds_movement = {
            'id': match_id,
            'bookie': bookie_name,
            'competition': competition,
            'season': config.season,
            'home_team': home_team,
            'away_team': away_team,
            'opening_time': datetime.strptime('1899-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            'closing_time': datetime.strptime('1899-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
            'home_win_opening': None,
            'draw_opening': None,
            'away_win_opening': None,
            'home_win_closing': None,
            'draw_closing': None,
            'away_win_closing': None,
            'home_score': None,
            'away_score': None
            # 'opening_time': datetime.fromtimestamp(opening_time),
            # 'closing_time': datetime.fromtimestamp(closing_time),
            # 'home_win_opening': opening_odds["0"],
            # 'draw_opening': opening_odds["1"],
            # 'away_win_opening': opening_odds["2"],
            # 'home_win_closing': closing_odds["0"],
            # 'draw_closing': closing_odds["1"],
            # 'away_win_closing': closing_odds["2"],
            # 'home_score': '0',
            # 'away_score': '0'
        }
    print(f"URL {url} processed successfully...")
    return pd.DataFrame([odds_movement])


async def run_odds_async(urls: list, league: str, season: str, bookie_id: str, bookie_name: str, competition: str) -> pd.DataFrame:

    all_data = []
    async with async_playwright() as playwright:
        semaphore = asyncio.Semaphore(10)  # limit concurrency

        async def sem_task(url_data):
            async with semaphore:
                try:
                    return await process_url(url_data, playwright, bookie_id=bookie_id, bookie_name=bookie_name, competition=competition)
                except Exception as e:
                    print(f"Error processing URL {url_data.get('url')}: {e}")
                    return None

        tasks = [sem_task(url_data) for url_data in urls]

        # for coro in tqdm.as_completed(tasks, total=len(tasks), desc=f"Processing {league} {season} matches."):
        #     result = await coro
        #     if result is not None:
        #         all_data.append(result)

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Processing {league} {season} matches."):
            try:
                result = await coro # result = await asyncio.wait_for(coro, timeout=60)  # Set a timeout for each task
                if result is not None:
                    all_data.append(result)
            except asyncio.TimeoutError as e:
                print(f"Task timed out. : {e}")
                print(f"Result: {result}")
            except Exception as e:
                print(f"Error in task: {e}")
        df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
        if not df.empty:
            df = df[df['id'].notnull() & (df['id'] != '')]
            df['opening_time'] = pd.to_datetime(df['opening_time'], errors='coerce')
            df['closing_time'] = pd.to_datetime(df['closing_time'], errors='coerce')
            for col in df.columns:
                if col in ['opening_time', 'closing_time', 'id', 'bookie', 'competition', 'season', 'home_team', 'away_team']:
                    continue
                elif col in ['home_score', 'away_score']:
                    df[col] = (
                        pd.to_numeric(
                            df[col]
                            .replace(['', ' ', 'null'], np.nan),  # clean dirty values
                            errors='coerce'
                        ).astype('Int64')
                    )
                else:
                    df[col] = pd.to_numeric(
                        df[col].replace(['', ' ', 'null'], np.nan),
                        errors='coerce'
                    )
    return df
