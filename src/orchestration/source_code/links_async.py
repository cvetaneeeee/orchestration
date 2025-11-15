from typing import cast
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
from orchestration.source_code import config, utils
from playwright.async_api import async_playwright, TimeoutError


type = config.type
# if type == 'historic':
#     pages = 8 if config_dict.get("league") == "bundesliga" else 9
# else:
#     pages = 2


def parse_item(html_page: str) -> list[dict]:
    soup = BeautifulSoup(html_page, "lxml")
    base = cast(str, 'https://www.oddsportal.com')
    matches = []

    for match in soup.select('.hover\\:bg-\\[\\#f9e9cc\\]'):
        a_tag = match.select_one('a')
        href = a_tag.get('href') if a_tag else None
        if not isinstance(href, str):
            continue

        # Extract team names from within the match block
        team_elements = match.select('a[title]')
        teams = [el.get('title') for el in team_elements if el.get('title')]

        matches.append({
            "url": base + href,
            "home_team": teams[0] if len(teams) > 0 else None,
            "away_team": teams[1] if len(teams) > 1 else None
        })

    return matches


async def main(collect: bool, country: str, league: str, season: str | None = None) -> list:
    # agent = (
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    #     "(KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36"
    # )

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)  # Set to False for debugging
        context = await browser.new_context(user_agent=config.headers['User-Agent'])
        page = await context.new_page()

        await page.goto('https://www.oddsportal.com/', wait_until='networkidle')

        # Try dismissing the banner with the X button
        await utils.dismiss_banner_if_present(page)
            
        # Login to OddsPortal
        # await page.get_by_text("Login").first.click()
        # await page.get_by_label("Username").fill(config.ODDSPORTAL_USERNAME)
        # await page.get_by_label("Password").fill(config.ODDSPORTAL_PASSWORD)
        # await page.get_by_role("button", name="Login").click()
        # await page.wait_for_timeout(2000)  # wait for login to settle

        matches = []
        if type == "historic":
            urls = [f"https://www.oddsportal.com/football/{country}/{league}-{season}/results/"]
        else:
            urls = [f"https://www.oddsportal.com/football/{country}/{league}/", 
                    f"https://www.oddsportal.com/football/{country}/{league}/results/"]
        # elif type == "current" and collect:
        #     url = f"https://www.oddsportal.com/football/{country}/{league}/results/"
        # else:    
        #     url = f"https://www.oddsportal.com/football/{country}/{league}/"

        for url in urls:
            await page.goto(url, wait_until='networkidle')

            # Dynamically determine the number of pages
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            pagination_links = soup.select('a.pagination-link[data-number]')
            pages = max(int(str(link['data-number'])) for link in pagination_links) if pagination_links else 1
            print(f"✅ Found {pages} pages for {country.capitalize()} {league.capitalize()} {season}.")

            for p in tqdm(range(1, pages + 1), desc=f"Collecting {country.capitalize()} {league.capitalize()} {season} oddsportal links"):
                if p == 1:
                    await page.goto(url, wait_until='networkidle')
                else:
                    try:
                        pagination_button = page.locator(f'a.pagination-link[data-number="{p}"]')
                        await pagination_button.scroll_into_view_if_needed()
                        await pagination_button.click(force=True)
                        await page.wait_for_selector(f'a.pagination-link[data-number="{p}"].active', timeout=5000)
                        # await pagination_button.click(timeout=5000)
                        # await page.wait_for_load_state("networkidle")
                        
                        # page_url = f"{url}#/page/{p}/"
                        # await page.goto(page_url, wait_until="networkidle")
                    except TimeoutError as e:
                        print(f"⚠️ Could not navigate to page {p}, it doesn't exist. Moving forward.: {e}")
                        continue

                # Scroll to load dynamic content
                last_height = await page.evaluate("document.body.scrollHeight")
                while True:
                    await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1000)
                    new_height = await page.evaluate("document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height

                html = await page.content()
                parsed_result = parse_item(html)
                matches.extend(parsed_result)

        await browser.close()
        print(f"✅ Collected matches from OddsPortal: {matches}")
        return matches
