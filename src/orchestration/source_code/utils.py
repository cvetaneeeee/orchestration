async def dismiss_banner_if_present(page):
    try:
        close_button = page.locator('svg.cursor-pointer[width="32"][height="32"]')
        if await close_button.is_visible():
            await close_button.click()
            await page.wait_for_timeout(500)
            print("✅ Closed banner")
    except Exception as e:
        print("⚠️ Failed to close banner:", e)


async def handle_cookies(page):
    try:
        # Try Reject All first
        reject_cookies = page.locator('button:has-text("Reject All")').first
        if await reject_cookies.is_visible():
            await reject_cookies.click()
            print("✅ Rejected cookies")
        else:
            # Fallback: Accept All
            accept_cookies = page.locator('button:has-text("Accept All")').first
            if await accept_cookies.is_visible():
                await accept_cookies.click()
                print("✅ Accepted cookies")
            else:
                print("ℹ️ No cookie banner found")

        await page.wait_for_timeout(500)

    except Exception as e:
        print("⚠️ Failed to handle cookies:", e)