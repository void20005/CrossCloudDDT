import os
import logging


logger = logging.getLogger(__name__)


def _get_ui_host():
    return os.getenv("SF_UI_HOST", "auto360-qa1.lightning.force.com")


def _assert_logged_in(page):
    title = page.title()
    assert ("Salesforce" in title) or ("Lightning" in title), (
        f"Expected 'Salesforce' or 'Lightning' in title, got: {title}"
    )

    body_text = page.locator("body").text_content() or ""
    assert len(body_text.strip()) > 100, (
        f"Page content too minimal - may not be logged in. Got {len(body_text.strip())} chars"
    )

    login_form = page.locator("input#username, input[name='username']")
    assert login_form.count() == 0, "Found login form - session login failed!"


def test_salesforce_login_via_session_id(browser_page):
    page = browser_page
    ui_host = _get_ui_host()

    logger.info("[UI Test] Navigating to Salesforce home")
    page.goto(f"https://{ui_host}/lightning/app/home")

    page.wait_for_load_state("networkidle")

    logger.info("[UI Test] Waiting for Salesforce Lightning to load")
    _assert_logged_in(page)
    logger.info("[UI Test] Logged in without password entry")


def test_navigate_to_accounts_list(browser_page):
    page = browser_page
    ui_host = _get_ui_host()

    logger.info("[UI Test] Logging in and navigating to Accounts")
    page.goto(f"https://{ui_host}/lightning/app/home")

    accounts_url = f"https://{ui_host}/lightning/o/Account/list"
    logger.info("[UI Test] Navigating to %s", accounts_url)
    page.goto(accounts_url)

    logger.info("[UI Test] Waiting for Accounts list to load")
    list_container = page.locator("div[role='main'] >> text=Accounts")
    try:
        list_container.wait_for(timeout=10000)
        logger.info("[UI Test] Accounts list page loaded")
    except Exception:
        table = page.locator("table, div[role='grid']")
        table.first.wait_for(timeout=10000)
        logger.info("[UI Test] Accounts grid/table found")

    headers = page.locator("th, [role='columnheader']")
    header_count = headers.count()
    assert header_count > 0, "No table headers found - page might not have loaded correctly"
    logger.info("[UI Test] Accounts list is accessible: %s headers", header_count)
