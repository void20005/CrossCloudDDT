import pytest
import os
import logging
from simple_salesforce import Salesforce
from src.mc_client import MarketingCloudClient
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# Configure basic logging for tests
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 1. Command line option
def pytest_addoption(parser):
    parser.addoption(
        "--env", 
        action="store", 
        default="qa", 
        help="Choose environment: qa or stage"
    )
    parser.addoption(
        "--browser",
        action="store",
        default="firefox",
        help="Choose browser: firefox (default), chromium, edge, webkit"
    )

# 2. Loading config fixture (Runs first)
@pytest.fixture(scope="session", autouse=True)
def load_env_config(request):
    env_name = request.config.getoption("--env")
    dotenv_file = f".env.{env_name}"

    if not os.path.exists(dotenv_file):
        pytest.fail(f"ERROR: Configuration file '{dotenv_file}' not found!")

    logger.info("Loading environment settings: %s (from %s)", env_name.upper(), dotenv_file)
    load_dotenv(dotenv_file, override=True)

# 3. SALES CLOUD FIXTURE
@pytest.fixture(scope="session")
def sc_client():
    logger.info("[SalesCloud] Connecting...")
    
    # --- DEBUG ---
    username = os.getenv("SC_USERNAME")
    password = os.getenv("SC_PASSWORD")
    token = os.getenv("SC_TOKEN")
    domain = os.getenv("SC_DOMAIN")

    logger.debug("DEBUG: Username = %s", username)
    logger.debug("DEBUG: Password = %s", '***' if password else 'None')
    logger.debug("DEBUG: Token    = %s", '***' if token else 'None')
    logger.debug("DEBUG: Domain   = %s", domain)
    # ----------------

    # If something is missing, the library will fail. We want to see this BEFORE failure.
    if not username or not password:
        pytest.fail("ERROR: Username or Password not loaded from .env file!")

    sf = Salesforce(
        username=username,
        password=password,
        security_token=token if token else "", # If token is empty/None, pass empty string
        domain=domain
    )
    return sf

# 4. MARKETING CLOUD FIXTURE
@pytest.fixture(scope="session")
def mc_client():
    logger.info("[MarketingCloud] Connecting...")
    # It's important to read os.getenv here, 
    # to ensure we use the values loaded above.
    mc = MarketingCloudClient(
        client_id=os.getenv("MC_CLIENT_ID"),
        client_secret=os.getenv("MC_CLIENT_SECRET"),
        subdomain=os.getenv("MC_SUBDOMAIN"),
        account_id=os.getenv("MC_ACCOUNT_ID")
    )
    mc.connect()
    return mc

# 5. PLAYWRIGHT BROWSER FIXTURE (WITH SESSION-BASED LOGIN)
@pytest.fixture(scope="function")
def browser_page(request, sc_client):
    """
    Fixture that:
    1. Gets a Salesforce session ID (sid) from an already-authenticated API connection
    2. Opens a Playwright browser
    3. Injects the session cookie so no password entry is needed
    4. Returns a logged-in browser page
    
    Usage:
        def test_something(browser_page):
            page = browser_page
            page.goto("https://...")
    """
    with sync_playwright() as p:
        # --- BROWSER SELECTION (CLI param > ENV var > default) ---
        browser_choice = (
            request.config.getoption("--browser") or 
            os.getenv("BROWSER", "firefox")
        ).lower()
        valid_browsers = ["firefox", "chromium", "edge", "webkit"]
        if browser_choice not in valid_browsers:
            logger.warning("Unknown browser '%s'. Using 'firefox'.", browser_choice)
            browser_choice = "firefox"
        
        # --- UI & PERFORMANCE ---
        show_ui_env = os.getenv("SHOW_UI")
        if show_ui_env is None:
            show_ui = os.getenv("CI", "false").lower() not in ("1", "true", "yes")
        else:
            show_ui = show_ui_env.lower() in ("1", "true", "yes")
        
        try:
            slow_mo = int(os.getenv("PLAYWRIGHT_SLOWMO", "0"))
        except ValueError:
            slow_mo = 0
        
        # --- LAUNCH BROWSER ---
        if browser_choice == "chromium":
            browser = p.chromium.launch(headless=not show_ui, slow_mo=slow_mo)
        elif browser_choice == "edge":
            browser = p.chromium.launch(headless=not show_ui, slow_mo=slow_mo, channel="msedge")
        elif browser_choice == "webkit":
            browser = p.webkit.launch(headless=not show_ui, slow_mo=slow_mo)
        else:  # firefox (default)
            browser = p.firefox.launch(headless=not show_ui, slow_mo=slow_mo)
        
        # Create context with Salesforce domain
        domain = os.getenv("SC_DOMAIN", "test")
        
        context = browser.new_context()
        
        # Inject Salesforce session cookie (authenticates without password!)
        session_id = sc_client.session_id
        context.add_cookies([
            {
                "name": "sid",
                "value": session_id,
                "domain": ".salesforce.com",
                "path": "/"
            }
        ])
        
        page = context.new_page()
        
        logger.info("[UI] Browser: %s", browser_choice.upper())
        logger.info("[UI] Window: %s", 'VISIBLE' if show_ui else 'HEADLESS')
        logger.info("[UI] Session ID (first 20 chars): %s...", session_id[:20])
        logger.info("[UI] Domain: %s", domain)
        
        yield page
        
        # Cleanup
        browser.close()
