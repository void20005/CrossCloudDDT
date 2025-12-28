import pytest
import os
from simple_salesforce import Salesforce
from src.mc_client import MarketingCloudClient
from dotenv import load_dotenv

# 1. Command line option
def pytest_addoption(parser):
    parser.addoption(
        "--env", 
        action="store", 
        default="qa", 
        help="Choose environment: qa or stage"
    )

# 2. Loading config fixture (Runs first)
@pytest.fixture(scope="session", autouse=True)
def load_env_config(request):
    env_name = request.config.getoption("--env")
    dotenv_file = f".env.{env_name}"

    if not os.path.exists(dotenv_file):
        pytest.fail(f"🛑 ERROR: Configuration file '{dotenv_file}' not found!")

    print(f"\n🌍 Loading environment settings: {env_name.upper()} (from {dotenv_file})")
    load_dotenv(dotenv_file, override=True)

# 3. SALES CLOUD FIXTURE
@pytest.fixture(scope="session")
def sc_client():
    print("\n[SalesCloud] Connecting...")
    
    # --- DEBUG ---
    username = os.getenv("SC_USERNAME")
    password = os.getenv("SC_PASSWORD")
    token = os.getenv("SC_TOKEN")
    domain = os.getenv("SC_DOMAIN")

    print(f"DEBUG: Username = {username}")
    print(f"DEBUG: Password = {'***' if password else 'None'}") # Hide password, but check if it exists
    print(f"DEBUG: Token    = '{token}'") # Check if empty or None
    print(f"DEBUG: Domain   = {domain}")
    # ----------------

    # If something is missing, the library will fail. We want to see this BEFORE failure.
    if not username or not password:
        pytest.fail("🛑 ERROR: Username or Password not loaded from .env file!")

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
    print("\n[MarketingCloud] Connecting...")
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