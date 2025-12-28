import os
import sys
from simple_salesforce import Salesforce
from dotenv import load_dotenv

def get_client(env="qa"):
    """
    Loads environment variables from .env.{env} and returns a connected Salesforce client.
    """
    dotenv_file = f".env.{env}"
    if not os.path.exists(dotenv_file):
        print(f"🛑 ERROR: Configuration file '{dotenv_file}' not found!")
        sys.exit(1)

    print(f"\n🌍 Loading environment settings: {env.upper()} (from {dotenv_file})")
    load_dotenv(dotenv_file, override=True)

    username = os.getenv("SC_USERNAME")
    password = os.getenv("SC_PASSWORD")
    token = os.getenv("SC_TOKEN")
    domain = os.getenv("SC_DOMAIN")

    if not username or not password:
        print("🛑 ERROR: Username or Password not loaded from .env file!")
        sys.exit(1)

    print("\n[SalesCloud] Connecting...")
    try:
        sf = Salesforce(
            username=username,
            password=password,
            security_token=token if token else "",
            domain=domain
        )
        print("   ✅ Connected successfully.")
        return sf
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        sys.exit(1)
