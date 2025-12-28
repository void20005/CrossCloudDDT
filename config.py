import os
from dotenv import load_dotenv


load_dotenv()

class Config:
    # Sales Cloud
    SC_USERNAME = os.getenv("SC_USERNAME")
    SC_PASSWORD = os.getenv("SC_PASSWORD")
    SC_TOKEN = os.getenv("SC_TOKEN")
    SC_DOMAIN = os.getenv("SC_DOMAIN", "test")

    # Marketing Cloud
    MC_CLIENT_ID = os.getenv("MC_CLIENT_ID")
    MC_CLIENT_SECRET = os.getenv("MC_CLIENT_SECRET")
    MC_SUBDOMAIN = os.getenv("MC_SUBDOMAIN")
    MC_ACCOUNT_ID = os.getenv("MC_ACCOUNT_ID")