import requests

class MarketingCloudClient:
    def __init__(self, client_id, client_secret, subdomain, account_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.subdomain = subdomain
        self.account_id = account_id
        self.base_url = f"https://{subdomain}.rest.marketingcloudapis.com"
        self.auth_url = f"https://{subdomain}.auth.marketingcloudapis.com/v2/token"
        self.access_token = None
        self.headers = {}  
        
    def connect(self):
        """Recieve Access Token"""
        if not self.client_id:
            raise ValueError("⚠️ MC Credentials missing! Check your .env file.")

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "account_id": self.account_id
        }
        
        print(f"\n[MC] Connecting to Auth Endpoint...")
        
        try:
            response = requests.post(self.auth_url, json=payload)
            
            if response.status_code == 200:
                self.access_token = response.json().get("access_token")
                self.headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json'
                }
                masked_token = self.access_token[:10] + "..." if self.access_token else "None"
                print(f"✅ Success! Token received: {masked_token}")
            else:
                print(f"❌ Auth Error: {response.text}")
                response.raise_for_status()
                
        except Exception as e:
            raise ConnectionError(f"Critical Error connecting to Marketing Cloud: {e}")

    def get_automation_id(self, automation_name):
        """
        Retrieves Automation Object ID by Name.
        """
        endpoint = f"{self.base_url}/automation/v1/automations"
        params = {"$filter": f"name eq '{automation_name}'"}
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            
            items = response.json().get('items', [])
            if not items:
                print(f"⚠️ Automation '{automation_name}' not found.")
                return None
            
            return items[0]['id']
            
        except Exception as e:
            print(f"🛑 Error fetching automation ID: {e}")
            return None

    def run_automation(self, automation_name):
        """
        Triggers an Automation by Name.
        """
        auto_id = self.get_automation_id(automation_name)
        if not auto_id:
            return False
            
        endpoint = f"{self.base_url}/automation/v1/automations/{auto_id}/actions/start"
        
        print(f"\n[MC] 🚀 Starting Automation: {automation_name}...")
        try:
            response = requests.post(endpoint, headers=self.headers, json={})
            response.raise_for_status()
            print(f"   ✅ Automation started successfully.")
            return True
        except Exception as e:
            print(f"   ❌ Failed to start automation: {e}")
            return False

    def fetch_de_rows(self, de_key, filter_col, filter_val):
        """
        Fetches rows from a Data Extension using a filter.
        Returns a LIST of rows.
        """
        endpoint = f"{self.base_url}/data/v1/customobjectdata/key/{de_key}/rowset"
        params = {"$filter": f"{filter_col} eq '{filter_val}'"}
        
        try:
            response = requests.get(endpoint, headers=self.headers, params=params)
            
            # 400/404 handling logic varies by MC instance/endpoint version
            if response.status_code == 404:
                return [] 
            
            response.raise_for_status()
            
            return response.json().get('items', [])
            
        except Exception as e:
            print(f"🛑 Error fetching DE rows: {e}")
            return []

    def get_journey_count(self):
        """
        Retrieves the total number of Journeys.
        """
        endpoint = f"{self.base_url}/interaction/v1/interactions"
        
        try:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            return data.get('count', 0)
            
        except Exception as e:
            print(f"🛑 Error fetching Journey count: {e}")
            return 0