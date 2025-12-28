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

    def get_journey_count(self):
        endpoint = f"{self.base_url}/interaction/v1/interactions"
        try:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status() 
            
            return response.json().get('count', 0)
            
        except Exception as e:
            print(f"🛑 Error fetching journeys: {e}")
            raise e