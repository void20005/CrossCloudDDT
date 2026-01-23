import requests
import uuid

class MarketingCloudClient:
    def __init__(self, client_id, client_secret, subdomain, account_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.subdomain = subdomain
        self.account_id = account_id
        self.base_url = f"https://{subdomain}.rest.marketingcloudapis.com"
        self.auth_url = f"https://{subdomain}.auth.marketingcloudapis.com/v2/token"
        self.soap_url = f"https://{subdomain}.soap.marketingcloudapis.com/Service.asmx"
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
                # SOAP Header is different (raw XML usually), but we pass token in body or header
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

    def fetch_de_rows(self, de_key):
        """
        Fetches ALL rows from a Data Extension (Simplified for the task).
        Returns a LIST of rows.
        """
        endpoint = f"{self.base_url}/data/v1/customobjectdata/key/{de_key}/rowset"
        
        try:
            response = requests.get(endpoint, headers=self.headers)
            
            if response.status_code == 404:
                print(f"   ⚠️ DE Not Found: {de_key}")
                return [] 
            
            response.raise_for_status()
            
            return response.json().get('items', [])
            
        except Exception as e:
            print(f"🛑 Error fetching DE rows: {e}")
            return []

    def delete_subscriber(self, subscriber_key):
        """
        Deletes a Subscriber via SOAP API.
        """
        soap_envelope = f"""
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
           <soap:Header>
              <wsa:Action>Delete</wsa:Action>
              <wsa:MessageID>urn:uuid:16852436-e86b-4395-885d-85591350756a</wsa:MessageID>
              <wsa:ReplyTo>
                 <wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
              </wsa:ReplyTo>
              <wsa:To>{self.soap_url}</wsa:To>
              <fueloauth xmlns="http://exacttarget.com">{self.access_token}</fueloauth>
           </soap:Header>
           <soap:Body>
              <DeleteRequest xmlns="http://exacttarget.com/wsdl/partnerAPI">
                 <Options></Options>
                 <Objects xsi:type="Subscriber">
                    <SubscriberKey>{subscriber_key}</SubscriberKey>
                 </Objects>
              </DeleteRequest>
           </soap:Body>
        </soap:Envelope>
        """
        
        headers = {
            'Content-Type': 'text/xml',
            'SOAPAction': 'Delete'
        }
        
        try:
            response = requests.post(self.soap_url, data=soap_envelope, headers=headers)
            
            if response.status_code == 200 and "<StatusCode>OK</StatusCode>" in response.text:
                 return True, "OK"
            elif "Error" in response.text:
                 return False, response.text
            else:
                 return True, "Assumed OK (No Error)" # Sometimes Delete returns OK even if not found
                 
        except Exception as e:
            return False, str(e)

    def get_de_customer_key(self, de_name):
        """
        Resolves Data Extension Name to CustomerKey (External Key) using SOAP.
        Returns the CustomerKey string or None if not found.
        """
        soap_envelope = f"""
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
           <soap:Header>
              <wsa:Action>Retrieve</wsa:Action>
              <wsa:MessageID>urn:uuid:16852436-e86b-4395-885d-85591350756a</wsa:MessageID>
              <wsa:ReplyTo>
                 <wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
              </wsa:ReplyTo>
              <wsa:To>{self.soap_url}</wsa:To>
              <fueloauth xmlns="http://exacttarget.com">{self.access_token}</fueloauth>
           </soap:Header>
           <soap:Body>
              <RetrieveRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
                 <RetrieveRequest>
                    <ObjectType>DataExtension</ObjectType>
                    <Properties>CustomerKey</Properties>
                    <Properties>Name</Properties>
                    <Filter xsi:type="SimpleFilterPart">
                       <Property>Name</Property>
                       <SimpleOperator>equals</SimpleOperator>
                       <Value>{de_name}</Value>
                    </Filter>
                 </RetrieveRequest>
              </RetrieveRequestMsg>
           </soap:Body>
        </soap:Envelope>
        """
        
        headers = {
            'Content-Type': 'text/xml',
            'SOAPAction': 'Retrieve'
        }
        
        try:
            response = requests.post(self.soap_url, data=soap_envelope, headers=headers)
            if response.status_code == 200:
                # Simple parsing logic
                start_tag = "<CustomerKey>"
                end_tag = "</CustomerKey>"
                if start_tag in response.text:
                    start_idx = response.text.find(start_tag) + len(start_tag)
                    end_idx = response.text.find(end_tag, start_idx)
                    return response.text[start_idx:end_idx]
            
            # Additional debug if needed
            # print(response.text)
            return None
                 
        except Exception as e:
            print(f"🛑 Error resolving DE Key: {e}")
            return None

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

    def clear_data_extension(self, de_key):
        """
        Clears (Truncates) a Data Extension via SOAP PerformRequest (Action: ClearData).
        Ref: Salesforce Marketing Cloud SOAP API Documentation.
        """
        # Generate unique ID
        message_id = str(uuid.uuid4())
        
        # Action is 'Perform' for this request type
        soap_action = "Perform"

        soap_envelope = f"""
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
           <soap:Header>
              <wsa:Action>{soap_action}</wsa:Action>
              <wsa:MessageID>urn:uuid:{message_id}</wsa:MessageID>
              <wsa:ReplyTo>
                 <wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
              </wsa:ReplyTo>
              <wsa:To>{self.soap_url}</wsa:To>
              <fueloauth xmlns="http://exacttarget.com">{self.access_token}</fueloauth>
           </soap:Header>
           <soap:Body>
              <PerformRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
                 <Action>ClearData</Action>
                 <Definitions>
                    <Definition xsi:type="DataExtension">
                       <CustomerKey>{de_key}</CustomerKey>
                    </Definition>
                 </Definitions>
              </PerformRequestMsg>
           </soap:Body>
        </soap:Envelope>
        """
        
        headers = {
            'Content-Type': 'text/xml',
            'SOAPAction': soap_action
        }
        
        print(f"\n[MC] 🧹 Clearing Data Extension: {de_key}...")
        
        try:
            response = requests.post(self.soap_url, data=soap_envelope, headers=headers)
            
            # Check 200 OK and "OK" in Status
            if response.status_code == 200:
                # PerformResponseMsg usually contains <StatusCode>OK</StatusCode> inside <Result>
                if "<StatusCode>OK</StatusCode>" in response.text:
                     print("   ✅ DE Cleared successfully.")
                     return True, "OK"
                # Check for OverallStatus as well
                elif "<OverallStatus>OK</OverallStatus>" in response.text:
                     print("   ✅ DE Cleared successfully.")
                     return True, "OK"
                elif "Fault" in response.text:
                     print(f"   ❌ SOAP Fault: {response.text}")
                     return False, response.text
                else:
                     # If status is 200 but we don't see explicit OK, log it.
                     print(f"   ⚠️  Response (200 OK) but explicit Success missing: {response.text[:200]}...")
                     return True, "Assumed OK"
            
            elif "Fault" in response.text or "Error" in response.text:
                 print(f"   ❌ SOAP Fault: {response.text}")
                 return False, response.text
            
            else:
                 print(f"   ❌ HTTP Error {response.status_code}: {response.text}")
                 return False, response.text
                 
        except Exception as e:
            print(f"   🛑 Request Failed: {e}")
            return False, str(e)
