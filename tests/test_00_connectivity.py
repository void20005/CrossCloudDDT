import pytest

def test_sales_cloud_connection(sc_client):
    """Verifies read access to Sales Cloud Organization data."""
    print("\n**** SALES CLOUD CONNECTION TEST ****")
    
    # Query Organization details
    results = sc_client.query("SELECT Id, Name, OrganizationType FROM Organization LIMIT 1")
    
    # Verify result is not empty
    assert results['totalSize'] > 0
    
    record = results['records'][0]
    org_name = record['Name']
    org_type = record['OrganizationType']
    
    print(f"\n✅ SUCCESS! Connected to Salesforce.")
    print(f"🏢 Org Name: {org_name}")
    print(f"📋 Org Type: {org_type}")
    print("------------------------------------")

def test_marketing_cloud_connection(mc_client):
    """Verifies connection to Marketing Cloud and Journeys API retrieval."""
    print("\n**** MARKETING CLOUD CONNECTION TEST ****")
    
    # 1. Verify Token existence
    assert mc_client.access_token is not None, "🛑 Error: Access Token was not received!"
    print(f"✅ Auth Token: OK")
    
    # 2. Verify real API data request
    try:
        count = mc_client.get_journey_count()
        print(f"✅ SUCCESS! Total Journeys in account: {count}")
        
        # Ensure we got a number back (even if it is 0)
        assert isinstance(count, int), "Error: Journey count is not an integer"
        
    except Exception as e:
        pytest.fail(f"🛑 API Error: {e}")