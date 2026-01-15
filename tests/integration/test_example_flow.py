import pytest
import time
from src.testing.verifier import CSVVerifier

# Pytest fixture to setup the environment (defined in conftest.py)
# We assume 'sc_client' (Salesforce) and 'mc_client' (Marketing Cloud) are available.

def test_lost_sale_funding_step1(sc_client, mc_client):
    """
    Step 1: Prepare test data and verify entry into 'Funding' DE.
    """
    from src.data_factory import Auto360DataFactory
    
    # 1. PREPARE DATA (Salesforce)
    print("\n--- [Step 1.1] Generating Data in Salesforce ---")
    factory = Auto360DataFactory(sc_client)
    
    # Run the specific scenario folder
    # This creates Accounts, Opportunities, Contacts etc.
    scenario_path = "data/lost_sale_cnst" 
    factory.run_scenario(scenario_path)

    # 2. TRIGGER AUTOMATIONS (Marketing Cloud)
    print("\n--- [Step 1.2] Processing in Marketing Cloud ---")
    
    # A. Wait for Synchronization (Real world: 15 mins, or forced)
    # print("Waiting for Sync...")
    # time.sleep(60) 
    
    # B. Run Consent Collection Automation
    auto_consent = "Auto360_ConsentCollection_MBU_NO_ENT"
    mc_client.run_automation(auto_consent)
    
    # Wait for it to finish? (We can add polling logic later)
    time.sleep(10) 

    # C. Run Main Journey Automation
    auto_journey = "Auto360_Lost_Sale_Journey_Flows_with_Consents"
    mc_client.run_automation(auto_journey)
    
    # 3. VERIFY (The Core Logic)
    print("\n--- [Step 1.3] Verifying Results ---")
    verifier = CSVVerifier(mc_client, factory)
    
    # We want to check if the OPPORTUNITIES created ended up in the Funding DE.
    # factory.key_map holds {'Funding_Opp': '006...', 'Excluded_Opp': '006...'}
    
    # Extract just the Opportunity records we created to check them
    # (In reality, we might filter this list better)
    created_opps = [
        item for item in factory.created_ids_full_log 
        if item['object'] == 'Opportunity'
    ]
    
    # TARGET DE: Auto360_Lost_Sale_Funding_Engagement
    # KEY: The column in that DE that holds the Opportunity Id is likely "OpportunityId" or "Id"
    target_de = "Auto360_Lost_Sale_Funding_Engagement"
    de_key_column = "OpportunityId" 
    
    verifier.verify_mc_logic(
        csv_rows=created_opps, 
        de_target=target_de, 
        mc_key_column=de_key_column
    )
    
    # 4. ASSERTIONS (Manual Check in Code)
    # Check specific logic for specific records
    
    # Case 1: "Funding_Opp_1" SHOULD be there
    opp_1_id = factory.key_map.get('Funding_Opp_1')
    rows_1 = mc_client.fetch_de_rows(target_de, de_key_column, opp_1_id)
    assert len(rows_1) > 0, "Error: Funding_Opp_1 NOT found in Funding DE!"
    
    # Case 6: "Excluded_Opp" should NOT be there
    opp_6_id = factory.key_map.get('Excluded_Opp')
    rows_6 = mc_client.fetch_de_rows(target_de, de_key_column, opp_6_id)
    assert len(rows_6) == 0, "Error: Excluded_Opp WAS found in Funding DE (Should be excluded)!"

    print("\n✅ Test Step 1 Passed!")
