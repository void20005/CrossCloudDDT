import os
import sys
import argparse
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from mc_client import MarketingCloudClient

import csv

# --- USER CONFIG ---
# DE_NAMES list removed in favor of CSV input

def load_environment(args):
    """
    Loads environment variables based on arguments.
    """
    if args.env:
        env_file = f".env.{args.env}"
    else:
        env_file = ".env.qa" # Default
        
    if not os.path.exists(env_file):
        print(f"🛑 Error: Configuration file '{env_file}' not found.")
        sys.exit(1)

    print(f"🌍 Loading configuration from: {env_file}")
    load_dotenv(env_file, override=True)

def read_de_names_from_csv(csv_path):
    """
    Reads DE names from a CSV file.
    Expects a header row with a column 'Name' (case-insensitive).
    If 'Name' column is not found, uses the first column.
    """
    de_names = []
    if not os.path.exists(csv_path):
        print(f"🛑 Error: CSV file '{csv_path}' not found.")
        sys.exit(1)

    try:
        with open(csv_path, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            # Identify the column to use
            if reader.fieldnames:
                name_col = next((col for col in reader.fieldnames if col.lower() in ['name', 'de name', 'de_name']), None)
                if not name_col:
                    print(f"⚠️  Warning: No 'Name' column found in CSV. Using the first column: '{reader.fieldnames[0]}'")
                    name_col = reader.fieldnames[0]
            else:
                print("🛑 Error: CSV file appears to be empty or invalid.")
                sys.exit(1)

            for row in reader:
                if row[name_col]:
                    de_names.append(row[name_col].strip())
                    
    except Exception as e:
        print(f"🛑 Error reading CSV: {e}")
        sys.exit(1)
        
    return de_names

def clean_subscribers():
    parser = argparse.ArgumentParser(description="Cleanup MC Subscribers")
    parser.add_argument("--env", help="Environment to use (qa, stage, prod). Default: qa", default="qa")
    parser.add_argument("--csv", required=True, help="Path to CSV file containing DE names")
    args = parser.parse_args()

    load_environment(args)
    
    de_names = read_de_names_from_csv(args.csv)

    print("🚀 Initializing Marketing Cloud Client...")
    
    mc = MarketingCloudClient(
        os.getenv("MC_CLIENT_ID"),
        os.getenv("MC_CLIENT_SECRET"),
        os.getenv("MC_SUBDOMAIN"),
        os.getenv("MC_ACCOUNT_ID")
    )
    mc.connect()
    
    all_subscribers_found = []
    
    print(f"\n📂 Processing {len(de_names)} Data Extensions from CSV...")
    
    for de_name in de_names:
        print(f"\n   🔍 Checking DE: {de_name}")
        
        # 1. Resolve Name -> Key
        customer_key = mc.get_de_customer_key(de_name)
        if not customer_key:
             print(f"      ⚠️  Could not find DE Key for Name '{de_name}' (Check permissions or exact name match).")
             continue

        # 2. Fetch using Key
        rows = mc.fetch_de_rows(de_key=customer_key)
        
        if not rows:
            print("      No records found.")
            continue
            
        print(f"      Found {len(rows)} rows. Cleaning Subscribers...")
        
        for row in rows:
            # MC REST API returns: { "keys": {...}, "values": {...} }
            values = row.get('values', {})
            keys = row.get('keys', {})
            
            # Merge
            full_row = {**values, **keys}
            
            # Case-insensitive lookup
            row_lower = {k.lower(): v for k, v in full_row.items()}
            contact_id = row_lower.get('contactid') or row_lower.get('id') or row_lower.get('subscriberkey')
            
            if not contact_id:
                print(f"      ⚠️  Row missing 'ContactId'. Attributes: {list(full_row.keys())}")
                continue
            
            # Collect for Final Report
            all_subscribers_found.append(contact_id)
            
            # Delete Subscriber
            # print(f"      🗑️  Deleting Subscriber: {contact_id}...")
            success, msg = mc.delete_subscriber(contact_id)
            
            if success:
                print(f"         ✅ Deleted: {contact_id}")
            else:
                print(f"         ❌ Failed to delete {contact_id}: {msg}")

    # --- SUMMARY & SQL ---
    unique_subs = list(set(all_subscribers_found))
    print(f"\n\n✨ CLEANUP COMPLETE. Processed {len(unique_subs)} unique subscribers from DEs.")
    
    if unique_subs:
        print("\n📝 Use this SQL in Query Studio to verify ALL found subscribers:")
        
        # Format list for SQL IN clause
        ids_str = "'" + "','".join(unique_subs) + "'"
        sql = f"""
        SELECT 
            SubscriberKey, 
            Status 
        FROM 
            _Subscribers 
        WHERE 
            SubscriberKey IN ({ids_str})
        """
        print(sql)
        print("\n(Ideally, this result should be EMPTY if everyone was deleted)")
    else:
        print("\nNo subscribers were found in the scanned Data Extensions.")

if __name__ == "__main__":
    clean_subscribers()
