from datetime import datetime
import time

class CSVVerifier:
    def __init__(self, mc_client, data_factory, logger=print):
        self.mc = mc_client
        self.factory = data_factory
        self.log = logger
        self.errors = []
        self.warnings = []

    def verify_mc_logic(self, csv_rows, de_target, id_column="Id", mc_key_column="SubscriberKey"):
        """
        Verifies that records from CSV ended up (or didn't end up) in the Target DE.
        
        Args:
            csv_rows (list): List of dicts (payload + metadata) from Data Factory.
            de_target (str): Name/Key of the Target Data Extension.
            id_column (str): Field in CSV to resolve ID (usually 'Id' from Salesforce).
            mc_key_column (str): Field in MC DE that holds the ID (e.g. 'SubscriberKey' or 'Id').
        """
        self.log(f"\n🔍 [VERIFIER] Inspecting Target DE: {de_target}...")
        
        for item in csv_rows:
            metadata = item.get('metadata', {})
            base_name = metadata.get('base_name')
            
            # 1. Resolve Salesforce ID
            # Use base_name (Key) to find the created SF ID
            sf_id = self.factory.key_map.get(base_name)
            
            if not sf_id:
                self.log(f"   ⚠️ SKIP: Could not resolve SF ID for '{base_name}'. Record might not be created.")
                continue

            # 2. Check Expectations (Parsed from item metadata or passed externally)
            # You can pass expectations in the metadata or derive them.
            # For now, let's assume we check PRESENCE by default, unless specified otherwise.
            # TODO: Add logic to read _ER:ExpectAbsent from CSV
            
            # 3. Check MC Data Extension
            # We look for ANY records linked to this SF ID
            rows = self.mc.fetch_de_rows(de_target, mc_key_column, sf_id)
            
            if rows:
                self.log(f"   ✅ FOUND: {len(rows)} record(s) for {base_name} ({sf_id}) in {de_target}")
                for r in rows:
                    self._inspect_row(base_name, r)
            else:
                # If we EXPECTED it to be there -> FAIL
                # If we EXPECTED it NOT to be there -> PASS (logic needs to be handled by caller)
                self.log(f"   ❓ ABSENT: {base_name} ({sf_id}) not found in {de_target}")
                self.warnings.append(f"{base_name} missing from {de_target}")

    def _inspect_row(self, base_name, row):
        """
        Deep inspection of a found row.
        """
        empty_fields = [k for k, v in row.items() if v is None or v == ""]
        if empty_fields:
            self.log(f"      ⚠️ WARNING: Empty Fields for {base_name}: {', '.join(empty_fields)}")
