import os
import datetime
import time
from simple_salesforce import Salesforce
import csv
from .handlers.base_handler import BaseHandler
from .handlers.account_handler import AccountHandler
from .handlers.vehicle_handler import VehicleHandler
from .handlers.other_handlers import AssetHandler, VehicleDefinitionHandler, AssetAccountParticipantHandler

# --- HANDLER REGISTRY ---
# Register object-specific handlers here. Objects not in registry use BaseHandler.
HANDLER_REGISTRY = {
    'Account': AccountHandler,
    'Vehicle': VehicleHandler,
    'Asset': AssetHandler,
    'VehicleDefinition': VehicleDefinitionHandler,
    'AssetAccountParticipant': AssetAccountParticipantHandler,
}

class Auto360DataFactory:
    def __init__(self, sc_client: Salesforce):
        self.sc = sc_client
        self.created_ids = []
        self.key_map = {}

    def _get_handler(self, object_name):
        """Factory method to get the appropriate handler for an object."""
        handler_class = HANDLER_REGISTRY.get(object_name, BaseHandler)
        handler = handler_class(self)
        handler.object_name = object_name
        return handler 

    def _log(self, message, level="INFO"):
        """Pretty log output with timestamp"""
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        icons = {"INFO": "ℹ️", "SUCCESS": "✅", "ERROR": "🛑", "WARN": "⚠️", "DEBUG": "🐛"}
        icon = icons.get(level, "")
        print(f"[{timestamp}] {icon} {message}")

    def _calculate_date(self, shift):
        if shift is None or shift == "": return None
        try:
            return (datetime.date.today() + datetime.timedelta(days=int(shift))).isoformat()
        except ValueError:
            return None

    def _get_object_name(self, filename):
        """
        Extracts Salesforce Object Name from filename.
        Supports formats:
        - "01_Account.csv" -> "Account"
        - "Account.csv" -> "Account"
        - "Prefix - 02_Product2.csv" -> "Product2"
        - "06_BranchUnit_update.csv" -> "BranchUnit" (strips _update suffix)
        """
        clean_name = os.path.splitext(filename)[0]
        
        # 1. Handle "Prefix - ..." separator
        if " - " in clean_name:
            clean_name = clean_name.split(" - ")[-1]
            
        # 2. Strip leading sorting usage (digits/underscores)
        clean_name = clean_name.lstrip("0123456789_")
        
        # 3. Strip _update suffix (case-insensitive)
        if clean_name.lower().endswith('_update'):
            clean_name = clean_name[:-7]  # Remove last 7 chars ("_update")
        
        return clean_name

    def _get_existing_records(self, object_name, names, use_like_match=False):
        """
        Query Salesforce for existing records by Name.
        Returns dict: { Name: Id }
        If use_like_match is True, searches using LIKE '%name%' for each provided name.
        """
        if not names:
            return {}
        
        # We assume matching by 'Name'. 
        self._log(f"   🔎 Checking for {len(names)} existing records ({'LIKE' if use_like_match else 'EXACT'})...", "DEBUG")
        existing_map = {}
        
        unique_names = list(set(names))
        
        # Chunking: LIKE queries are longer, so reduce chunk size if using LIKE
        chunk_size = 50 if use_like_match else 200
        
        for i in range(0, len(unique_names), chunk_size):
            chunk = unique_names[i:i+chunk_size]
            # Escape single quotes in names
            safe_names = [n.replace("'", "\\'") for n in chunk]
            
            if use_like_match:
                # Construct OR clauses: Name LIKE '%val1%' OR Name LIKE '%val2%'
                or_clauses = [f"Name LIKE '%{n}%'" for n in safe_names]
                where_clause = " OR ".join(or_clauses)
                q = f"SELECT Id, Name FROM {object_name} WHERE ({where_clause})"
            else:
                # Standard IN clause
                ids_str = "'" + "','".join(safe_names) + "'"
                q = f"SELECT Id, Name FROM {object_name} WHERE Name IN ({ids_str})"
            
            try:
                res = self.sc.query_all(q)
                for r in res['records']:
                    existing_map[r['Name']] = r['Id']
            except Exception as e:
                self._log(f"   ⚠️ Warning: Could not query existing records: {e}", "WARN")
        
        return existing_map

    def _parse_row(self, row, row_idx, object_name):
        """
        Parses a single CSV row into a Salesforce payload.
        Returns: (sf_payload, base_name, row_copy) or (None, None, None) if skipped.
        """
        sf_payload = {}
        base_name = None 
        
        # --- PARSING ---
        for col, val in row.items():
            if val is None or val == "": continue
            
            if col == "_BaseName":
                base_name = val
                continue
            
            # Generic Reference Resolution
            # Syntax: _Ref:FieldName -> Value is the BaseName of the parent
            if col.startswith("_Ref:"):
                target_field = col.replace("_Ref:", "")
                # Support direct key match (including dot notation for extended keys)
                if val in self.key_map:
                    sf_payload[target_field] = self.key_map[val]
                else:
                   pass
                continue

            # Skip directive fields
            if col.startswith("_Return:"): continue
            if col.startswith("_"): continue 
            
            # Boolean Conversion (Fix for "TRUE"/"FALSE" strings)
            if isinstance(val, str):
                clean_val = val.strip().upper()
                if clean_val == 'TRUE':
                    val = True
                elif clean_val == 'FALSE':
                    val = False

            if col.endswith("__date"):
                real_field = col.replace("__date", "")
                sf_payload[real_field] = self._calculate_date(val)
                continue
                
            sf_payload[col] = val

        if not base_name and not sf_payload:
            return None, None, None

        row_copy = row.copy()
        row_copy['_EffectiveTo__date'] = self._calculate_date(row.get('_EffectiveTo__date'))

        return sf_payload, base_name, row_copy

    def process_csv_bulk(self, object_name, file_path, upsert_mode=False):
        """
        Reads CSV and creates (or updates) records using Bulk API.
        """
        abs_path = os.path.abspath(file_path)
        if not os.path.exists(file_path): 
            self._log(f"FILE NOT FOUND: {object_name}", "ERROR")
            return

        self._log(f"Processing {object_name} from {os.path.basename(file_path)} (Mode: {'UPSERT' if upsert_mode else 'INSERT ONLY'})...", "INFO")
        
        # Get Handler
        handler = self._get_handler(object_name)

        rows_data = [] # All processed rows ready for split
        names_to_check = [] # tailored for Upsert check
        
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                for row_idx, row in enumerate(reader, start=1):
                    
                    sf_payload, base_name, row_copy = self._parse_row(row, row_idx, object_name)
                    if not sf_payload: continue

                    # Collect Name for Upsert check
                    record_name = sf_payload.get('Name')
                    if upsert_mode and record_name:
                        names_to_check.append(record_name)
                    
                    rows_data.append({
                        "payload": sf_payload,
                        "metadata": {
                            "base_name": base_name,
                            "row_data": row_copy,
                            "csv_row_idx": row_idx
                        }
                    })

            if not rows_data:
                self._log("No records to process.", "WARN")
                return

            # --- UPSERT LOGIC SPLIT ---
            to_insert = []
            to_update = []
            
            existing_map = {} # Name -> Id
            
            # 1. Exact Name Match (Priority)
            if upsert_mode and names_to_check:
                exact_res = self._get_existing_records(object_name, names_to_check, use_like_match=False)
                existing_map.update(exact_res)

            # 2. Fuzzy BaseName Match (Fallback)
            if upsert_mode:
                base_names_fuzzy = []
                for item in rows_data:
                     p_name = item['payload'].get('Name')
                     if p_name and p_name in existing_map:
                         continue
                     if item['metadata']['base_name']:
                         base_names_fuzzy.append(item['metadata']['base_name'])

                if base_names_fuzzy:
                    fuzzy_res = self._get_existing_records(object_name, base_names_fuzzy, use_like_match=True)
                    existing_map.update(fuzzy_res)
                    
            if existing_map:
                self._log(f"   ℹ️ Found {len(existing_map)} existing records (Exact/Fuzzy).", "INFO")

            for item in rows_data:
                payload = item['payload']
                meta = item['metadata']
                p_name = payload.get('Name')
                base_name = meta['base_name']
                
                match_id = None
                
                # A. Exact Match
                if p_name and p_name in existing_map:
                    match_id = existing_map[p_name]
                
                # B. Fuzzy Match
                if not match_id and base_name:
                    for sf_name, sf_id in existing_map.items():
                        if base_name in sf_name:
                            match_id = sf_id
                            break
                
                # Check match
                if upsert_mode and match_id:
                    # UPDATE
                    payload['Id'] = match_id
                    to_update.append(item)
                    if base_name:
                        self.key_map[base_name] = match_id
                else:
                    # INSERT
                    to_insert.append(item)

            # --- EXECUTE BATCHES ---
            # 1. Update (process these first, they are faster/safer)
            if to_update:
                self._send_batch(to_update, 'update', object_name, handler)
            
            # 2. Insert
            if to_insert:
                self._send_batch(to_insert, 'insert', object_name, handler)

        except Exception as file_e:
             self._log(f"Error reading CSV file: {file_e}", "ERROR")

    def _send_batch(self, batch_items, operation, object_name, handler):
        """
        Executes a bulk insert or update operation.
        
        Args:
            batch_items: List of items with payload and metadata
            operation: 'insert' or 'update'
            object_name: Salesforce object API name
            handler: Handler instance for post-processing
        """
        if not batch_items: 
            return
        
        payloads = [x['payload'] for x in batch_items]
        
        # Filter Immutable Fields for UPDATE using Handler
        if operation == 'update':
            forbidden = handler.get_immutable_fields()
            if forbidden:
                for p in payloads:
                    for f in forbidden:
                        if f in p: 
                            del p[f]

        # --- BATCH SPLITTING FOR ACCOUNT (Trigger Workaround) ---
        # Account triggers fail silently with >20 records due to governor limits
        ACCOUNT_BATCH_SIZE = 20
        chunk_size = ACCOUNT_BATCH_SIZE if object_name == 'Account' and operation == 'insert' else len(payloads)
        
        for chunk_idx in range(0, len(payloads), chunk_size):
            chunk_payloads = payloads[chunk_idx:chunk_idx + chunk_size]
            chunk_items = batch_items[chunk_idx:chunk_idx + chunk_size]
            
            if chunk_size < len(payloads):
                self._log(f"   🚀 Sending {len(chunk_payloads)} records (Chunk {chunk_idx//chunk_size + 1}/{(len(payloads) + chunk_size - 1)//chunk_size}) ({operation.upper()})...", "INFO")
            else:
                self._log(f"   🚀 Sending {len(chunk_payloads)} records ({operation.upper()})...", "INFO")
            
            # Bulk call
            try:
                if operation == 'insert':
                    results = getattr(self.sc.bulk, object_name).insert(chunk_payloads)
                else:
                    results = getattr(self.sc.bulk, object_name).update(chunk_payloads)
                    
                success_count = 0

                for i, res in enumerate(results):
                    item = chunk_items[i]
                    meta = item['metadata']
                    base_name = meta['base_name']

                    if res['success']:
                        success_count += 1
                        real_id = res['id']
                        
                        if operation == 'insert':
                            self.created_ids.insert(0, (object_name, real_id))
                        
                        if base_name:
                            self.key_map[base_name] = real_id
                    else:
                        self._log(f"   ❌ Error {operation} {base_name or 'Row'}: {res['errors']}", "ERROR")

                self._log(f"   ✅ {operation.title()} Complete. Success: {success_count}/{len(results)}", "SUCCESS")
                
                # --- POST PROCESSING (Handled by Strategy) ---
                handler.after_insert_batch(chunk_items, results, operation)

            except Exception as e:
                self._log(f"CRITICAL BULK {operation.upper()} ERROR: {e}", "ERROR")

    def cleanup_scenario(self, folder_path):
        """
        Cleanup data defined in the scenario files.
        Processed in REVERSE alpha order (children first).
        """
        abs_folder = os.path.abspath(folder_path)
        if not os.path.exists(abs_folder):
            self._log(f"Scenario folder not found: {folder_path}", "ERROR")
            return

        self._log(f"🧹 Starting SCENARIO CLEANUP from: {os.path.basename(abs_folder)}", "INFO")

        files = [f for f in os.listdir(abs_folder) if f.lower().endswith(".csv")]
        files.sort(reverse=True) # REVERSE ORDER for deletion

        if not files:
            self._log("No CSV files found.", "WARN")
            return

        for filename in files:
            object_name = self._get_object_name(filename)
            full_path = os.path.join(abs_folder, filename)
            
            self._log(f"   Analysing {object_name} from {filename}...", "INFO")
            
            # 1. Parse file to find BaseNames or Names
            base_names_fuzzy = [] # For LIKE '%...%'
            names_exact = []      # For exact match
            
            try:
                with open(full_path, mode='r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row_idx, row in enumerate(reader, start=1):
                         parsed_result = self._parse_row(row, row_idx, object_name)
                         if not parsed_result or parsed_result[0] is None:
                             continue
                         
                         payload, base_name, _ = parsed_result
                         
                         # User Rule: 
                         # 1. If 'Name' exists -> Exact match (Priority)
                         # 2. Else if '_BaseName' exists -> Fuzzy match
                         if payload and 'Name' in payload:
                             names_exact.append(payload['Name'])
                         elif base_name:
                             base_names_fuzzy.append(base_name)

            except Exception as e:
                self._log(f"   ❌ Error reading file: {e}", "ERROR")
                continue
            
            if not base_names_fuzzy and not names_exact:
                self._log(f"      No '_BaseName' or 'Name' found to use for deletion in {filename}.", "WARN")
                continue

            ids_to_delete = []

            # 2a. Fuzzy Match (_BaseName)
            if base_names_fuzzy:
                 self._log(f"      Searching for {len(base_names_fuzzy)} patterns (LIKE match)...", "DEBUG")
                 existing = self._get_existing_records(object_name, base_names_fuzzy, use_like_match=True)
                 ids_to_delete.extend(existing.values())
            
            # 2b. Exact Match (Name)
            if names_exact:
                 self._log(f"      Searching for {len(names_exact)} names (Exact match)...", "DEBUG")
                 existing = self._get_existing_records(object_name, names_exact, use_like_match=False)
                 ids_to_delete.extend(existing.values())
            
            # Deduplicate IDs
            ids_to_delete = list(set(ids_to_delete))

            if ids_to_delete:
                # Delegate deletion to Handler (encapsulates cascade logic)
                handler = self._get_handler(object_name)
                handler.delete_records(ids_to_delete)
            else:
                self._log(f"      No matching records found in Salesforce.", "INFO")

    def run_scenario(self, folder_path, upsert=False):
        """
        Generic scenario runner.
        1. Scans folder for .csv files
        2. Sorts them alphabetically
        3. Parses ObjectName from filename (e.g. '01_Account.csv' -> 'Account')
        4. Processes each file
        """
        abs_folder = os.path.abspath(folder_path)
        if not os.path.exists(abs_folder):
            self._log(f"Scenario folder not found: {folder_path}", "ERROR")
            return

        self._log(f"🚀 Starting Scenario from: {os.path.basename(abs_folder)}", "INFO")

        files = [f for f in os.listdir(abs_folder) if f.lower().endswith(".csv")]
        files.sort() # Ensure 01_ runs before 02_

        if not files:
            self._log("No CSV files found in scenario folder.", "WARN")
            return

        for filename in files:
            # Uses shared extraction logic
            object_name = self._get_object_name(filename)
            
            # --- AUTO-DETECT UPDATE MODE ---
            # Convention: files with '_update' suffix trigger UPSERT mode
            # Example: "03_BranchUnit_update.csv" -> UPDATE existing BranchUnit records
            # This solves circular dependency issues (e.g., BranchUnit <-> BranchUnitBusinessMember)
            file_basename = os.path.splitext(filename)[0]  # Remove .csv
            is_update_file = '_update' in file_basename.lower()
            
            # Use file-specific upsert OR global --upsert flag
            use_upsert = upsert or is_update_file
            
            full_path = os.path.join(abs_folder, filename)
            self.process_csv_bulk(object_name, full_path, upsert_mode=use_upsert)

    def delete_by_pattern(self, object_name, name_pattern):
        """
        Delete records by LIKE pattern using handler cascade logic.
        
        Args:
            object_name: Salesforce object API name (e.g. 'Account', 'Opportunity')
            name_pattern: SQL LIKE pattern (e.g. 'LostSale%', '%Test%')
        """
        self._log(f"🔥 Deleting {object_name} by pattern: {name_pattern}", "INFO")
        
        handler = self._get_handler(object_name)
        existing = self._get_existing_records(object_name, [name_pattern], use_like_match=True)
        
        if existing:
            handler.delete_records(list(existing.values()))
        else:
            self._log(f"   No matching {object_name} records found.", "INFO")
