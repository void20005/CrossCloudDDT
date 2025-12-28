import time

class BaseHandler:
    """
    Base Strategy Handler for Salesforce Objects.
    Defines standard behavior which can be overridden by specific object handlers.
    """
    def __init__(self, factory):
        self.factory = factory
        self.sc = factory.sc
        self.object_name = None # Set by subclass

    def get_immutable_fields(self):
        """Returns list of fields that cannot be updated (Insert-only)"""
        return []

    def delete_records(self, ids_to_delete):
        """Standard deletion logic"""
        if not ids_to_delete: return

        # Deduplicate
        ids_to_delete = list(set(ids_to_delete))
        self.factory._log(f"      Found {len(ids_to_delete)} records to delete.", "INFO")
        
        try:
            payload = [{"Id": x} for x in ids_to_delete]
            getattr(self.sc.bulk, self.object_name).delete(payload)
            self.factory._log(f"      ✅ Deleted {len(ids_to_delete)} records from {self.object_name}", "SUCCESS")
        except Exception as e:
            self.factory._log(f"      ❌ Error deleting {self.object_name}: {e}", "ERROR")

    def after_insert_batch(self, batch_items, results, operation='insert'):
        """
        Post-processing hook after a batch insert/update.
        Default: Check for '_Return:Field' columns and capture them.
        """
        # --- RETURN FIELDS PROCESSING (Generic) ---
        newly_created_map = {} # Id -> BaseName
        
        # 1. Map Results to BaseNames
        for i, res in enumerate(results):
            if res['success']:
                item = batch_items[i]
                meta = item['metadata']
                base_name = meta['base_name']
                real_id = res['id']
                if base_name:
                    newly_created_map[real_id] = base_name

        # 2. Identify requested return fields
        return_fields_map = {} # { CSV_Col_Name : Real_Field_Name }
        if batch_items:
            first_row = batch_items[0]['metadata']['row_data']
            for k in first_row.keys():
                if k.startswith("_Return:"):
                    real_field = k.replace("_Return:", "")
                    return_fields_map[k] = real_field

        # 3. Poll and Capture
        if return_fields_map and newly_created_map:
            fields_to_query = list(return_fields_map.values())
            ids_to_query = list(newly_created_map.keys())
            
            max_retries = 10
            self.factory._log(f"   ⏳ Parsing '_Return' fields. Polling up to {max_retries} times...", "INFO")

            for attempt in range(1, max_retries + 1):
                captured_count = 0
                
                q_chunk_size = 200
                for i in range(0, len(ids_to_query), q_chunk_size):
                        chunk_ids = ids_to_query[i:i+q_chunk_size]
                        ids_str = "'" + "','".join(chunk_ids) + "'"
                        fields_str = ", ".join(fields_to_query)
                        q = f"SELECT Id, {fields_str} FROM {self.object_name} WHERE Id IN ({ids_str})"
                        
                        try:
                            res_ret = self.sc.query_all(q)
                            for r in res_ret['records']:
                                r_id = r['Id']
                                base_name = newly_created_map.get(r_id)
                                if base_name:
                                    for csv_col, real_field in return_fields_map.items():
                                        val = r.get(real_field)
                                        if val:
                                            compound_key = f"{base_name}.{real_field}"
                                            self.factory.key_map[compound_key] = val
                                            captured_count += 1
                        except Exception as e:
                            self.factory._log(f"      ❌ Error fetching return fields: {e}", "ERROR")
                
                if captured_count >= len(ids_to_query):
                    break
                
                if attempt < max_retries:
                    time.sleep(0.5)

    def get_field_values_batch(self, record_ids, field_name, chunk_size=200):
        """
        Retrieves the value of 'field_name' for the given record_ids on the current object.
        Returns a list of values (deduplicated, None removed).
        Useful for finding parent/related IDs (e.g. Vehicle -> AssetId).
        """
        if not record_ids: return []
        
        values = []
        record_ids = list(set(record_ids))
        
        for i in range(0, len(record_ids), chunk_size):
            chunk = record_ids[i:i+chunk_size]
            ids_str = "'" + "','".join(chunk) + "'"
            try:
                q = f"SELECT {field_name} FROM {self.object_name} WHERE Id IN ({ids_str})"
                res = self.sc.query_all(q)
                batch_vals = [r[field_name] for r in res['records'] if r.get(field_name)]
                values.extend(batch_vals)
            except Exception as e:
                self.factory._log(f"      ⚠️ Warning: Could not query {field_name} on {self.object_name}: {e}", "WARN")
        
        return list(set(values))

    def get_child_records_batch(self, parent_ids, child_object, foreign_key_field, chunk_size=200):
        """
        Finds records in 'child_object' where 'foreign_key_field' is in 'parent_ids'.
        Returns list of child IDs.
        Useful for finding CPTC where PartyId IN (IndividualIds).
        """
        if not parent_ids: return []
        
        child_ids = []
        parent_ids = list(set(parent_ids))

        for i in range(0, len(parent_ids), chunk_size):
            chunk = parent_ids[i:i+chunk_size]
            ids_str = "'" + "','".join(chunk) + "'"
            try:
                q = f"SELECT Id FROM {child_object} WHERE {foreign_key_field} IN ({ids_str})"
                res = self.sc.query_all(q)
                child_ids.extend([r['Id'] for r in res['records']])
            except Exception as e:
                self.factory._log(f"      ⚠️ Warning: Could not query related {child_object}: {e}", "WARN")
                
        return list(set(child_ids))
