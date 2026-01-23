import time
from .base_handler import BaseHandler

class AccountHandler(BaseHandler):
    def __init__(self, factory):
        super().__init__(factory)
        self.object_name = 'Account'

    def get_immutable_fields(self):
        return []

    def delete_records(self, ids_to_delete):
        """
        Cascade delete for Accounts (Person Accounts):
        1. Find Individuals
        2. Find CPTC
        3. Delete CPTC -> Account -> Individual
        """
        if not ids_to_delete: return
        
        # Deduplicate
        ids_to_delete = list(set(ids_to_delete))
        self.factory._log(f"      Found {len(ids_to_delete)} records to delete.", "INFO")
        
        # --- CASCADE LOOKUP ---
        self.factory._log("      🔎 Checking for related Person Account artifacts (Individual, CPTC)...", "DEBUG")
        
        # 1. Get Individuals (PersonIndividualId) from Accounts
        ind_ids = self.get_field_values_batch(
            record_ids=ids_to_delete, 
            field_name='PersonIndividualId'
        )
        
        # 2. Get CPTC from Individuals
        cptc_ids = self.get_child_records_batch(
            parent_ids=ind_ids,
            child_object='ContactPointTypeConsent',
            foreign_key_field='PartyId'
        )

        # --- EXECUTION ---
        # 1. CPTC
        if cptc_ids:
            try:
                self.factory._log(f"      🗑️ Cascade Deleting {len(cptc_ids)} related CPTC records...", "INFO")
                payload = [{"Id": x} for x in list(set(cptc_ids))]
                self.sc.bulk.ContactPointTypeConsent.delete(payload)
                self.factory._log(f"      ✅ Deleted related CPTC.", "SUCCESS")
            except Exception as e:
                self.factory._log(f"      ❌ Error deleting CPTC: {e}", "ERROR")

        # 2. Account (Self)
        super().delete_records(ids_to_delete)

        # 3. Individual
        if ind_ids:
            try:
                self.factory._log(f"      🗑️ Cascade Deleting {len(ind_ids)} related Individual records...", "INFO")
                payload = [{"Id": x} for x in ind_ids]
                self.sc.bulk.Individual.delete(payload)
                self.factory._log(f"      ✅ Deleted related Individuals.", "SUCCESS")
            except Exception as e:
                self.factory._log(f"      ❌ Error deleting Individuals: {e}", "ERROR")


    def after_insert_batch(self, batch_items, results, operation='insert'):
        """
        Handle Consents after Account creation/update.
        """
        # Call base (RETURN FIELDS)
        super().after_insert_batch(batch_items, results, operation)

        # --- ENRICHMENT (PersonContactId) ---
        if operation == 'insert':
            self._enrich_contact_ids(batch_items, results)
        
        # --- CONSENT LOGIC ---
        # Detect if we need to process consents
        acc_map_consents = {}
        
        # Check if consent fields are present in the batch rows
        # We need to peek at one row
        if not batch_items: return
        first_row = batch_items[0]['metadata']['row_data']
        consent_field_names = {'_HasOptedOutSolicit', '_EmailConsent', '_SMSConsent'}
        has_consent_cols = any(n in first_row for n in consent_field_names)
        
        if has_consent_cols:
            for i, res in enumerate(results):
                if res['success']:
                    real_id = res['id']
                    item = batch_items[i]
                    acc_map_consents[real_id] = item['metadata']['row_data']

            if acc_map_consents:
                 self._manage_consents(acc_map_consents)

    def _enrich_contact_ids(self, batch_items, results):
        """
        Retrieves PersonContactId for created Accounts and updates key_map.
        Format: key_map["BaseName.PersonContactId"] = ContactId
        """
        # 1. Collect successful Account Ids
        success_map_idx = {} # { real_id: batch_index }
        for i, res in enumerate(results):
            if res['success']:
                success_map_idx[res['id']] = i
        
        if not success_map_idx: return

        account_ids = list(success_map_idx.keys())
        self.factory._log(f"   🔎 Fetching PersonContactId for {len(account_ids)} Accounts...", "INFO")
        
        # 2. Query with Retry
        contact_map = {} # { AccountId: ContactId }
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            missing_ids = [aid for aid in account_ids if aid not in contact_map]
            if not missing_ids: break
            
            # Chunking query just in case (though batch is small ~20)
            chunk_size = 200
            for i in range(0, len(missing_ids), chunk_size):
                chunk = missing_ids[i:i+chunk_size]
                ids_str = "'" + "','".join(chunk) + "'"
                try:
                    q = f"SELECT Id, PersonContactId FROM Account WHERE Id IN ({ids_str})"
                    res = self.sc.query_all(q)
                    for r in res['records']:
                        pc_id = r.get('PersonContactId')
                        if pc_id:
                            contact_map[r['Id']] = pc_id
                except Exception as e:
                    self.factory._log(f"      ⚠️ Error querying PersonContactId: {e}", "WARN")
            
            if len(contact_map) < len(account_ids):
                if attempt < max_retries:
                    time.sleep(2) # Wait for propagation
                else:
                    self.factory._log(f"      ❌ Failed to retrieve PersonContactId for some records after {max_retries} attempts.", "ERROR")

        # 3. Update KeyMap
        count_updated = 0
        for acc_id, contact_id in contact_map.items():
            idx = success_map_idx[acc_id]
            item = batch_items[idx]
            base_name = item['metadata']['base_name']
            
            if base_name:
                # Append special key
                key = f"{base_name}.PersonContactId"
                self.factory.key_map[key] = contact_id
                count_updated += 1
                
        self.factory._log(f"      ✅ Captured {count_updated} PersonContactIds.", "SUCCESS")

    def _manage_consents(self, account_map):
        """
        Internal logic to handle Consents for Accounts.
        """
        if not account_map: return

        self.factory._log(f"   Managing Consents for {len(account_map)} Accounts (Bulk)...", "INFO")
        account_ids = list(account_map.keys())
        
        # --- PHASE 1: Wait for Individuals ---
        ind_map = {} # { individual_id: account_id }
        
        # Optimized polling (using BaseHandler helper?)
        # BaseHandler has `get_field_values_batch`, but we need Map {IndId -> AccId}.
        # Helper returns just values.
        # Let's keep manual query loop for Mapping purposes.
        
        max_attempts_ind = 20
        found_all = False
        
        for attempt in range(1, max_attempts_ind + 1):
            missing_acc_ids = [aid for aid in account_ids if aid not in ind_map.values()]
            if not missing_acc_ids:
                found_all = True
                break
                
            chunk_size = 200
            for i in range(0, len(missing_acc_ids), chunk_size):
                chunk = missing_acc_ids[i:i+chunk_size]
                ids_str = "'" + "','".join(chunk) + "'"
                try:
                    q = f"SELECT Id, PersonIndividualId FROM Account WHERE Id IN ({ids_str})"
                    res = self.sc.query_all(q)
                    for r in res['records']:
                        pid = r.get('PersonIndividualId')
                        if pid:
                            ind_map[pid] = r['Id']
                except Exception as e:
                    self.factory._log(f"      ⚠️ Warning checking Individuals: {e}", "WARN")

            if len(ind_map) == len(account_ids):
                found_all = True
                break
            
            if attempt < max_attempts_ind:
                time.sleep(1) # Fast poll
        
        if not ind_map:
             self.factory._log("      ❌ No Individuals found. Skipping consents.", "ERROR")
             return

        # --- Update Individuals (HasOptedOutSolicit) ---
        self.factory._log(f"      🔎 Found {len(ind_map)} Individual records linked to these Accounts.", "INFO")
        
        ind_updates = []
        for ind_id, acc_id in ind_map.items():
            row = account_map[acc_id]
            dont_market_str = row.get('_HasOptedOutSolicit', '').upper()
            if dont_market_str == 'TRUE':
                ind_updates.append({"Id": ind_id, "HasOptedOutSolicit": True})
        
        if ind_updates:
            try:
                self.factory._log(f"      📝 Identified {len(ind_updates)} Individuals requiring update...", "INFO")
                self.sc.bulk.Individual.update(ind_updates)
                self.factory._log(f"      ✅ Successfully updated {len(ind_updates)} Individuals.", "SUCCESS")
            except Exception as e:
                self.factory._log(f"      ❌ Error updating Individuals: {e}", "ERROR")
        else:
             self.factory._log(f"      ℹ️ No Individual updates needed.", "INFO")

        # --- PHASE 2: Wait for CPTC ---
        ind_ids = list(ind_map.keys())
        cptc_map = {} # { ind_id: [cptc_records] }
        
        max_attempts_cptc = 15
        
        for attempt in range(1, max_attempts_cptc + 1):
             # Check if we have CPTC for all inds?
             # CPTC creation is async.
             
             # Identify which Inds we don't have CPTC for yet
             missing_ind_ids = [iid for iid in ind_ids if iid not in cptc_map]
             if not missing_ind_ids:
                 break
                 
             chunk_size = 200
             for i in range(0, len(missing_ind_ids), chunk_size):
                 chunk = missing_ind_ids[i:i+chunk_size]
                 ids_str = "'" + "','".join(chunk) + "'"
                 try:
                     q = f"SELECT Id, PartyId, EngagementChannelType.Name, DataUsePurpose.Name, PrivacyConsentStatus FROM ContactPointTypeConsent WHERE PartyId IN ({ids_str})"
                     res = self.sc.query_all(q)
                     for r in res['records']:
                         pid = r['PartyId']
                         if pid not in cptc_map: cptc_map[pid] = []
                         cptc_map[pid].append(r)
                 except Exception:
                     pass
             
             if len(cptc_map) >= len(ind_ids):
                 break
             
             if attempt < max_attempts_cptc:
                 time.sleep(1)

        # --- Prepare CPTC Updates ---
        cptc_updates = []
        for ind_id, records in cptc_map.items():
            acc_id = ind_map.get(ind_id)
            if not acc_id: continue
            row = account_map[acc_id]
            
            for r in records:
                # Safely get Channel and Purpose
                channel_obj = r.get('EngagementChannelType')
                channel = channel_obj['Name'] if channel_obj else None
                
                purpose_obj = r.get('DataUsePurpose')
                purpose = purpose_obj['Name'] if purpose_obj else None
                
                if not channel: continue

                # Determine Target Status
                # Priority 1: Specific Override (e.g. "_DataUsePurpose_Email:Marketing")
                status = None
                
                if purpose:
                    specific_key = f"_DataUsePurpose_{channel}:{purpose}"
                    if specific_key in row:
                        status = row[specific_key]
                
                # Priority 2: General Channel Fallback
                if status is None:
                    if channel == 'Email':
                        status = row.get('_EmailConsent', 'OptOut')
                    elif channel == 'SMS':
                        status = row.get('_SMSConsent', 'OptOut')
                
                # If we still have no status (unknown channel and no override), skip
                if status is None:
                    continue

                # Prepare Update
                eff_date = row.get('_EffectiveTo__date')
                upd = {"Id": r['Id'], "PrivacyConsentStatus": status}
                
                if status == 'OptIn' and eff_date: 
                    upd["EffectiveTo"] = eff_date
                elif status == 'OptOut': 
                    upd["EffectiveTo"] = None
                
                cptc_updates.append(upd)

                
        
        total_found = sum(len(recs) for recs in cptc_map.values())
        self.factory._log(f"      🔎 Found {total_found} CPTC records linked to these Accounts.", "INFO")

        if cptc_updates:
            try:
                self.factory._log(f"      � Identified {len(cptc_updates)} records requiring update...", "INFO")
                self.sc.bulk.ContactPointTypeConsent.update(cptc_updates)
                self.factory._log(f"      ✅ Successfully updated {len(cptc_updates)} CPTC records.", "SUCCESS")
            except Exception as e:
                self.factory._log(f"      ❌ Error updating CPTC: {e}", "ERROR")
        else:
            self.factory._log(f"      ℹ️ No updates needed (all {total_found} records matched criteria or skipped).", "INFO")
