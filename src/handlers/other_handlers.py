from .base_handler import BaseHandler

class AssetHandler(BaseHandler):
    def __init__(self, factory):
        super().__init__(factory)
        self.object_name = 'Asset'
        
    def get_immutable_fields(self):
        return ['AccountId', 'ContactId']

class VehicleDefinitionHandler(BaseHandler):
    def __init__(self, factory):
        super().__init__(factory)
        self.object_name = 'VehicleDefinition'

    def get_immutable_fields(self):
        return ['ProductId']

class AssetAccountParticipantHandler(BaseHandler):
    def __init__(self, factory):
        super().__init__(factory)
        self.object_name = 'AssetAccountParticipant'

    def get_immutable_fields(self):
        return ['AssetId', 'VehicleId', 'AccountId']

    def after_insert_batch(self, batch_items, results, operation='insert'):
        """
        Override to Handle Managed Package Trigger interference.
        The trigger forces AUTO360__IsOwnership__c = False on Insert.
        We must FORCE UPATE it back to True immediately if that was the intent.
        """
        # 1. Run Standard Logic (Return Fields, etc.)
        super().after_insert_batch(batch_items, results, operation)
        
        # 2. Force Update Logic (Only needed on Insert)
        if operation != 'insert':
            return
            
        ids_to_fix = []
        
        for i, res in enumerate(results):
            if res['success']:
                item = batch_items[i]
                payload = item['payload']
                
                # Check if we INTENDED to set it to True
                # Note: payload logic was fixed to use booleans, but check safe
                is_ownership = payload.get('AUTO360__IsOwnership__c')
                
                if is_ownership is True: # Strict True check or Truthy
                    ids_to_fix.append(res['id'])
        
        if ids_to_fix:
            self.factory._log(f"   🔧 Trigger Workaround: Force Updating 'IsOwnership=True' for {len(ids_to_fix)} records...", "INFO")
            
            # Prepare minimal update payload
            update_payload = [{"Id": x, "AUTO360__IsOwnership__c": True} for x in ids_to_fix]
            
            try:
                # Use Bulk Update
                res_fix = self.sc.bulk.AssetAccountParticipant.update(update_payload)
                
                # Simple success check
                success_count = sum(1 for r in res_fix if r['success'])
                self.factory._log(f"      ✅ Force Update Complete. Success: {success_count}/{len(ids_to_fix)}", "SUCCESS")
                
            except Exception as e:
                 self.factory._log(f"      ❌ Force Update Failed: {e}", "ERROR")
