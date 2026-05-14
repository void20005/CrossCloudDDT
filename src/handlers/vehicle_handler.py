import time
from .base_handler import BaseHandler

class VehicleHandler(BaseHandler):
    def __init__(self, factory):
        super().__init__(factory)
        self.object_name = 'Vehicle'

    def get_immutable_fields(self):
        # We handle immutable fields for both Vehicle and VehicleDefinition separately?
        # Typically BaseHandler handles generic, but specialized immutable fields 
        # are usually defined per object. 
        # VehicleDefinition is a separate object, so it will get its own Handler (or default).
        return [] 

    def delete_records(self, ids_to_delete):
        """
        Cascade delete for Vehicle:
        1. Find related Asset (Vehicle.AssetId)
        2. Delete Asset -> Vehicle
        """
        if not ids_to_delete: return
        
        ids_to_delete = list(set(ids_to_delete))
        self.factory._log(f"      Found {len(ids_to_delete)} records to delete.", "INFO")

        # --- CASCADE LOOKUP ---
        self.factory._log("      🔎 Checking for related Asset records...", "DEBUG")
        
        # 1. Get Assets (AssetId) from Vehicle
        asset_ids = self.get_field_values_batch(
            record_ids=ids_to_delete, 
            field_name='AssetId'
        )

        # --- EXECUTION ---
        # 1. Asset
        if asset_ids:
            self.factory._log(f"      🗑️ Cascade Deleting {len(asset_ids)} related Asset records...", "INFO")
            self._bulk_delete('Asset', asset_ids, log_prefix="   ")

        # 2. Vehicle (Self)
        super().delete_records(ids_to_delete)

