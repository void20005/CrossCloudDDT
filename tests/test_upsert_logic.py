import unittest
from unittest.mock import MagicMock, patch
import os
import tempfile
import csv
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from data_factory import Auto360DataFactory

class TestUpsertLogic(unittest.TestCase):
    def setUp(self):
        self.mock_sf = MagicMock()
        self.factory = Auto360DataFactory(self.mock_sf)
        
        # Create a temp CSV file
        self.test_dir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.test_dir, "test_upsert.csv")
        
        with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Name', 'Phone', '_BaseName'])
            writer.writerow(['ExistingAccount', '12345', 'ExistingRef'])
            writer.writerow(['NewAccount', '67890', 'NewRef'])

    def tearDown(self):
        if os.path.exists(self.csv_path):
            os.remove(self.csv_path)
        os.rmdir(self.test_dir)

    def test_upsert_correctly_splits_insert_and_update(self):
        # Setup mock behavior
        # 1. Mock query_all to return one existing record
        self.mock_sf.query_all.return_value = {
            'records': [
                {'Id': '001ExistingId', 'Name': 'ExistingAccount'}
            ]
        }
        
        # 2. Mock bulk operations response
        # We need to mock success responses for both update and insert
        self.mock_sf.bulk.Account.update.return_value = [
            {'success': True, 'id': '001ExistingId', 'errors': []}
        ]
        self.mock_sf.bulk.Account.insert.return_value = [
            {'success': True, 'id': '001NewId', 'errors': []}
        ]

        # Execute
        self.factory.process_csv_bulk('Account', self.csv_path, upsert_mode=True)

        # Assertions
        
        # 1. Verify query_all was called to check for existing records
        # It should query for Name IN ('ExistingAccount','NewAccount')
        self.mock_sf.query_all.assert_called()
        call_args = self.mock_sf.query_all.call_args[0][0]
        self.assertIn("SELECT Id, Name FROM Account WHERE Name IN", call_args)
        
        # 2. Verify UPDATE was called for 'ExistingAccount'
        self.mock_sf.bulk.Account.update.assert_called_once()
        update_args = self.mock_sf.bulk.Account.update.call_args[0][0]
        self.assertEqual(len(update_args), 1)
        self.assertEqual(update_args[0]['Id'], '001ExistingId')
        self.assertEqual(update_args[0]['Name'], 'ExistingAccount')
        
        # 3. Verify INSERT was called for 'NewAccount'
        self.mock_sf.bulk.Account.insert.assert_called_once()
        insert_args = self.mock_sf.bulk.Account.insert.call_args[0][0]
        self.assertEqual(len(insert_args), 1)
        self.assertEqual(insert_args[0]['Name'], 'NewAccount')
        self.assertNotIn('Id', insert_args[0])

if __name__ == '__main__':
    unittest.main()
