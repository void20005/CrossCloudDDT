import argparse
import os
import sys

# Ensure src can be imported
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.salesforce_client import get_client
from src.data_factory import Auto360DataFactory

def main():
    parser = argparse.ArgumentParser(description="Auto360 Data Factory Runner")
    parser.add_argument("--scenario", help="Name of the scenario folder in data/ (e.g. lost_sale)")
    parser.add_argument("--env", default="qa", help="Environment to use (default: qa)")
    parser.add_argument("--upsert", action="store_true", help="Try to update existing records by Name instead of creating duplicates")
    parser.add_argument("--delete", action="store_true", help="Delete data defined in the scenario (reverse order)")
    parser.add_argument("--clean", help="SQL LIKE pattern to delete (e.g. 'LostS%%')")
    parser.add_argument("--object", default="Account", help="Object to clean (used with --clean, default: Account)")
    
    args = parser.parse_args()

    # 1. Init Client
    sf = get_client(args.env)

    # 2. Init Factory
    factory = Auto360DataFactory(sf)

    # 3. Pattern-based cleanup mode
    if args.clean:
        factory.delete_by_pattern(args.object, args.clean)
        return

    if args.scenario:
        # SCENARIO MODE
        data_dir = os.path.join(os.path.dirname(__file__), 'data')
        scenario_path = os.path.join(data_dir, args.scenario)

        if not os.path.exists(scenario_path):
            print(f"🛑 ERROR: Scenario folder not found: {scenario_path}")
            # List available
            print("Available scenarios:")
            try:
                for name in os.listdir(data_dir):
                    if os.path.isdir(os.path.join(data_dir, name)):
                        print(f" - {name}")
            except FileNotFoundError:
                print("   Data directory not found.")
            sys.exit(1)

        if args.delete:
             factory.cleanup_scenario(scenario_path)
        else:
             factory.run_scenario(scenario_path, upsert=args.upsert)
        return

    # If neither
    parser.print_help()


if __name__ == "__main__":
    main()
