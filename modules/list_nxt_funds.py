#!/usr/bin/env python3
"""
Script to list available NXT funds for financial sync configuration.

This script helps identify the correct fund IDs to use in fund_mappings.json.
Focuses on "40105 - Mission Trip Donations" category funds and their descriptions.
"""
import sys
import logging
import json
import re
import os
from config import Config
from token_service import NXTTokenService
from nxt_gift_client import NXTGiftClient

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger('fund_lookup')

def main():
    """Main entry point for the script."""
    logger = setup_logging()
    logger.info("Starting NXT funds lookup")
    
    try:
        # Initialize services
        config = Config()
        token_service = NXTTokenService(config)
        nxt_client = NXTGiftClient(token_service)
        
        # Get all funds with pagination
        logger.info("Retrieving NXT funds...")
        funds = []
        offset = 0
        limit = 100
        
        while True:
            batch = nxt_client.get_funds(limit=limit, offset=offset)
            if not batch:
                break
                
            funds.extend(batch)
            logger.info(f"Retrieved {len(batch)} funds (offset: {offset})")
            
            if len(batch) < limit:
                break
                
            offset += limit
            
        logger.info(f"Total funds retrieved: {len(funds)}")
        
        if not funds:
            logger.error("No funds returned from NXT API")
            return 1
            
        # Define the category we're looking for (should contain "Mission Trip" or similar)
        mission_trip_categories = []
        mission_trip_funds = []
        all_categories = set()
        
        # First pass: collect all unique categories
        for fund in funds:
            if 'category' in fund:
                all_categories.add(fund['category'])
        
        # Print all unique categories for analysis
        print("\nAll fund categories:")
        print("-" * 100)
        for i, category in enumerate(sorted(all_categories)):
            print(f"{i+1:3}. {category}")
            if 'mission' in category.lower() or 'trip' in category.lower() or '40105' in category:
                mission_trip_categories.append(category)
                
        print(f"\nFound {len(all_categories)} unique fund categories")
        
        # Print likely mission trip categories if found
        if mission_trip_categories:
            print("\nPotential Mission Trip Categories:")
            for category in mission_trip_categories:
                print(f"- {category}")
        
        # Print all available funds to see what we're working with
        print("\nAll available funds:")
        print("-" * 100)
        print(f"{'ID':<10} {'Category':<40} {'Description':<50}")
        print("-" * 100)
        
        for fund in funds:
            print(f"{fund.get('id', 'N/A'):<10} {fund.get('category', 'N/A'):<40} {fund.get('description', 'N/A')[:50]:<50}")
            
            # Check if this is a mission trip fund based on the exact category and description pattern
            if fund.get('category') == '40105 - Mission Trip Donations':
                mission_trip_funds.append(fund)
            elif fund.get('description', '').startswith('Mission Trip : '):
                mission_trip_funds.append(fund)
        
        # Process mission trip related funds
        if mission_trip_funds:
            print(f"\nFound {len(mission_trip_funds)} potential Mission Trip Funds:")
            print("-" * 100)
            print(f"{'ID':<10} {'Category':<40} {'Description':<50} {'Trip Code':<15}")
            print("-" * 100)
            
            # Extract mission trip codes and update the fund mappings
            fund_mappings = {}
            for fund in mission_trip_funds:
                fund_id = fund.get('id')
                description = fund.get('description', '')
                category = fund.get('category', '')
                
                # Try to extract a trip code from the description
                trip_code = None
                
                # Extract the specific trip name/code from "Mission Trip : [name/code]"
                if description.startswith('Mission Trip : '):
                    trip_name = description[14:].strip()
                    
                    # First try to find specific trip code patterns
                    trip_code_patterns = [
                        r'SR\d{1,6}',             # SR followed by digits
                        r'[A-Z]{2,4}\d{2,4}',     # 2-4 letters followed by 2-4 digits
                        r'(?:TRIP|MISSION|MT)-\d{1,4}', # TRIP/MISSION/MT followed by dash and digits
                        r'T\d{4,6}'              # T followed by 4-6 digits
                    ]
                    
                    for pattern in trip_code_patterns:
                        match = re.search(pattern, trip_name, re.IGNORECASE)
                        if match:
                            trip_code = match.group(0).upper()
                            break
                    
                    # If no specific pattern found, use a normalized version of the trip name
                    if not trip_code:
                        # Create a normalized code from the trip name
                        # Replace spaces with underscores and remove special chars
                        trip_code = re.sub(r'[^A-Za-z0-9]', '_', trip_name).upper()
                        # Limit to a reasonable length
                        trip_code = trip_code[:15]
                
                print(f"{fund_id:<10} {category[:40]:<40} {description[:50]:<50} {trip_code if trip_code else 'N/A':<15}")
                
                if trip_code:
                    fund_mappings[trip_code] = fund_id
            
            # Add a default mapping if we found mission trip funds
            if mission_trip_funds:
                fund_mappings['default'] = mission_trip_funds[0].get('id')
                print(f"\nDefault fund ID set to: {fund_mappings['default']} ({mission_trip_funds[0].get('description')})")
        else:
            print("\nNo mission trip funds found. Please check the categories and descriptions.")
            return 1
        
        # Update or create fund_mappings.json
        if fund_mappings:
            try:
                # Ensure data directory exists
                data_dir = config.paths.get('data')
                if not data_dir:
                    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
                    config.paths['data'] = data_dir
                
                # Create data directory if it doesn't exist
                if not os.path.exists(data_dir):
                    logger.info(f"Creating data directory: {data_dir}")
                    os.makedirs(data_dir)
                    
                fund_mappings_path = os.path.join(data_dir, 'fund_mappings.json')
                logger.info(f"Updating fund mappings in {fund_mappings_path}")
                
                # First check if we can load existing fund mappings
                existing_mappings = {}
                if os.path.exists(fund_mappings_path):
                    try:
                        with open(fund_mappings_path, 'r') as f:
                            existing_mappings = json.load(f)
                        logger.info(f"Loaded existing fund mappings from {fund_mappings_path}")
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse existing fund_mappings.json, will create new file")
                
                # Update with new mappings
                existing_mappings.update(fund_mappings)
                
                # Write back the updated mappings
                with open(fund_mappings_path, 'w') as f:
                    json.dump(existing_mappings, f, indent=4)
                
                logger.info(f"Successfully updated fund mappings with {len(fund_mappings)} trip codes")
                
                # Print the updated mappings
                print("\nUpdated fund mappings:")
                print("-" * 50)
                for code, fund_id in existing_mappings.items():
                    print(f"{code:<15} -> {fund_id}")
            except Exception as e:
                logger.error(f"Error updating fund mappings file: {e}")
                return 1
        
        logger.info(f"Fund mapping script completed successfully")
        return 0
        
    except Exception as e:
        logger.exception(f"Error listing NXT funds: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
