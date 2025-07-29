#!/usr/bin/env python3
"""
Script to list NXT fund custom field categories.

Helps identify available fund categories for financial sync configuration.
"""
import sys
import logging
import json
import argparse
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
    return logging.getLogger('fund_categories')

def main():
    """Main entry point for the script."""
    logger = setup_logging()
    logger.info("Starting NXT fund categories lookup")
    
    try:
        # Initialize services using the same pattern as other scripts
        config = Config()
        token_service = NXTTokenService(config)
        gift_client = NXTGiftClient(token_service)
        
        # First try the dedicated fund categories endpoint
        logger.info('Retrieving NXT fund categories...')
        categories = gift_client.get_fund_categories()
        
        if not categories:
            logger.warning('No fund categories found from primary endpoint')
            
            # Fall back to custom field categories as a backup
            logger.info('Falling back to custom field categories endpoint...')
            categories = gift_client.get_fund_custom_field_categories(category_name='Mission Trip Donations')
            
            if not categories:
                logger.warning('No fund categories found from fallback endpoint either')
                return 1
            
        # Display category values
        print("\nFund Custom Field Category Values:")
        print("-" * 100)
        
        for i, category in enumerate(categories):
            print(f"{i+1}. {category}")
        
        print("-" * 100)
        logger.info(f"Found {len(categories)} category values")
        
        # If there's a category that looks like it might be related to mission trips, highlight it
        mission_categories = [cat for cat in categories if 'mission' in cat.lower() or 'trip' in cat.lower()]
        if mission_categories:
            print("\nPotential Mission Trip related categories:")
            for cat in mission_categories:
                print(f"- {cat}")
                
        return 0
        
    except Exception as e:
        logger.error(f"Error retrieving fund categories: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
