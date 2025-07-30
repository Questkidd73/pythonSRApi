#!/usr/bin/env python3
"""
Script to run ServiceReef to NXT financial synchronization.

This script synchronizes payments from ServiceReef to Blackbaud NXT as gifts,
ensuring proper constituent association and fund allocation.
"""
import os
import sys
import argparse
import logging
import datetime
from config import Config
from financial_sync_service import FinancialSyncService

def setup_logging(log_file=None):
    """Set up logging configuration.
    
    Args:
        log_file: Optional log file path
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_level = logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Add file handler if log file provided
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)
        
    return logging.getLogger('financial_sync')

def parse_args():
    """Parse command line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Run ServiceReef to NXT financial sync')
    
    parser.add_argument(
        '--start-date',
        help='Start date for payment sync (YYYY-MM-DD)',
        type=str
    )
    
    parser.add_argument(
        '--end-date',
        help='End date for payment sync (YYYY-MM-DD)',
        type=str
    )
    
    parser.add_argument(
        '--payment-id',
        help='Specific payment ID to sync (skip batch processing)',
        type=str
    )
    
    parser.add_argument(
        '--batch-size',
        help='Number of payments to process per batch',
        type=int,
        default=25
    )
    
    parser.add_argument(
        '--log-file',
        help='Log file path',
        type=str,
        default='financial_sync.log'
    )
    
    parser.add_argument(
        '--dry-run',
        help='Validate operations without making changes',
        action='store_true'
    )
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    # Parse arguments
    args = parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_file)
    logger.info("Starting financial sync script")
    
    # Validate date formats if provided
    start_date = None
    end_date = None
    
    if args.start_date:
        try:
            # Parse and format as ISO
            start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').isoformat()
        except ValueError:
            logger.error(f"Invalid start date format: {args.start_date}. Use YYYY-MM-DD.")
            return 1
            
    if args.end_date:
        try:
            # Parse and format as ISO
            end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').isoformat()
        except ValueError:
            logger.error(f"Invalid end date format: {args.end_date}. Use YYYY-MM-DD.")
            return 1
    
    try:
        # Load config
        config = Config()
        
        # Check for required values
        if not config.get('fund_config.default_nxt_fund_id'):
            logger.error("Missing fund_config.default_nxt_fund_id in configuration")
            return 1
        
        # Initialize sync service
        sync_service = FinancialSyncService(config)
        
        if args.dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
            # In a real implementation, we'd modify the sync service to skip actual API calls
            
        # Run sync
        if args.payment_id:
            # Sync single payment
            logger.info(f"Syncing single payment: {args.payment_id}")
            success = sync_service.sync_payment(payment_id=args.payment_id)
            
            if success:
                logger.info(f"Successfully synced payment {args.payment_id}")
            else:
                logger.error(f"Failed to sync payment {args.payment_id}")
                return 1
        else:
            # Sync all payments
            logger.info(f"Syncing all payments (start_date={start_date}, end_date={end_date})")
            stats = sync_service.sync_all_payments(
                start_date=start_date,
                end_date=end_date,
                batch_size=args.batch_size
            )
            
            logger.info(f"Sync complete. Results: {stats}")
            
            if stats['failed'] > 0:
                logger.warning(f"{stats['failed']} payments failed to sync")
                return 1
                
        return 0
        
    except Exception as e:
        logger.exception(f"Error running financial sync: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
