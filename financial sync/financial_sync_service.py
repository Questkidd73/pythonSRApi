"""
Financial synchronization service for ServiceReef payments to NXT gifts.

This module handles the synchronization of financial data from ServiceReef to Blackbaud NXT,
including payment retrieval, constituent verification, and gift creation.
"""
import os
import json
import logging
import datetime
from pathlib import Path

from service_reef_payment_client import ServiceReefPaymentClient
from nxt_gift_client import NXTGiftClient
from token_service import ServiceReefTokenService, NXTTokenService
from mapping_service import MappingService
from config import Config

class FinancialSyncService:
    """Service for synchronizing ServiceReef payments to NXT gifts."""
    
    def __init__(self, config=None):
        """Initialize financial sync service.
        
        Args:
            config: Optional Config instance
        """
        # Initialize config and logging
        self.config = config or Config()
        self.logger = logging.getLogger('FinancialSyncService')
        
        # Initialize token services
        self.sr_token_service = ServiceReefTokenService(self.config)
        self.nxt_token_service = NXTTokenService(self.config)
        
        # Initialize API clients
        self.sr_client = ServiceReefPaymentClient(self.sr_token_service)
        self.nxt_client = NXTGiftClient(self.nxt_token_service)
        
        # Initialize mapping service
        self.mapping_service = MappingService(
            config=self.config
        )
        
        # Load fund mappings from config
        self.fund_mappings = self.config.get('fund_config.mappings', {})
        if not self.fund_mappings:
            self.logger.warning("No fund mappings configured. Using default Mission Trips Donations fund.")
            
        # Default mission trips fund ID
        self.default_fund_id = self.config.get('fund_config.default_nxt_fund_id')
        if not self.default_fund_id:
            self.logger.warning("No default NXT fund ID configured. Gifts may not be properly categorized.")
    
    def sync_all_payments(self, start_date=None, end_date=None, batch_size=25):
        """Synchronize all ServiceReef payments to NXT gifts.
        
        Args:
            start_date: Optional start date for filtering payments (ISO format)
            end_date: Optional end date for filtering payments (ISO format)
            batch_size: Number of payments to process per batch
            
        Returns:
            Dict with sync results statistics
        """
        self.logger.info(f"Starting financial sync (start_date={start_date}, end_date={end_date})")
        
        stats = {
            'total_payments': 0,
            'processed': 0,
            'skipped': 0,
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        # Process payments in batches
        page = 1
        while True:
            self.logger.info(f"Retrieving payments batch (page={page}, size={batch_size})")
            response = self.sr_client.get_payments(
                page=page,
                page_size=batch_size,
                start_date=start_date,
                end_date=end_date
            )
            
            if not response or not isinstance(response, dict):
                self.logger.error(f"Invalid response from ServiceReef payments API: {response}")
                break
                
            # Log the full response format for debugging
            self.logger.info(f"ServiceReef payments API response format: {type(response)}, keys: {response.keys() if isinstance(response, dict) else 'not a dict'}") 
            
            # Extract payments from response
            page_info = response.get('PageInfo', {})
            payments = response.get('Results', [])
            
            self.logger.info(f"Payments count: {len(payments)}, sample payment keys: {payments[0].keys() if payments else 'no payments'}")
            
            if not payments:
                self.logger.info("No more payments to process")
                break
                
            # Update stats
            stats['total_payments'] += len(payments)
            
            # Process each payment
            for payment in payments:
                payment_id = payment.get('TransactionId')  # Using TransactionId instead of PaymentId
                try:
                    # Check if payment already processed
                    if self.nxt_client.check_gift_exists(reference=f"SR-Payment-{payment_id}"):
                        self.logger.info(f"Payment {payment_id} already processed, skipping")
                        stats['skipped'] += 1
                        continue
                        
                    # Process payment
                    self.logger.info(f"Processing payment {payment_id}")
                    success = self.sync_payment(payment)
                    
                    # Update stats
                    stats['processed'] += 1
                    if success:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
                        
                except Exception as e:
                    self.logger.exception(f"Error processing payment {payment_id}: {str(e)}")
                    stats['failed'] += 1
                    stats['errors'].append({
                        'payment_id': payment_id,
                        'error': str(e)
                    })
            
            # Check if we've processed all pages
            total_pages = page_info.get('TotalPages', 0)
            if page >= total_pages:
                self.logger.info(f"Processed all {total_pages} pages of payments")
                break
                
            # Move to next page
            page += 1
        
        self.logger.info(f"Financial sync completed. Stats: {stats}")
        return stats
    
    def sync_payment(self, payment_data=None, payment_id=None):
        """Synchronize a single ServiceReef payment to NXT gift.
        
        Args:
            payment_data: Payment data dict (if already retrieved)
            payment_id: ServiceReef payment ID (if data not provided)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get payment data if not provided
            if not payment_data and payment_id:
                self.logger.info(f"Retrieving payment {payment_id} details")
                payment_data = self.sr_client.get_payment(payment_id)
                
            if not payment_data:
                self.logger.error("No payment data provided or retrieved")
                return False
                
            payment_id = payment_data.get('TransactionId')  # Using TransactionId instead of PaymentId
            amount = payment_data.get('Amount')
            date = payment_data.get('Date')
            donor_id = payment_data.get('UserId')
            donated_to_id = payment_data.get('DonatedToUserId')
            event_code = payment_data.get('EventCode')
            
            # Use EventCode as the fund mapping key instead of FundId
            fund_id = event_code
            
            # Add detailed logging for debugging payment data
            self.logger.info(f"Payment data keys: {payment_data.keys() if payment_data else 'None'}")
            self.logger.info(f"Processing payment: ID={payment_id}, Amount={amount}, Date={date}, UserId={donor_id}, DonatedToUserId={donated_to_id}")
            
            # Check if we have direct donor information in the payment data first
            if payment_data.get('FirstName') and payment_data.get('LastName'):
                # We can use the payment data directly as donor details
                self.logger.info(f"Using payment data directly for donor: {payment_data.get('FirstName')} {payment_data.get('LastName')}")
                donor_details = payment_data
            # Otherwise, try to use UserId (not DonatedToUserId) to get donor details
            elif donor_id is not None:
                # Get donor information by UserId
                self.logger.info(f"Retrieving donor {donor_id} information by UserId")
                donor_details = self.sr_client.get_donor_details(donor_id)
            else:
                self.logger.error(f"Payment {payment_id} missing required donor identification (no UserId and no FirstName/LastName)")
                return False
            
            # Check if we have enough donor information
            if not donor_details:
                self.logger.error(f"Could not retrieve details for donor {donor_id}")
                return False
                
            # Get or create NXT constituent
            constituent_id = self._get_or_create_constituent(donor_details)
            
            if not constituent_id:
                self.logger.error(f"Could not find or create constituent for donor {donor_id}")
                return False
                
            # Map ServiceReef fund to NXT fund
            nxt_fund_id = self._map_fund_id(fund_id)
            
            # Check if gift already exists by lookup_id
            lookup_id = f"SR-Payment-{payment_id}"
            existing_gift = self.nxt_client.get_gift_by_lookup_id(lookup_id)
            
            if existing_gift:
                # Gift already exists, check if it's associated with the correct fund
                gift_id = existing_gift.get('id')
                gift_fund_id = None
                if 'gift_splits' in existing_gift and existing_gift['gift_splits']:
                    gift_fund_id = existing_gift['gift_splits'][0].get('fund_id')
                
                self.logger.info(f"Gift already exists for payment {payment_id} with NXT gift ID {gift_id}, fund_id={gift_fund_id}")
                
                # TODO: In the future, we could implement logic to update the fund_id if it's incorrect
                # For now, we'll just log it and consider it a success
                
                return True  # Count as success since gift exists
            
            # Create gift in NXT
            gift_data = {
                'constituent_id': constituent_id,
                'amount': {'value': float(amount)},  # Format as object with value property
                'date': date,
                'type': 'Donation',
                'reference': f"SR-Payment-{payment_id}",
                'lookup_id': lookup_id,
                'gift_status': 'Active',  # Explicitly set to Active based on API docs
                'is_anonymous': False,    # Set explicit value based on API docs
                'post_status': 'NotPosted',  # Required for proper gift processing
                'gift_splits': [
                    {
                        'amount': {'value': float(amount)},  # Format as object with value property
                        'fund_id': nxt_fund_id
                    }
                ],
                'payments': [
                    {
                        'payment_method': 'Cash'  # Default to Cash if method unknown
                    }
                ]
            }
            
            # Add payment method if available
            payment_method = payment_data.get('PaymentMethod', '').lower()
            if payment_method:
                if 'credit' in payment_method or 'card' in payment_method:
                    gift_data['payments'][0]['payment_method'] = 'CreditCard'
                elif 'check' in payment_method:
                    gift_data['payments'][0]['payment_method'] = 'PersonalCheck'
                    
            self.logger.info(f"Creating gift for payment {payment_id} (constituent: {constituent_id}, fund: {nxt_fund_id})")
            try:
                gift_result = self.nxt_client.add_gift(gift_data)
                # Check if result contains error flag (API error response)
                if gift_result and isinstance(gift_result, dict) and gift_result.get('error'):
                    self.logger.error(f"Failed to create gift for payment {payment_id}: API error {gift_result.get('status_code')} - {gift_result.get('details')}")
                    return False
                # Check for successful result with ID
                elif gift_result and isinstance(gift_result, dict) and gift_result.get('id'):
                    self.logger.info(f"Successfully created gift {gift_result.get('id')} for payment {payment_id}")
                    return True
                # Any other response is an error
                else:
                    self.logger.error(f"Failed to create gift for payment {payment_id}: Unexpected response {gift_result}")
                    return False
            except Exception as e:
                self.logger.error(f"Failed to create gift for payment {payment_id}: {str(e)}")
                return False
                
            gift_id = gift_result.get('id')
            self.logger.info(f"Successfully created gift {gift_id} for payment {payment_id}")
            return True
            
        except Exception as e:
            self.logger.exception(f"Error processing payment: {str(e)}")
            return False
    
    def _get_or_create_constituent(self, donor_details):
        """Get or create NXT constituent for ServiceReef donor or direct payment donor.
        
        Args:
            donor_details: ServiceReef donor details or payment data
            
        Returns:
            NXT constituent ID or None if error
        """
        # Extract donor information first to ensure we have the basics
        email = None
        if 'Email' in donor_details:
            email = donor_details.get('Email')
        
        first_name = donor_details.get('FirstName')
        last_name = donor_details.get('LastName')
        
        # Get ServiceReef ID if available
        sr_id = donor_details.get('Id') or donor_details.get('UserId')
        has_sr_id = sr_id is not None
        
        if has_sr_id:
            self.logger.info(f"Processing donor with ServiceReef ID: {sr_id}")
            # Check mapping first
            constituent_id = self.mapping_service.get_nxt_constituent_id(sr_id)
            
            if constituent_id:
                self.logger.info(f"Found constituent mapping: ServiceReef {sr_id} -> NXT {constituent_id}")
                
                # Verify constituent exists in NXT
                constituent = self.nxt_client.get_constituent(constituent_id)
                if constituent:
                    self.logger.info(f"Verified constituent {constituent_id} exists in NXT")
                    return constituent_id
                else:
                    self.logger.warning(f"Constituent {constituent_id} from mapping not found in NXT")
                    # Mapping is invalid, continue with search/create
        else:
            self.logger.info(f"Processing direct payment donor without ServiceReef ID: {first_name} {last_name}")
        
        # Validate we have minimum required information
        if not email and not (first_name and last_name):
            self.logger.error(f"Insufficient donor information {'for ServiceReef ID ' + str(sr_id) if has_sr_id else ''}")
            return None
        
        # Search for constituent by email
        if email:
            self.logger.info(f"Searching for constituent by email: {email}")
            search_results = self.nxt_client.search_constituents(email=email)
            
            # Handle NXT API response format which is {'count': N, 'value': [...]} 
            constituents = []
            if isinstance(search_results, dict) and 'value' in search_results:
                constituents = search_results.get('value', [])
                self.logger.info(f"Found {len(constituents)} constituents in API response format")
            elif isinstance(search_results, list):
                constituents = search_results
                self.logger.info(f"Found {len(constituents)} constituents in direct list format")
            
            # Look for exact email match
            matched_constituent = None
            for constituent in constituents:
                if 'email' in constituent and constituent['email'].lower() == email.lower():
                    matched_constituent = constituent
                    self.logger.info(f"Found exact email match: {constituent.get('name')} ({constituent.get('id')})")
                    break
            
            if matched_constituent:
                constituent_id = matched_constituent.get('id')
                self.logger.info(f"Using constituent with ID: {constituent_id}")
                
                # Update mapping if we have a ServiceReef ID
                if has_sr_id:
                    self.mapping_service.add_mapping(sr_id, constituent_id)
                return constituent_id
            else:
                self.logger.info(f"No constituents found with matching email: {email}")

        
        # Search by name if email search failed
        if first_name and last_name:
            self.logger.info(f"Searching for constituent by name: {first_name} {last_name}")
            search_results = self.nxt_client.search_constituents(
                first_name=first_name,
                last_name=last_name
            )
            
            # Handle NXT API response format which is {'count': N, 'value': [...]} 
            constituents = []
            if isinstance(search_results, dict) and 'value' in search_results:
                constituents = search_results.get('value', [])
                self.logger.info(f"Found {len(constituents)} constituents in API response format")
            elif isinstance(search_results, list):
                constituents = search_results
                self.logger.info(f"Found {len(constituents)} constituents in direct list format")
            
            # Look for exact name match
            matched_constituent = None
            for constituent in constituents:
                # Check for exact name match in 'name' field (format: 'First Last')
                if 'name' in constituent and f"{first_name} {last_name}".lower() in constituent['name'].lower():
                    matched_constituent = constituent
                    self.logger.info(f"Found name match: {constituent.get('name')} ({constituent.get('id')})")
                    # If we also have email match, prioritize this match
                    if email and 'email' in constituent and email.lower() == constituent['email'].lower():
                        self.logger.info(f"Found exact name AND email match: {constituent.get('name')}")
                        break
            
            if matched_constituent:
                constituent_id = matched_constituent.get('id')
                self.logger.info(f"Using constituent with ID: {constituent_id}")
                
                # Update mapping if we have a ServiceReef ID
                if has_sr_id:
                    self.mapping_service.add_mapping(sr_id, constituent_id)
                return constituent_id
            else:
                self.logger.info(f"No constituents found with matching name: {first_name} {last_name}")
        
        # Create new constituent if not found
        if has_sr_id:
            self.logger.info(f"Creating new constituent for ServiceReef donor {sr_id}")
        else:
            self.logger.info(f"Creating new constituent for direct payment donor {first_name} {last_name}")
        
        # Prepare constituent data
        constituent_data = {
            'type': 'Individual',
            'first': first_name,
            'last': last_name
        }
        
        # Add lookup_id only if we have ServiceReef ID
        # For direct donors, we'll let NXT generate the lookup_id automatically
        if has_sr_id:
            constituent_data['lookup_id'] = f"SR-{sr_id}"
        else:
            # Skip lookup_id for direct donors - NXT will generate one automatically
            self.logger.info(f"Skipping lookup_id for direct donor - NXT will generate one automatically")

        
        # Add email if available
        if email:
            constituent_data['email'] = {
                'address': email,
                'type': 'Email',
                'primary': True,
                'do_not_email': False
            }
        
        # Add phone if available
        phone = donor_details.get('Phone')
        if phone:
            constituent_data['phone'] = {
                'number': phone,
                'type': 'Home',
                'primary': True,
                'do_not_call': False
            }
        
        # Add address if available
        address_fields = ['Address1', 'Address2', 'City', 'State', 'Zip']
        if any(field in donor_details for field in address_fields):
            address_lines = []
            if donor_details.get('Address1'):
                address_lines.append(donor_details.get('Address1'))
            if donor_details.get('Address2'):
                address_lines.append(donor_details.get('Address2'))
                
            constituent_data['address'] = {
                'type': 'Home',
                'address_lines': address_lines,
                'city': donor_details.get('City', ''),
                'state': donor_details.get('State', ''),
                'postal_code': donor_details.get('Zip', ''),
                'country': donor_details.get('Country', 'United States'),
                'primary': True,
                'do_not_mail': False
            }
        
        # Create constituent in NXT
        result = self.nxt_client.create_constituent(constituent_data)
        
        if not result or 'id' not in result:
            self.logger.error(f"Failed to create constituent for ServiceReef donor {sr_id}")
            return None
            
        constituent_id = result.get('id')
        
        if has_sr_id:
            self.logger.info(f"Created constituent {constituent_id} for ServiceReef donor {sr_id}")
            # Update mapping only if we have a ServiceReef ID
            self.mapping_service.add_mapping(sr_id, constituent_id)
        else:
            self.logger.info(f"Created constituent {constituent_id} for direct payment donor {first_name} {last_name}")
            
        return constituent_id
    
    def _map_fund_id(self, sr_fund_id):
        """Map ServiceReef fund ID to NXT fund ID.
        
        Args:
            sr_fund_id: ServiceReef fund ID
            
        Returns:
            NXT fund ID
        """
        # Try to get from mappings
        nxt_fund_id = None
        if sr_fund_id:
            nxt_fund_id = self.fund_mappings.get(str(sr_fund_id))
            
        # Use default if no mapping found
        if not nxt_fund_id:
            self.logger.warning(f"No fund mapping for ServiceReef fund {sr_fund_id}, using default")
            nxt_fund_id = self.default_fund_id
            
        if not nxt_fund_id:
            self.logger.error("No NXT fund ID available for mapping")
            raise ValueError("Missing NXT fund ID for gift")
            
        return nxt_fund_id
