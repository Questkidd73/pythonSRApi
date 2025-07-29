"""ServiceReef Payment API client implementation."""
import logging
import os
from service_reef_client import ServiceReefClient

class ServiceReefPaymentClient(ServiceReefClient):
    """ServiceReef API client for interacting with payment-related endpoints."""
    
    def __init__(self, token_service):
        """Initialize ServiceReef Payment API client.
        
        Args:
            token_service: ServiceReef token service
        """
        super().__init__(token_service)
        self.logger = logging.getLogger('ServiceReefPaymentClient')
    
    def get_payments(self, page=1, page_size=100, start_date=None, end_date=None):
        """Get payments from ServiceReef.
        
        Args:
            page: Page number
            page_size: Number of payments per page
            start_date: Optional start date filter (ISO format)
            end_date: Optional end date filter (ISO format)
            
        Returns:
            Payment data or None if error
        """
        params = {
            'page': page,
            'pageSize': page_size
        }
        
        # Add date filters if provided
        if start_date:
            params['startDate'] = start_date
        if end_date:
            params['endDate'] = end_date
            
        self.logger.info(f"Retrieving payments (page {page}, size {page_size})")
        return self.request('GET', '/v1/payments', params=params)
    
    def get_payment(self, payment_id):
        """Get single payment from ServiceReef.
        
        Args:
            payment_id: ServiceReef payment ID
            
        Returns:
            Payment data or None if error
        """
        self.logger.info(f"Retrieving payment {payment_id}")
        return self.request('GET', f'/v1/payments/{payment_id}')
    
    def get_payment_transactions(self, payment_id):
        """Get transactions for a payment from ServiceReef.
        
        Args:
            payment_id: ServiceReef payment ID
            
        Returns:
            Payment transaction data or None if error
        """
        self.logger.info(f"Retrieving transactions for payment {payment_id}")
        return self.request('GET', f'/v1/payments/{payment_id}/transactions')
    
    def get_donor(self, donor_id):
        """Get donor information from ServiceReef.
        
        Args:
            donor_id: ServiceReef donor/user ID
            
        Returns:
            Donor data or None if error
        """
        self.logger.info(f"Retrieving donor {donor_id}")
        return self.get_user(donor_id)
    
    def get_donor_details(self, donor_id):
        """Get detailed donor information from ServiceReef.
        
        Args:
            donor_id: ServiceReef donor/user ID
            
        Returns:
            Donor details or None if error
        """
        self.logger.info(f"Retrieving detailed information for donor {donor_id}")
        return self.get_member_details(donor_id)
