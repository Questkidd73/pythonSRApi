"""Blackbaud NXT Gift API client implementation."""
import logging
import os
from nxt_client import NXTClient

class NXTGiftClient(NXTClient):
    """NXT API client for interacting with gift-related endpoints."""
    
    def __init__(self, token_service):
        """Initialize NXT Gift API client.
        
        Args:
            token_service: NXT token service
        """
        super().__init__(token_service)
        self.logger = logging.getLogger('NXTGiftClient')
    
    def add_gift(self, gift_data):
        """Add a gift in NXT.
        
        Args:
            gift_data: Gift data payload must include:
                - constituent_id: ID of the constituent making the gift
                - amount: Total gift amount
                - date: Gift date (ISO format)
                - gift_splits: Array with fund_id and amount
                - payments: Array with payment_method
                - type: Gift type (usually "Donation")
                
        Returns:
            Created gift data or None if error
        """
        self.logger.info(f"Creating gift for constituent {gift_data.get('constituent_id')}")
        # Log the full gift payload for debugging
        import json
        self.logger.info(f"Gift payload: {json.dumps(gift_data, indent=2)}")
        return self.request('POST', '/gift/v1/gifts', json_data=gift_data)
    
    def get_gift(self, gift_id):
        """Get a gift from NXT.
        
        Args:
            gift_id: NXT gift ID
            
        Returns:
            Gift data or None if error
        """
        self.logger.info(f"Retrieving gift {gift_id}")
        return self.request('GET', f'/gift/v1/gifts/{gift_id}')
    
    def search_gifts(self, constituent_id=None, reference=None, start_date=None, end_date=None):
        """Search for gifts in NXT.
        
        Args:
            constituent_id: Optional NXT constituent ID filter
            reference: Optional reference ID filter (useful for checking ServiceReef payment ID)
            start_date: Optional start date filter (ISO format)
            end_date: Optional end date filter (ISO format)
            
        Returns:
            Search results as list or empty list if error
        """
        params = {}
        
        # Add search filters
        if constituent_id:
            params['constituent_id'] = constituent_id
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        # Send reference as a query parameter if available
        if reference:
            params['reference'] = reference
            
        self.logger.info(f"Searching for gifts with params: {params}")
        response = self.request('GET', '/gift/v1/gifts', params=params)
        
        # Handle different response formats
        if not response:
            self.logger.info("No gift results received from API")
            return []
            
        # If response is already a list of gifts, return it
        if isinstance(response, list):
            self.logger.info(f"Received {len(response)} gift results as list")
            if response:
                self.logger.info(f"First result: {response[0]}")
            return response
            
        # If response has a 'value' property containing gifts (typical Blackbaud format)
        if isinstance(response, dict) and 'value' in response:
            results = response.get('value', [])
            self.logger.info(f"Received {len(results)} gift results in response['value']")
            if results:
                self.logger.info(f"First result: {results[0]}")
            return results
            
        # If we got an unexpected format
        self.logger.warning(f"Unexpected gift response format: {type(response)}")
        return []
    
    def get_funds(self, limit=100, offset=0):
        """Get list of funds from NXT.
        
        Args:
            limit: Number of funds per page
            offset: Offset for pagination
            
        Returns:
            List of fund objects or empty list if error
        """
        params = {
            'limit': limit,
            'offset': offset
        }
        self.logger.info(f"Retrieving funds (limit {limit}, offset {offset})")
        response = self.request('GET', '/fundraising/v1/funds', params=params)
        
        # Handle different response formats
        if not response:
            self.logger.warning("No response received from funds endpoint")
            return []
            
        # If response is already a list of funds, return it
        if isinstance(response, list):
            return response
            
        # If response has a 'value' property containing funds (typical Blackbaud format)
        if isinstance(response, dict) and 'value' in response:
            self.logger.info(f"Found {len(response['value'])} funds in response")
            return response['value']
            
        # If we got a string or other unexpected format
        self.logger.warning(f"Unexpected funds response format: {type(response)}")
        return []
        
    def get_fund_custom_field_categories(self, category_name=None):
        """Get fund custom field category values.
        
        Args:
            category_name: Optional name of the category to filter by
            
        Returns:
            List of category values or empty list if error
        """
        params = {}
        if category_name:
            params['category_name'] = category_name
            
        self.logger.info(f"Retrieving fund custom field categories {category_name if category_name else 'all'}")
        response = self.request('GET', '/fundraising/v1/funds/customfields/categories/values', params=params)
        
        # Handle different response formats
        if not response:
            self.logger.warning("No response received from fund custom fields endpoint")
            return []
            
        # If response has a 'value' property containing category values (typical Blackbaud format)
        if isinstance(response, dict) and 'value' in response:
            self.logger.info(f"Found {len(response['value'])} category values in response")
            return response['value']
            
        # If we got a string or other unexpected format
        self.logger.warning(f"Unexpected category values response format: {type(response)}")
        return []
    
    def get_fund(self, fund_id):
        """Get a single fund from NXT.
        
        Args:
            fund_id: NXT fund ID
            
        Returns:
            Fund data or None if error
        """
        self.logger.info(f"Retrieving fund {fund_id}")
        return self.request('GET', f'/fundraising/v1/funds/{fund_id}')
    
    def check_gift_exists(self, reference):
        """Check if a gift with the given reference exists in NXT.
        
        Args:
            reference: Reference ID (ServiceReef payment ID)
            
        Returns:
            True if gift exists, False otherwise
        """
        self.logger.info(f"Checking if gift with reference {reference} exists")
        gifts = self.search_gifts(reference=reference)
        
        # Check if any gifts have an exact match on reference
        if gifts:
            for gift in gifts:
                if gift.get('reference') == reference:
                    self.logger.info(f"Found existing gift {gift.get('id')} with reference {reference}")
                    return True
        
        self.logger.info(f"No gifts found with exact reference match {reference}")
        return False
        
    def get_gift_by_lookup_id(self, lookup_id):
        """Get a gift by its lookup_id.
        
        Args:
            lookup_id: The lookup_id to search for
            
        Returns:
            The gift object if found, None otherwise
        """
        self.logger.info(f"Searching for gift with lookup_id {lookup_id}")
        
        # Use the Blackbaud API's filter capability for lookup_id
        params = {
            'lookup_id': lookup_id
        }
        
        response = self.request('GET', '/gift/v1/gifts', params=params)
        
        # Handle different response formats
        if not response:
            self.logger.info(f"No gift found with lookup_id {lookup_id}")
            return None
            
        # If response has a 'value' property containing gifts (typical Blackbaud format)
        if isinstance(response, dict) and 'value' in response:
            results = response.get('value', [])
            # Verify exact lookup_id match
            for result in results:
                if result.get('lookup_id') == lookup_id:
                    self.logger.info(f"Found exact match: gift with ID {result.get('id')} has lookup_id {lookup_id}")
                    return result
            self.logger.info(f"No exact match found for lookup_id {lookup_id} among {len(results)} results")
        
        # If response is already a list of gifts
        elif isinstance(response, list):
            # Verify exact lookup_id match
            for result in response:
                if result.get('lookup_id') == lookup_id:
                    self.logger.info(f"Found exact match: gift with ID {result.get('id')} has lookup_id {lookup_id}")
                    return result
            self.logger.info(f"No exact match found for lookup_id {lookup_id} among {len(response)} results")
            
        self.logger.info(f"No gift found with exact lookup_id {lookup_id}")
        return None
        
    def get_fund_categories(self):
        """Get the list of fund categories from NXT.
        
        Returns:
            List of fund category strings or empty list if error
        """
        self.logger.info("Retrieving fund categories")
        response = self.request('GET', '/fundraising/v1/funds/categories')
        
        # Handle different response formats
        if not response:
            self.logger.warning("No response received from fund categories endpoint")
            return []
            
        # If response has a 'value' property containing categories (typical Blackbaud format)
        if isinstance(response, dict) and 'value' in response:
            self.logger.info(f"Found {len(response['value'])} fund categories")
            return response['value']
            
        # If we got a string or other unexpected format
        self.logger.warning(f"Unexpected fund categories response format: {type(response)}")
        return []
