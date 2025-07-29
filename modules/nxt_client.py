"""Blackbaud NXT API client implementation."""
import logging
import os
from api_client import ApiClient

class NXTClient(ApiClient):
    """NXT API client for interacting with Blackbaud NXT endpoints."""
    
    def __init__(self, token_service):
        """Initialize NXT API client.
        
        Args:
            token_service: NXT token service
        """
        super().__init__(
            name='NXT',
            base_url=os.getenv('NXT_BASE_URL', 'https://api.sky.blackbaud.com'),
            token_service=token_service,
            logger=logging.getLogger('NXTClient')
        )
        self.subscription_key = os.getenv('NXT_SUBSCRIPTION_KEY')
    
    def _get_headers(self):
        """Get request headers with NXT auth token.
        
        Returns:
            Dict of headers
        """
        token = self.token_service.get_valid_access_token()
        return {
            'Authorization': f'Bearer {token}',
            'Bb-Api-Subscription-Key': self.subscription_key,
            'Content-Type': 'application/json'
        }
        
    def _refresh_token(self):
        """Refresh NXT authentication token."""
        self.token_service.refresh_access_token()
    
    # --- NXT specific API methods ---
    
    def get_event(self, event_id):
        """Get single event from NXT.
        
        Args:
            event_id: NXT event ID
            
        Returns:
            Event data or None if error
        """
        return self.request('GET', f'/event/v1/events/{event_id}')
    
    def create_event(self, event_data):
        """Create event in NXT.
        
        Args:
            event_data: Event data payload
            
        Returns:
            Created event data or None if error
        """
        return self.request('POST', '/event/v1/events', json_data=event_data)
    
    def get_event_participants(self, event_id, limit=100, offset=0):
        """Get participants for an event from NXT.
        
        Args:
            event_id: NXT event ID
            limit: Number of participants per page
            offset: Offset for pagination
            
        Returns:
            Participant data or None if error
        """
        params = {
            'limit': limit,
            'offset': offset
        }
        return self.request('GET', f'/event/v1/events/{event_id}/participants', params=params)
    
    def add_participant(self, event_id, participant_data):
        """Add participant to an event in NXT.
        
        Args:
            event_id: NXT event ID
            participant_data: Participant data payload
            
        Returns:
            Created participant data or None if error
        """
        return self.request('POST', f'/event/v1/events/{event_id}/participants', 
                          json_data=participant_data)
    
    def update_participant(self, participant_id, participant_data):
        """Update participant in NXT.
        
        Args:
            participant_id: NXT participant ID
            participant_data: Participant data payload
            
        Returns:
            Updated participant data or None if error
        """
        return self.request('PATCH', f'/event/v1/participants/{participant_id}', 
                          json_data=participant_data)
    
    def get_constituent(self, constituent_id):
        """Get constituent details from NXT.
        
        Args:
            constituent_id: NXT constituent ID
            
        Returns:
            Constituent data or None if error
        """
        return self.request('GET', f'/constituent/v1/constituents/{constituent_id}')
    
    def create_constituent(self, constituent_data):
        """Create constituent in NXT.
        
        Args:
            constituent_data: Constituent data payload
            
        Returns:
            Created constituent data or None if error
        """
        return self.request('POST', '/constituent/v1/constituents', json_data=constituent_data)
    
    def search_constituents(self, email=None, first_name=None, last_name=None, search_text=None):
        """Search for constituents in NXT.
        
        Args:
            email: Email to search for (will be used as search_text)
            first_name: First name to search for
            last_name: Last name to search for
            search_text: Direct search text to use (overrides other params)
            
        Returns:
            Search results or None if error
        """
        search_params = {}
        
        # Build search text parameter
        if search_text:
            search_params['search_text'] = search_text
        elif email:
            search_params['search_text'] = email
        elif first_name and last_name:
            search_params['search_text'] = f"{first_name} {last_name}"
            
        if not search_params:
            self.logger.warning("No search parameters provided for constituent search")
            return []
            
        results = self.request('GET', '/constituent/v1/constituents/search', params=search_params)
        
        # Filter exact email matches if email was provided
        if email and results and isinstance(results, list):
            exact_matches = []
            for constituent in results:
                constituent_email = constituent.get('email', {}).get('address', '')
                if constituent_email and constituent_email.lower() == email.lower():
                    exact_matches.append(constituent)
            return exact_matches
        
        # Filter exact name matches if name was provided
        if first_name and last_name and results and isinstance(results, list):
            exact_matches = []
            for constituent in results:
                c_first = constituent.get('first', '')
                c_last = constituent.get('last', '')
                if (c_first and c_last and 
                    c_first.lower() == first_name.lower() and 
                    c_last.lower() == last_name.lower()):
                    exact_matches.append(constituent)
            if exact_matches:
                return exact_matches
                
        return results or []
    
    def add_email(self, constituent_id, email_data):
        """Add email to constituent in NXT.
        
        Args:
            constituent_id: NXT constituent ID
            email_data: Email data payload
            
        Returns:
            Created email data or None if error
        """
        return self.request('POST', f'/constituent/v1/constituents/{constituent_id}/emails', 
                          json_data=email_data)
