"""ServiceReef API client implementation."""
import logging
import os
from api_client import ApiClient

class ServiceReefClient(ApiClient):
    """ServiceReef API client for interacting with ServiceReef endpoints."""
    
    def __init__(self, token_service):
        """Initialize ServiceReef API client.
        
        Args:
            token_service: ServiceReef token service
        """
        super().__init__(
            name='ServiceReef',
            base_url=os.getenv('SERVICE_REEF_BASE_URL'),
            token_service=token_service,
            logger=logging.getLogger('ServiceReefClient')
        )
    
    def _get_headers(self):
        """Get request headers with ServiceReef auth token.
        
        Returns:
            Dict of headers
        """
        token = self.token_service.get_valid_access_token()
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
    def _refresh_token(self):
        """Refresh ServiceReef authentication token."""
        self.token_service.refresh_access_token()
    
    # --- ServiceReef specific API methods ---
    
    def get_events(self, page=1, page_size=100):
        """Get events from ServiceReef.
        
        Args:
            page: Page number
            page_size: Number of events per page
            
        Returns:
            Event data or None if error
        """
        params = {
            'page': page,
            'pageSize': page_size
        }
        return self.request('GET', '/v1/events', params=params)
    
    def get_event(self, event_id):
        """Get single event from ServiceReef.
        
        Args:
            event_id: ServiceReef event ID
            
        Returns:
            Event data or None if error
        """
        return self.request('GET', f'/v1/events/{event_id}')
    
    def get_event_participants(self, event_id, page=1, page_size=100):
        """Get participants for an event from ServiceReef.
        
        Args:
            event_id: ServiceReef event ID
            page: Page number
            page_size: Number of participants per page
            
        Returns:
            Participant data or None if error
        """
        params = {
            'page': page,
            'pageSize': page_size
        }
        return self.request('GET', f'/v1/events/{event_id}/participants', params=params)
    
    def get_user(self, user_id):
        """Get user profile from ServiceReef.
        
        Args:
            user_id: ServiceReef user ID
            
        Returns:
            User data or None if error
        """
        return self.request('GET', f'/v1/users/{user_id}')
    
    def get_member_details(self, user_id):
        """Get detailed member information from ServiceReef.
        
        Args:
            user_id: ServiceReef user ID
            
        Returns:
            Member details including contact info, or None if error
        """
        try:
            # Use the correct endpoint for ServiceReef member details
            member_data = self.request('GET', f'/v1/members/{user_id}')
            
            if not member_data:
                self.logger.warning(f"No member data found for ServiceReef ID {user_id}")
                return None
                
            self.logger.info(f"Retrieved member data for ServiceReef ID {user_id}")
            return member_data
                
        except Exception as e:
            self.logger.error(f"Error retrieving member details for ServiceReef ID {user_id}: {str(e)}")
            return None
