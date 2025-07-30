"""API client for service calls with standardized error handling."""
import logging
import time
import requests

class ApiClient:
    """Base API client with unified request handling."""
    
    def __init__(self, name, base_url, token_service, logger=None):
        """Initialize API client.
        
        Args:
            name: Service name for logging
            base_url: Base API URL
            token_service: Token service for authentication
            logger: Optional logger instance
        """
        self.name = name
        self.base_url = base_url
        self.token_service = token_service
        self.logger = logger or logging.getLogger(name)
        self.retry_delay = 2
        self.max_retries = 3
    
    def request(self, method, endpoint, json_data=None, params=None, retry_count=0):
        """Send request with retry logic and error handling.
        
        Args:
            method: HTTP method (GET, POST, etc)
            endpoint: API endpoint path
            json_data: Optional request payload
            params: Optional query parameters
            retry_count: Current retry attempt
            
        Returns:
            API response data or None if error
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        self.logger.debug(f"==== {self.name} API CALL ====")
        self.logger.debug(f"Method: {method}")
        self.logger.debug(f"URL: {url}")
        
        try:
            response = requests.request(method, url, headers=headers, 
                                       json=json_data, params=params)
            
            # Log response data
            self.logger.debug(f"Response status code: {response.status_code}")
            
            if response.ok:
                if not response.content:
                    return None
                return response.json()
            else:
                # Handle common error cases
                if response.status_code == 401 and retry_count < self.max_retries:
                    self.logger.warning(f"Authentication error, refreshing token and retrying")
                    self._refresh_token()
                    return self.request(method, endpoint, json_data, params, retry_count + 1)
                    
                error_text = response.text
                self.logger.error(f"API error: {response.status_code} - {error_text}")
                
                # Try to parse and return error details for the caller to handle
                try:
                    error_data = response.json()
                    return {"error": True, "status_code": response.status_code, "details": error_data}
                except Exception:
                    return {"error": True, "status_code": response.status_code, "details": error_text}
                
        except Exception as e:
            self.logger.exception(f"Request error: {str(e)}")
            if retry_count < self.max_retries:
                self.logger.info(f"Retrying in {self.retry_delay}s...")
                time.sleep(self.retry_delay)
                return self.request(method, endpoint, json_data, params, retry_count + 1)
            return None
    
    def _get_headers(self):
        """Get request headers with auth token.
        
        Returns:
            Dict of headers
        """
        raise NotImplementedError("Subclasses must implement this method")
        
    def _refresh_token(self):
        """Refresh authentication token."""
        raise NotImplementedError("Subclasses must implement this method")
