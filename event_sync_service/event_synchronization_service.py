import os
import json
import time
from datetime import datetime
from pathlib import Path
import logging
import requests
import urllib.parse
import base64
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
log_dir = Path(__file__).parent / 'logs'
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    filename=log_dir / 'event_sync.log',
    level=logging.INFO,
    format='[%(asctime)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class TokenService:
    def __init__(self, service_type):
        self.service_type = service_type
        self.logger = logging.getLogger(f"{service_type}TokenService")
        self.token_file = None
        
        if service_type == 'ServiceReef':
            # ServiceReef uses client credentials flow
            self.client_id = os.getenv('SERVICE_REEF_CLIENT_ID')
            self.client_secret = os.getenv('SERVICE_REEF_CLIENT_SECRET')
            self.token_endpoint = os.getenv('SERVICE_REEF_TOKEN_ENDPOINT')
            
            if not all([self.client_id, self.client_secret, self.token_endpoint]):
                raise ValueError(
                    'SERVICE_REEF_CLIENT_ID, SERVICE_REEF_CLIENT_SECRET, and '
                    'SERVICE_REEF_TOKEN_ENDPOINT are required in .env file'
                )
            
            # Set up token file path
            token_dir = Path.home() / '.tokens'
            token_dir.mkdir(exist_ok=True)
            self.token_file = token_dir / 'servicereef_token.json'
            
        elif service_type == 'NXT':
            # NXT uses OAuth2 authorization code flow with refresh tokens
            self.client_id = os.getenv('NXT_CLIENT_ID')
            self.client_secret = os.getenv('NXT_CLIENT_SECRET')
            self.token_endpoint = 'https://oauth2.sky.blackbaud.com/token'
            
            if not all([self.client_id, self.client_secret]):
                raise ValueError(
                    'NXT_CLIENT_ID and NXT_CLIENT_SECRET are required in .env file'
                )
            
            # Set up token file path for NXT
            token_dir = Path.home() / '.tokens'
            token_dir.mkdir(exist_ok=True)
            self.token_file = token_dir / 'nxt_token.json'
            
            # Load existing token or use environment variable
            token_data = self._load_token_from_file()
            if not token_data:
                # If no token file, try environment variable
                access_token = os.getenv('NXT_ACCESS_TOKEN')
                refresh_token = os.getenv('NXT_REFRESH_TOKEN')
                if access_token:
                    token_data = {
                        'access_token': access_token,
                        'token_type': 'bearer',
                        'expires_in': 3600,  # Default 1 hour expiry
                        'fetched_at': time.time()
                    }
                    if refresh_token:  # Optional refresh token
                        token_data['refresh_token'] = refresh_token
                    self._save_token_to_file(token_data)
                else:
                    raise ValueError(
                        'No valid NXT token found. Set NXT_ACCESS_TOKEN in .env '
                        'or complete the OAuth2 flow to get a token.'
                    )
            
            # Additional NXT-specific settings
            self.nxt_base_url = os.getenv('NXT_BASE_URL', 'https://api.sky.blackbaud.com')
        else:
            raise ValueError(f"Unknown service type: {service_type}")
        self.token_file.parent.mkdir(exist_ok=True)

    def get_valid_access_token(self):
        """Get a valid access token, refreshing if necessary."""
        token_data = self._load_token_from_file()
        if token_data and 'access_token' in token_data:
            if self.service_type == 'NXT':
                # For NXT, just use the token we have since it's long-lived
                self.logger.info("Using existing NXT token")
                return token_data['access_token']
            else:
                # For ServiceReef, check expiry
                fetched_at = token_data.get('fetched_at', 0)
                expires_in = token_data.get('expires_in', 3600)
                if time.time() - fetched_at < (expires_in - 120):
                    self.logger.info("Using existing ServiceReef token")
                    return token_data['access_token']
                self.logger.info("ServiceReef token expired, getting new token")
                return self._get_new_token()
        
        # Get new token if we get here
        self.logger.info("No token found, getting new token")
        return self._get_new_token()

    def _get_new_token(self):
        """Get a new access token."""
        try:
            if self.service_type == 'ServiceReef':
                # Use client credentials flow for ServiceReef
                data = {
                    'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                }
            else:  # NXT
                # For NXT, we need an authorization code first
                # This should be obtained through the OAuth2 authorization flow
                # and passed to this method
                raise NotImplementedError(
                    'For NXT, you must first complete the OAuth2 authorization flow:\n'
                    '1. Go to https://app.blackbaud.com/oauth/authorize\n'
                    '2. Provide your client_id and redirect_uri\n'
                    '3. Get the authorization code\n'
                    '4. Exchange the code for tokens using _exchange_code()'
                )
            
            # Make the request
            response = requests.post(self.token_endpoint, data=data)
            
            if response.ok:
                token_data = response.json()
                token_data['fetched_at'] = time.time()
                
                # Save the token data
                self._save_token_to_file(token_data)
                
                return token_data['access_token']
            else:
                raise Exception(f"Failed to get token: {response.text}")
        except Exception as e:
            self.logger.error(f"Error getting new token: {str(e)}")
            raise

    def _refresh_token(self, refresh_token):
        """Refresh an expired token using the refresh token."""
        self.logger.info("Attempting to refresh token...")
        
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        try:
            response = requests.post(self.token_endpoint, data=data, headers={
                'Content-Type': 'application/x-www-form-urlencoded'
            })
            
            # Log response for debugging
            self.logger.info(f"Token refresh response status: {response.status_code}")
            
            if response.status_code == 401:
                self.logger.error("Refresh token is invalid or expired")
                return None
            
            response.raise_for_status()
            
            token_data = response.json()
            if 'access_token' in token_data:
                token_data['fetched_at'] = time.time()
                
                # Preserve refresh_token if not returned
                if 'refresh_token' not in token_data:
                    token_data['refresh_token'] = refresh_token
                
                self._save_token_to_file(token_data)
                self.logger.info("Token refreshed successfully")
                return token_data
            
            self.logger.error("No access token in refresh response")
            raise ValueError("No access_token in refresh response")
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error refreshing token: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error refreshing token: {str(e)}")
            raise
    
    def _exchange_code(self, code, redirect_uri):
        """Exchange an authorization code for tokens."""
        try:
            data = {
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': redirect_uri,
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            
            response = requests.post(self.token_endpoint, data=data)
            
            if response.ok:
                token_data = response.json()
                token_data['fetched_at'] = time.time()
                
                # Save the token data
                if self.token_file:
                    with open(self.token_file, 'w') as f:
                        json.dump(token_data, f)
                
                return token_data
            else:
                raise Exception(f"Failed to exchange code: {response.text}")
                
        except Exception as e:
            self.logger.error(f"Error exchanging code: {str(e)}")
            raise

    def _load_token_from_file(self):
        try:
            if self.token_file and self.token_file.exists():
                token_data = json.loads(self.token_file.read_text())
                self.logger.info(f"Loaded token data from {self.token_file}")
                return token_data
        except Exception as e:
            self.logger.error(f"Error loading token from file: {str(e)}")
        return None

    def _save_token_to_file(self, token_data):
        try:
            if self.token_file:
                # Ensure parent directory exists
                self.token_file.parent.mkdir(parents=True, exist_ok=True)
                # Save token data
                self.token_file.write_text(json.dumps(token_data))
                self.logger.info(f"Saved token data to {self.token_file}")
        except Exception as e:
            self.logger.error(f"Error saving token to file: {str(e)}")


class EventSyncService:
    def __init__(self):
        self.logger = logging.getLogger('EventSync')
        
        # Initialize file paths
        base_dir = Path(__file__).parent
        self.event_mapping_file = base_dir / 'data' / 'event_mapping.json'
        self.constituent_mapping_file = base_dir / 'data' / 'constituent_mapping.json'
        
        # Ensure data directory exists
        self.event_mapping_file.parent.mkdir(exist_ok=True)
        
        # Load mappings
        self._load_mappings()
        
        # Service configuration
        self.service_reef_base_url = os.getenv('SERVICE_REEF_BASE_URL')
        self.nxt_base_url = os.getenv('NXT_BASE_URL', 'https://api.sky.blackbaud.com')
        self.page_size = 100
        self.retry_delay = 2
        self.max_retries = 3
        
        # Initialize token services
        self.sr_token_service = TokenService('ServiceReef')
        
        # Get NXT subscription key
        self.nxt_subscription_key = os.getenv('NXT_SUBSCRIPTION_KEY')
        if not self.nxt_subscription_key:
            raise ValueError('NXT_SUBSCRIPTION_KEY is required')
            
        # Initialize NXT token service
        self.nxt_token_service = TokenService('NXT')
        
        # Additional NXT-specific settings
        self.nxt_base_url = os.getenv('NXT_BASE_URL', 'https://api.sky.blackbaud.com')

    def _load_mappings(self):
        # Load event mapping
        if self.event_mapping_file.exists():
            self.event_mapping = json.loads(self.event_mapping_file.read_text())
        else:
            self.event_mapping = {}
            self.event_mapping_file.write_text(json.dumps(self.event_mapping))
        
        # Load constituent mapping
        self.logger.info(f"Checking constituent mapping file at: {self.constituent_mapping_file}")
        if self.constituent_mapping_file.exists():
            self.logger.info("Loading existing constituent mapping file")
            self.constituent_mapping = json.loads(self.constituent_mapping_file.read_text())
        else:
            self.logger.info("Creating new constituent mapping file")
            self.constituent_mapping = {}
            self.constituent_mapping_file.write_text(json.dumps(self.constituent_mapping))
        
        self.constituent_cache = {}
        
    def _save_mapping(self, mapping_file, mapping_data):
        """Save mapping data to a file"""
        try:
            mapping_file.write_text(json.dumps(mapping_data, indent=2))
            self.logger.info(f"Saved mapping to {mapping_file}")
        except Exception as e:
            self.logger.error(f"Error saving mapping to {mapping_file}: {str(e)}")
            raise

    def _handle_service_reef_request(self, method, endpoint, **kwargs):
        """Handle a request to the ServiceReef API."""
        try:
            # Get valid access token
            access_token = self.sr_token_service.get_valid_access_token()
            
            # Set up headers
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Make request
            url = f"{self.service_reef_base_url}{endpoint}"
            self.logger.info(f"Making ServiceReef API request to: {url}")
            response = requests.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            
            # Log response data
            response_data = response.json()
            self.logger.info(f"ServiceReef API Response: {json.dumps(response_data, indent=2)}")
            
            return response_data
            
        except Exception as e:
            self.logger.error(f"Error making ServiceReef API request: {str(e)}")
            raise

    def _handle_nxt_request(self, method, endpoint, **kwargs):
        """Handle a request to the NXT API with retries and token refresh."""
        retries = 0
        max_retries = self.max_retries
        retry_delay = self.retry_delay
        last_error = None
        
        while retries <= max_retries:
            try:
                url = f"{self.nxt_base_url}{endpoint}"
                self.logger.info(f"Making NXT API request to: {url}")
                
                # Create a session with default headers
                session = requests.Session()
                session.headers.update({
                    'Bb-Api-Subscription-Key': self.nxt_subscription_key,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                })
                
                # Add token to session headers
                token = self.nxt_token_service.get_valid_access_token()
                session.headers['Authorization'] = f'Bearer {token}'
                
                # Log request details (excluding sensitive info)
                safe_headers = session.headers.copy()
                safe_headers['Authorization'] = 'Bearer [REDACTED]'
                safe_headers['Bb-Api-Subscription-Key'] = '[REDACTED]'
                self.logger.info(f"Request headers: {safe_headers}")
                if 'json' in kwargs:
                    self.logger.info(f"Request body: {kwargs['json']}")
                
                # Make the request
                response = session.request(method, url, **kwargs)
                
                # Log the response status and content for debugging
                self.logger.info(f"NXT API Response Status: {response.status_code}")
                
                if response.ok:
                    self.logger.info("NXT API request successful")
                    return response.json()
                
                # Log error response
                self.logger.error(f"NXT API Error Response: {response.text}")
                
                # Handle specific error cases
                if response.status_code == 401:
                    self.logger.error("Token expired. Attempting to refresh...")
                    # Force token refresh by creating a new token service instance
                    self.nxt_token_service = TokenService('NXT')
                    retries += 1
                    if retries <= max_retries:
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.logger.error("Max retries reached for token refresh")
                
                # For other errors, raise immediately
                response.raise_for_status()
                
            except Exception as e:
                last_error = e
                self.logger.error(f"Error making NXT API request: {str(e)}")
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 401:
                    # Already handled above
                    continue
                retries += 1
                if retries <= max_retries:
                    time.sleep(retry_delay)
                    continue
                break
            
            return response
        
        # If we get here, we've exhausted retries
        raise last_error or Exception("Max retries exceeded")

    def create_nxt_event(self, sr_event):
        try:
            def parse_date(date_str):
                if not date_str:
                    return None
                # Handle ISO 8601 dates with optional timezone
                date_str = date_str.replace('Z', '+00:00')  # Replace Z with +00:00
                try:
                    return datetime.fromisoformat(date_str)
                except ValueError as e:
                    self.logger.error(f"Error parsing date {date_str}: {str(e)}")
                    return None

            start_date = parse_date(sr_event.get('StartDate'))
            end_date = parse_date(sr_event.get('EndDate'))

            event_data = {
                'name': sr_event.get('Name', 'Unnamed Event'),
                'start_date': start_date.strftime('%Y-%m-%d') if start_date else datetime.now().strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d') if end_date else None,
                'description': sr_event.get('Description', ''),
                'capacity': sr_event.get('MaxParticipants'),
                'inactive': False,
                'category': {
                    'id': '3211'  # Mission Trip category ID
                }
            }
            
            # Add times if available
            if start_date:
                event_data['start_time'] = start_date.strftime('%H:%M')
            if end_date:
                event_data['end_time'] = end_date.strftime('%H:%M')
            
            result = self._handle_nxt_request('POST', '/event/v1/events', json=event_data)
            self.logger.info(f"Created NXT event with ID: {result['id']}")
            return result
        except Exception as e:
            self.logger.error(f"Error creating NXT event: {str(e)}")
            raise

    def get_service_reef_member_details(self, member_id):
        try:
            # Get member details
            response = self._handle_service_reef_request('GET', f'/v1/members/{member_id}')
            if not response:
                self.logger.error(f"No response for member {member_id}")
                return {}
                
            # Extract required fields
            member_data = {
                'FirstName': response.get('FirstName'),
                'LastName': response.get('LastName'),
                'Email': response.get('Email'),
                'Phone': response.get('Phone'),
                'Address': response.get('Address', {})
            }
            
            # Validate required fields
            if not member_data['FirstName'] or not member_data['LastName']:
                self.logger.error(f"Member {member_id} missing required fields: FirstName={member_data['FirstName']}, LastName={member_data['LastName']}")
                return {}
                
            return member_data
            
        except Exception as e:
            self.logger.error(f"Error getting ServiceReef member details: {str(e)}")
            return {}
            
    def _map_participant_status(self, sr_status):
        """Map ServiceReef status to NXT status"""
        status_mapping = {
            'Registered': 'Registered',
            'Accepted': 'Registered',
            'Pending': 'Pending',
            'Declined': 'Cancelled',
            'Withdrawn': 'Cancelled'
        }
        return status_mapping.get(sr_status, 'Registered')
        
    def create_nxt_participant(self, nxt_event_id, participant_data):
        """Create a new participant in NXT.
        
        Args:
            nxt_event_id: The NXT event ID
            participant_data: Dict containing participant data from ServiceReef
            
        Returns:
            str: NXT participant ID if successful, None if failed
        """
        try:
            # Get or create constituent in NXT
            constituent_id = self.get_or_create_constituent(participant_data)
            if not constituent_id:
                self.logger.error("Failed to get or create constituent")
                return None
            
            # Create participant record
            participant = {
                'constituent_id': constituent_id,
                'status': self._map_participant_status(participant_data.get('Status', 'Registered')),
                'attended': False,  # Required field
                'host': False,  # Required field
                'invitation_status': 'Invited'  # Required field
            }
            
            # Make request to event-specific participant endpoint
            response = self._handle_nxt_request(
                'POST',
                f'/event/v1/events/{nxt_event_id}/participants',
                json=participant
            )
            
            return response.get('id')
            
        except Exception as e:
            self.logger.error(f"Error creating NXT participant: {str(e)}")
            return None

    def _create_nxt_constituent(self, participant_data):
        """Create a new constituent in NXT
        
        Args:
            participant_data: Dict containing participant data from ServiceReef
            
        Returns:
            str: NXT constituent ID if successful
            
        Raises:
            Exception: If creation fails
        """
        try:
            # Validate required fields
            first_name = participant_data.get('FirstName')
            last_name = participant_data.get('LastName')
            
            if not first_name or not last_name:
                self.logger.error(f"Missing required fields for constituent. FirstName={first_name}, LastName={last_name}")
                return None
            
            # Extract address from nested structure if present
            address = participant_data.get('Address', {})
            
            # Build constituent data with required fields
            constituent_data = {
                'type': 'Individual',  # Required field
                'first': first_name,
                'last': last_name
            }
            
            # Add optional email if present and valid
            if participant_data.get('Email'):
                constituent_data['email'] = {
                    'address': participant_data['Email'],
                    'type': 'Email',
                    'primary': True,
                    'do_not_email': False
                }
            
            # Add optional phone if present and valid
            phone = participant_data.get('Phone')
            if phone and isinstance(phone, str) and phone.strip():
                constituent_data['phone'] = {
                    'number': phone.strip(),
                    'type': 'Home',
                    'primary': True,
                    'do_not_call': False
                }
            
            # Handle address if complete
            if isinstance(address, dict):
                # Extract address data from nested structure
                addr = address.get('Address1', {})
                if isinstance(addr, dict):
                    addr_line = addr.get('Address1')
                    city = addr.get('City')
                    state = addr.get('State')
                    postal = addr.get('Zip')
                else:
                    addr_line = address.get('Address1')
                    city = address.get('City')
                    state = address.get('State')
                    postal = address.get('Zip')
                    
                # Only add address if all required fields are present and valid
                if all(x and isinstance(x, str) and x.strip() for x in [addr_line, city, state, postal]):
                    constituent_data['address'] = {
                        'address_lines': addr_line.strip(),
                        'city': city.strip(),
                        'state': state.strip(),
                        'postal_code': postal.strip(),
                        'type': 'Home',
                        'do_not_mail': False
                    }
            
            # Make the API request
            response = self._handle_nxt_request(
                'POST',
                '/constituent/v1/constituents',
                json=constituent_data
            )
            
            # Return the constituent ID
            return response.get('id')
            
        except Exception as e:
            self.logger.error(f"Error creating NXT constituent: {str(e)}")
            raise

    def get_or_create_constituent(self, participant_data):
        service_reef_id = participant_data.get('ServiceReefId')
        
        # Check mapping cache first
        if service_reef_id in self.constituent_mapping:
            return self.constituent_mapping[service_reef_id]
        
        # Create new constituent in NXT
        constituent_id = self._create_nxt_constituent(participant_data)
        
        # Update mapping
        self.constituent_mapping[service_reef_id] = constituent_id
        self.constituent_mapping_file.write_text(json.dumps(self.constituent_mapping))
        
        return constituent_id

    def sync_events(self):
        """Sync events from ServiceReef to NXT
        
        This method:
        1. Gets all events from ServiceReef
        2. Creates any new events in NXT that don't exist
        3. Updates the event mapping file
        4. Does NOT sync participants (that's handled in sync_all)
        
        Returns:
            dict: Mapping of ServiceReef event IDs to NXT event IDs
        """
        try:
            # Get all ServiceReef events
            events = self._handle_service_reef_request('GET', '/v1/events')
            sr_events = events.get('Results', [])
            self.logger.info(f"Found {len(sr_events)} events in ServiceReef")
            
            # Track which events we've processed to avoid duplicates
            processed_events = set()
            
            for sr_event in sr_events:
                try:
                    # Get ServiceReef event ID
                    sr_event_id = str(sr_event.get('EventId') or sr_event.get('Id'))
                    event_name = sr_event.get('Name', 'Unknown')
                    
                    # Skip if no ID
                    if not sr_event_id:
                        self.logger.error(f"Event {event_name} missing ID")
                        self.logger.error(f"Event data: {json.dumps(sr_event, indent=2)}")
                        continue
                        
                    # Skip if already processed (avoid duplicates)
                    if sr_event_id in processed_events:
                        self.logger.warning(f"Skipping duplicate event {event_name} (ID: {sr_event_id})")
                        continue
                        
                    processed_events.add(sr_event_id)
                    
                    # Create event in NXT if not already mapped
                    if sr_event_id not in self.event_mapping:
                        try:
                            nxt_event = self.create_nxt_event(sr_event)
                            nxt_event_id = nxt_event['id']
                            self.event_mapping[sr_event_id] = nxt_event_id
                            self._save_mapping(self.event_mapping_file, self.event_mapping)
                            self.logger.info(f"Created event {event_name} in NXT (SR ID: {sr_event_id}, NXT ID: {nxt_event_id})")
                        except Exception as e:
                            self.logger.error(f"Failed to create event {event_name} in NXT: {str(e)}")
                            continue
                    else:
                        self.logger.info(f"Event {event_name} already exists in NXT (SR ID: {sr_event_id}, NXT ID: {self.event_mapping[sr_event_id]})")
                    
                except Exception as e:
                    self.logger.error(f"Error processing event {sr_event.get('Name', 'Unknown')}: {str(e)}")
                    continue
                    
            return self.event_mapping
                    
        except Exception as e:
            self.logger.error(f"Error in event sync: {str(e)}")
            raise
            
    def sync_participants(self, sr_event_id, nxt_event_id, event_name):
        try:
            if not sr_event_id:
                self.logger.error(f"Missing ServiceReef event ID for event {event_name}")
                return
            if not nxt_event_id:
                self.logger.error(f"Missing NXT event ID for event {event_name}")
                return
            
            # Get ServiceReef participants
            participants = self._handle_service_reef_request('GET', f'/v1/events/{sr_event_id}/participants')
            sr_participants = participants.get('Results', [])
            
            self.logger.info(f"Got {len(sr_participants)} participants for event {event_name}")
            
            # Debug: Log first participant data structure
            if sr_participants:
                self.logger.info(f"First participant data structure: {json.dumps(sr_participants[0], indent=2)}")
                self.logger.info(f"Available fields: {', '.join(sr_participants[0].keys())}")
            
            for participant in sr_participants:
                try:
                    # Add ServiceReef ID using UserId
                    participant['ServiceReefId'] = participant.get('UserId') or participant.get('GUID')
                    if not participant['ServiceReefId']:
                        self.logger.warning("Warning: Participant missing UserId and GUID, skipping")
                        continue
                    
                    self.logger.info(f"Processing participant with ServiceReef ID: {participant['ServiceReefId']}")
                    
                    # Get additional member details and merge
                    member = self.get_service_reef_member_details(participant['ServiceReefId'])
                    if member:
                        participant.update(member)
                        self.logger.info("Merged member details into participant data")
                    
                    self.create_nxt_participant(nxt_event_id, participant)
                except Exception as e:
                    self.logger.error(f"Error creating participant: {str(e)}")
                except Exception as e:
                    self.logger.error(f"Error processing ServiceReef event {sr_event.get('Name', 'Unknown')}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error in ServiceReef events request: {str(e)}")
            raise
    
    def _get_service_reef_participants(self):
        """Get all participants from ServiceReef.
        
        This method performs a comprehensive search for participants by:
        1. Getting all members from the /v1/members endpoint
        2. Getting all events and their participants
        3. Merging both sets while avoiding duplicates
        4. Fetching full member details for any participants not in the members list
        
        This ensures we don't miss anyone who might be:
        - A member but not in any events
        - In events but not a full member
        - Both a member and in events
        """
        try:
            # Step 1: Get all members
            # Members typically have complete profile information
            response = self._handle_service_reef_request('GET', '/v1/members')
            members = response.get('Results', [])
            self.logger.info(f"Found {len(members)} members in ServiceReef")
            
            # Step 2: Get all event participants
            # This ensures we catch anyone who might be in events but not a member
            events_response = self._handle_service_reef_request('GET', '/v1/events')
            events = events_response.get('Results', [])
            
            # Collect all unique participant IDs from events
            participants = set()
            for event in events:
                event_id = event.get('Id')
                if event_id:
                    try:
                        event_participants = self._get_service_reef_event_participants(event_id)
                        for participant in event_participants:
                            # Participants might have either UserId or GUID
                            participant_id = participant.get('UserId') or participant.get('GUID')
                            if participant_id:
                                participants.add(participant_id)
                    except Exception as e:
                        self.logger.error(f"Error getting participants for event {event_id}: {str(e)}")
                        continue
            
            # Step 3: Merge members and participants
            all_participants = []
            seen_ids = set()  # Track IDs we've processed to avoid duplicates
            
            # Add members first since they have complete information
            for member in members:
                member_id = str(member.get('Id'))
                if member_id and member_id not in seen_ids:
                    all_participants.append(member)
                    seen_ids.add(member_id)
            
            # Step 4: Get full details for participants not in members
            for participant_id in participants:
                if str(participant_id) not in seen_ids:
                    try:
                        # Fetch complete member details for this participant
                        member_details = self.get_service_reef_member_details(participant_id)
                        if member_details:
                            all_participants.append(member_details)
                            seen_ids.add(str(participant_id))
                    except Exception as e:
                        self.logger.error(f"Error getting member details for {participant_id}: {str(e)}")
                        continue
            
            self.logger.info(f"Total unique participants found: {len(all_participants)}")
            return all_participants
            
        except Exception as e:
            self.logger.error(f"Error getting ServiceReef participants: {str(e)}")
            raise

    def _get_service_reef_event_participants(self, event_id):
        """Get participants for a specific event from ServiceReef"""
        try:
            response = self._handle_service_reef_request('GET', f'/v1/events/{event_id}/participants')
            participants = response.get('Results', [])
            self.logger.info(f"Found {len(participants)} participants for event {event_id}")
            return participants
        except Exception as e:
            self.logger.error(f"Error getting ServiceReef event participants: {str(e)}")
            raise
    
    def _create_nxt_constituent(self, participant_data):
        """Create a new constituent in NXT.
        
        This method transforms ServiceReef participant data into NXT's constituent format
        and creates a new constituent record. It handles:
        1. Field name variations between systems
        2. Proper structuring of nested data (email, phone, address)
        3. Optional fields that may not be present
        
        Args:
            participant_data: Dict containing participant data from ServiceReef
            
        Returns:
            str: NXT constituent ID if successful, None if failed
        """
        try:
            # Step 1: Map basic constituent data
            # Handle both camelCase and snake_case field names from ServiceReef
            constituent_data = {
                'first': participant_data.get('FirstName') or participant_data.get('first_name', ''),
                'last': participant_data.get('LastName') or participant_data.get('last_name', ''),
                # NXT requires email and phone in nested format
                'email': {
                    'address': participant_data.get('Email') or participant_data.get('email', '')
                },
                'phone': {
                    'number': participant_data.get('Phone') or participant_data.get('phone', '')
                }
            }
            
            # Log field presence/absence
            self.logger.info(f"Required fields present: first={bool(constituent_data['first'])}, last={bool(constituent_data['last'])}")
            
            # Log any empty required fields
            if not constituent_data['first'] or not constituent_data['last']:
                self.logger.warning(f"Missing required fields for participant: {participant_data.get('Id') or participant_data.get('UserId') or participant_data.get('GUID')}")
            
            # Step 2: Handle address fields
            # Only include address if we have any address data
            address = {
                'street1': participant_data.get('Address') or participant_data.get('address', ''),
                'city': participant_data.get('City') or participant_data.get('city', ''),
                'state': participant_data.get('State') or participant_data.get('state', ''),
                'zip': participant_data.get('PostalCode') or participant_data.get('postal_code', '')
            }
            
            # Add address only if any field has a value
            if any(address.values()):
                constituent_data['address'] = address
            
            # Log the exact API request being sent
            self.logger.info(f"NXT API Request payload: {constituent_data}")
            
            # Step 3: Create constituent in NXT
            result = self._handle_nxt_request('POST', '/constituent/v1/constituents', json=constituent_data)
            
            # Log the NXT API response in detail
            self.logger.info(f"NXT API Response: {result}")
            
            # Step 4: Validate and return result
            if result and 'id' in result:
                constituent_id = result['id']
                self.logger.info(f"Successfully created constituent with ID: {constituent_id}")
                return constituent_id
            else:
                self.logger.error(f"Failed to create constituent, unexpected response: {result}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating constituent: {str(e)}")
            return None
    
    def _update_nxt_constituent(self, constituent_id, participant_data):
        """Update an existing constituent in NXT"""
        try:
            # Map ServiceReef participant data to NXT constituent format
            constituent = {
                'first': participant_data.get('first_name'),
                'last': participant_data.get('last_name'),
                'email': {
                    'address': participant_data.get('email'),
                    'type': 'Email'
                } if participant_data.get('email') else None,
                'phone': {
                    'number': participant_data.get('phone'),
                    'type': 'Mobile'
                } if participant_data.get('phone') else None,
                'address': {
                    'street1': participant_data.get('address'),
                    'city': participant_data.get('city'),
                    'state': participant_data.get('state'),
                    'zip': participant_data.get('zip'),
                    'country': participant_data.get('country'),
                    'type': 'Home'
                } if any([participant_data.get(f) for f in ['address', 'city', 'state', 'zip']]) else None
            }
            
            # Update constituent in NXT
            self._handle_nxt_request('PATCH', f'/constituent/v1/constituents/{constituent_id}', json=constituent)
            
        except Exception as e:
            self.logger.error(f"Error updating NXT constituent {constituent_id}: {str(e)}")
            raise
    
    def _add_participant_to_event(self, event_id, constituent_id):
        """Add a constituent as a participant to an NXT event"""
        try:
            participant = {
                'constituent_id': constituent_id,
                'event_id': event_id,
                'attended': True  # Default to True since we're syncing from ServiceReef
            }
            
            # Add participant to event
            self._handle_nxt_request('POST', f'/event/v1/events/{event_id}/participants', json=participant)
            self.logger.info(f"Added constituent {constituent_id} to event {event_id}")
            
        except Exception as e:
            if 'already exists' in str(e).lower():
                self.logger.info(f"Constituent {constituent_id} already in event {event_id}")
            else:
                self.logger.error(f"Error adding constituent {constituent_id} to event {event_id}: {str(e)}")
                raise
    
    def sync_all(self):
        """Main sync method that handles participants first, then events, then event participants.
        
        The sync process follows these steps:
        1. First, sync all ServiceReef participants to NXT constituents
           This ensures all people exist in NXT before we try to associate them with events
        2. Then, sync all ServiceReef events to NXT events
        3. Finally, sync event participants
           This associates the previously created constituents with their respective events
        
        This order is important because:
        - Constituents must exist before they can be added to events
        - Events must exist before participants can be added to them
        - We want to avoid orphaned records or failed associations
        """
        try:
            self.logger.info("Starting sync...")
            
            # First sync all participants to create constituents
            self.logger.info("Step 1: Creating/updating constituents...")
            self._sync_all_participants()
            
            # Then sync events
            self.logger.info("Step 2: Creating/updating events...")
            self.sync_events()
            
            # Finally sync event participants
            self.logger.info("Step 3: Syncing event participants...")
            events = self._handle_service_reef_request('GET', '/v1/events')
            sr_events = events.get('Results', [])
            
            for sr_event in sr_events:
                try:
                    sr_event_id = str(sr_event.get('EventId') or sr_event.get('Id'))
                    if not sr_event_id:
                        self.logger.error(f"Event {sr_event.get('Name')} missing ID")
                        continue
                        
                    if sr_event_id in self.event_mapping:
                        nxt_event_id = self.event_mapping[sr_event_id]
                        self.sync_participants(sr_event_id, nxt_event_id, sr_event.get('Name'))
                    else:
                        self.logger.info(f"Skipping event {sr_event.get('Name')} - not yet synced to NXT")
                    
                except Exception as e:
                    self.logger.error(f"Error syncing participants for event {event.get('Name')}: {str(e)}")
                    continue  # Continue with next event even if this one fails
            
        except Exception as e:
            self.logger.error(f"Error in sync: {str(e)}")
            raise
    
    def _sync_all_participants(self):
        """Get all ServiceReef participants and create/update them in NXT"""
        try:
            # Get all ServiceReef participants
            participants = self._get_service_reef_participants()
            self.logger.info(f"Found {len(participants)} participants in ServiceReef")
            
            # Process each participant
            for participant in participants:
                try:
                    self._sync_participant(participant)
                except Exception as e:
                    self.logger.error(f"Error syncing participant {participant.get('Id')}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error in participant sync: {str(e)}")
            raise
    
    def _sync_participant(self, participant):
        """Create or update a participant in NXT.
        
        This method handles the synchronization of a single participant by:
        1. Identifying the participant using various possible ID fields
        2. Checking if they already exist in NXT (using our mapping)
        3. Either updating the existing constituent or creating a new one
        4. Maintaining the mapping between ServiceReef and NXT IDs
        
        Args:
            participant: Dict containing participant data from ServiceReef
            
        Returns:
            str: NXT constituent ID if successful, None if failed
        """
        try:
            # Log the raw participant data when received
            self.logger.info(f"Processing raw participant data: {participant}")
            
            # Step 1: Get participant ID
            # ServiceReef might provide the ID in different fields:
            # - Id: Usually for members
            # - UserId: Usually for event participants
            # - GUID: Alternative identifier
            service_reef_id = str(participant.get('Id') or participant.get('UserId') or participant.get('GUID'))
            if not service_reef_id:
                self.logger.error("Participant missing ID fields")
                return None
            
            # Log which ID field was found
            id_source = 'Id' if participant.get('Id') else 'UserId' if participant.get('UserId') else 'GUID'
            self.logger.info(f"Using ID {service_reef_id} from field: {id_source}")
                
            self.logger.info(f"Processing participant {service_reef_id}")
            
            # Step 2: Check if we've seen this participant before
            if service_reef_id in self.constituent_mapping:
                # Step 3a: Update existing constituent
                nxt_id = self.constituent_mapping[service_reef_id]
                self._update_nxt_constituent(nxt_id, participant)
                self.logger.info(f"Updated NXT constituent {nxt_id} for ServiceReef participant {service_reef_id}")
            else:
                # Step 3b: Create new constituent
                nxt_id = self._create_nxt_constituent(participant)
                if nxt_id:
                    # Step 4: Save the mapping for future reference
                    self.constituent_mapping[service_reef_id] = nxt_id
                    self._save_mapping(self.constituent_mapping_file, self.constituent_mapping)
                    self.logger.info(f"Created NXT constituent {nxt_id} for ServiceReef participant {service_reef_id}")
                else:
                    self.logger.error(f"Failed to create NXT constituent for ServiceReef participant {service_reef_id}")
                    return None
            
            return nxt_id
        except Exception as e:
            self.logger.error(f"Error syncing participant {service_reef_id}: {str(e)}")
            return None

    def _get_service_reef_participants(self):
        """Get all participants from ServiceReef"""
        try:
            response = self._handle_service_reef_request('GET', '/v1/members')
            return response.get('Results', [])
        except Exception as e:
            self.logger.error(f"Error getting ServiceReef participants: {str(e)}")
            raise

    def _get_service_reef_event_participants(self, event_id):
        """Get participants for a specific event from ServiceReef"""
        try:
            response = self._handle_service_reef_request('GET', f'/v1/events/{event_id}/participants')
            return response.get('Results', [])
        except Exception as e:
            self.logger.error(f"Error getting ServiceReef event participants: {str(e)}")
            raise

def _create_nxt_constituent(self, participant_data):
    """Create a new constituent in NXT
    
    Args:
        participant_data: Dict containing participant data from ServiceReef
        
    Returns:
        str: NXT constituent ID if successful
        
    Raises:
        Exception: If creation fails
    """
    try:
        # Extract name fields from various possible sources
        first_name = participant_data.get('FirstName') or participant_data.get('first_name')
        last_name = participant_data.get('LastName') or participant_data.get('last_name')
        email = participant_data.get('Email') or participant_data.get('email')
        phone = participant_data.get('Phone') or participant_data.get('phone')
        
        # Log the data we're working with
        self.logger.debug(f"Creating constituent with data: {participant_data}")
        
        # Validate required fields
        if not first_name or not last_name:
            raise ValueError(f"Missing required name fields for participant")
            
        # Build address if available
        address = None
        street = participant_data.get('Address') or participant_data.get('address')
        city = participant_data.get('City') or participant_data.get('city')
        state = participant_data.get('State') or participant_data.get('state')
        zip_code = participant_data.get('Zip') or participant_data.get('zip')
        country = participant_data.get('Country') or participant_data.get('country')
        
        if any([street, city, state, zip_code, country]):
            address = {
                'address_lines': [street] if street else [],
                'city': city,
                'state': state,
                'postal_code': zip_code,
                'country': country
            }
        
        # Build constituent data
        constituent = {
            'first': first_name,
            'last': last_name,
            'type': 'Individual'
        }
        
        # Add optional fields if present
        if email:
            constituent['email'] = {'address': email, 'type': 'Email'}
        if phone:
            constituent['phone'] = {'number': phone, 'type': 'Mobile'}
        if address:
            constituent['address'] = address
            
        # Create constituent in NXT
        response = self._handle_nxt_request('POST', '/constituent/v1/constituents', json=constituent)
        constituent_id = response.get('id')
        
        if not constituent_id:
            raise ValueError(f"No constituent ID returned from NXT")
            
        self.logger.info(f"Created constituent {constituent_id} ({first_name} {last_name})")
        return constituent_id
        
    except Exception as e:
        self.logger.error(f"Error creating NXT constituent: {str(e)}")
        raise

def _update_nxt_constituent(self, constituent_id, participant_data):
    """Update an existing constituent in NXT
    
    Args:
        constituent_id: str, NXT constituent ID to update
        participant_data: Dict containing participant data from ServiceReef
        
    Raises:
        Exception: If update fails
    """
    try:
        # Extract name fields from various possible sources
        first_name = participant_data.get('FirstName') or participant_data.get('first_name')
        last_name = participant_data.get('LastName') or participant_data.get('last_name')
        email = participant_data.get('Email') or participant_data.get('email')
        phone = participant_data.get('Phone') or participant_data.get('phone')
        
        # Log the data we're working with
        self.logger.debug(f"Updating constituent {constituent_id} with data: {participant_data}")
        
        # Build update data
        update_data = {}
        
        # Only include fields that are present
        if first_name:
            update_data['first'] = first_name
        if last_name:
            update_data['last'] = last_name
            
        # Handle address if any fields are present
        street = participant_data.get('Address') or participant_data.get('address')
        city = participant_data.get('City') or participant_data.get('city')
        state = participant_data.get('State') or participant_data.get('state')
        zip_code = participant_data.get('Zip') or participant_data.get('zip')
        country = participant_data.get('Country') or participant_data.get('country')
        
        if any([street, city, state, zip_code, country]):
            update_data['address'] = {
                'address_lines': [street] if street else [],
                'city': city,
                'state': state,
                'postal_code': zip_code,
                'country': country
            }
            
        # Handle email and phone
        if email:
            update_data['email'] = {'address': email, 'type': 'Email'}
        if phone:
            update_data['phone'] = {'number': phone, 'type': 'Mobile'}
            
        # Only update if we have data to update
        if update_data:
            self._handle_nxt_request('PATCH', f'/constituent/v1/constituents/{constituent_id}', json=update_data)
            self.logger.info(f"Updated constituent {constituent_id}")
        else:
            self.logger.info(f"No updates needed for constituent {constituent_id}")
            
    except Exception as e:
        self.logger.error(f"Error updating NXT constituent {constituent_id}: {str(e)}")
        raise

def _add_participant_to_event(self, event_id, constituent_id):
    """Add a constituent as a participant to an NXT event"""
    try:
        participant = {
            'constituent_id': constituent_id,
            'event_id': event_id,
            'attended': True  # Default to True since we're syncing from ServiceReef
        }
        
        # Add participant to event
        self._handle_nxt_request('POST', f'/event/v1/events/{event_id}/participants', json=participant)
        self.logger.info(f"Added constituent {constituent_id} to event {event_id}")
        
    except Exception as e:
        if 'already exists' in str(e).lower():
            self.logger.info(f"Constituent {constituent_id} already in event {event_id}")
        else:
            self.logger.error(f"Error adding constituent {constituent_id} to event {event_id}: {str(e)}")
            raise

def _sync_all_participants(self):
    """Sync all participants from ServiceReef to NXT"""
    try:
        # Get all ServiceReef participants
        participants = self._get_service_reef_participants()
        self.logger.info(f"Found {len(participants)} participants in ServiceReef")
        
        for participant in participants:
            try:
                # Get ServiceReef ID
                sr_id = str(participant.get('Id') or participant.get('id'))
                if not sr_id:
                    self.logger.warning(f"Skipping participant - missing ID")
                    continue
                
                # Check if already exists in NXT
                nxt_id = self.constituent_mapping.get(sr_id)
                if nxt_id:
                    # Update existing constituent
                    self._update_nxt_constituent(nxt_id, participant)
                    self.logger.info(f"Updated constituent {nxt_id} in NXT")
                else:
                    # Create new constituent
                    nxt_id = self._create_nxt_constituent(participant)
                    self.constituent_mapping[sr_id] = nxt_id
                    self._save_mapping(self.constituent_mapping_file, self.constituent_mapping)
                    self.logger.info(f"Created constituent {nxt_id} in NXT")
                    
            except Exception as e:
                self.logger.error(f"Error syncing participant {participant.get('Id')}: {str(e)}")
                continue
                
    except Exception as e:
        self.logger.error(f"Error in participant sync: {str(e)}")
        raise

def sync_all(self):
    """Main sync method that handles participants first, then events, then event participants"""
    try:
        # 1. Get all ServiceReef participants and sync to NXT constituents
        self.logger.info('Starting participant sync...')
        self._sync_all_participants()

        # 2. Process events and create new ones in NXT
        self.logger.info('Starting event sync...')
        self.sync_events()

        # 3. Update event participants
        self.logger.info('Starting event participant sync...')
        events = self._handle_service_reef_request('GET', '/v1/events')
        service_reef_events = events.get('Results', [])
        for event in service_reef_events:
            try:
                sr_event_id = str(event.get('Id'))
                nxt_event_id = self.event_mapping.get(sr_event_id)

                if not sr_event_id:
                    self.logger.warning(f"Skipping event {event.get('Name')} - missing ServiceReef ID")
                    continue

                if not nxt_event_id:
                    self.logger.warning(f"Skipping event {event.get('Name')} - not yet synced to NXT")
                    continue

                self.sync_participants(sr_event_id, nxt_event_id, event.get('Name'))
            except Exception as e:
                self.logger.error(f"Error syncing participants for event {event.get('Name')}: {str(e)}")
                continue

    except Exception as e:
        self.logger.error(f"Error in sync: {str(e)}")
        raise

if __name__ == '__main__':
    try:
        sync_service = EventSyncService()
        sync_service.sync_all()
    except Exception as e:
        logging.error(f"Error running sync service: {str(e)}")
