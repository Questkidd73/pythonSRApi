import os
import json
import time
from datetime import datetime
from pathlib import Path
import logging
import requests
import urllib.parse
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
        self.logger = logging.getLogger(f'{service_type}TokenService')
        
        if service_type == 'ServiceReef':
            self.client_id = os.getenv('SERVICE_REEF_APP_ID', '')
            self.client_secret = os.getenv('SERVICE_REEF_APP_SECRET', '')
            self.token_endpoint = 'https://api.servicereef.com/OAuth/Token'
            self.token_file = Path(__file__).parent / 'tokens' / 'servicereef_token.json'
        else:  # NXT
            self.client_id = os.getenv('NXT_CLIENT_ID', '')
            self.client_secret = os.getenv('NXT_CLIENT_SECRET', '')
            self.token_endpoint = 'https://oauth2.sky.blackbaud.com/token'
            self.token_file = Path(__file__).parent / 'tokens' / 'blackbaud_token.json'
            
            # Additional NXT-specific settings
            self.nxt_base_url = os.getenv('NXT_BASE_URL', 'https://api.sky.blackbaud.com')
            self.subscription_key = os.getenv('NXT_SUBSCRIPTION_KEY')
            if not self.subscription_key:
                raise ValueError('NXT_SUBSCRIPTION_KEY is required')
        
        # Ensure tokens directory exists
        self.token_file.parent.mkdir(exist_ok=True)

    def get_valid_access_token(self):
        try:
            # For NXT, always use the access token from environment
            # This is because NXT uses authorization code flow which requires user interaction
            if self.service_type == 'NXT':
                token = os.getenv('NXT_ACCESS_TOKEN')
                if not token:
                    raise ValueError(
                        'NXT_ACCESS_TOKEN is required in .env file.\n'
                        'To get a new token:\n'
                        '1. Go to https://erportal.back2back.org/ServiceReefAPI/\n'
                        '2. Use the NXT token management interface to get a new token\n'
                        '3. Add the new token to your .env file as NXT_ACCESS_TOKEN=<token>'
                    )
                return token
            
            # For ServiceReef, handle OAuth client credentials flow
            token_data = self._load_token_from_file()
            
            if token_data and 'access_token' in token_data:
                expires_in = token_data.get('expires_in', 3600)
                fetched_at = token_data.get('fetched_at', 0)
                
                # Check if token is still valid (with 2-minute buffer)
                if (time.time() - fetched_at) < (expires_in - 120):
                    return token_data['access_token']
            
            # Get new ServiceReef token
            return self._get_new_token()
                
        except Exception as e:
            self.logger.error(f"Error getting token: {str(e)}")
            raise

    def _get_new_token(self):
        try:
            # Set up the request data
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
            
            # Log request details (excluding sensitive info)
            self.logger.info(f"Getting new token from {self.token_endpoint}")
            
            # Make the request
            response = requests.post(
                self.token_endpoint,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            # Log response status
            self.logger.info(f"Token response status: {response.status_code}")
            if not response.ok:
                self.logger.error(f"Token error response: {response.text}")
            
            response.raise_for_status()
            token_data = response.json()
            
            # Add fetched timestamp
            token_data['fetched_at'] = int(time.time())
            
            # Save token to file
            self._save_token_to_file(token_data)
            
            return token_data['access_token']
        except Exception as e:
            self.logger.error(f"Error getting new token: {str(e)}")
            raise

    def _load_token_from_file(self):
        if self.token_file.exists():
            return json.loads(self.token_file.read_text())
        return None

    def _save_token_to_file(self, token_data):
        self.token_file.write_text(json.dumps(token_data))


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
        self.page_size = 100
        self.retry_delay = 2
        self.max_retries = 3
        
        # Initialize token services
        self.sr_token_service = TokenService('ServiceReef')
        self.nxt_token_service = TokenService('NXT')

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
        try:
            token = self.sr_token_service.get_valid_access_token()
            headers = {
                'Authorization': f'Bearer {token}',
                **kwargs.get('headers', {})
            }
            kwargs['headers'] = headers
            
            response = requests.request(
                method,
                f"{self.service_reef_base_url}{endpoint}",
                **kwargs
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Error in ServiceReef request: {str(e)}")
            raise

    def _handle_nxt_request(self, method, endpoint, **kwargs):
        retries = 0
        max_retries = self.max_retries
        retry_delay = self.retry_delay
        
        while retries <= max_retries:
            try:
                # Get token from environment
                token = self.nxt_token_service.get_valid_access_token()
                
                # Set up headers
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Bb-Api-Subscription-Key': self.nxt_token_service.subscription_key,
                    'Content-Type': 'application/json'
                }
                
                # Add any additional headers from kwargs
                if 'headers' in kwargs:
                    headers.update(kwargs.pop('headers'))
                
                # Prepare the request
                url = f"{self.nxt_token_service.nxt_base_url}{endpoint}"
                self.logger.info(f"Making NXT API request to: {url}")
                
                # Log request details (excluding sensitive info)
                safe_headers = headers.copy()
                safe_headers['Authorization'] = 'Bearer [REDACTED]'
                self.logger.info(f"Request headers: {safe_headers}")
                if 'json' in kwargs:
                    self.logger.info(f"Request body: {kwargs['json']}")
                
                # Make the request
                response = requests.request(
                    method,
                    url,
                    headers=headers,
                    **kwargs
                )
                
                # Log the response status and content for debugging
                self.logger.info(f"NXT API Response Status: {response.status_code}")
                
                if response.ok:
                    self.logger.info("NXT API request successful")
                    return response.json()
                
                # Handle specific error cases
                if response.status_code == 401:
                    self.logger.error("Token expired. Attempting to refresh...")
                    # Force token refresh on next iteration
                    os.environ['NXT_ACCESS_TOKEN'] = ''
                    if retries < max_retries:
                        retries += 1
                        time.sleep(retry_delay)
                        continue
                    else:
                        self.logger.error("Max retries reached. Please obtain a new token through the Blackbaud OAuth flow.")
                
                self.logger.error(f"NXT API Error Response: {response.text}")
                response.raise_for_status()
                
            except requests.exceptions.RequestException as e:
                if retries < max_retries:
                    retries += 1
                    time.sleep(retry_delay)
                    continue
                self.logger.error(f"Error in NXT request after {max_retries} retries: {str(e)}")
                raise
            
        raise Exception(f"Failed to complete NXT request after {max_retries} retries")

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
            response = self._handle_service_reef_request('GET', f'/v1/members/{member_id}')
            return response
        except Exception as e:
            self.logger.error(f"Error getting ServiceReef member details: {str(e)}")
            return {}

    def create_nxt_participant(self, nxt_event_id, participant_data):
        try:
            # Get or create constituent in NXT
            constituent_id = self.get_or_create_constituent(participant_data)
            
            # Create participant record
            participant = {
                'constituent_id': constituent_id,
                'event_id': nxt_event_id,
                'status': self._map_participant_status(participant_data.get('Status', 'Registered'))
            }
            
            return self._handle_nxt_request('POST', '/event/v1/participants', json=participant)
        except Exception as e:
            self.logger.error(f"Error creating NXT participant: {str(e)}")
            raise

    def _map_participant_status(self, sr_status):
        # Map ServiceReef status to NXT status
        status_mapping = {
            'Registered': 'Registered',
            'Accepted': 'Registered',
            'Pending': 'Pending',
            'Declined': 'Cancelled',
            'Withdrawn': 'Cancelled'
        }
        return status_mapping.get(sr_status, 'Registered')

    def get_or_create_constituent(self, participant_data):
        service_reef_id = participant_data.get('ServiceReefId')
        
        # Check mapping cache first
        if service_reef_id in self.constituent_mapping:
            return self.constituent_mapping[service_reef_id]
        
        # Create new constituent in NXT
        constituent_data = {
            'first': participant_data.get('FirstName', ''),
            'last': participant_data.get('LastName', ''),
            'email': participant_data.get('Email', ''),
            'phone': participant_data.get('Phone', ''),
            'address': {
                'street1': participant_data.get('Address', ''),
                'city': participant_data.get('City', ''),
                'state': participant_data.get('State', ''),
                'zip': participant_data.get('PostalCode', '')
            }
        }
        
        try:
            result = self._handle_nxt_request('POST', '/constituent/v1/constituents', json=constituent_data)
            constituent_id = result['id']
            
            # Update mapping
            self.constituent_mapping[service_reef_id] = constituent_id
            self.constituent_mapping_file.write_text(json.dumps(self.constituent_mapping))
            
            return constituent_id
        except Exception as e:
            self.logger.error(f"Error creating constituent: {str(e)}")
            raise

    def sync_participants(self, sr_event_id, nxt_event_id, event_name):
        try:
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
                    participant.update(member)
                    self.logger.info("Merged member details into participant data")
                    
                    self.create_nxt_participant(nxt_event_id, participant)
                except Exception as e:
                    self.logger.error(f"Error creating participant: {str(e)}")
                    continue
        except Exception as e:
            self.logger.error(f"Error syncing participants for event {event_name}: {str(e)}")

    def sync_events(self):
        """Sync events from ServiceReef to NXT"""
        try:
            # Get events from ServiceReef
            events = self._handle_service_reef_request('GET', '/v1/events')
            sr_events = events.get('Results', [])
            
            for sr_event in sr_events:
                try:
                    # Get event ID from either Id or id field
                    sr_event_id = str(sr_event.get('Id', sr_event.get('id', '')))
                    if not sr_event_id:
                        self.logger.error(f"Event missing ID: {sr_event}")
                        continue
                    if not sr_event_id:
                        self.logger.error(f"Event {sr_event.get('Name', 'Unknown')} missing ID field")
                        continue
                    
                    # Check if event exists in NXT
                    if sr_event_id in self.event_mapping:
                        self.logger.info(f"Event {sr_event.get('Name')} already exists in NXT")
                        continue
                    
                    # Create event in NXT
                    nxt_event = self.create_nxt_event(sr_event)
                    nxt_event_id = nxt_event['id']
                    
                    # Save mapping
                    self.event_mapping[sr_event_id] = nxt_event_id
                    self._save_mapping(self.event_mapping_file, self.event_mapping)
                    self.logger.info(f"Created event {sr_event.get('Name')} in NXT")
                    
                except Exception as e:
                    self.logger.error(f"Error processing ServiceReef event {sr_event.get('Name', 'Unknown')}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error in ServiceReef events request: {str(e)}")
            raise
    
    def _get_service_reef_participants(self):
        """Get all participants from ServiceReef"""
        try:
            # First get all members
            response = self._handle_service_reef_request('GET', '/v1/members')
            members = response.get('Results', [])
            self.logger.info(f"Found {len(members)} members in ServiceReef")
            
            # Then get all event participants to ensure we have everyone
            events_response = self._handle_service_reef_request('GET', '/v1/events')
            events = events_response.get('Results', [])
            
            participants = set()
            for event in events:
                event_id = event.get('Id')
                if event_id:
                    try:
                        event_participants = self._get_service_reef_event_participants(event_id)
                        for participant in event_participants:
                            # Add participant ID to set to avoid duplicates
                            participant_id = participant.get('UserId') or participant.get('GUID')
                            if participant_id:
                                participants.add(participant_id)
                    except Exception as e:
                        self.logger.error(f"Error getting participants for event {event_id}: {str(e)}")
                        continue
            
            # Merge members and participants
            all_participants = []
            seen_ids = set()
            
            # Add members first
            for member in members:
                member_id = str(member.get('Id'))
                if member_id and member_id not in seen_ids:
                    all_participants.append(member)
                    seen_ids.add(member_id)
            
            # Then get details for any participants not in members
            for participant_id in participants:
                if str(participant_id) not in seen_ids:
                    try:
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
        """Create a new constituent in NXT"""
        try:
            # Map ServiceReef participant data to NXT constituent format
            constituent_data = {
                'first': participant_data.get('FirstName') or participant_data.get('first_name', ''),
                'last': participant_data.get('LastName') or participant_data.get('last_name', ''),
                'email': {
                    'address': participant_data.get('Email') or participant_data.get('email', '')
                },
                'phone': {
                    'number': participant_data.get('Phone') or participant_data.get('phone', '')
                }
            }
            
            # Add address if any address fields are present
            address = {
                'street1': participant_data.get('Address') or participant_data.get('address', ''),
                'city': participant_data.get('City') or participant_data.get('city', ''),
                'state': participant_data.get('State') or participant_data.get('state', ''),
                'zip': participant_data.get('PostalCode') or participant_data.get('postal_code', '')
            }
            
            if any(address.values()):
                constituent_data['address'] = address
            
            # Log the data we're sending
            self.logger.info(f"Creating constituent with data: {constituent_data}")
            
            # Create the constituent
            result = self._handle_nxt_request('POST', '/constituent/v1/constituents', json=constituent_data)
            
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
                'email': participant_data.get('email'),
                'phone': participant_data.get('phone'),
                'address': {
                    'street1': participant_data.get('address'),
                    'city': participant_data.get('city'),
                    'state': participant_data.get('state'),
                    'zip': participant_data.get('zip'),
                    'country': participant_data.get('country')
                }
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
        """Create or update a participant in NXT"""
        try:
            # Get participant ID from either Id or UserId or GUID
            service_reef_id = str(participant.get('Id') or participant.get('UserId') or participant.get('GUID'))
            if not service_reef_id:
                self.logger.error("Participant missing ID fields")
                return None
                
            self.logger.info(f"Processing participant {service_reef_id}")
            
            # Get or create constituent in NXT
            if service_reef_id in self.constituent_mapping:
                # Update existing constituent
                nxt_id = self.constituent_mapping[service_reef_id]
                self._update_nxt_constituent(nxt_id, participant)
                self.logger.info(f"Updated NXT constituent {nxt_id} for ServiceReef participant {service_reef_id}")
            else:
                # Create new constituent
                nxt_id = self._create_nxt_constituent(participant)
                if nxt_id:
                    # Save mapping
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
        return response.get('members', [])
    except Exception as e:
        self.logger.error(f"Error getting ServiceReef participants: {str(e)}")
        raise

def _get_service_reef_event_participants(self, event_id):
    """Get participants for a specific event from ServiceReef"""
    try:
        response = self._handle_service_reef_request('GET', f'/v1/events/{event_id}/participants')
        return response.get('participants', [])
    except Exception as e:
        self.logger.error(f"Error getting ServiceReef event participants: {str(e)}")
        raise

def _create_nxt_constituent(self, participant_data):
    """Create a new constituent in NXT"""
    try:
        # Map ServiceReef participant data to NXT constituent format
        constituent = {
            'first': participant_data.get('first_name'),
            'last': participant_data.get('last_name'),
            'email': participant_data.get('email'),
            'phone': participant_data.get('phone'),
            'address': {
                'street1': participant_data.get('address'),
                'city': participant_data.get('city'),
                'state': participant_data.get('state'),
                'zip': participant_data.get('zip'),
                'country': participant_data.get('country')
            }
        }
        
        # Create constituent in NXT
        response = self._handle_nxt_request('POST', '/constituent/v1/constituents', json=constituent)
        return response['id']
        
    except Exception as e:
        self.logger.error(f"Error creating NXT constituent: {str(e)}")
        raise

def _update_nxt_constituent(self, constituent_id, participant_data):
    """Update an existing constituent in NXT"""
    try:
        # Map ServiceReef participant data to NXT constituent format
        constituent = {
            'first': participant_data.get('first_name'),
            'last': participant_data.get('last_name'),
            'email': participant_data.get('email'),
            'phone': participant_data.get('phone'),
            'address': {
                'street1': participant_data.get('address'),
                'city': participant_data.get('city'),
                'state': participant_data.get('state'),
                'zip': participant_data.get('zip'),
                'country': participant_data.get('country')
            }
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
