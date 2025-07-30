"""
Sync orchestrator that coordinates event synchronization between ServiceReef and NXT.
Uses modular components for API access, mapping, and token management.
"""
import logging
import json
import os
from datetime import datetime
import traceback

from config import Config
from token_service import ServiceReefTokenService, NXTTokenService
from service_reef_client import ServiceReefClient
from nxt_client import NXTClient
from mapping_service import MappingService

class SyncOrchestrator:
    """Main orchestrator for event synchronization."""
    
    def __init__(self):
        """Initialize the sync orchestrator with all required services."""
        # Set up logging
        self._setup_logging()
        
        # Load configuration
        self.config = Config()
        valid, message = self.config.validate()
        if not valid:
            self.logger.error(f"Configuration error: {message}")
            raise ValueError(f"Configuration error: {message}")
            
        # Initialize token services
        self.sr_token_service = ServiceReefTokenService(self.config)
        self.nxt_token_service = NXTTokenService(self.config)
        
        # Initialize API clients
        self.sr_client = ServiceReefClient(self.sr_token_service)
        self.nxt_client = NXTClient(self.nxt_token_service)
        
        # Initialize mapping service
        self.mapping_service = MappingService(self.config)
    
    def _setup_logging(self):
        """Set up logging configuration."""
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'
        
        # Set up file handler
        log_file = os.path.join(log_dir, f'sync_{datetime.now().strftime("%Y%m%d")}.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        file_handler.setLevel(logging.DEBUG)
        
        # Set up console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format, date_format))
        console_handler.setLevel(logging.INFO)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        
        # Create logger for this class
        self.logger = logging.getLogger('EventSync')
    
    def sync_all_events(self):
        """Sync all ServiceReef events to NXT."""
        self.logger.info("Starting sync for all ServiceReef events")
        
        # Get all ServiceReef events
        events = self._get_all_service_reef_events()
        if not events:
            self.logger.error("Failed to retrieve ServiceReef events")
            return False
            
        self.logger.info(f"Found {len(events)} ServiceReef events to sync")
        
        # Process each event
        success_count = 0
        for event in events:
            sr_event_id = event.get('EventId')
            if not sr_event_id:
                self.logger.warning(f"Event missing EventId: {event}")
                continue
                
            if self.sync_event(sr_event_id):
                success_count += 1
                
        self.logger.info(f"Completed sync of {success_count}/{len(events)} events")
        return success_count > 0
    
    def sync_event(self, sr_event_id):
        """Sync a single ServiceReef event to NXT.
        
        Args:
            sr_event_id: ServiceReef event ID
            
        Returns:
            True if sync successful, False otherwise
        """
        self.logger.info(f"Starting sync for ServiceReef event {sr_event_id}")
        
        # Get ServiceReef event details
        sr_event = self.sr_client.get_event(sr_event_id)
        if not sr_event:
            self.logger.error(f"Failed to retrieve ServiceReef event {sr_event_id}")
            return False
            
        # Check if event already exists in NXT
        nxt_event_id = self.mapping_service.get_nxt_event_id(sr_event_id)
        
        if not nxt_event_id:
            # Create event in NXT
            nxt_event_id = self._create_nxt_event(sr_event)
            if not nxt_event_id:
                self.logger.error(f"Failed to create NXT event for ServiceReef event {sr_event_id}")
                return False
                
            # Add event mapping
            self.mapping_service.add_event_mapping(sr_event_id, nxt_event_id)
            
        # Sync event participants
        self.logger.info(f"Syncing participants for event {sr_event_id} -> {nxt_event_id}")
        return self.sync_event_participants(sr_event_id, nxt_event_id)
    
    def sync_event_participants(self, sr_event_id, nxt_event_id):
        """Sync all participants for a ServiceReef event to NXT.
        
        Args:
            sr_event_id: ServiceReef event ID
            nxt_event_id: NXT event ID
            
        Returns:
            True if sync successful, False otherwise
        """
        # Get all ServiceReef participants
        sr_participants = self._get_service_reef_event_participants(sr_event_id)
        if not sr_participants:
            self.logger.warning(f"No participants found for ServiceReef event {sr_event_id}")
            return True  # Empty participant list is not a failure
            
        self.logger.info(f"Found {len(sr_participants)} participants for event {sr_event_id}")
        
        # Get existing NXT participants
        nxt_participants = self._get_nxt_event_participants(nxt_event_id)
        if nxt_participants is None:  # Error case
            self.logger.error(f"Failed to retrieve NXT participants for event {nxt_event_id}")
            return False
            
        self.logger.info(f"Found {len(nxt_participants) if nxt_participants else 0} participants in NXT event {nxt_event_id}")
        
        # Debug output for participant data
        self.logger.debug("=== DEBUG: NXT Participants Summary ===")
        self.logger.debug(f"Total: {len(nxt_participants) if nxt_participants else 0} participants")
        
        # Process each ServiceReef participant
        success_count = 0
        for sr_participant in sr_participants:
            # Standardize participant data
            sr_participant = self.mapping_service.standardize_servicereef_participant(sr_participant)
            
            # Get ServiceReef user ID
            sr_user_id = sr_participant.get('UserId')
            if not sr_user_id:
                self.logger.warning(f"Participant missing UserId: {sr_participant}")
                continue
                
            # Check if participant exists in NXT
            nxt_constituent_id = self.mapping_service.get_nxt_constituent_id(sr_user_id)
            
            if not nxt_constituent_id:
                # Create constituent in NXT
                nxt_constituent_id = self._create_nxt_constituent(sr_participant)
                if not nxt_constituent_id:
                    self.logger.error(f"Failed to create NXT constituent for ServiceReef user {sr_user_id}")
                    continue  # Skip this participant if constituent creation fails
                    
                # Add constituent mapping
                self.mapping_service.add_constituent_mapping(sr_user_id, nxt_constituent_id)
            
            # Verify we have a valid constituent ID before proceeding
            if not nxt_constituent_id:
                self.logger.error(f"No valid NXT constituent ID available for ServiceReef user {sr_user_id}")
                continue
                
            # Check if participant exists in NXT event
            existing_participant = self._find_nxt_participant(nxt_participants, nxt_constituent_id)
            
            if existing_participant:
                # Update participant status if needed
                if self._update_nxt_participant_status(nxt_event_id, existing_participant, sr_participant):
                    self.logger.info(f"Successfully updated participant {nxt_constituent_id} in event {nxt_event_id}")
                else:
                    self.logger.info(f"Successfully verified participant {nxt_constituent_id} in event {nxt_event_id}")
                    
                success_count += 1
            else:
                # Create participant in NXT event
                if self._create_nxt_participant(nxt_event_id, nxt_constituent_id, sr_participant):
                    self.logger.info(f"Successfully synced participant {sr_user_id} to NXT")
                    success_count += 1
        
        self.logger.info(f"Completed participant sync: {success_count}/{len(sr_participants)} successful")
        return success_count > 0
    
    def _get_all_service_reef_events(self):
        """Get all events from ServiceReef.
        
        Returns:
            List of ServiceReef events
        """
        events = []
        page = 1
        page_size = 100
        
        while True:
            response = self.sr_client.get_events(page, page_size)
            if not response or 'Results' not in response:
                self.logger.error(f"Invalid response from ServiceReef events API: {response}")
                break
                
            page_events = response['Results']
            events.extend(page_events)
            
            # Check if we've got all events
            page_info = response.get('PageInfo', {})
            total_records = page_info.get('TotalRecords', 0)
            current_page = page_info.get('Page', 1)
            records_per_page = page_info.get('PageSize', 0)
            
            self.logger.info(f"Retrieved page {current_page} with {len(page_events)} events")
            
            if not page_events or len(events) >= total_records:
                break
                
            page += 1
            
        self.logger.info(f"Retrieved {len(events)} total ServiceReef events")
        return events
    
    def _get_service_reef_event_participants(self, event_id):
        """Get all participants for a ServiceReef event.
        
        Args:
            event_id: ServiceReef event ID
            
        Returns:
            List of ServiceReef participants with complete data
        """
        participants = []
        page = 1
        page_size = 100
        
        while True:
            response = self.sr_client.get_event_participants(event_id, page, page_size)
            if not response or 'Results' not in response:
                self.logger.error(f"Invalid response from ServiceReef participants API: {response}")
                break
                
            page_participants = response['Results']
            
            # Process and validate each participant record
            for participant in page_participants:
                # Ensure participant has required fields
                if not participant.get('FirstName') or not participant.get('LastName'):
                    self.logger.warning(f"Skipping participant with incomplete name data: {participant}")
                    continue
                    
                # Ensure registration status is present
                if 'RegistrationStatus' not in participant:
                    self.logger.warning(f"Participant missing RegistrationStatus, setting to 'Unknown': {participant}")
                    participant['RegistrationStatus'] = 'Unknown'
                
                # Add to validated list
                participants.append(participant)
            
            # Check if we've got all participants
            page_info = response.get('PageInfo', {})
            total_records = page_info.get('TotalRecords', 0)
            
            self.logger.info(f"Retrieved {len(page_participants)} participants on page {page}")
            
            if not page_participants or len(participants) >= total_records:
                break
                
            page += 1
            
        self.logger.info(f"Retrieved {len(participants)} participants for event {event_id}")
        return participants
    
    def _get_nxt_event_participants(self, event_id):
        """Get all participants for an NXT event.
        
        Args:
            event_id: NXT event ID
            
        Returns:
            List of NXT participants or None if failed
        """
        try:
            participants = []
            limit = 100
            offset = 0
            
            while True:
                response = self.nxt_client.get_event_participants(event_id, limit, offset)
                if not response:
                    self.logger.error(f"Failed to retrieve NXT participants for event {event_id}")
                    return None
                    
                # Check response format
                if not isinstance(response, dict) or 'value' not in response:
                    self.logger.error(f"Invalid response format from NXT participants API: {response}")
                    return None
                    
                page_participants = response['value']
                participants.extend(page_participants)
                
                # Check if we've got all participants
                total_count = response.get('count', 0)
                
                if not page_participants or len(participants) >= total_count:
                    break
                    
                offset += limit
            
            self.logger.info(f"Retrieved {len(participants)} participants for event {event_id}")
            return participants
            
        except Exception as e:
            self.logger.error(f"Error getting NXT event participants: {str(e)}")
            return None
            
    def _get_service_reef_member_details(self, user_id):
        """Get detailed member information from ServiceReef.
        
        Args:
            user_id: ServiceReef user ID
            
        Returns:
            Member details dictionary or None if failed
        """
        try:
            self.logger.info(f"Fetching ServiceReef member details for user {user_id}")
            member_details = self.sr_client.get_member_details(user_id)
            
            if not member_details:
                self.logger.warning(f"No member details found for ServiceReef user {user_id}")
                return None
                
            # Standardize the data format
            std_details = self.mapping_service.standardize_servicereef_participant(member_details)
            
            # Log retrieved fields for debugging
            self.logger.debug(f"Retrieved member details for user {user_id}: " + 
                             f"FirstName={std_details.get('FirstName')}, " +
                             f"LastName={std_details.get('LastName')}, " + 
                             f"Email={std_details.get('Email')}, " +
                             f"Phone={std_details.get('Phone')}")
            
            return std_details
            
        except Exception as e:
            self.logger.error(f"Error getting ServiceReef member details for user {user_id}: {str(e)}")
            return None
    
    def _create_nxt_event(self, sr_event):
        """Create an event in NXT from ServiceReef event data.
        
        Args:
            sr_event: ServiceReef event data
            
        Returns:
            NXT event ID or None if failed
        """
        # Extract required fields
        name = sr_event.get('Name')
        start_date = sr_event.get('StartDate')
        
        if not name or not start_date:
            self.logger.error(f"Missing required fields for NXT event creation: {sr_event}")
            return None
            
        # Create event payload
        event_data = {
            'name': name,
            'start_date': start_date,
            # Add other relevant fields
        }
        
        # Create event in NXT
        response = self.nxt_client.create_event(event_data)
        if not response or 'id' not in response:
            self.logger.error(f"Failed to create NXT event: {response}")
            return None
            
        nxt_event_id = response['id']
        self.logger.info(f"Created NXT event {nxt_event_id} for ServiceReef event {sr_event.get('EventId')}")
        return nxt_event_id
    
    def _create_nxt_constituent(self, sr_participant):
        """Get or create a constituent in NXT from ServiceReef participant data.
        
        This implementation follows the robust multi-step process from the legacy code:
        1. Check mapping file
        2. Verify constituent exists in NXT
        3. Search by email
        4. Search by name
        5. Create new constituent only if all lookups fail
        
        Args:
            sr_participant: ServiceReef participant data
            
        Returns:
            NXT constituent ID or None if failed
        """
        try:
            # First check if we already have a mapping for this constituent
            service_reef_id = str(sr_participant.get('UserId'))
            if not service_reef_id:
                self.logger.warning("No ServiceReef ID found in participant data")
                return None
                
            # Get member details from ServiceReef - we'll need this regardless of path
            # This should include complete details including phone, email, etc.
            sr_member_details = self._get_service_reef_member_details(service_reef_id)
            if not sr_member_details:
                self.logger.error(f"Failed to get member details for ServiceReef ID {service_reef_id}")
                # Fall back to the participant data we have
                sr_member_details = sr_participant
                
            # Ensure we have minimum required fields
            first_name = sr_member_details.get('FirstName') or sr_participant.get('FirstName')
            last_name = sr_member_details.get('LastName') or sr_participant.get('LastName')
            email = sr_member_details.get('Email') or sr_participant.get('Email')
            phone = sr_member_details.get('Phone') or sr_participant.get('Phone')
            
            if not first_name or not last_name:
                self.logger.error(f"Missing required name fields for NXT constituent: {sr_member_details}")
                return None
                
            # Check existing mapping first
            nxt_id = self.mapping_service.get_nxt_constituent_id(service_reef_id)
            if nxt_id:
                self.logger.info(f"Found existing constituent mapping for ServiceReef ID {service_reef_id} -> NXT ID {nxt_id}")
                
                # Verify constituent still exists in NXT
                try:
                    nxt_constituent = self.nxt_client.get_constituent(nxt_id)
                    if nxt_constituent:
                        # Check if constituent needs to be updated
                        self._update_nxt_constituent(nxt_id, first_name, last_name, email, phone)
                        self.logger.info(f"Verified existing constituent {nxt_id}")
                        return nxt_id
                    else:
                        self.logger.warning(f"NXT constituent {nxt_id} no longer exists, will search for matches")
                except Exception as e:
                    self.logger.warning(f"Failed to verify NXT constituent {nxt_id}, will search for matches: {str(e)}")
                
            # Search for existing constituent by email
            if email:
                try:
                    self.logger.info(f"Searching for existing constituent by email: {email}")
                    existing = self.nxt_client.search_constituents(search_text=email)
                    
                    # Log the search result for debugging
                    self.logger.debug(f"Email search result for {email}: {existing}")
                    
                    # Validate the search results
                    if existing and isinstance(existing, (list, tuple)) and len(existing) > 0:
                        # Get the first valid result with an 'id' field
                        for result in existing:
                            if isinstance(result, dict) and 'id' in result:
                                nxt_id = str(result['id'])  # Ensure ID is a string
                                self.logger.info(f"Found existing constituent by email: {nxt_id}")
                                # Update mapping
                                self.mapping_service.add_constituent_mapping(service_reef_id, nxt_id)
                                # Update constituent details if needed
                                self._update_nxt_constituent(nxt_id, first_name, last_name, email, phone)
                                return nxt_id
                        
                        # If we got here, no valid results with 'id' were found
                        self.logger.warning(f"No valid constituent found in search results for email: {email}")
                    else:
                        self.logger.info(f"No existing constituent found with email: {email}")
                        
                except Exception as search_error:
                    self.logger.warning(f"Error searching for constituent by email {email}: {str(search_error)}")
                    # Continue with creation if search fails
                    
            # Search by name as fallback
            if first_name and last_name:
                try:
                    search_name = f"{first_name} {last_name}"
                    self.logger.info(f"Searching for constituent by name: {search_name}")
                    existing = self.nxt_client.search_constituents(search_text=search_name)
                    
                    # Log the search result for debugging
                    self.logger.debug(f"Name search result for {search_name}: {existing}")
                    
                    if existing and isinstance(existing, (list, tuple)) and len(existing) > 0:
                        # Handle the case where multiple constituents are found
                        if len(existing) > 1:
                            self.logger.info(f"Found {len(existing)} constituents with name '{search_name}'")
                            
                            # Filter out any invalid results first
                            valid_results = [r for r in existing if isinstance(r, dict) and 'id' in r]
                            
                            if not valid_results:
                                self.logger.warning("No valid constituents found in search results")
                            else:
                                # If we have an email, try to find a match
                                best_match = None
                                if email:
                                    for constituent in valid_results:
                                        # Handle different possible email field structures
                                        constituent_email = None
                                        if 'email' in constituent and isinstance(constituent['email'], dict):
                                            constituent_email = constituent['email'].get('address', '')
                                        elif 'email' in constituent and isinstance(constituent['email'], str):
                                            constituent_email = constituent['email']
                                            
                                        if email and constituent_email and email.lower() == constituent_email.lower():
                                            best_match = constituent
                                            self.logger.info(f"Found constituent with matching email: {email}")
                                            break
                                
                                # If we found a match by email, use it; otherwise use the first valid result
                                if best_match:
                                    nxt_id = str(best_match['id'])
                                    self.logger.info(f"Selected best constituent match by email verification: {nxt_id}")
                                else:
                                    nxt_id = str(valid_results[0]['id'])
                                    self.logger.info(f"Multiple matches found, using first constituent: {nxt_id}")
                                
                                # Update mapping and return the ID
                                self.mapping_service.add_constituent_mapping(service_reef_id, nxt_id)
                                self._update_nxt_constituent(nxt_id, first_name, last_name, email, phone)
                                return nxt_id
                        else:
                            # Single result found
                            if isinstance(existing[0], dict) and 'id' in existing[0]:
                                nxt_id = str(existing[0]['id'])
                                self.logger.info(f"Found existing constituent by name: {nxt_id}")
                                self.mapping_service.add_constituent_mapping(service_reef_id, nxt_id)
                                self._update_nxt_constituent(nxt_id, first_name, last_name, email, phone)
                                return nxt_id
                            else:
                                self.logger.warning("Invalid constituent data in search results")
                    else:
                        self.logger.info(f"No existing constituent found with name: {search_name}")
                        
                except Exception as name_search_error:
                    self.logger.warning(f"Error searching for constituent by name: {str(name_search_error)}")
                    # Continue with creation if search fails
                    
            # No existing constituent found, create new one
            constituent_data = {
                'type': 'Individual',
                'first': first_name,
                'last': last_name
            }
            
            # Add email if available
            if email:
                constituent_data['email'] = self.mapping_service.create_nxt_email_payload(email)
                
            # Add phone if available
            if phone:
                constituent_data['phone'] = self.mapping_service.create_nxt_phone_payload(phone)
                
            # Create constituent in NXT with enhanced error handling
            try:
                self.logger.info(f"Attempting to create NXT constituent with data: {json.dumps(constituent_data, indent=2)}")
                response = self.nxt_client.create_constituent(constituent_data)
                
                if not response:
                    self.logger.error("Failed to create NXT constituent: Empty response from API")
                    return None
                    
                if 'id' not in response:
                    self.logger.error(f"Failed to create NXT constituent. Response missing 'id' field. Full response: {json.dumps(response, indent=2)}")
                    return None
                    
                nxt_id = str(response['id'])  # Ensure ID is a string
                self.logger.info(f"Successfully created NXT constituent {nxt_id} for ServiceReef user {service_reef_id}")
                
                # Save mapping
                self.mapping_service.add_constituent_mapping(service_reef_id, nxt_id)
                return nxt_id
                
            except Exception as api_error:
                self.logger.error(f"API Error in _create_nxt_constituent: {str(api_error)}")
                self.logger.error(f"Error type: {type(api_error).__name__}")
                if hasattr(api_error, 'response') and hasattr(api_error.response, 'text'):
                    self.logger.error(f"API Response: {api_error.response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Unexpected error in _create_nxt_constituent: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None
            
    def _create_nxt_participant(self, nxt_event_id, nxt_constituent_id, sr_participant):
        """Create a new participant in NXT event.
        
        Args:
            nxt_event_id: NXT event ID
            nxt_constituent_id: NXT constituent ID
            sr_participant: ServiceReef participant data
            
        Returns:
            bool: True if participant was created successfully, False otherwise
        """
        try:
            # Transform ServiceReef participant data to NXT format
            participant_data = self._transform_servicereef_to_nxt_participant(sr_participant, nxt_constituent_id)
            if not participant_data:
                self.logger.error(f"Failed to transform participant data for NXT constituent {nxt_constituent_id}")
                return False
                
            self.logger.info(f"Creating NXT participant for event {nxt_event_id}, constituent {nxt_constituent_id}")
            self.logger.debug(f"NXT participant data: {json.dumps(participant_data, indent=2, default=str)}")
            
            # Create participant in NXT
            response = self.nxt_client.create_event_participant(nxt_event_id, participant_data)
            
            if not response or 'id' not in response:
                self.logger.error(f"Failed to create participant in NXT: {response}")
                return False
                
            self.logger.info(f"Successfully created NXT participant {response['id']} for constituent {nxt_constituent_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating NXT participant: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                self.logger.error(f"API Response: {e.response.text}")
            return False
            
    def _update_nxt_constituent(self, nxt_id, first_name, last_name, email, phone):
        """Update an existing constituent in NXT if ServiceReef data has changed.
        
        This implementation follows the logic from the original event_synchronization_service.py:
        1. Check if any fields need updating (name, email, phone)
        2. Update properties if changed
        3. Create/update email if needed
        4. Create/update phone if needed
        
        Args:
            nxt_id: NXT constituent ID
            first_name: First name from ServiceReef
            last_name: Last name from ServiceReef
            email: Email from ServiceReef
            phone: Phone from ServiceReef
            
        Returns:
            bool: True if any updates were made, False otherwise
        """
        try:
            if not nxt_id:
                self.logger.error("Cannot update constituent: missing NXT ID")
                return False
                
            # Get current constituent data from NXT
            nxt_constituent = self.nxt_client.get_constituent(nxt_id)
            if not nxt_constituent:
                self.logger.error(f"Cannot update constituent {nxt_id}: not found in NXT")
                return False
                
            # Initialize tracking variables
            changed = False
            update_data = {}
            
            # Check if name fields need updating
            current_first = nxt_constituent.get('first', '')
            current_last = nxt_constituent.get('last', '')
            
            if first_name and first_name != current_first:
                update_data['first'] = first_name
                changed = True
                self.logger.info(f"First name change detected for constituent {nxt_id}: '{current_first}' -> '{first_name}'")
                
            if last_name and last_name != current_last:
                update_data['last'] = last_name
                changed = True
                self.logger.info(f"Last name change detected for constituent {nxt_id}: '{current_last}' -> '{last_name}'")
            
            # Handle email update/creation if needed
            if email:
                # Check if existing emails need updating
                existing_emails = self.nxt_client.get_constituent_emails(nxt_id)
                
                if not existing_emails or not existing_emails.get('value'):
                    # No existing emails, create one
                    self.logger.info(f"No existing emails found for constituent {nxt_id}, creating new email")
                    self._create_email_for_constituent(nxt_id, email)
                    changed = True
                else:
                    # Check if the email needs to be updated
                    normalized_email = self.mapping_service.normalize_email(email)
                    email_found = False
                    
                    for existing_email in existing_emails.get('value', []):
                        existing_address = existing_email.get('address', '').lower().strip()
                        if existing_address == normalized_email:
                            email_found = True
                            break
                            
                    if not email_found:
                        self.logger.info(f"Email change detected for constituent {nxt_id}, creating new email")
                        self._create_email_for_constituent(nxt_id, email)
                        changed = True
            
            # Handle phone update/creation if needed
            if phone:
                # Check if existing phones need updating
                existing_phones = self.nxt_client.get_constituent_phones(nxt_id)
                
                if not existing_phones or not existing_phones.get('value'):
                    # No existing phones, create one
                    self.logger.info(f"No existing phones found for constituent {nxt_id}, creating new phone")
                    self._create_phone_for_constituent(nxt_id, phone)
                    changed = True
                else:
                    # Check if the phone needs to be updated
                    formatted_phone = self.mapping_service.format_phone_number(phone)
                    if not formatted_phone:
                        self.logger.warning(f"Could not format phone number '{phone}'")
                    else:
                        phone_found = False
                        
                        for existing_phone in existing_phones.get('value', []):
                            existing_number = existing_phone.get('number', '')
                            # Remove all non-digit characters for comparison
                            existing_digits = ''.join(c for c in existing_number if c.isdigit())
                            
                            if existing_digits == formatted_phone:
                                phone_found = True
                                break
                                
                        if not phone_found:
                            self.logger.info(f"Phone change detected for constituent {nxt_id}, creating new phone")
                            self._create_phone_for_constituent(nxt_id, phone)
                            changed = True
            
            # If no changes detected, skip update
            if not changed:
                self.logger.info(f"No changes detected for NXT constituent {nxt_id}, skipping update")
                return False
                
            # Perform update for non-email/phone fields if any changed
            constituent_updated = False
            if update_data:
                self.logger.info(f"Sending update to NXT for constituent {nxt_id} with payload: {update_data}")
                response = self.nxt_client.update_constituent(nxt_id, update_data)
                
                if response and isinstance(response, dict) and 'id' in response:
                    self.logger.info(f"Successfully updated NXT constituent {nxt_id} properties")
                    constituent_updated = True
                elif response is None:  # None response might mean 204 No Content (success)
                    self.logger.info(f"Successfully updated NXT constituent {nxt_id} properties (no content response)")
                    constituent_updated = True
                else:
                    self.logger.warning(f"Unexpected response when updating constituent {nxt_id}: {response}")
            
            # Return True if either constituent properties or email/phone were successfully updated
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating constituent {nxt_id}: {str(e)}")
            return False
            
    def _create_email_for_constituent(self, constituent_id, email_address):
        """Create a new email for an NXT constituent.
        Only updates or creates email if needed, without deleting existing emails.
        
        Args:
            constituent_id (str): The NXT constituent ID
            email_address (str): The email address to add
            
        Returns:
            bool: True if successful, False if failed
        """
        try:
            # Ensure we have a valid constituent ID and email
            if not constituent_id or not email_address:
                self.logger.error("Cannot create email: missing required parameters")
                return False
                
            # Ensure constituent_id is a string - API requires this
            constituent_id = str(constituent_id).strip()
                
            # Format email for NXT API acceptance
            formatted_email = self.mapping_service.normalize_email(email_address)
            if not formatted_email:
                self.logger.error(f"Email '{email_address}' could not be formatted properly")
                return False
                
            # First check if the constituent exists
            self.logger.info(f"Verifying constituent exists before adding email: {constituent_id}")
            constituent = self.nxt_client.get_constituent(constituent_id)
            if not constituent:
                self.logger.error(f"Cannot create email: constituent {constituent_id} not found in NXT")
                return False
            
            # Check existing email addresses to see if we need to make changes
            existing_emails = self.nxt_client.get_constituent_emails(constituent_id)
            
            # Check if the email already exists and is the same - if so, no need to change
            email_exists = False
            if existing_emails and 'value' in existing_emails and existing_emails['value']:
                for email in existing_emails['value']:
                    if email.get('address', '').lower() == formatted_email.lower():
                        self.logger.info(f"Email {formatted_email} already exists for constituent {constituent_id} - no change needed")
                        email_exists = True
                        return True
            
            # If email doesn't exist already, create a new one (without deleting existing emails)
            if not email_exists:
                # Create payload for new email - all fields required by API documentation
                email_payload = {
                    'constituent_id': constituent_id,  # API requires this as string
                    'address': formatted_email,       # API requires this
                    'type': 'Email',                  # API requires this - must be 'Email' not 'Home'
                    'primary': True,                  # API requires this
                    'inactive': False,                # API requires this
                    'do_not_email': False             # API requires this
                }
                
                # Log detailed request information
                self.logger.info(f"Creating email for constituent {constituent_id} with payload: {email_payload}")
                
                # Make the API call to create the new email
                create_result = self.nxt_client.create_email(email_payload)
                
                if create_result and isinstance(create_result, dict) and 'id' in create_result:
                    self.logger.info(f"Successfully created new email {formatted_email} for constituent {constituent_id}, email ID: {create_result['id']}")
                    return True
                else:
                    # Try to extract more detailed error information
                    error_detail = "Unknown error"
                    if isinstance(create_result, dict):
                        if 'message' in create_result:
                            error_detail = create_result['message']
                        elif 'errors' in create_result:
                            error_detail = str(create_result['errors'])
                    elif isinstance(create_result, str):
                        error_detail = create_result
                    
                    self.logger.error(f"Failed to create email for constituent {constituent_id}: {error_detail}")
                    return False
            
            # No email changes made
            return True
            
        except Exception as e:
            self.logger.error(f"Error in _create_email_for_constituent: {str(e)}")
            return False
    
    def _create_phone_for_constituent(self, constituent_id, phone_number):
        """Create a new phone number for an NXT constituent.
        
        Args:
            constituent_id (str): The NXT constituent ID
            phone_number (str): The phone number to add
            
        Returns:
            bool: True if successful, False if failed
        """
        try:
            # Ensure we have a valid constituent ID
            if not constituent_id or not phone_number:
                self.logger.error("Cannot create phone: missing required parameters")
                return False
                
            # Ensure constituent_id is a string - API requires this
            constituent_id = str(constituent_id).strip()
                
            # Format phone number for NXT API acceptance
            formatted_phone = self.mapping_service.format_phone_number(phone_number)
            
            if not formatted_phone:
                self.logger.error(f"Phone number '{phone_number}' could not be formatted properly")
                return False
                
            # First check if the constituent exists
            self.logger.info(f"Verifying constituent exists before adding phone: {constituent_id}")
            constituent = self.nxt_client.get_constituent(constituent_id)
            if not constituent:
                self.logger.error(f"Cannot create phone: constituent {constituent_id} not found in NXT")
                return False
                
            # Create payload for new phone - all fields required by API documentation
            phone_payload = {
                'constituent_id': constituent_id,  # API requires this as string
                'number': formatted_phone,         # API requires this
                'type': 'Home',                    # API requires this
                'primary': True,                   # Optional but recommended
                'inactive': False,                 # Optional but recommended
                'do_not_call': False              # Optional but recommended
            }
            
            # Log detailed request information
            self.logger.info(f"Creating phone for constituent {constituent_id} with payload: {phone_payload}")
            
            # Create phone using dedicated endpoint
            create_result = self.nxt_client.create_phone(phone_payload)
            
            if create_result and isinstance(create_result, dict) and 'id' in create_result:
                self.logger.info(f"Created new phone {formatted_phone} for constituent {constituent_id}, phone ID: {create_result['id']}")
                return True
            else:
                # Try to extract more detailed error information
                error_detail = "Unknown error"
                if isinstance(create_result, dict):
                    if 'message' in create_result:
                        error_detail = create_result['message']
                    elif 'errors' in create_result:
                        error_detail = str(create_result['errors'])
                elif isinstance(create_result, str):
                    error_detail = create_result
                
                self.logger.error(f"Failed to create phone for constituent {constituent_id}: {error_detail}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error creating phone for constituent {constituent_id}: {str(e)}")
            return False
    
    def _find_nxt_participant(self, nxt_participants, constituent_id):
        """Find a participant in NXT event by constituent ID or other identifying information.
        
        This method attempts to find a participant using multiple strategies:
        1. Direct constituent_id match
        2. lookup_id match (if constituent details can be retrieved)
        3. Email match (if constituent details can be retrieved)
        4. Name match (if constituent details can be retrieved)
        
        Args:
            nxt_participants: List of NXT participants
            constituent_id: NXT constituent ID to find
            
        Returns:
            NXT participant data or None if not found
        """
        if not nxt_participants:
            self.logger.debug("No participants in NXT event to check against")
            return None
            
        # First try direct constituent_id match (most reliable)
        for participant in nxt_participants:
            if participant.get('constituent_id') == constituent_id:
                self.logger.debug(f"Found participant by constituent_id match: {constituent_id}")
                return participant
                
        # Get constituent details to try alternative matching methods
        constituent_details = self.nxt_client.get_constituent(constituent_id)
        if not constituent_details:
            self.logger.warning(f"Could not retrieve constituent details for ID {constituent_id}")
            return None
            
        # Get lookup_id for comparison
        constituent_lookup_id = str(constituent_details.get('lookup_id', '')).strip()
        if constituent_lookup_id:
            for participant in nxt_participants:
                participant_lookup_id = str(participant.get('lookup_id', '')).strip()
                if participant_lookup_id and participant_lookup_id == constituent_lookup_id:
                    self.logger.info(f"Found participant by lookup_id match: {constituent_lookup_id}")
                    return participant
        
        # Try email match
        constituent_email = constituent_details.get('email', {}).get('address', '').lower().strip()
        if constituent_email:
            for participant in nxt_participants:
                participant_email = participant.get('email', '').lower().strip()
                if participant_email and participant_email == constituent_email:
                    self.logger.info(f"Found participant by email match: {constituent_email}")
                    return participant
                    
        # Try name match as last resort
        constituent_first = constituent_details.get('first', '').lower().strip()
        constituent_last = constituent_details.get('last', '').lower().strip()
        constituent_name = f"{constituent_first} {constituent_last}".strip()
        
        if constituent_name:
            for participant in nxt_participants:
                participant_first = participant.get('first_name', '').lower().strip()
                participant_last = participant.get('last_name', '').lower().strip()
                participant_name = f"{participant_first} {participant_last}".strip()
                
                if participant_name and participant_name == constituent_name:
                    self.logger.info(f"Found participant by name match: {constituent_name}")
                    return participant
        
        self.logger.debug(f"No matching participant found for constituent {constituent_id}")
        return None
        
    def _create_nxt_participant(self, event_id, constituent_id, servicereef_participant):
        """Create a new participant in NXT.

        Args:
            event_id: NXT event ID
            constituent_id: NXT constituent ID
            servicereef_participant: ServiceReef participant data

        Returns:
            NXT participant ID or None if failed
        """
        # First verify that the constituent exists in NXT
        constituent = self.nxt_client.get_constituent(constituent_id)

        # Handle error responses or missing constituent
        if isinstance(constituent, dict) and constituent.get('error'):
            self.logger.error(f"Cannot create participant: Constituent {constituent_id} not found or error: {constituent.get('details')}")
            return None

        if not constituent:
            self.logger.error(f"Cannot create participant: Constituent {constituent_id} not found in NXT")
            return None

        # Map ServiceReef status to NXT RSVP
        rsvp_status = self.mapping_service.map_service_reef_status_to_nxt_rsvp(servicereef_participant.get('RegistrationStatus'))

        # Create participant payload
        participant_data = {
            'constituent_id': constituent_id,
            'rsvp_status': rsvp_status,
            'invitation_status': 'NotApplicable'
        }

        # Check if attended flag is present and convert to boolean
        attended = servicereef_participant.get('Attended')
        if attended is not None:
            participant_data['attended'] = attended in [True, 'true', 'True', '1', 1]

        # Create participant in NXT
        response = self.nxt_client.add_participant(event_id, participant_data)

        # Handle detailed error responses
        if isinstance(response, dict) and response.get('error'):
            self.logger.error(f"Failed to create NXT participant: {response}")
            return None
        elif not response:
            self.logger.error(f"Failed to create NXT participant: No response")
            return None

        # Extract participant ID from response
        participant_id = response.get('id')
        self.logger.info(f"Created NXT participant: {participant_id}")

        # Add to participant mapping
        self.mapping_service.add_participant_mapping(servicereef_participant.get('UserId'), participant_id)

        return participant_id
        
    def _update_nxt_participant_status(self, event_id, existing_participant, sr_participant_data):
        """Update a participant's RSVP status in NXT if changed in ServiceReef.

        Args:
            event_id: NXT event ID
            existing_participant: Existing NXT participant data
            sr_participant_data: ServiceReef participant data

        Returns:
            True if update was performed, False if no update needed
        """
        # Get the current RSVP status in NXT
        current_rsvp = existing_participant.get('rsvp_status')
        current_attended = existing_participant.get('attended', False)
        participant_id = existing_participant.get('id')

        if not participant_id:
            self.logger.warning("Cannot update participant status: missing participant ID")
            return False

        # Ensure we have complete ServiceReef participant data
        if not sr_participant_data.get('FirstName') or 'RegistrationStatus' not in sr_participant_data:
            self.logger.warning(f"Incomplete ServiceReef participant data detected for status update")
            return False

        # Get the new RSVP status from ServiceReef
        sr_status = sr_participant_data.get('RegistrationStatus')
        sr_status = sr_participant_data.get('Status', sr_status)  # Fallback to Status
        new_rsvp = self.mapping_service.map_service_reef_status_to_nxt_rsvp(sr_status)

        # Properly format attended as a boolean
        sr_attended = sr_participant_data.get('Attended')
        # Ensure attended is always a proper boolean, not None or any other type
        new_attended = False if sr_attended is None else bool(sr_attended)

        # Check if status has changed
        status_changed = current_rsvp != new_rsvp or current_attended != new_attended

        if status_changed:
            self.logger.info(f"Status change detected: '{current_rsvp}' -> '{new_rsvp}' for participant {participant_id}")

            # Prepare update payload - only include rsvp_status
            # According to API errors, the attended field is causing issues
            update_payload = {
                'rsvp_status': new_rsvp
            }

            # Log the payload for debugging
            self.logger.info(f"Update payload for participant {participant_id}: {update_payload}")

            # Update participant in NXT
            response = self.nxt_client.update_participant(participant_id, update_payload)

            # Handle error responses with detailed info
            if isinstance(response, dict) and response.get('error'):
                error_details = response.get('details')
                self.logger.error(f"Failed to update participant status: {participant_id} - Error: {error_details}")

                # If the payload format is causing the error, try a different format
                # The NXT API might be picky about the fields it accepts
                if response.get('status_code') == 400:
                    self.logger.info(f"Attempting alternate update format for participant {participant_id}")

                    # Try simpler payload without any extra fields
                    simple_payload = {
                        'rsvp_status': new_rsvp
                    }

                    # Log the retry attempt with simple payload
                    self.logger.info(f"Retrying with simplified payload: {simple_payload}")
                    retry_response = self.nxt_client.update_participant(participant_id, simple_payload)

                    if not isinstance(retry_response, dict) or not retry_response.get('error'):
                        self.logger.info(f"Successfully updated participant {participant_id} status to {new_rsvp} with simplified payload")
                        return True
                    else:
                        self.logger.error(f"Simplified payload also failed: {retry_response.get('details')}")

                return False
            elif not response:
                self.logger.error(f"Failed to update NXT participant status: {participant_id} (no response)")
                return False

            self.logger.info(f"Successfully updated participant {participant_id} status to {new_rsvp}")
            return True

        return False


# Main execution
if __name__ == "__main__":
    try:
        orchestrator = SyncOrchestrator()
        orchestrator.sync_all_events()
        
        print("Sync complete - check sync.log for details")
    except Exception as e:
        logging.exception(f"Sync failed: {str(e)}")
        print(f"Sync failed: {str(e)} - check sync.log for details")
