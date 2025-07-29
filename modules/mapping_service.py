"""
Mapping service for translating data between ServiceReef and NXT formats.
Handles all data transformations, normalizations, and standardizations.
"""
import logging
import json
from pathlib import Path

class MappingService:
    """Service for mapping and transforming data between systems."""
    
    def __init__(self, config):
        """Initialize mapping service with configuration.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.logger = logging.getLogger('MappingService')
        self.event_mapping = {}
        self.constituent_mapping = {}
        
        # Load existing mappings
        self._load_mappings()
        
    def _load_mappings(self):
        """Load existing mappings from files."""
        self._load_event_mapping()
        self._load_constituent_mapping()
    
    def _load_event_mapping(self):
        """Load event mapping from file."""
        mapping_path = self.config.paths['event_mapping']
        self.logger.info(f"Checking event mapping file at: {mapping_path}")
        
        if Path(mapping_path).exists():
            self.logger.info("Loading existing event mapping file")
            try:
                with open(mapping_path, 'r') as f:
                    self.event_mapping = json.load(f)
                self.logger.debug(f"Loaded {len(self.event_mapping)} event mappings")
            except Exception as e:
                self.logger.error(f"Error loading event mapping file: {e}")
                self.event_mapping = {}
        else:
            self.logger.info("No event mapping file found, creating new mapping")
            self.event_mapping = {}
            
            # Create directory if it doesn't exist
            mapping_dir = Path(mapping_path).parent
            if not mapping_dir.exists():
                mapping_dir.mkdir(parents=True)
                
            # Create empty mapping file
            self._save_event_mapping()
    
    def _save_event_mapping(self):
        """Save event mapping to file."""
        mapping_path = self.config.paths['event_mapping']
        try:
            with open(mapping_path, 'w') as f:
                json.dump(self.event_mapping, f, indent=2)
            self.logger.info(f"Saved {len(self.event_mapping)} event mappings")
        except Exception as e:
            self.logger.error(f"Error saving event mapping file: {e}")
    
    def _load_constituent_mapping(self):
        """Load constituent mapping from file."""
        mapping_path = self.config.paths['constituent_mapping']
        self.logger.info(f"Checking constituent mapping file at: {mapping_path}")
        
        if Path(mapping_path).exists():
            self.logger.info("Loading existing constituent mapping file")
            try:
                with open(mapping_path, 'r') as f:
                    self.constituent_mapping = json.load(f)
                self.logger.debug(f"Loaded {len(self.constituent_mapping)} constituent mappings")
            except Exception as e:
                self.logger.error(f"Error loading constituent mapping file: {e}")
                self.constituent_mapping = {}
        else:
            self.logger.info("No constituent mapping file found, creating new mapping")
            self.constituent_mapping = {}
            
            # Create directory if it doesn't exist
            mapping_dir = Path(mapping_path).parent
            if not mapping_dir.exists():
                mapping_dir.mkdir(parents=True)
                
            # Create empty mapping file
            self._save_constituent_mapping()
    
    def _save_constituent_mapping(self):
        """Save constituent mapping to file."""
        mapping_path = self.config.paths['constituent_mapping']
        try:
            with open(mapping_path, 'w') as f:
                json.dump(self.constituent_mapping, f, indent=2)
            self.logger.info(f"Saved {len(self.constituent_mapping)} constituent mappings")
        except Exception as e:
            self.logger.error(f"Error saving constituent mapping file: {e}")
    
    def map_service_reef_status_to_nxt_rsvp(self, status):
        """Map ServiceReef status to NXT RSVP status.
        
        Args:
            status: ServiceReef status value
            
        Returns:
            Corresponding NXT RSVP status
        """
        if not status:
            return 'NoResponse'
            
        # Normalize status for consistent comparison
        normalized = status.lower().strip()
        return self.config.status_mappings.get(normalized, 'NoResponse')
    
    def standardize_servicereef_participant(self, participant_data):
        """Standardize ServiceReef participant data format.
        
        Args:
            participant_data: Raw ServiceReef participant data
            
        Returns:
            Standardized participant data
        """
        if not participant_data:
            return {}
            
        # Create a copy to avoid modifying the original
        std_data = dict(participant_data)
        
        # Ensure consistent ID field
        if 'UserId' not in std_data and 'Id' in std_data:
            std_data['UserId'] = std_data['Id']
            
        # Ensure consistent status field - prioritize 'RegistrationStatus'
        if 'Status' not in std_data and 'RegistrationStatus' in std_data:
            std_data['Status'] = std_data['RegistrationStatus']
        elif 'Status' not in std_data:
            std_data['Status'] = 'Unknown'
            
        # Ensure consistent name fields
        if 'FirstName' not in std_data and 'First' in std_data:
            std_data['FirstName'] = std_data['First']
        if 'LastName' not in std_data and 'Last' in std_data:
            std_data['LastName'] = std_data['Last']
            
        # Ensure consistent email field
        if 'Email' not in std_data and 'EmailAddress' in std_data:
            std_data['Email'] = std_data['EmailAddress']
            
        return std_data
    
    def transform_servicereef_to_nxt_participant(self, participant_data, constituent_id):
        """Transform ServiceReef participant to NXT format.
        
        Args:
            participant_data: ServiceReef participant data
            constituent_id: NXT constituent ID
            
        Returns:
            NXT formatted participant data
        """
        # First standardize the data to ensure consistent field access
        std_data = self.standardize_servicereef_participant(participant_data)
        
        # Get status - we now have confidence that 'Status' exists
        status = std_data.get('Status', 'Unknown')
        
        # Map status to NXT RSVP status
        rsvp_status = self.map_service_reef_status_to_nxt_rsvp(status)
        
        # Build NXT participant payload
        nxt_participant = {
            'constituent_id': constituent_id,
            'rsvp_status': rsvp_status,
            'invitation_status': 'Invited',
            'attended': std_data.get('Attended', False)
        }
        
        # Add host_id if participant is a guest
        host_id = std_data.get('HostId')
        if host_id:
            nxt_participant['host_id'] = host_id
            
        self.logger.debug(f'Transformed NXT payload: {json.dumps(nxt_participant, indent=2)}')
        return nxt_participant
    
    def normalize_email(self, email):
        """Normalize email for consistent comparison.
        
        Args:
            email: Email address
            
        Returns:
            Normalized email address
        """
        if not email:
            return ""
            
        return email.lower().strip()
    
    def format_phone_number(self, phone_number):
        """Format phone number for API compatibility.
        
        Args:
            phone_number: Raw phone number
            
        Returns:
            Formatted phone number
        """
        if not phone_number:
            return None
            
        # Remove all non-digit characters
        digits_only = ''.join(c for c in phone_number if c.isdigit())
        
        # Check if we have a valid number of digits
        if len(digits_only) < 7:
            self.logger.warning(f"Phone number too short: {phone_number}")
            return None
            
        # For test data with repeated digits, make it more realistic
        if all(c == digits_only[0] for c in digits_only[:3]) and all(c == digits_only[3] for c in digits_only[3:6]):
            self.logger.info(f"Converting test phone {phone_number} to realistic format")
            return "555123" + digits_only[-4:]
        
        return digits_only
    
    def create_nxt_email_payload(self, email):
        """Create NXT email payload.
        
        Args:
            email: Email address
            
        Returns:
            NXT email payload
        """
        if not email:
            return None
            
        return {
            'address': email,
            'type': 'Email',  # Changed from 'Home' to 'Email'
            'primary': True,
            'do_not_email': False
        }
    
    def create_nxt_phone_payload(self, phone):
        """Create NXT phone payload.
        
        Args:
            phone: Phone number
            
        Returns:
            NXT phone payload
        """
        formatted_phone = self.format_phone_number(phone)
        if not formatted_phone:
            return None
            
        return {
            'number': formatted_phone,
            'type': 'Home',
            'primary': True,
            'do_not_call': False
        }
    
    def add_event_mapping(self, sr_event_id, nxt_event_id):
        """Add event mapping and save to file.
        
        Args:
            sr_event_id: ServiceReef event ID
            nxt_event_id: NXT event ID
        """
        self.event_mapping[str(sr_event_id)] = nxt_event_id
        self._save_event_mapping()
    
    def add_constituent_mapping(self, sr_user_id, nxt_constituent_id):
        """Add constituent mapping and save to file.
        
        Args:
            sr_user_id: ServiceReef user ID
            nxt_constituent_id: NXT constituent ID
        """
        self.constituent_mapping[str(sr_user_id)] = nxt_constituent_id
        self._save_constituent_mapping()
    
    def get_nxt_event_id(self, sr_event_id):
        """Get NXT event ID for ServiceReef event.
        
        Args:
            sr_event_id: ServiceReef event ID
            
        Returns:
            NXT event ID or None if not found
        """
        return self.event_mapping.get(str(sr_event_id))
    
    def get_nxt_constituent_id(self, sr_user_id):
        """Get NXT constituent ID for ServiceReef user.
        
        Args:
            sr_user_id: ServiceReef user ID
            
        Returns:
            NXT constituent ID or None if not found
        """
        return self.constituent_mapping.get(str(sr_user_id))
    
    def get_sr_event_id(self, nxt_event_id):
        """Get ServiceReef event ID for NXT event using reverse lookup.
        
        Args:
            nxt_event_id: NXT event ID
            
        Returns:
            ServiceReef event ID or None if not found
        """
        for sr_id, nxt_id in self.event_mapping.items():
            if str(nxt_id) == str(nxt_event_id):
                return sr_id
        return None
    
    def get_sr_user_id(self, nxt_constituent_id):
        """Get ServiceReef user ID for NXT constituent using reverse lookup.
        
        Args:
            nxt_constituent_id: NXT constituent ID
            
        Returns:
            ServiceReef user ID or None if not found
        """
        for sr_id, nxt_id in self.constituent_mapping.items():
            if str(nxt_id) == str(nxt_constituent_id):
                return sr_id
        return None
