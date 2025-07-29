"""
Configuration module for event synchronization service.
Centralizes settings and environment variables.
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(dotenv_path)

class Config:
    """Central configuration for the sync application."""
    
    def __init__(self):
        """Initialize configuration from environment variables and defaults."""
        self.base_dir = Path(__file__).parent
        
        # API endpoints and credentials
        self.service_reef = {
            'base_url': os.getenv('SERVICE_REEF_BASE_URL'),
            'client_id': os.getenv('SERVICE_REEF_CLIENT_ID'),
            'client_secret': os.getenv('SERVICE_REEF_CLIENT_SECRET'),
            'token_endpoint': os.getenv('SERVICE_REEF_TOKEN_ENDPOINT')
        }
        
        self.nxt = {
            'base_url': os.getenv('NXT_BASE_URL', 'https://api.sky.blackbaud.com'),
            'client_id': os.getenv('NXT_CLIENT_ID'),
            'client_secret': os.getenv('NXT_CLIENT_SECRET'),
            'subscription_key': os.getenv('NXT_SUBSCRIPTION_KEY')
        }
        
        # File paths
        self.paths = {
            'event_mapping': self.base_dir / 'data' / 'event_mapping.json',
            'constituent_mapping': self.base_dir / 'data' / 'constituent_mapping.json',
            'fund_mapping': self.base_dir / 'data' / 'fund_mappings.json',
            'sr_token': Path(os.getenv('SR_TOKEN_FILE', self.base_dir / 'tokens' / 'servicereef_token.json')),
            'nxt_token': Path(os.getenv('NXT_TOKEN_FILE', 
                               self.base_dir.parent / 'ServiceReefAPI' / 'tokens' / 'blackbaud_token.json'))
        }
        
        # Load fund mappings
        self.fund_config = {}
        fund_mapping_path = self.paths['fund_mapping']
        if fund_mapping_path.exists():
            try:
                with open(fund_mapping_path, 'r') as f:
                    self.fund_config = json.load(f)
                print(f"Loaded fund mappings from {fund_mapping_path}")
            except Exception as e:
                print(f"Error loading fund mappings: {e}")
        
        # API settings
        self.api = {
            'page_size': 100,
            'retry_delay': 2,
            'max_retries': 3
        }
        
        # Status mappings
        self.status_mappings = {
            'approved': 'Attending',
            'registered': 'Attending',
            'waitingapproval': 'Attending',  # Updated mapping
            'declined': 'Declined',
            'cancelled': 'Declined',
            'draft': 'Declined',
            'unknown': 'NoResponse',
            '': 'NoResponse'
        }
        
    def get(self, key, default=None):
        """Get configuration value by key path.
        
        Args:
            key: Key path (e.g., 'fund_config.default_nxt_fund_id')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self.__dict__
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
                
        return value
    
    def validate(self):
        """Validate that all required configuration is present."""
        missing = []
        
        # Check ServiceReef credentials
        for key in ['base_url', 'client_id', 'client_secret']:
            if not self.service_reef.get(key):
                missing.append(f"SERVICE_REEF_{key.upper()}")
                
        # Check NXT credentials
        for key in ['base_url', 'client_id', 'client_secret', 'subscription_key']:
            if not self.nxt.get(key):
                missing.append(f"NXT_{key.upper()}")
                
        # Check if mapping directories exist
        mapping_dir = self.base_dir / 'data'
        if not mapping_dir.exists():
            missing.append("data directory")
            
        # Check fund configuration
        if not self.fund_config.get('default_nxt_fund_id'):
            missing.append("default_nxt_fund_id in fund_mappings.json")
            
        if missing:
            return False, f"Missing required configuration: {', '.join(missing)}"
        return True, "Configuration valid"
