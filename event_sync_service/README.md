# Event Synchronization Service

This Python service synchronizes events between ServiceReef and NXT platforms. It handles event creation, participant synchronization, and maintains mappings between the two systems.

## Requirements

- Python 3.13.5 or higher
- Required packages are listed in `requirements.txt`
- Blackbaud NXT API access (requires OAuth setup)
- ServiceReef API access

## Setup

1. Install the required packages:
```bash
pip install -r requirements.txt
```

2. Set up Blackbaud NXT API access:
   1. Go to https://developer.blackbaud.com/apps/
   2. Create a new application or select your existing one
   3. Set up OAuth 2.0 with the following:
      - Redirect URI: Your callback URL (e.g., https://your-domain.com/callback)
      - Required scopes: `event.read event.write constituent.read constituent.write`
   4. Note your application's:
      - Client ID
      - Client Secret
      - Subscription Key
   5. Use the OAuth 2.0 Authorization Code Flow to get an access token:
      - Direct user to: `https://oauth2.sky.blackbaud.com/authorization?client_id=<your-client-id>&response_type=code&redirect_uri=<your-redirect-uri>&scope=event.read+event.write+constituent.read+constituent.write`
      - User logs in and authorizes
      - Get code from redirect URL
      - Exchange code for token using your client credentials

3. Create a `.env` file in the root directory with the following variables:
```
# ServiceReef API credentials
SERVICE_REEF_BASE_URL=https://api.servicereef.com
SERVICE_REEF_APP_ID=your_app_id
SERVICE_REEF_APP_SECRET=your_app_secret

# Blackbaud NXT API credentials
NXT_CLIENT_ID=your_nxt_client_id
NXT_CLIENT_SECRET=your_nxt_client_secret
NXT_SUBSCRIPTION_KEY=your_subscription_key
NXT_ACCESS_TOKEN=your_access_token  # Required - obtained through OAuth flow
NXT_REDIRECT_URI=your_redirect_uri
```

4. Ensure the following directories exist (they will be created automatically if missing):
   - `logs/` - For log files
   - `data/` - For mapping files
   - `tokens/` - For token storage

## Running the Service

Simply run the Python script:
```bash
python event_synchronization_service.py
```

The service will:
1. Load environment variables
2. Set up logging
3. Initialize token services for both platforms
4. Sync events from ServiceReef to NXT
5. Sync participants for each event

## Token Management

### ServiceReef
- Uses OAuth2 client credentials flow
- Tokens are automatically refreshed when expired
- Tokens are cached in the `tokens/` directory

### Blackbaud NXT
- Uses OAuth2 authorization code flow via ServiceReefAPI
- Token management:
  1. Go to https://erportal.back2back.org/ServiceReefAPI/
  2. Use the NXT token management interface to get a new token
  3. Add the token to your `.env` file as `NXT_ACCESS_TOKEN`
  4. When the token expires, repeat steps 1-3 to get a new token

## Features

- Token management with automatic refresh for ServiceReef
- Manual token management for Blackbaud NXT (due to OAuth requirements)
- Event synchronization with mapping storage
- Participant synchronization with constituent mapping
- Participant synchronization with constituent creation
- Detailed logging
- Error handling and retries
- Mapping cache for constituents

## Logging

Logs are written to `logs/event_sync.log`. The log file contains detailed information about:
- Token operations
- Event creation and updates
- Participant synchronization
- Errors and warnings

## Data Storage

The service maintains two JSON files in the `data/` directory:
- `event_mapping.json`: Maps ServiceReef event IDs to NXT event IDs
- `constituent_mapping.json`: Maps ServiceReef member IDs to NXT constituent IDs
