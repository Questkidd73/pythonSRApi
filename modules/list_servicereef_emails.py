#!/usr/bin/env python3
"""
Script to list all email addresses from ServiceReef for debugging purposes.
"""

import logging
import sys
import json
from event_synchronization_service import EventSyncService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('ServiceReefEmails')

def list_servicereef_emails():
    """List all email addresses from ServiceReef participants."""
    
    # Create the sync service
    sync_service = EventSyncService()
    
    # Get all ServiceReef events
    logger.info("Fetching events from ServiceReef...")
    events_response = sync_service._handle_service_reef_request('GET', '/v1/events')
    
    # Debug output - simplified version
    logger.info(f"ServiceReef API response type: {type(events_response)}")
    logger.info(f"Found {len(events_response)} ServiceReef events")
    
    # Display just the event names and IDs for reference
    for i, event in enumerate(events_response[:5], 1):
        logger.info(f"Event {i}: {event.get('Name', 'Unknown')} (ID: {event.get('EventId', 'Unknown')})")
    
    if len(events_response) > 5:
        logger.info(f"...and {len(events_response) - 5} more events")
    
    
    if not events_response:
        logger.error("Failed to get ServiceReef events")
        return
    
    # Process each event to get participants and their emails
    logger.info(f"Found {len(events_response)} ServiceReef events")
    
    # Events appear to be directly in the response list, not in a 'Results' key
    events = events_response
    
    emails_by_event = {}
    for event in events:
        event_id = event.get('EventId')
        event_name = event.get('Name', 'Unknown Event Name')
        
        if not event_id:
            continue
            
        logger.info(f"Processing event {event_id}: {event_name}")
        
        # Get participants for this event
        logger.info(f"Retrieving participants for event {event_id}")
        participants = sync_service._handle_service_reef_request('GET', f'/v1/events/{event_id}/participants')
        
        # Debug participant response
        logger.info(f"Participant response type: {type(participants)}")
        if isinstance(participants, dict) and 'PageInfo' in participants:
            logger.info(f"Participant PageInfo: {participants['PageInfo']}")
        else:
            logger.info(f"Direct participant list length: {len(participants) if isinstance(participants, list) else 'Not a list'}")
        
        participant_list = []
        if isinstance(participants, dict) and 'Results' in participants:
            participant_list = participants['Results']
        elif isinstance(participants, list):
            participant_list = participants
        
        if not participant_list:
            logger.warning(f"No participants found for event {event_id}")
            continue
            
        logger.info(f"Found {len(participant_list)} participants for event {event_id}")
        
        # Debug first participant to see structure
        if participant_list:
            logger.info(f"Sample participant data: {json.dumps(participant_list[0], indent=2)[:200]}...")

            
        # Extract emails from participants
        event_emails = []
        for participant in participant_list:
            email = participant.get('Email')
            user_id = participant.get('UserId')
            first_name = participant.get('FirstName', 'Unknown')
            last_name = participant.get('LastName', 'Unknown')
            status = participant.get('RegistrationStatus', 'Unknown')
            
            if email:
                email_data = {
                    'email': email,
                    'user_id': user_id,
                    'name': f"{first_name} {last_name}",
                    'status': status
                }
                event_emails.append(email_data)
                logger.info(f"Found email: {email} for {first_name} {last_name} (Status: {status})")
            else:
                logger.warning(f"No email found for participant {user_id}: {first_name} {last_name}")
                
        # Store emails for this event
        if event_emails:
            emails_by_event[f"{event_id} - {event_name}"] = event_emails
    
    # Display results
    logger.info("\n\n=== SERVICEREEF EMAILS BY EVENT ===\n")
    
    print("\n=== SERVICEREEF EMAILS BY EVENT ===\n")
    
    email_counter = 0
    for event_name, emails in emails_by_event.items():
        if emails:
            print(f"\nEvent: {event_name}")
            print("-" * 60)
            
            for email_data in emails:
                email_counter += 1
                print(f"{email_counter:3d}. Email: {email_data['email']}")
                print(f"     Name: {email_data['name']}")
                print(f"     Status: {email_data['status']}")
                print(f"     User ID: {email_data['user_id']}")
                print()
    
    print(f"\nTotal unique participants with emails: {email_counter}")

if __name__ == "__main__":
    list_servicereef_emails()
