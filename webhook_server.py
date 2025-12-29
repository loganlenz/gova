#!/usr/bin/env python3
"""
HubSpot Webhook Sync Server (Simplified)

Automatically syncs contacts to a partner HubSpot portal when:
- A contact submits a form
- A contact is created
- A contact is updated

Environment Variables Required:
    HUBSPOT_SOURCE_TOKEN    - Your HubSpot private app token
    HUBSPOT_DEST_TOKEN      - Partner's HubSpot access token
"""

import os
import json
import logging
import time
from datetime import datetime

import requests
from flask import Flask, request, jsonify

# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """App configuration from environment variables."""
    
    # HubSpot tokens
    SOURCE_TOKEN = os.environ.get("HUBSPOT_SOURCE_TOKEN", "")
    DEST_TOKEN = os.environ.get("HUBSPOT_DEST_TOKEN", "")
    
    # Server settings
    PORT = int(os.environ.get("PORT", 8080))
    
    # Sync settings - customize these as needed
    PROPERTIES_TO_SYNC = [
        "email", "firstname", "lastname", "phone", "company",
        "jobtitle", "address", "city", "state", "zip", "country",
        "website", "lifecyclestage", "hs_lead_status"
    ]


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# =============================================================================
# FLASK APP
# =============================================================================

app = Flask(__name__)


# =============================================================================
# HUBSPOT API CLIENT
# =============================================================================

class HubSpotAPI:
    """Simple HubSpot API client."""
    
    BASE_URL = "https://api.hubapi.com"
    
    def __init__(self, access_token: str):
        self.token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    def _request(self, method: str, endpoint: str, data: dict = None, retries: int = 3) -> dict:
        """Make API request with retry logic."""
        url = f"{self.BASE_URL}{endpoint}"
        
        for attempt in range(retries):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    json=data,
                    timeout=30
                )
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    logger.warning(f"Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.json() if response.text else {}
                
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise
        
        return {}
    
    def get_contact(self, contact_id: str, properties: list) -> dict:
        """Get contact by ID."""
        props = ",".join(properties)
        return self._request("GET", f"/crm/v3/objects/contacts/{contact_id}?properties={props}")
    
    def search_by_email(self, email: str) -> dict:
        """Find contact by email."""
        data = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email
                }]
            }]
        }
        result = self._request("POST", "/crm/v3/objects/contacts/search", data)
        results = result.get("results", [])
        return results[0] if results else None
    
    def create_contact(self, properties: dict) -> dict:
        """Create a new contact."""
        return self._request("POST", "/crm/v3/objects/contacts", {"properties": properties})
    
    def update_contact(self, contact_id: str, properties: dict) -> dict:
        """Update existing contact."""
        return self._request("PATCH", f"/crm/v3/objects/contacts/{contact_id}", {"properties": properties})


# =============================================================================
# SYNC LOGIC
# =============================================================================

def sync_contact_to_partner(contact_id: str, event_type: str = "unknown") -> dict:
    """Sync a single contact to the partner portal."""
    
    result = {
        "contact_id": contact_id,
        "event_type": event_type,
        "status": "unknown",
        "message": ""
    }
    
    try:
        source = HubSpotAPI(Config.SOURCE_TOKEN)
        contact = source.get_contact(contact_id, Config.PROPERTIES_TO_SYNC)
        
        if not contact:
            result["status"] = "error"
            result["message"] = "Contact not found in source portal"
            return result
        
        props = contact.get("properties", {})
        email = props.get("email")
        
        if not email:
            result["status"] = "skipped"
            result["message"] = "Contact has no email"
            return result
        
        sync_props = {
            k: v for k, v in props.items() 
            if k in Config.PROPERTIES_TO_SYNC and v
        }
        
        dest = HubSpotAPI(Config.DEST_TOKEN)
        existing = dest.search_by_email(email)
        
        if existing:
            dest.update_contact(existing["id"], sync_props)
            result["status"] = "updated"
            result["message"] = f"Updated contact: {email}"
        else:
            dest.create_contact(sync_props)
            result["status"] = "created"
            result["message"] = f"Created contact: {email}"
        
        logger.info(f"[{event_type}] {result['message']}")
        
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)
        logger.error(f"[{event_type}] Error syncing contact {contact_id}: {e}")
    
    return result


# =============================================================================
# ROUTES
# =============================================================================

@app.route("/")
def index():
    """Health check and info page."""
    return jsonify({
        "service": "HubSpot Webhook Sync",
        "status": "running",
        "endpoints": {
            "webhooks": "/webhooks/hubspot",
            "health": "/health",
            "test_sync": "/test/sync/<contact_id>"
        },
        "config": {
            "source_token_set": bool(Config.SOURCE_TOKEN),
            "dest_token_set": bool(Config.DEST_TOKEN),
            "properties": Config.PROPERTIES_TO_SYNC
        }
    })


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/webhooks/hubspot", methods=["POST"])
def hubspot_webhook():
    """
    Handle HubSpot webhook events.
    
    NOTE: Signature verification is disabled for simplicity.
    For production, you should enable it for security.
    """
    
    try:
        events = request.json
        logger.info(f"Webhook received: {json.dumps(events)[:200]}")
        
        if not isinstance(events, list):
            events = [events]
        
        results = []
        
        for event in events:
            subscription_type = event.get("subscriptionType", "")
            object_id = str(event.get("objectId", ""))
            
            logger.info(f"Processing: {subscription_type} for contact {object_id}")
            
            if subscription_type in [
                "contact.creation",
                "contact.propertyChange",
            ]:
                result = sync_contact_to_partner(object_id, subscription_type)
                results.append(result)
            else:
                logger.debug(f"Ignoring event type: {subscription_type}")
        
        return jsonify({
            "received": len(events),
            "processed": len(results),
            "results": results
        })
        
    except Exception as e:
        logger.error(f"Webhook processing error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/test/sync/<contact_id>", methods=["GET", "POST"])
def test_sync(contact_id):
    """Manually trigger a sync for testing. Works with GET or POST."""
    if not Config.SOURCE_TOKEN or not Config.DEST_TOKEN:
        return jsonify({"error": "Tokens not configured"}), 500
    
    result = sync_contact_to_partner(contact_id, "manual_test")
    return jsonify(result)


@app.route("/test/connection", methods=["GET"])
def test_connection():
    """Test connections to both HubSpot portals."""
    results = {}
    
    try:
        source = HubSpotAPI(Config.SOURCE_TOKEN)
        source._request("GET", "/crm/v3/objects/contacts?limit=1")
        results["source"] = {"status": "connected"}
    except Exception as e:
        results["source"] = {"status": "error", "message": str(e)}
    
    try:
        dest = HubSpotAPI(Config.DEST_TOKEN)
        dest._request("GET", "/crm/v3/objects/contacts?limit=1")
        results["destination"] = {"status": "connected"}
    except Exception as e:
        results["destination"] = {"status": "error", "message": str(e)}
    
    return jsonify(results)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    if not Config.SOURCE_TOKEN:
        logger.warning("HUBSPOT_SOURCE_TOKEN not set")
    if not Config.DEST_TOKEN:
        logger.warning("HUBSPOT_DEST_TOKEN not set")
    
    logger.info(f"Starting server on port {Config.PORT}")
    app.run(host="0.0.0.0", port=Config.PORT)
