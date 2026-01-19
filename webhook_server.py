#!/usr/bin/env python3
"""
HubSpot Webhook Sync Server

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
    
    # Minimal safe properties that should exist in any HubSpot portal
    SAFE_PROPERTIES = ["email", "firstname", "lastname", "phone", "address", "city", "state", "zip"]
    
    # Full list of properties to sync (from form mapping)
    ALL_PROPERTIES = [
        # Standard fields
        "email", "firstname", "lastname", "phone",
        "address", "city", "state", "zip",
        # Military fields
        "military_rank",
        # Date fields
        "verify_date_of_birth",
        "edge_sso_sign_up",
        # Membership & scores
        "edge_xp_earned__all_time_",
        "edge_initial_financial_assessment_completed_date",
        "stress_score_total",
        "stress_score_completed_date",
        # Opt-out
        "opted_out_of_communications_afm"
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
    
    def _request(self, method: str, endpoint: str, data: dict = None) -> tuple:
        """Make API request. Returns (success, response_or_error)."""
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                timeout=30
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logger.warning(f"Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._request(method, endpoint, data)
            
            if response.status_code >= 400:
                error_msg = response.text[:500]
                return False, f"{response.status_code}: {error_msg}"
            
            return True, response.json() if response.text else {}
            
        except Exception as e:
            return False, str(e)
    
    def get_contact(self, contact_id: str, properties: list) -> tuple:
        """Get contact by ID."""
        props = ",".join(properties)
        return self._request("GET", f"/crm/v3/objects/contacts/{contact_id}?properties={props}")
    
    def search_by_email(self, email: str) -> tuple:
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
        success, result = self._request("POST", "/crm/v3/objects/contacts/search", data)
        if success:
            results = result.get("results", [])
            return True, results[0] if results else None
        return False, result
    
    def create_contact(self, properties: dict) -> tuple:
        """Create a new contact."""
        return self._request("POST", "/crm/v3/objects/contacts", {"properties": properties})
    
    def update_contact(self, contact_id: str, properties: dict) -> tuple:
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
        # Get contact from source
        source = HubSpotAPI(Config.SOURCE_TOKEN)
        success, contact = source.get_contact(contact_id, Config.ALL_PROPERTIES)
        
        if not success:
            result["status"] = "error"
            result["message"] = f"Failed to get contact from source: {contact}"
            logger.error(f"[{event_type}] {result['message']}")
            return result
        
        props = contact.get("properties", {})
        email = props.get("email")
        
        if not email:
            result["status"] = "skipped"
            result["message"] = "Contact has no email"
            logger.info(f"[{event_type}] Skipped contact {contact_id} - no email")
            return result
        
        # Check if contact exists in destination
        dest = HubSpotAPI(Config.DEST_TOKEN)
        success, existing = dest.search_by_email(email)
        
        if not success:
            result["status"] = "error"
            result["message"] = f"Failed to search destination: {existing}"
            logger.error(f"[{event_type}] {result['message']}")
            return result
        
        # Try full properties first
        full_props = {k: v for k, v in props.items() if k in Config.ALL_PROPERTIES and v}
        logger.info(f"Attempting sync with properties: {list(full_props.keys())}")
        
        if existing:
            success, response = dest.update_contact(existing["id"], full_props)
        else:
            success, response = dest.create_contact(full_props)
        
        if success:
            action = "Updated" if existing else "Created"
            result["status"] = "updated" if existing else "created"
            result["message"] = f"{action} contact: {email}"
            logger.info(f"[{event_type}] {result['message']}")
            return result
        
        # If full properties failed, try with safe/minimal properties
        logger.warning(f"Full sync failed: {response}")
        logger.info("Retrying with minimal properties...")
        
        safe_props = {k: v for k, v in props.items() if k in Config.SAFE_PROPERTIES and v}
        logger.info(f"Using minimal properties: {list(safe_props.keys())}")
        
        if existing:
            success, response = dest.update_contact(existing["id"], safe_props)
        else:
            success, response = dest.create_contact(safe_props)
        
        if success:
            action = "Updated" if existing else "Created"
            result["status"] = "updated" if existing else "created"
            result["message"] = f"{action} contact (minimal): {email}"
            logger.info(f"[{event_type}] {result['message']}")
        else:
            result["status"] = "error"
            result["message"] = f"Failed even with minimal properties: {response}"
            logger.error(f"[{event_type}] {result['message']}")
        
        return result
        
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)
        logger.error(f"[{event_type}] Exception syncing contact {contact_id}: {e}")
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
            "properties": Config.ALL_PROPERTIES,
            "safe_properties": Config.SAFE_PROPERTIES
        }
    })


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/webhooks/hubspot", methods=["POST"])
def hubspot_webhook():
    """Handle HubSpot webhook events."""
    
    try:
        events = request.json
        logger.info(f"Webhook received with {len(events) if isinstance(events, list) else 1} event(s)")
        
        if not isinstance(events, list):
            events = [events]
        
        results = []
        
        for event in events:
            subscription_type = event.get("subscriptionType", "")
            object_id = str(event.get("objectId", ""))
            
            logger.info(f"Processing: {subscription_type} for contact {object_id}")
            
            if subscription_type in ["contact.creation", "contact.propertyChange"]:
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
    """Manually trigger a sync for testing."""
    if not Config.SOURCE_TOKEN or not Config.DEST_TOKEN:
        return jsonify({"error": "Tokens not configured"}), 500
    
    result = sync_contact_to_partner(contact_id, "manual_test")
    return jsonify(result)


@app.route("/test/connection", methods=["GET"])
def test_connection():
    """Test connections to both HubSpot portals."""
    results = {}
    
    source = HubSpotAPI(Config.SOURCE_TOKEN)
    success, response = source._request("GET", "/crm/v3/objects/contacts?limit=1")
    results["source"] = {"status": "connected"} if success else {"status": "error", "message": response}
    
    dest = HubSpotAPI(Config.DEST_TOKEN)
    success, response = dest._request("GET", "/crm/v3/objects/contacts?limit=1")
    results["destination"] = {"status": "connected"} if success else {"status": "error", "message": response}
    
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
