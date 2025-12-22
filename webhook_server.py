#!/usr/bin/env python3
"""
HubSpot Webhook Sync Server

Automatically syncs contacts to a partner HubSpot portal when:
- A contact submits a form
- A contact is created
- A contact is updated

Deploy to: Railway, Render, or Fly.io (all have free tiers)

Environment Variables Required:
    HUBSPOT_SOURCE_TOKEN    - Your HubSpot private app token
    HUBSPOT_DEST_TOKEN      - Partner's HubSpot access token
    HUBSPOT_CLIENT_SECRET   - Your app's client secret (for webhook verification)
    PORT                    - Server port (set automatically by hosting platforms)
"""

import os
import json
import hmac
import hashlib
import logging
import time
from datetime import datetime
from functools import wraps

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
    CLIENT_SECRET = os.environ.get("HUBSPOT_CLIENT_SECRET", "")
    
    # Server settings
    PORT = int(os.environ.get("PORT", 8080))
    DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
    SKIP_SIGNATURE_VERIFICATION = os.environ.get("SKIP_SIGNATURE_VERIFICATION", "false").lower() == "true"
    
    # Properties to fetch from source (GOVA HubSpot)
    PROPERTIES_TO_SYNC = [
        # Standard contact fields
        "firstname",
        "lastname", 
        "phone",
        "email",
        "address",
        "city",
        "state",
        "zip",
        # Military fields
        "military_status___dropdown",
        "military_branch___dropdown",
        "military_rank",
        # Other custom fields
        "verify_date_of_birth",
        "armed_forces_mutual_member",
        "edge_sso_sign_up",
        "edge_xp_earned__all_time_",
        "edge_initial_financial_assessment_completed_date",
        "stress_score_total",
        "stress_score_completed_date",
        "opted_out_of_communications_afm",
    ]
    
    # Property mapping: source (GOVA) -> destination (Armed Forces Mutual)
    # Only include properties that have DIFFERENT names in destination
    PROPERTY_MAPPING = {
        "opted_out_of_communications_afm": "hs_email_optout_27547260",
    }
    
    # Properties that exist in source but should NOT be synced to destination
    # (because destination doesn't have these fields)
    PROPERTIES_TO_SKIP = [
        "military_status___dropdown",
        "military_branch___dropdown", 
        "armed_forces_mutual_member",
    ]
    
    # Optional: Only sync contacts from specific forms (leave empty for all)
    FORM_FILTER = os.environ.get("FORM_FILTER", "").split(",") if os.environ.get("FORM_FILTER") else []


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.DEBUG if Config.DEBUG else logging.INFO,
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
                
                # Handle rate limiting
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
# WEBHOOK VERIFICATION
# =============================================================================

def verify_hubspot_signature(f):
    """Decorator to verify HubSpot webhook signatures."""
    @wraps(f)
    def decorated(*args, **kwargs):
        # Skip verification if explicitly disabled or no client secret
        if Config.SKIP_SIGNATURE_VERIFICATION:
            logger.debug("Signature verification disabled - skipping")
            return f(*args, **kwargs)
        
        if not Config.CLIENT_SECRET:
            logger.warning("No CLIENT_SECRET set - skipping signature verification")
            return f(*args, **kwargs)
        
        # Get signature from headers (v3 signature)
        signature = request.headers.get("X-HubSpot-Signature-v3")
        timestamp = request.headers.get("X-HubSpot-Request-Timestamp")
        
        if not signature or not timestamp:
            # Try v1 signature as fallback
            signature_v1 = request.headers.get("X-HubSpot-Signature")
            if signature_v1:
                # V1 verification
                source_string = Config.CLIENT_SECRET + request.get_data(as_text=True)
                expected = hashlib.sha256(source_string.encode()).hexdigest()
                if hmac.compare_digest(expected, signature_v1):
                    return f(*args, **kwargs)
            
            logger.warning("Missing webhook signature headers")
            return jsonify({"error": "Missing signature"}), 401
        
        # V3 verification
        uri = request.url
        body = request.get_data(as_text=True)
        source_string = f"{request.method}{uri}{body}{timestamp}"
        
        expected_signature = hmac.new(
            Config.CLIENT_SECRET.encode(),
            source_string.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(expected_signature, signature):
            logger.warning("Invalid webhook signature")
            return jsonify({"error": "Invalid signature"}), 401
        
        return f(*args, **kwargs)
    
    return decorated


# =============================================================================
# PROPERTY MAPPING
# =============================================================================

def map_properties_for_destination(source_props: dict) -> dict:
    """
    Transform source properties to destination format.
    
    - Skips properties that don't exist in destination
    - Renames properties that have different names in destination
    - Only includes properties that have values
    """
    dest_props = {}
    
    for source_key, value in source_props.items():
        # Skip empty values
        if not value:
            continue
            
        # Skip properties that shouldn't be synced
        if source_key in Config.PROPERTIES_TO_SKIP:
            logger.debug(f"Skipping property {source_key} - not in destination")
            continue
        
        # Check if this property needs to be renamed
        if source_key in Config.PROPERTY_MAPPING:
            dest_key = Config.PROPERTY_MAPPING[source_key]
            logger.debug(f"Mapping {source_key} -> {dest_key}")
        else:
            dest_key = source_key
        
        dest_props[dest_key] = value
    
    return dest_props


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
        # Get contact from source portal
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
        
        # Map properties to destination format
        sync_props = map_properties_for_destination(props)
        
        logger.debug(f"Syncing properties: {list(sync_props.keys())}")
        
        # Sync to destination portal
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
            "client_secret_set": bool(Config.CLIENT_SECRET),
            "properties_to_sync": Config.PROPERTIES_TO_SYNC,
            "property_mapping": Config.PROPERTY_MAPPING,
            "properties_skipped": Config.PROPERTIES_TO_SKIP,
            "form_filter": Config.FORM_FILTER or "all forms"
        }
    })


@app.route("/health")
def health():
    """Health check endpoint for monitoring."""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/webhooks/hubspot", methods=["POST"])
@verify_hubspot_signature
def hubspot_webhook():
    """
    Handle HubSpot webhook events.
    
    HubSpot sends an array of events. Each event contains:
    - subscriptionType: e.g., "contact.creation", "contact.propertyChange"
    - objectId: The contact ID
    - propertyName: (for propertyChange) which property changed
    - propertyValue: (for propertyChange) the new value
    """
    
    try:
        events = request.json
        
        if not isinstance(events, list):
            events = [events]
        
        results = []
        
        for event in events:
            subscription_type = event.get("subscriptionType", "")
            object_id = str(event.get("objectId", ""))
            
            logger.info(f"Received webhook: {subscription_type} for contact {object_id}")
            
            # Handle different event types
            if subscription_type in [
                "contact.creation",
                "contact.propertyChange",
                "form.submitted"  # If using form submission webhooks
            ]:
                # Check form filter if this is a form submission
                if subscription_type == "form.submitted":
                    form_id = event.get("formId", "")
                    if Config.FORM_FILTER and form_id not in Config.FORM_FILTER:
                        logger.info(f"Skipping form {form_id} - not in filter list")
                        results.append({
                            "contact_id": object_id,
                            "status": "skipped",
                            "message": f"Form {form_id} not in filter"
                        })
                        continue
                
                # Sync the contact
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


@app.route("/test/sync/<contact_id>", methods=["POST"])
def test_sync(contact_id):
    """
    Manually trigger a sync for testing.
    
    Usage: POST /test/sync/12345
    """
    if not Config.SOURCE_TOKEN or not Config.DEST_TOKEN:
        return jsonify({"error": "Tokens not configured"}), 500
    
    result = sync_contact_to_partner(contact_id, "manual_test")
    return jsonify(result)


@app.route("/test/connection", methods=["GET"])
def test_connection():
    """Test connections to both HubSpot portals."""
    results = {}
    
    # Test source
    try:
        source = HubSpotAPI(Config.SOURCE_TOKEN)
        source._request("GET", "/crm/v3/objects/contacts?limit=1")
        results["source"] = {"status": "connected"}
    except Exception as e:
        results["source"] = {"status": "error", "message": str(e)}
    
    # Test destination
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
    # Validate configuration
    missing = []
    if not Config.SOURCE_TOKEN:
        missing.append("HUBSPOT_SOURCE_TOKEN")
    if not Config.DEST_TOKEN:
        missing.append("HUBSPOT_DEST_TOKEN")
    
    if missing:
        logger.warning(f"Missing environment variables: {', '.join(missing)}")
        logger.warning("The server will start but syncing won't work until these are set.")
    
    logger.info(f"Starting HubSpot Webhook Sync Server on port {Config.PORT}")
    logger.info(f"Webhook URL: http://localhost:{Config.PORT}/webhooks/hubspot")
    
    app.run(host="0.0.0.0", port=Config.PORT, debug=Config.DEBUG)
