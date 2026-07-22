
import requests, json, os, http.client
from django.conf import settings

from supabase import create_client, Client


## import the save tracking heler functions
from Demo.supporting_files.save_tracking_helpers import *

# Constructing the marketing files
# ------------------------------------
# initialize database client
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)


def subscribe_store_to_order_update(authorization_token, access_token):

    conn = http.client.HTTPSConnection(settings.ZID_API_HOST)

    payload = json.dumps({
        "event": "order.update",
        "target_url": settings.TARGET_URL_ORDER_HOOK,
        "original_id": settings.ZID_CLIENT_ID,
        "subscriber": settings.ZID_CLIENT_ID
        
    })

    headers = {
        'Authorization': f'Bearer {authorization_token}',
        'X-Manager-Token': access_token,
        'Content-Type': 'application/json'
    }

    try:
        conn.request("POST", settings.ZID_WEBHOOK_ENDPOINT, payload, headers)
        res = conn.getresponse()
        data = res.read()
        decoded = data.decode("utf-8")
        print(f"Webhook subscription response: {decoded}")
        return json.loads(decoded)
    except Exception as e:
        print(f"Failed to create webhook subscription: {e}")
        return None
    
def subscribe_store_to_order_create(authorization_token, access_token):

    conn = http.client.HTTPSConnection(settings.ZID_API_HOST)

    payload = json.dumps({
        "event": "order.create",
        "target_url": settings.TARGET_URL_ORDER_CREATE_HOOK,
        "original_id": settings.ZID_CLIENT_ID
    })

    headers = {
        "Authorization": f"Bearer {authorization_token}",
        "X-Manager-Token": access_token,
        "Accept-Language": "en",
        "Content-Type": "application/json"
    }

    try:
        conn.request("POST", settings.ZID_WEBHOOK_ENDPOINT, payload, headers)
        res = conn.getresponse()
        data = res.read()
        decoded = data.decode("utf-8")
        print(f"Webhook subscription response: {decoded}")
        return json.loads(decoded)
    except Exception as e:
        print(f"Failed to create webhook subscription: {e}")
        return None
    
   
def subscribe_store_to_customer_create(authorization_token, access_token):

    conn = http.client.HTTPSConnection(settings.ZID_API_HOST)

    payload = json.dumps({
        "event": "customer.create",
        "target_url": settings.TARGET_URL_CUSTOMER_CREATE_HOOK,
        "original_id": settings.ZID_CLIENT_ID,
        "subscriber": settings.ZID_CLIENT_ID
        
    })

    headers = {
        'Authorization': f'Bearer {authorization_token}',
        'X-Manager-Token': access_token,
        'Content-Type': 'application/json'
    }

    try:
        conn.request("POST", settings.ZID_WEBHOOK_ENDPOINT, payload, headers)
        res = conn.getresponse()
        data = res.read()
        decoded = data.decode("utf-8")
        print(f"Webhook subscription response: {decoded}")
        return json.loads(decoded)
    except Exception as e:
        print(f"Failed to create webhook subscription: {e}")
        return None
    
