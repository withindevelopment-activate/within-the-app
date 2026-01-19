from datetime import datetime, timedelta
import pytz, traceback, urllib.parse
from django.contrib import messages

def get_uae_current_date():
    # Define the UAE timezone
    uae_timezone = pytz.timezone('Asia/Dubai')
    
    # Get the current time in the UAE timezone
    now_uae = datetime.now(uae_timezone)
    
    # Format the current date and time
    current_date = now_uae.strftime('%Y-%m-%d %H:%M:%S')
    
    return current_date
    
# -------------------------------------------
# SOURCE DETECTION LOGIC (ported from Excel version)
# -------------------------------------------
DOMAIN_MAPPING = {
    "facebook.com": "instagram",
    "instagram.com": "instagram",
    "tiktok.com": "tiktok",
    "twitter.com": "twitter",
    "x.com": "twitter",
    "linkedin.com": "linkedin",
    "snapchat.com": "Snapchat",
    "google.com": "google",
    "youtube.com": "youtube"
}

PARAM_MAPPING = {
    "fbclid": "instagram",
    "fb_source": "instagram",
    "fb_ref": "instagram",
    "ttclid": "tiktok",
    "twclid": "twitter",
    "li_fat_id": "linkedin",
    "gclid": "google",
    "wbraid": "google",
    "gbraid": "google",
    "sccid": "Snapchat",
    "sc_cid": "Snapchat",
    "srsltid": "google"
}

USER_AGENT_SOURCE_MAPPING = {
    "musical_ly": "tiktok",
    "instagram": "instagram",
    "snapchat": "Snapchat",
    "FB_IAB": "instagram",
    "google": "google",
    "tiktok": "tiktok"
}

OWN_DOMAIN = "sleepy-cloud.ae"

def detect_source_from_user_agent(user_agent: str) -> str | None:
    if not user_agent:
        return None

    ua = user_agent.lower()

    for keyword, source in USER_AGENT_SOURCE_MAPPING.items():
        if keyword in ua:
            return source

    return None

def detect_source_from_url_or_domain(url):
    """Detect source based on parameters and domain."""
    if not isinstance(url, str) or url.strip() == "":
        return None

    url_lower = url.lower()
    parsed = urllib.parse.urlparse(url_lower)
    params = urllib.parse.parse_qs(parsed.query or "")

    # 1) URL parameters
    for key, source in PARAM_MAPPING.items():
        if key in params:
            return source

    # 2) Domain detection
    netloc = parsed.netloc or ""
    for domain, source in DOMAIN_MAPPING.items():
        if domain in netloc:
            return source

    return None

# def detect_primary_source(url):
#     """
#     Attribution priority:
#     1) UTM parameters (primary)
#     2) Referrer domain mapping
#     3) Click / tracking parameters
#     4) Internal
#     5) Direct
#     """
#     if not isinstance(url, str) or not url.strip():
#         return "direct"

#     url_lower = url.lower()
#     parsed = urllib.parse.urlparse(url_lower)
#     params = urllib.parse.parse_qs(parsed.query or "")
#     netloc = parsed.netloc or ""

#     # 1) UTM-based primary source
#     utm_source = params.get("utm_source", [None])[0]
#     if utm_source:
#         return utm_source.lower()

#     # 2) Internal traffic
#     if OWN_DOMAIN in netloc:
#         return "internal"

#     # 3) Domain-based detection
#     for domain, source in DOMAIN_MAPPING.items():
#         if domain in netloc:
#             return source

#     # 4) Parameter-based detection (click IDs, etc.)
#     for key, source in PARAM_MAPPING.items():
#         if key in params:
#             return source

#     # 5) Direct
#     return "direct"

import urllib.parse

def extract_params_from_raw_url(url):
    if not isinstance(url, str):
        return {}

    if "?" not in url:
        return {}

    query = url.split("?", 1)[1]
    return urllib.parse.parse_qs(query)

def detect_source_from_row(url):
    if not isinstance(url, str) or not url.strip():
        return "direct"

    raw = url.lower()
    params = extract_params_from_raw_url(raw)

    # 1️⃣ UTM source
    utm_source = params.get("utm_source", [None])[0]
    if utm_source:
        return utm_source.lower()

    # 2️⃣ Paid click IDs
    for key, source in PARAM_MAPPING.items():
        if key in params:
            return source

    # 3️⃣ Domain mapping
    try:
        parsed = urllib.parse.urlparse(raw)
        netloc = parsed.netloc or ""
    except Exception:
        netloc = ""

    for domain, source in DOMAIN_MAPPING.items():
        if domain in netloc:
            return source

    return "direct"
