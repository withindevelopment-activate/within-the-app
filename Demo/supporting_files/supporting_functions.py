from datetime import datetime, timedelta
import pytz
import urllib.parse


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
    "snapchat.com": "snapchat",
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
    "sccid": "snapchat",
    "sc_cid": "snapchat",
    "srsltid": "google"
}

OWN_DOMAIN = "sleepy-cloud.ae"


def detect_source_from_url_or_domain(url):
    """Detect source based on parameters and domain."""
    if not isinstance(url, str) or url.strip() == "":
        return None

    url_lower = url.lower()
    parsed = urllib.parse.urlparse(url)
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

    # 3) Internal referral
    if OWN_DOMAIN in netloc:
        return "internal"

    return None
