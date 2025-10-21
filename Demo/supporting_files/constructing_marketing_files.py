from django.conf import settings
import requests
import json
from Demo.supporting_files.supabase_functions import get_token

### Creating the tiktok file
def create_tiktok_file(start_date, end_date, store_id):
    ## Get the access token and advertiser_id
    # Get store id from session 
    print("\n========== GETTING TOKENS FROM DATABASE ==========\n")
    tokens = get_token(store_id)
    print("RETREIEVED TOKENS ARE", tokens)
    access_token = tokens['tiktok_access']
    advertiser_id = tokens['tiktok_org']

    print("\n========== TIKTOK CAMPAIGN RETRIEVAL STARTED ==========\n")

    if not access_token:
        raise ValueError("Missing TikTok access token.")
    if not advertiser_id:
        raise ValueError("Missing TikTok advertiser ID.")

    print(f"Fetching stats from {start_date} → {end_date}\n")

    page_size = 100
    status_filter = "CAMPAIGN_STATUS_ENABLE"
    headers = {"Access-Token": access_token}

    # Fetch Campaigns
    # =============================
    campaigns_url = f"{settings.API_BASE}/campaign/get/"
    campaigns_params = {
        "advertiser_id": advertiser_id,
        "page_size": page_size,
        "page": 1,
        "filtering": json.dumps({"secondary_status": status_filter})
    }

    print("Fetching campaigns...")
    campaigns_resp = requests.get(campaigns_url, headers=headers, params=campaigns_params)
    print("Campaigns Response Code:", campaigns_resp.status_code)
    campaigns_data = campaigns_resp.json()

    if "data" not in campaigns_data or "list" not in campaigns_data["data"]:
        print("No campaign list found. Full response:")
        print(json.dumps(campaigns_data, indent=2))
        return []

    campaigns = campaigns_data["data"]["list"]
    print(f"Retrieved {len(campaigns)} campaigns.\n")

    
    # Fetch Ad Groups (for budgets)
    # =============================
    adgroup_url = f"{settings.API_BASE}/adgroup/get/"
    adgroup_params = {"advertiser_id": advertiser_id, "page_size": 100, "page": 1}
    print("➡️ Fetching ad groups for budgets...")
    adgroup_resp = requests.get(adgroup_url, headers=headers, params=adgroup_params)
    adgroup_data = adgroup_resp.json()

    adgroup_map = {}
    if "data" in adgroup_data and "list" in adgroup_data["data"]:
        for ad in adgroup_data["data"]["list"]:
            cid = ad.get("campaign_id")
            adgroup_map[cid] = {
                "budget": ad.get("budget", 0),
                "status": ad.get("secondary_status", "UNKNOWN")
            }

    print(f"Retrieved {len(adgroup_map)} ad groups.\n")

    # Fetch Campaign Stats
    # =============================
    stats_url = f"{settings.API_BASE}/report/integrated/get/"
    stats_params = {
        "advertiser_id": advertiser_id,
        "service_type": "AUCTION",
        "report_type": "BASIC",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": json.dumps(["campaign_id"]),
        "metrics": json.dumps([
            "spend",
            "impressions",
            "clicks",
            "ctr",
            "cpc",
            "cpm",
            "conversion_rate",
            "complete_payment",
            "cost_per_complete_payment",
            "complete_payment_roas",
            "total_complete_payment_rate",
            "result",
            "cost_per_result",
            "result_rate",
            "currency"
        ]),
        "start_date": start_date,
        "end_date": end_date,
        "page": 1,
        "page_size": page_size,
    }

    print("Fetching campaign stats...")
    stats_resp = requests.get(stats_url, headers=headers, params=stats_params)
    print("Stats Response Code:", stats_resp.status_code)
    stats_data = stats_resp.json()

    if "data" not in stats_data or "list" not in stats_data["data"]:
        print("Stats data not found. Full response:")
        print(json.dumps(stats_data, indent=2))
        return []

    stats_lookup = {
        stat["dimensions"]["campaign_id"]: stat["metrics"]
        for stat in stats_data["data"]["list"]
    }

    print(f"Retrieved metrics for {len(stats_lookup)} campaigns.\n")


    # Merge 
    # =============================
    print("Merging campaign + adgroup + stats data...\n")

    enriched = []
    for campaign in campaigns:
        cid = campaign["campaign_id"]
        metrics = stats_lookup.get(cid, {})
        adgroup_info = adgroup_map.get(cid, {"budget": 0, "status": "UNKNOWN"})

        record = {
            "Campaign name": campaign.get("campaign_name"),
            "Primary status": campaign.get("secondary_status", "UNKNOWN").replace("_", " ").title(),
            "Campaign Budget": adgroup_info.get("budget", 0),
            "Cost": float(metrics.get("spend", 0)),
            "Conversions": int(metrics.get("complete_payment", 0)),
            "Cost per conversion": float(metrics.get("cost_per_complete_payment", 0)),
            "Payment completion ROAS (website)": float(metrics.get("complete_payment_roas", 0)),
            "CPM": float(metrics.get("cpm", 0)),
            "CPC (destination)": float(metrics.get("cpc", 0)),
            "Impressions": int(metrics.get("impressions", 0)),
            "Clicks (destination)": int(metrics.get("clicks", 0)),
            "CTR (destination)": float(metrics.get("ctr", 0)),
            "Conversion rate (CVR)": float(metrics.get("conversion_rate", 0)),
            "Results": metrics.get("result"),
            "Cost per result": metrics.get("cost_per_result"),
            "Result rate": metrics.get("result_rate"),
            "Currency": metrics.get("currency", "AED")
        }

        print(f"Campaign: {record['Campaign name']}")
        print(json.dumps(record, indent=2))
        print("-" * 60)
        enriched.append(record)

    print(f"\n✅ Final dataset prepared with {len(enriched)} rows.")
    print("========== END OF TIKTOK CAMPAIGN FETCH ==========\n")

    return enriched

def create_snapchat_file():
    return