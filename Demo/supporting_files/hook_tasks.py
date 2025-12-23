import os
from Demo.supporting_files.supabase_functions import get_next_id_from_supabase_compatible_all, fetch_data_from_supabase, batch_insert_to_supabase, fetch_data_from_supabase_specific
from Demo.supporting_files.supporting_functions import get_uae_current_date
import requests
from supabase import create_client, Client
import json
import pandas as pd
import time
import http.client

# Import keys
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)


class settings:
    ZID_CLIENT_ID = os.environ.get('ZID_CLIENT_ID')
    ZID_CLIENT_SECRET = os.environ.get('ZID_CLIENT_SECRET')
    ZID_REDIRECT_URI = "https://translation-sc.onrender.com/zid/callback"
    ZID_AUTH_URL = "https://oauth.zid.sa/oauth/authorize"
    ZID_TOKEN_URL = "https://oauth.zid.sa/oauth/token"
    ZID_API_BASE = "https://api.zid.sa/v1"
    TARGET_URL_PRODUCT_HOOK = 'https://translation-sc.onrender.com/zid-webhook/product-update'
    TARGET_URL_ORDER_HOOK = 'https://translation-sc.onrender.com/zid-webhook/order-update'
    ZID_WEBHOOK_ENDPOINT = "/v1/managers/webhooks"
    ZID_API_HOST = "api.zid.sa"
    WATI_API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6InJlbWF6QHNsZWVweS1jbG91ZC5hZSIsIm5hbWVpZCI6InJlbWF6QHNsZWVweS1jbG91ZC5hZSIsImVtYWlsIjoicmVtYXpAc2xlZXB5LWNsb3VkLmFlIiwiYXV0aF90aW1lIjoiMTIvMjMvMjAyNSAwODo0NjozMiIsInRlbmFudF9pZCI6IjEwNTMxODIiLCJkYl9uYW1lIjoibXQtcHJvZC1UZW5hbnRzIiwiaHR0cDovL3NjaGVtYXMubWljcm9zb2Z0LmNvbS93cy8yMDA4LzA2L2lkZW50aXR5L2NsYWltcy9yb2xlIjoiQURNSU5JU1RSQVRPUiIsImV4cCI6MjUzNDAyMzAwODAwLCJpc3MiOiJDbGFyZV9BSSIsImF1ZCI6IkNsYXJlX0FJIn0.BVqXkslhB4UhPKMeqOgc93hpKk_rT9B6BY7YIyQqBCw"
    WATI_CHANNEL_ID = "1053182"

### Suppotring functions #######################################################################################
################################################################################################################
################################################################################################################
def get_all_order_ids_from_supabase():
    """
    Fetch all order IDs currently stored in the 'Orders_Metadata' table on Supabase.
    Returns a set of order IDs for fast lookup.
    """
    try:
        orders_df = fetch_data_from_supabase("Orders_Meta_Data")
        
        if orders_df.empty or "Order_ID" not in orders_df.columns:
            print("[DEBUG] No orders found in Supabase yet.")
            return set()

        all_order_ids = set(orders_df["Order_ID"].dropna())
        print(f"[DEBUG] Fetched {len(all_order_ids)} order IDs from Supabase.")
        return all_order_ids

    except Exception as e:
        print(f"[ERROR] Failed to fetch order IDs from Supabase: {e}")
        return set()
    except Exception as e:
        print(f"[ERROR] Failed to fetch order IDs from Supabase: {e}")
        return set()
    
### Getting the order directly from ZID API -- -DIRECTLY NO PGE NUMBER NO NOTHING --- -
def get_order_from_zid(order_id, headers):
    """
    Fetch a single order from Zid API.
    Returns the full order dict as returned by Zid, or None on error.
    """
    url = f"https://api.zid.sa/v1/managers/store/orders/{order_id}/view"

    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        order = data.get("order")
        return order  # return the full order object
    except Exception as e:
        print(f"Error fetching order {order_id}: {e}")
        return None

def order_exists_in_supabase(order_id):
    try:
        df = fetch_data_from_supabase_specific("Orders_Meta_Data", filters={
                                                                "Order_ID": ("eq", order_id)
                                                                })
        return not df.empty
    except Exception as e:
        print(f"[WARN] Duplicate check failed: {e}")
        return False
##======================================================================================================
##======================================================================================================

#### Function to fetch and store products
def initial_fetch_products(auth_token, token, store_id):
    """
    Fetch all products from Zid API (with pagination) and store minimal metadata in Supabase:
    - Parent_Product_ID
    - Product display name
    - Variation SKUs and their prices
    - Parent product price
    """
    headers_product = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token, 
        'accept': 'application/json',
        'Accept-Language': 'all-languages',
        'Store-Id': str(store_id),
        'Role': 'Manager',
    }

    all_products = []
    page = 1
    per_page = 50
    total_count = None  

    print("[DEBUG] Fetching products from Zid API with pagination...")

    while True:
        print(f"[DEBUG] Now processing page {page}...")
        
        try:
            products_res = requests.get(
                f"{settings.ZID_API_BASE}/products",
                headers=headers_product,
                params={"page": page, "per_page": per_page},
                timeout=15
            )
            products_res.raise_for_status()
            products_data = products_res.json()
        except Exception as e:
            print(f"[ERROR] Failed to fetch products page {page}: {e}")
            break

        products = products_data.get("results", [])
        if not products:
            break

        all_products.extend(products)

        if total_count is None:
            total_count = products_data.get("count", len(products))

        print(f"[DEBUG] Page {page} retrieved with {len(products)} products.")

        if len(all_products) >= total_count:
            break

        page += 1

    print(f"[DEBUG] Total {len(all_products)} products retrieved (expected ~{total_count}).")
    pd.DataFrame(all_products).to_excel("ALL_PRODS.xlsx", index=False)

    # Filter for published products
    published_products = [p for p in all_products if p.get("is_published") is True]
    print(f"[DEBUG] {len(published_products)} products are published. Skipping {len(all_products) - len(published_products)} unpublished products.")

    # -------- Process Each Product -------- #
    for product in published_products:
        parent_id = product.get("id")
        parent_sku = str(product.get("sku", "")).strip()
        name_obj = product.get("name", {})
        parent_name = name_obj.get("ar") or name_obj.get("en") or "Unnamed"

        # --- Extract parent price (direct field, not nested) ---
        parent_price = product.get("price", 0.0)
        try:
            parent_price = float(parent_price)
        except (TypeError, ValueError):
            parent_price = 0.0

        # --- Handle variations ---
        variations = []
        variation_price_map = {}

        if product.get("structure") == "parent":
            try:
                product_by_id_res = requests.get(
                    f"{settings.ZID_API_BASE}/products/{parent_id}",
                    headers=headers_product,
                    timeout=10
                )
                product_by_id_res.raise_for_status()
                product_by_id_data = product_by_id_res.json()

                variations = product_by_id_data.get("variants", [])
                print(f"[DEBUG] Pulled {len(variations)} variants for parent {parent_id}.")

                # Build a SKU → price map (direct extraction)
                for v in variations:
                    sku = str(v.get("sku", "")).strip()
                    price = v.get("price", 0.0)
                    try:
                        price = float(price)
                    except (TypeError, ValueError):
                        price = 0.0
                    if sku:
                        variation_price_map[sku] = price

            except Exception as e:
                print(f"[ERROR] Failed to fetch product by ID {parent_id}: {e}")
                variations = []
                variation_price_map = {}

        variation_skus_json = json.dumps(list(variation_price_map.keys()))
        variation_price_map_json = json.dumps(variation_price_map)

        # --- Check existing ---
        existing = supabase.table("Products_Metadata") \
            .select("Distinct_ID") \
            .eq("Parent_Product_ID", parent_id) \
            .execute()

        # --- Update or Insert ---
        payload = {
            "Parent_SKU": parent_sku,
            "Product_Name": parent_name,
            "Parent_Price": parent_price,
            "Variation_SKUs": variation_skus_json,
            "Variation_Price_Map": variation_price_map_json,
            "Price_Updates": {},
            "Last_Updated": get_uae_current_date()
        }

        if existing.data:
            supabase.table("Products_Metadata").update(payload) \
                .eq("Parent_Product_ID", parent_id).execute()
            print(f"[DEBUG] Updated product {parent_id} with {len(variation_price_map)} variants.")
        else:
            payload["Distinct_ID"] = int(get_next_id_from_supabase_compatible_all(
                name='Products_Metadata', column='Distinct_ID'))
            payload["Parent_Product_ID"] = parent_id

            supabase.table("Products_Metadata").insert(payload).execute()
            print(f"[DEBUG] Inserted new product {parent_id} with {len(variation_price_map)} variants.")

    return f"Initial product sync complete: {len(all_products)} products stored"

# Function to fetch and store orders
def fetch_new_orders(auth_token, token, store_id, batch_size=10, max_pages=10000):
    """
    Fetch only new orders from Zid API. Handles dynamic pagination.
    - Stops fetching once it hits orders already processed.
    - Fetches order details in batches to respect API rate limits.
    - Stores order-level metadata per page for analysis.
    - Sleeps 10 seconds between pages.
    """
    headers = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
    }

    all_orders = []
    processed_order_ids = set(get_all_order_ids_from_supabase()) 

    print(f"[DEBUG] Already processed {len(processed_order_ids)} orders. Starting fetch...")

    page = 1
    per_page = 50
    stop_fetching = False

    distinct_id = int(get_next_id_from_supabase_compatible_all(
        name='Orders_Meta_Data', column='Distinct_ID'
    ))

    while page <= max_pages and not stop_fetching:
        print(f"[DEBUG] Fetching page {page}...")
        try:
            res = requests.get(
                f"{settings.ZID_API_BASE}/managers/store/orders",
                headers=headers,
                params={"page": page, "per_page": per_page},
                timeout=15
            )
            res.raise_for_status()
            page_orders = res.json().get("orders", [])
        except Exception as e:
            print(f"[ERROR] Failed to fetch page {page}: {e}")
            break

        if not page_orders:
            print(f"[DEBUG] No orders on page {page}. Stopping.")
            break

        new_orders_batch = []
        for order in page_orders:
            if order["id"] in processed_order_ids:
                stop_fetching = True
                break
            new_orders_batch.append(order)

        if not new_orders_batch:
            print("[DEBUG] No new orders in this page. Stopping.")
            break

        page_order_metadata = []

        # ---- Fetch order details in smaller batches ---- #
        for i in range(0, len(new_orders_batch), batch_size):
            batch = new_orders_batch[i:i+batch_size]
            batch_details = []

            for order in batch:
                detailed_order = get_order_from_zid(order["id"], headers)
                if detailed_order:
                    batch_details.append(detailed_order)

            print(f"[DEBUG] Fetched {len(batch_details)} orders in batch. Sleeping 3.5s...")
            time.sleep(3.5)

            # ---- Process batch details ---- #
            for detailed_order in batch_details:
                order_id = detailed_order["id"]
                created_at = detailed_order.get("created_at")
                products_df = pd.DataFrame(detailed_order.get("products", []))
                order_items = []
                net_total, total_with_vat = 0.0, 0.0

                for _, row in products_df.iterrows():
                    qty = float(row.get("quantity", 0))
                    net_price = float(row.get("net_price_with_additions", 0))
                    gross_price = float(row.get("price_with_additions", 0))
                    order_items.append({
                        "sku": row.get("sku"),
                        "quantity": qty,
                        "price_without_vat": net_price,
                        "price_with_vat": gross_price
                    })
                    net_total += net_price
                    total_with_vat += gross_price

                all_orders.append(detailed_order)
                page_order_metadata.append({
                    "Distinct_ID": distinct_id,
                    "Order_ID": order_id,
                    "Order_Date": created_at,
                    "Order_Items": order_items,
                    "Net_Total": round(net_total, 2),
                    "Total_w_Vat": round(total_with_vat, 2),
                    "Last_Updated": get_uae_current_date()
                })
                distinct_id += 1

        # ---- Insert this page's orders into Supabase ---- #
        if page_order_metadata:
            batch_insert_to_supabase(pd.DataFrame(page_order_metadata), "Orders_Meta_Data")
            print(f"[DEBUG] Inserted {len(page_order_metadata)} orders from page {page} into Supabase.")

        # ---- Sleep 10 seconds between pages ---- #
        print("[DEBUG] Sleeping 7 seconds before fetching next page...")
        time.sleep(7)
        page += 1

    print(f"[DEBUG] Fetched {len(all_orders)} new orders across {page-1} pages.")
    return all_orders

## The function that checks for the price change and updates the database with the entry of changes
def track_price_changes(product_payload):
    """
    Track price changes for both parent and variant products.
    Stores updates in Products_Metadata in Supabase.
    Creates an entry in Price_Change_Monitor for any new price change.
    Heavy debug logging added.
    """
    try:
        print("\n[DEBUG] Entered track_price_changes function")
        print(f"[DEBUG] Incoming payload: {json.dumps(product_payload, indent=2)}")

        parent_id = product_payload.get("parent_id")
        parent_sku = str(product_payload.get("sku", "")).strip()
        current_price = float(product_payload.get("price", 0.0) or 0.0)
        variants = product_payload.get("variants", [])

        print(f"[DEBUG] parent_id: {parent_id}, parent_sku: {parent_sku}, current_price: {current_price}")
        print(f"[DEBUG] Variants in payload: {variants}")

        # Fetch existing product record
        existing_res = supabase.table("Products_Metadata").select("*").eq("Parent_Product_ID", parent_id).execute()
        product_record = existing_res.data[0] if existing_res.data else None
        print(f"[DEBUG] Existing product_record loaded: {json.dumps(product_record, indent=2) if product_record else 'None'}")

        # Build the SKU -> price map
        stored_price_map = {}
        if variants:
            for v in variants:
                sku = str(v.get("sku", "")).strip()
                price = float(v.get("price", 0.0) or 0.0)
                stored_price_map[sku] = price
        else:
            stored_price_map[parent_sku] = current_price
        print(f"[DEBUG] Stored price map: {stored_price_map}")

        price_changed = False  # Track if any SKU price changed

        if product_record:
            # Load previous updates dict
            updates_dict = product_record.get("Price_Updates") or {}
            if isinstance(updates_dict, str):
                updates_dict = json.loads(updates_dict)
            print(f"[DEBUG] Current Price_Updates dict: {updates_dict}")

            for sku, new_price in stored_price_map.items():
                # Determine last price
                last_price = product_record.get("Parent_Price") if sku == parent_sku else \
                             json.loads(product_record.get("Variation_Price_Map", "{}")).get(sku, 0.0)
                print(f"[DEBUG] Checking SKU: {sku}, last_price: {last_price}, new_price: {new_price}")

                if last_price != new_price:
                    price_changed = True
                    sku_updates = updates_dict.get(sku, {})
                    update_number = str(len(sku_updates) + 1)
                    sku_updates[update_number] = {
                        "old_price": last_price,
                        "new_price": new_price,
                        "date": get_uae_current_date()
                    }
                    updates_dict[sku] = sku_updates
                    print(f"[DEBUG] Updated SKU {sku} updates: {sku_updates}")

                    # Update main price maps
                    if sku == parent_sku:
                        product_record["Parent_Price"] = new_price
                        print(f"[DEBUG] Parent price updated to {new_price}")
                    else:
                        variation_price_map = json.loads(product_record.get("Variation_Price_Map", "{}"))
                        variation_price_map[sku] = new_price
                        product_record["Variation_Price_Map"] = json.dumps(variation_price_map)
                        print(f"[DEBUG] Variation price map updated: {variation_price_map}")

            # Save updated Price_Updates dict
            product_record["Price_Updates"] = json.dumps(updates_dict)
            product_record["Last_Updated"] = get_uae_current_date()
            supabase.table("Products_Metadata").update(product_record).eq("Parent_Product_ID", parent_id).execute()
            print(f"[DEBUG] Updated price changes for {parent_id}: {updates_dict}")

        else:
            # Product does not exist yet, insert new record
            updates_dict = {}
            variation_price_map = stored_price_map
            payload = {
                "Distinct_ID": int(get_next_id_from_supabase_compatible_all(name='Products_Metadata', column='Distinct_ID')),
                "Parent_Product_ID": parent_id,
                "Parent_SKU": parent_sku,
                "Product_Name": product_payload.get("name", {}).get("ar") or product_payload.get("name", {}).get("en"),
                "Parent_Price": current_price,
                "Variation_SKUs": json.dumps(list(variation_price_map.keys())),
                "Variation_Price_Map": json.dumps(variation_price_map),
                "Price_Updates": json.dumps(updates_dict),
                "Last_Updated": get_uae_current_date()
            }
            supabase.table("Products_Metadata").insert(payload).execute()
            print(f"[DEBUG] Inserted new product {parent_id} into Products_Metadata.")
            price_changed = True  # Treat insert as a "change" to create monitor entry

        # Initialize Price_Change_Monitor entry if any price changed
        if price_changed:
            initialize_price_change_entry(product_payload)
            print(f"[DEBUG] Price_Change_Monitor entry created for {parent_id}")

    except Exception as e:
        print(f"[ERROR] Exception in track_price_changes: {e}")
        import traceback
        traceback.print_exc()



#### This function updates all the products with updates.
def update_order_count():
    """
    Reads Products_Metadata for products with price updates.
    For each SKU, tracks the number of orders since each price update.
    Stores results in Price_Change_Monitor table with history of order counts.
    Heavy debug logging added.
    """
    try:
        print("\n[DEBUG] Entered update_order_count function")

        all_products = supabase.table("Products_Metadata").select("*").execute().data
        print(f"[DEBUG] Fetched {len(all_products)} products from Products_Metadata")

        distinct_id = int(get_next_id_from_supabase_compatible_all(
            name='Price_Change_Monitor', column='Distinct_ID'))
        print(f"[DEBUG] Starting Distinct_ID for new inserts: {distinct_id}")

        # Filter products that have price updates
        products_with_updates = [
            p for p in all_products if p.get("Price_Updates") and p.get("Price_Updates").strip() not in ["", "{}"]
        ]
        print(f"[DEBUG] Found {len(products_with_updates)} products with price updates")

        # Fetch all orders once
        all_orders = supabase.table("Orders_Meta_Data").select("*").execute().data
        print(f"[DEBUG] Fetched {len(all_orders)} orders from Orders_Meta_Data")

        for product in products_with_updates:
            parent_id = product["Parent_Product_ID"]
            product_name = product.get("Product_Name")
            parent_sku = product.get("Parent_SKU")
            variation_price_map = json.loads(product.get("Variation_Price_Map", "{}"))
            price_updates = json.loads(product.get("Price_Updates", "{}"))

            print(f"\n[DEBUG] Processing product {parent_id} ({product_name}) with SKUs: {list(price_updates.keys())}")

            for sku, updates in price_updates.items():
                print(f"[DEBUG] Processing SKU: {sku} with {len(updates)} price updates")

                # Load current order count history
                monitor_res = supabase.table("Price_Change_Monitor").select("*").eq("SKU", sku).execute()
                if monitor_res.data:
                    monitor_record = monitor_res.data[0]
                    order_counts_history = json.loads(monitor_record.get("Order_Count_History", "{}"))
                    print(f"[DEBUG] Existing monitor record found for SKU {sku}")
                else:
                    monitor_record = None
                    order_counts_history = {}
                    print(f"[DEBUG] No existing monitor record found for SKU {sku}, will insert new")

                for update_number, update_info in updates.items():
                    last_update_date = update_info["date"]
                    order_count = 0

                    for order in all_orders:
                        order_date = order.get("Order_Date")
                        if order_date < last_update_date:
                            continue
                        order_items = order.get("Order_Items", [])
                        if any(item.get("sku") == sku for item in order_items):
                            order_count += 1

                    order_counts_history[update_number] = {
                        "date": last_update_date,
                        "orders_after_update": order_count
                    }
                    print(f"[DEBUG] Update #{update_number} for SKU {sku}: {order_count} orders since {last_update_date}")

                # Prepare monitor payload
                monitor_payload = {
                    "Distinct_ID": distinct_id,
                    "SKU": sku,
                    "Product_Name": product_name,
                    "Is_Variant": sku != parent_sku,
                    "Product_ID": parent_id,
                    "Current_Price": variation_price_map.get(sku) if sku != parent_sku else product.get("Parent_Price"),
                    "Price_Updates": json.dumps(updates),
                    "Order_Count_History": json.dumps(order_counts_history),
                    "Last_Updated": get_uae_current_date()
                }
                distinct_id += 1

                print(f"[DEBUG] Monitor payload prepared for SKU {sku}: {json.dumps(monitor_payload, indent=2)}")

                # Upsert into Price_Change_Monitor
                if monitor_record:
                    print(f"[DEBUG] Updating existing record for SKU {sku}")
                    supabase.table("Price_Change_Monitor").update(monitor_payload).eq("SKU", sku).execute()
                else:
                    print(f"[DEBUG] Inserting new record for SKU {sku}")
                    supabase.table("Price_Change_Monitor").insert(monitor_payload).execute()

        print("\n[DEBUG] update_order_count completed successfully")

    except Exception as e:
        print(f"[ERROR] Exception in update_order_count: {e}")
        import traceback
        traceback.print_exc()



#### This function updates the order count for only the product that triggered the webhook
def update_order_count_for_sku(product_payload):
    """
    Updates the order count for the product (or variants) that triggered the webhook.
    Only fetches orders from Orders_Meta_Data where Order_Date > last price update.
    Handles empty orders gracefully.
    Heavy debug logging added.
    """
    try:
        print("\n[DEBUG] Entered update_order_count_for_sku")
        print(f"[DEBUG] Incoming product_payload: {json.dumps(product_payload, indent=2)}")

        parent_id = product_payload.get("parent_id")
        parent_sku = str(product_payload.get("sku", "")).strip()
        product_name = product_payload.get("name", {}).get("ar") or product_payload.get("name", {}).get("en")
        product_structure = product_payload.get("structure")
        variation_price_map = {}

        # Fetch the product record from Products_Metadata
        res = supabase.table("Products_Metadata").select("*").eq("Parent_Product_ID", parent_id).execute()
        if not res.data:
            print(f"[DEBUG] Product {parent_id} not found in Products_Metadata.")
            return
        product_record = res.data[0]
        print(f"[DEBUG] Loaded product_record: {json.dumps(product_record, indent=2)}")

        # Determine SKUs to process
        if product_structure == "parent" and product_payload.get("variants"):
            for v in product_payload["variants"]:
                sku = str(v.get("sku", "")).strip()
                price = float(v.get("price", 0.0) or 0.0)
                variation_price_map[sku] = price
            print(f"[DEBUG] Processing variant SKUs: {variation_price_map}")
        else:
            variation_price_map[parent_sku] = float(product_payload.get("price", 0.0) or 0.0)
            print(f"[DEBUG] Processing standalone SKU: {variation_price_map}")

        # Load price updates
        price_updates = json.loads(product_record.get("Price_Updates", "{}"))
        print(f"[DEBUG] Loaded price_updates: {json.dumps(price_updates, indent=2)}")

        # Initialize distinct_id for new records
        distinct_id = int(get_next_id_from_supabase_compatible_all(
            name='Price_Change_Monitor', column='Distinct_ID'))
        print(f"[DEBUG] Starting distinct_id: {distinct_id}")

        for sku in variation_price_map.keys():
            if sku not in price_updates:
                print(f"[DEBUG] No price updates recorded for SKU {sku}. Skipping.")
                continue

            # Most recent update
            last_update_number = max(price_updates[sku].keys(), key=int)
            last_update_info = price_updates[sku][last_update_number]
            last_update_date = last_update_info["date"]
            print(f"[DEBUG] SKU {sku}, last_update_number: {last_update_number}, last_update_date: {last_update_date}")

            # Fetch relevant orders since last update
            orders_res = supabase.table("Orders_Meta_Data").select("*").gte("Order_Date", last_update_date).execute()
            orders = orders_res.data if orders_res.data else []
            print(f"[DEBUG] Fetched {len(orders)} orders since last update for SKU {sku}")

            # Count orders containing this SKU
            order_count = 0
            for order in orders:
                order_items = order.get("Order_Items", [])
                if any(item.get("sku") == sku for item in order_items):
                    order_count += 1
            print(f"[DEBUG] Orders containing SKU {sku} since last update: {order_count}")

            # Fetch or create Price_Change_Monitor record
            monitor_res = supabase.table("Price_Change_Monitor").select("*").eq("SKU", sku).execute()
            if monitor_res.data:
                monitor_record = monitor_res.data[0]
                order_counts_history = json.loads(monitor_record.get("Order_Count_History", "{}"))
                print(f"[DEBUG] Existing monitor record found for SKU {sku}")
            else:
                monitor_record = None
                order_counts_history = {}
                print(f"[DEBUG] No existing monitor record found for SKU {sku}, will insert new")

            # Update order count for this update
            order_counts_history[last_update_number] = {
                "date": last_update_date,
                "orders_after_update": order_count
            }

            # Prepare monitor payload
            monitor_payload = {
                "Distinct_ID": distinct_id,
                "SKU": sku,
                "Product_Name": product_name,
                "Is_Variant": sku != parent_sku,
                "Product_ID": parent_id,
                "Current_Price": variation_price_map[sku],
                "New_Price": last_update_info["new_price"],
                "Price_Updates": json.dumps(price_updates),
                "Order_Count_History": json.dumps(order_counts_history),
                "Last_Updated": get_uae_current_date()
            }
            print(f"[DEBUG] Monitor payload for SKU {sku}: {json.dumps(monitor_payload, indent=2)}")
            distinct_id += 1

            # Upsert into Price_Change_Monitor
            if monitor_record:
                print(f"[DEBUG] Updating existing Price_Change_Monitor record for SKU {sku}")
                supabase.table("Price_Change_Monitor").update(monitor_payload).eq("SKU", sku).execute()
            else:
                print(f"[DEBUG] Inserting new Price_Change_Monitor record for SKU {sku}")
                supabase.table("Price_Change_Monitor").insert(monitor_payload).execute()

        print("[DEBUG] update_order_count_for_sku completed successfully")

    except Exception as e:
        print(f"[ERROR] Exception in update_order_count_for_sku: {e}")
        import traceback
        traceback.print_exc()

## A function that initializes the entyr in the products moitoring, called after spotting price change for the product
def initialize_price_change_entry(product_payload):
    """
    Creates a new Price_Change_Monitor entry (or updates existing one)
    when a price change occurs. Initializes the order count as 0.
    Later increments will be handled by increment_order_count_for_skus().
    """
    try:
        print("\n[DEBUG] Entered initialize_price_change_entry")
        print(f"[DEBUG] Product payload: {json.dumps(product_payload, indent=2)}")

        parent_id = product_payload.get("id")
        parent_sku = str(product_payload.get("sku", "")).strip()
        product_name = (
            product_payload.get("name", {}).get("ar")
            or product_payload.get("name", {}).get("en")
        )
        product_structure = product_payload.get("structure")
        variation_price_map = {}

        # Determine SKUs to process
        if product_structure == "parent" and product_payload.get("variants"):
            for v in product_payload["variants"]:
                sku = str(v.get("sku", "")).strip()
                price = float(v.get("price", 0.0) or 0.0)
                variation_price_map[sku] = price
            print(f"[DEBUG] Found variant SKUs: {variation_price_map}")
        else:
            variation_price_map[parent_sku] = float(product_payload.get("price", 0.0) or 0.0)
            print(f"[DEBUG] Found standalone SKU: {variation_price_map}")

        # Load product metadata (for price update tracking)
        res = supabase.table("Products_Metadata").select("*").eq("Parent_Product_ID", parent_id).execute()
        if not res.data:
            print(f"[DEBUG] No product record found in Products_Metadata for {parent_id}")
            return
        product_record = res.data[0]

        # Load price updates safely
        raw_price_updates = product_record.get("Price_Updates")
        if not raw_price_updates:
            price_updates = {}
        else:
            try:
                price_updates = json.loads(raw_price_updates)
            except Exception:
                print("[WARN] Invalid JSON format in Price_Updates, resetting to empty dict.")
                price_updates = {}
        print(f"[DEBUG] Loaded price updates: {json.dumps(price_updates, indent=2)}")

        distinct_id = int(get_next_id_from_supabase_compatible_all("Price_Change_Monitor", "Distinct_ID"))

        for sku, price in variation_price_map.items():
            if sku not in price_updates:
                print(f"[DEBUG] No price update found for SKU {sku}. Skipping initialization.")
                continue

            # Get latest price update
            last_update_number = max(price_updates[sku].keys(), key=int)
            last_update_info = price_updates[sku][last_update_number]
            last_update_date = last_update_info["date"]

            print(f"[DEBUG] Initializing monitor entry for SKU {sku}, update #{last_update_number}")

            # Prepare a new entry with order count = 0
            order_counts_history = {
                last_update_number: {
                    "date": last_update_date,
                    "orders_after_update": 0
                }
            }

            monitor_payload = {
                "Distinct_ID": distinct_id,
                "SKU": sku,
                "Product_Name": product_name,
                "Is_Variant": sku != parent_sku,
                "Product_ID": parent_id,
                "Current_Price": price,
                "New_Price": last_update_info["new_price"],
                "Price_Updates": json.dumps(price_updates),
                "Order_Count_History": json.dumps(order_counts_history),
                "Last_Updated": get_uae_current_date()
            }

            # Check if entry already exists
            existing = supabase.table("Price_Change_Monitor").select("*").eq("SKU", sku).execute()
            if existing.data:
                print(f"[DEBUG] Updating existing Price_Change_Monitor entry for {sku}")
                supabase.table("Price_Change_Monitor").update(monitor_payload).eq("SKU", sku).execute()
            else:
                print(f"[DEBUG] Inserting new Price_Change_Monitor entry for {sku}")
                supabase.table("Price_Change_Monitor").insert(monitor_payload).execute()

            distinct_id += 1

        print("[DEBUG] initialize_price_change_entry completed successfully")

    except Exception as e:
        print(f"[ERROR] Exception in initialize_price_change_entry: {e}")
        import traceback
        traceback.print_exc()



### Supporting function to update order database.
def process_zid_order_logic(payload):
    """
    Background logic for inserting new order into Supabase.
    Handles duplicates, computes totals, and is thread-safe.
    Heavy debug logging added.
    """
    try:
        print("\n[DEBUG] Entered process_zid_order_logic")
        #print(f"[DEBUG] Incoming payload: {json.dumps(payload, indent=2)}")

        order = payload.get("order") or payload
        order_id = order.get("id")
        if not order_id:
            print("[WARN] Missing order ID in payload. Skipping processing.")
            return

        # --- Check for duplicate ---
        if order_exists_in_supabase(order_id):
            print(f"[DEBUG] Order {order_id} already exists in Supabase. Skipping insert.")
            return

        # --- Extract basic info ---
        customer = order.get("customer", {})
        products = order.get("products", [])
        created_at = order.get("created_at") or get_uae_current_date()
        order_status = order.get("order_status", {}).get("code", "unknown")
        store_id = order.get("store_id")
        store_name = order.get("store_name")
        payment_status = order.get("payment_status")

        print(f"[DEBUG] Order info - ID: {order_id}, Status: {order_status}, Store: {store_name}, Payment: {payment_status}")

        # --- Calculate totals ---
        net_total, total_with_vat = 0.0, 0.0
        order_items = []

        for product in products:
            try:
                qty = float(product.get("quantity", 0))
                net_price = float(product.get("net_price_with_additions", 0))
                gross_price = float(product.get("price_with_additions", 0))
            except Exception as ex:
                print(f"[WARN] Failed to parse product pricing: {product}, error: {ex}")
                qty, net_price, gross_price = 0, 0, 0

            order_items.append({
                "sku": product.get("sku"),
                "quantity": qty,
                "price_without_vat": net_price,
                "price_with_vat": gross_price
            })
            net_total += net_price
            total_with_vat += gross_price

        print(f"[DEBUG] Calculated totals - Net: {net_total}, Gross w/ VAT: {total_with_vat}")
        print(f"[DEBUG] Order items: {json.dumps(order_items, indent=2)}")

        # --- Prepare record ---
        distinct_id = int(get_next_id_from_supabase_compatible_all(
            name='Orders_Meta_Data', column='Distinct_ID'
        ))
        print(f"[DEBUG] Assigned Distinct_ID: {distinct_id}")

        record = {
            "Distinct_ID": distinct_id,
            "Order_ID": order_id,
            "Order_Date": created_at,
            "Order_Items": order_items,
            "Net_Total": round(net_total, 2),
            "Total_w_Vat": round(total_with_vat, 2),
            "Last_Updated": get_uae_current_date(),
        }


        print(f"[DEBUG] Prepared order record: {json.dumps(record, indent=2)}")

        # --- Insert to Supabase ---
        df = pd.DataFrame([record])
        batch_insert_to_supabase(df, "Orders_Meta_Data")
        print(f"[DEBUG] Order {order_id} inserted successfully into Orders_Meta_Data.")

        # --- Increment order count if SKU exists in Price_Change_Monitor ---
        if order_items:
            print(f"[DEBUG] Incrementing order counts for SKUs in this order.")
            increment_order_count_for_skus(order_items)
        else:
            print(f"[DEBUG] No order items to increment counts for.")

    except Exception as e:
        print(f"[ERROR] Async order processing failed for order ID {order_id if 'order_id' in locals() else 'unknown'}: {e}")
        import traceback
        traceback.print_exc()


#### Incrementing the order count in the Price Change Monitor Database.
def increment_order_count_for_skus(order_items):
    """
    For each SKU in the new order, update its order count in Price_Change_Monitor.
    Increments the 'orders_after_update' value in the most recent history entry.
    Heavy debug logging added.
    """
    try:
        print("\n[DEBUG] Entered increment_order_count_for_skus")
        print(f"[DEBUG] Order items received: {json.dumps(order_items, indent=2)}")

        for item in order_items:
            sku = item.get("sku")
            if not sku:
                print(f"[DEBUG] Skipping item with no SKU: {item}")
                continue

            # Fetch record from Price_Change_Monitor for this SKU
            monitor_res = supabase.table("Price_Change_Monitor").select("*").eq("SKU", sku).execute()
            if not monitor_res.data:
                print(f"[DEBUG] SKU {sku} not found in Price_Change_Monitor. Skipping increment.")
                continue

            monitor_record = monitor_res.data[0]
            order_counts_history = json.loads(monitor_record.get("Order_Count_History", "{}"))

            if not order_counts_history:
                print(f"[DEBUG] SKU {sku} has no previous price update history. Skipping increment.")
                continue

            # Get latest update entry (by highest numeric key)
            latest_key = max(order_counts_history.keys(), key=lambda k: int(k))
            latest_entry = order_counts_history[latest_key]

            # Increment the order count
            previous_count = latest_entry.get("orders_after_update", 0)
            latest_entry["orders_after_update"] = previous_count + 1
            order_counts_history[latest_key] = latest_entry

            print(f"[DEBUG] SKU {sku} - Update #{latest_key} order count incremented: {previous_count} -> {latest_entry['orders_after_update']}")

            # Update the record in Supabase
            supabase.table("Price_Change_Monitor").update({
                "Order_Count_History": json.dumps(order_counts_history),
                "Last_Updated": get_uae_current_date()
            }).eq("SKU", sku).execute()
            print(f"[DEBUG] SKU {sku} record updated in Price_Change_Monitor.")

        print("[DEBUG] increment_order_count_for_skus completed successfully")

    except Exception as e:
        print(f"[ERROR] Exception in increment_order_count_for_skus: {e}")
        import traceback
        traceback.print_exc()

# def send_wati_template_v3(phone=None, customer_name=None, link=None):
#     if not all([phone, customer_name, link]):
#         raise ValueError("Missing required WATI template parameters")

#     phone = phone.replace("+", "").strip()

#     url = "https://live-mt-server.wati.io/api/ext/v3/messageTemplates/send"

#     headers = {
#     "Authorization": f"Bearer {settings.WATI_API_TOKEN}",
#     "Accept": "*/*",
#     "Content-Type": "application/json",
# }

#     payload = {
#         "template_name": "abandon_carts_retargeting",
#         "broadcast_name": "abandon_carts_retargeting_test",
#         "recipients": [
#             {
#             "phone_number": phone,
#             "local_message_id": "python-test-001",
#             "custom_params": [
#                     {
#                     "name": "name",
#                     "value": customer_name
#                     },
#                     {
#                     "name": "link",
#                     "value": link
#                     }
#                 ]
#             }
#         ]
#     }


#     try:
#         res = requests.post(
#             url,
#             headers=headers,
#             data=payload,
#             timeout=10
#         )

#         if res.status_code != 200:
#             print("[ERROR] WATI v1 response:", res.status_code, res.text)
#             return None

#         result = res.json()
#         print("[DEBUG] WATI template sent (v1):", result)
#         return result

#     except requests.exceptions.RequestException as e:
#         print("[ERROR] WATI request failed:", str(e))
#         return None
    
# @app.post("/webhook/abandoned_cart")
# async def abandoned_cart_webhook(req: Request):
#     payload = await req.json()
    
#     cart_id = payload.get("id")
#     customer = payload.get("customer", {})
#     phone = customer.get("phone")
#     name = customer.get("name", "Customer")
#     cart_items = payload.get("items", [])
#     cart_total = payload.get("total_price", 0)
#     checkout_url = payload.get("checkout_url", f"https://yourstore.com/cart/{cart_id}")

#     if not phone:
#         return {"status": "skipped", "reason": "No phone number"}

#     send_wati_template_v3(
#         phone=phone,
#         cart_id=cart_id,
#         customer_name=name,
#         cart_items_count=len(cart_items),
#         cart_total=cart_total,
#         checkout_url=checkout_url
#     )

#     # Optional: store cart info in Supabase
#     store_abandoned_cart({
#         "Cart_ID": cart_id,
#         "Email": customer.get("email"),
#         "Phone": phone,
#         "Cart_Items": cart_items,
#         "Cart_Total": cart_total,
#         "Checkout_URL": checkout_url
#     })

#     return {"status": "success"}
