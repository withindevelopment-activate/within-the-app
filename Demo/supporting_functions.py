from datetime import datetime, timedelta
import pytz


def get_uae_current_date():
    # Define the UAE timezone
    uae_timezone = pytz.timezone('Asia/Dubai')
    
    # Get the current time in the UAE timezone
    now_uae = datetime.now(uae_timezone)
    
    # Format the current date and time
    current_date = now_uae.strftime('%Y-%m-%d %H:%M:%S')
    
    return current_date

import os,json,ast,logging,pandas as pd, pytz, itertools
from supabase import create_client, Client
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np


# Import keys
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)

def get_token(store_id=None):
    """
    Fetch token from 'tokens' table.
    - If store_id is given → fetch that specific store's token.
    - Otherwise fetch the most recent token (by Distinct_ID).
    """
    query = supabase.table("tokens").select("*")

    if store_id:
        query = query.eq("Store_ID", store_id)
    else:
        query = query.order("Distinct_ID", desc=True)

    res = query.limit(1).execute()

    if not res.data:
        raise ValueError("No matching tokens found in database")

    token_row = res.data[0]
    return {
        "access_token": token_row.get("Access"),
        "authorization_token": token_row.get("Authorization"),
        "refresh_token": token_row.get("Refresh"),
        "store_id": token_row.get("Store_ID"),
        "snapchat_access": token_row.get("Snapchat_Access"),
        "tiktok_access": token_row.get("Tiktok_Access"),
        "tiktok_org": token_row.get("Tiktok_Org")
    }

def fetch_data_from_supabase(table_name):
    response = supabase.table(table_name).select("*").execute()
    
    if response.data is not None:
        df = pd.DataFrame(response.data)
        if df.empty:
            raise Exception(f"No data found in '{table_name}' table.")
        
        # Replace NaNs with 0 in the 'Quantity' column if the table is 'Inventory' - lolz got the inven code on here
        if table_name == "Inventory":
            if 'Quantity' in df.columns:
                df['Quantity'].fillna(0)

        return df
    else:
        raise Exception("Error fetching data: " + response.error_message)

def batch_insert_to_supabase(df, table_name):
    """
    Inserts a batch of rows into a Supabase table without updating existing rows.

    Parameters:
        df (pd.DataFrame): DataFrame containing the rows to insert.
        table_name (str): Name of the Supabase table.

    Returns:
        bool: True if the insertion is successful, False otherwise.
    """
    try:
        # Ensure 'Distinct_ID' or 'id' columns are correctly formatted for specific tables
        int_columns = {
            'Current_Orders': 'id', 'Current_Orders_OM': 'id', 'Shipped_Orders': 'id',
            'All_Uploaded_Orders': 'Distinct_ID', 'All_Customers': 'Distinct_ID', 'Write_Inventory': 'Distinct_ID',
            'Tracking_Visitors': 'Distinct_ID',  
            'Customer_Tracking': 'distinct_id', 
        }

        # Convert ID columns to integer type if applicable
        if table_name in int_columns and int_columns[table_name] in df.columns:
            df[int_columns[table_name]] = pd.to_numeric(df[int_columns[table_name]], errors='coerce').astype('Int64')

        # Fill missing values for numeric and non-numeric columns
        for column in df.columns:
            if df[column].dtype.kind in 'biufc':  # Numeric columns
                df[column].fillna(0)
            else:  # Non-numeric columns (strings, objects)
                df[column].fillna("")

        # orient
        data = df.to_dict(orient='records')

        if not data:
            logging.warning(f"No valid rows to insert into '{table_name}'.")
            return {"success": False, "rows_inserted": 0, "errors": "No valid data to insert"}

        # Insert instead of upsert for batch insertion (avoids updating existing rows)
        response = supabase.table(table_name).insert(data).execute()
        
        if response.data:  # Check if there is data in the response
            logging.info(f"Inserted {len(response.data)} rows into '{table_name}'.")
            return {"success": True, "rows_inserted": len(response.data), "errors": None}
        else:
            logging.error(f"Failed to insert, no data returned: {response}")
            return {"success": False, "rows_inserted": 0, "errors": "No data returned in response"}

    except Exception as e:
        logging.error(f"Error during batch insertion into '{table_name}': {str(e)}")
        return {"success": False, "rows_inserted": 0, "errors": str(e)}

# This funciton is designed to update the table if fetched using fetch_Data_from_supabase_sepcific as we're only calling certain elements of the table
def upsert_partial(df, table_name, pk):
    if pk not in df.columns:
        raise ValueError(f"Primary key '{pk}' missing from DataFrame")

    payload = df.to_dict(orient='records')

    supabase.table(table_name) \
            .upsert(payload, on_conflict=pk) \
            .execute()

def fetch_data_from_supabase_specific(table_name, columns=None, filters=None, order_by=None, limit=None):
    # Construct the select query with specified columns or all columns
    if columns:
        # Wrap problematic column names with double quotes
        columns = [f'"{col}"' if " " in col or "(" in col else col for col in columns]
        select_query = ",".join(columns)
    else:
        select_query = "*"

        
    query = supabase.table(table_name).select(select_query)

    # Apply filters if specified
    if filters:
        for column, condition in filters.items():
            if isinstance(condition, tuple) and len(condition) == 2:
                op, value = condition
                if op == 'eq':
                    query = query.eq(column, value)
                elif op == 'neq':
                    if isinstance(value, list):
                        query = query.not_in(column, value)
                    else:
                        query = query.neq(column, value)
                elif op == 'lt':
                    query = query.lt(column, value)
                elif op == 'lte':
                    query = query.lte(column, value)
                elif op == 'gt':
                    query = query.gt(column, value)
                elif op == 'gte':
                    query = query.gte(column, value)
                elif op == 'in':
                    query = query.in_(column, value)
            else:
                query = query.eq(column, condition)

    # Apply sorting if specified
    if order_by:
        query = query.order(order_by, desc=True)  # Assuming descending order for 'id'

    # Apply limit if specified
    if limit:
        query = query.limit(limit)

    # Execute the query
    response = query.execute()
    
    if response.data is not None:
        df = pd.DataFrame(response.data)

        # If DataFrame is empty, return an empty one with proper columns
        if df.empty:
            # Try to infer column names from the table structure using Supabase's metadata API
            # But for now, fallback to using the `columns` argument if provided
            if columns:
                return pd.DataFrame(columns=columns)
            else:
                return pd.DataFrame()  # Empty with unknown columns

        # Replace NaNs with 0 in the 'Quantity' column if the table is 'Inventory'
        if table_name == "Inventory" and 'Quantity' in df.columns:
            df['Quantity'].fillna(0)

        return df
    else:
        raise Exception("Error fetching data: " + response.error_message)

# Write back to the database - Sometimes we're passing a whole df, sometimes we are passing a single entry. This function is written such that  it handles both cases.
def write_table_to_supabase(df, table_name):
    """
    Accepts DataFrame, list of dicts, or a single dict.
    Writes to Supabase using upsert.
    """
    # Accept single dict or list
    if isinstance(df, dict):
        df = pd.DataFrame([df])
    elif isinstance(df, list):
        df = pd.DataFrame(df)
    elif not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame, dict, or list of dicts")

    # Table-specific id cast (if present)
    if table_name == "Report_Num_Track" and "Current_ID" in df.columns:
        df["Current_ID"] = pd.to_numeric(df["Current_ID"], errors="coerce").fillna(0).astype("Int64")
    if table_name == "Funding_Requests" and "Distinct_ID" in df.columns:
        df["Distinct_ID"] = pd.to_numeric(df["Distinct_ID"], errors="coerce").fillna(0).astype("Int64")

    # Fill numeric NaNs and non-numeric with empty string (excluding id columns intentionally)
    for column in df.columns:
        if (table_name == "Report_Num_Track" and column == "Current_ID") or (table_name == "Funding_Requests" and column == "Distinct_ID"):
            continue
        if df[column].dtype.kind in "biufc":
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
        else:
            df[column] = df[column].fillna("")

    data = df.to_dict(orient="records")
    response = supabase.table(table_name).upsert(data).execute()

    resp_error = getattr(response, "error", None) or getattr(response, "error_message", None)
    if resp_error:
        raise Exception(f"Error writing to '{table_name}': {resp_error}")

    logging.info(f"Successfully wrote {len(data)} records to '{table_name}'.")

# This funciton is designed to update the table if fetched using fetch_Data_from_supabase_sepcific as we're only calling certain elements of the table
def upsert_partial(df, table_name, pk):
    if pk not in df.columns:
        raise ValueError(f"Primary key '{pk}' missing from DataFrame")

    payload = df.to_dict(orient='records')

    supabase.table(table_name) \
            .upsert(payload, on_conflict=pk) \
            .execute()
    
def get_next_id_from_supabase_compatible_all(name, column): # getting the next id
    """
    Retrieve the last ID from a specified column in the Supabase table and return the incremented value.

    Parameters:
    - name (str): The name of the Supabase table.
    - column (str): The column name to fetch the last ID from.

    Returns:
    - int: The next available ID.
    """
    try:
        response = supabase.table(name).select(column).order(column, desc=True).limit(1).execute()
        
        if not response.data:
            # If no data is found, start with 1 as the initial ID
            return 1
        
        # Extract the last ID
        last_id = int(response.data[0][column])
        
        # Increment the ID
        next_id = last_id + 1
        
        return next_id
    
    except Exception as e:
        print(f"Error fetching the next ID from Supabase: {e}")
        raise
    
########### --------------- THE ULTIMATE FUNCTION ---------------- #######################
def map_skus(df):
    '''
    This function is the function that maps all the OLD SKUs from olden orders to our current SKU Scheme.

    '''
    # Clean the df
    df['sku'] = df['sku'].str.strip().astype(str)
    # The functions to fetch and clean
    def clean_str_column(df, column):
        df[column] = df[column].astype(str).str.strip()
        return df

    def fetch_and_clean(table, rename_map=None):
            data = fetch_data_from_supabase(table)
            for col in data.columns:
                data = clean_str_column(data, col)
            if rename_map:
                data.rename(columns=rename_map, inplace=True)
            return data
    

    # Get the data from the database
    mappings = fetch_and_clean("SKU_Mappings")
    
    # Get the sku dbs
    og_prods = fetch_and_clean("Generated_Skus")
    old_prods = fetch_and_clean("Generated_Skus_OLD")
    
    # Filter out oman
    og_packages = fetch_data_from_supabase_specific("Package_Skus",filters={'Region': ('neq', ['OM'])})
    og_packages['Indication'] = og_packages['Indication'].astype(str).str.strip()
    og_packages['Package SKU'] = og_packages['Package SKU'].str.strip().astype(str)

    # Merge products + packages
    all_sku_df = pd.concat([og_prods, old_prods, og_packages], ignore_index=True)
    all_sku_df['SKU'] = all_sku_df['SKU'].str.strip().astype(str)
    all_sku_df['Indication'] = all_sku_df['Indication'].str.strip().astype(str)

    # Get the df entries where the sku is not in the all_sku_df
    df_filtered = df[~((df['sku'].isin(all_sku_df['Indication'])) |
                       (df['sku'].isin(all_sku_df['SKU'])))]
    ##
    if not df_filtered.empty:
        df_filtered['sku'] = df_filtered['sku'].str.strip().astype(str)

        # Map direct package SKUs to Indications prior to the dictionary mapping
        extended_skus = df_filtered[df_filtered['sku'].isin(og_packages['Package SKU'])]
        if not extended_skus.empty:
            # Create a mapping from Package SKU to Indication
            direct_package_map = dict(zip(og_packages['Package SKU'], og_packages['Indication']))

            # Apply this mapping directly to df using .loc
            df.loc[df['sku'].isin(direct_package_map.keys()), 'sku'] = \
                df['sku'].map(direct_package_map).fillna(df['sku'])

        # Fix associated_skus column in mappings to be actual lists
        mappings['Associated_SKUs'] = mappings['Associated_SKUs'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        # Build the replacement dictionary
        replacement_dict = {
            associated_sku: row['Actual_SKU']
            for _, row in mappings.iterrows()
            for associated_sku in row['Associated_SKUs']
        }

        # Apply replacement_dict to the full df (not just df_filtered)
        df['sku'] = df['sku'].map(lambda x: replacement_dict.get(x, x))
    
    return df

def get_tracking_customers_df():
    try:
        sync_customer_tracking_unified()

        data = supabase.table("Customer_Tracking").select("*").execute().data or []
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # updated_at -> Dubai tz safely
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
            uae_tz = pytz.timezone("Asia/Dubai")

            def to_uae(x):
                if pd.isna(x):
                    return pd.NaT
                try:
                    if x.tzinfo:
                        return x.tz_convert(uae_tz)
                    else:
                        return x.tz_localize("UTC").tz_convert(uae_tz)
                except Exception:
                    # fallback: try parsing as string
                    try:
                        xr = pd.to_datetime(str(x), errors="coerce")
                        if pd.isna(xr):
                            return pd.NaT
                        return xr.tz_localize("UTC").tz_convert(uae_tz)
                    except:
                        return pd.NaT

            df["updated_at"] = df["updated_at"].apply(to_uae)

        # helper to parse JSON strings or passthrough dict/list
        def parse_json_or_passthrough(x, expect_list=False):
            if isinstance(x, (dict, list)):
                return x
            if isinstance(x, str):
                try:
                    parsed = json.loads(x)
                    # ensure list if expected
                    if expect_list and not isinstance(parsed, list):
                        return []
                    return parsed
                except:
                    return [] if expect_list else {}
            return [] if expect_list else {}

        # customer_info
        if "customer_info" in df.columns:
            df["customer_info"] = df["customer_info"].apply(lambda x: parse_json_or_passthrough(x, expect_list=False))

        # visitor_ids
        if "visitor_ids" in df.columns:
            df["visitor_ids"] = df["visitor_ids"].apply(lambda x: parse_json_or_passthrough(x, expect_list=False))

        # campaigns & campaign_source
        for col in ["campaigns", "campaign_source"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: parse_json_or_passthrough(x, expect_list=True))

        # numeric fields
        for num_col in ["add_to_cart", "purchases", "distinct_id"]:
            if num_col not in df.columns:
                df[num_col] = 0
            else:
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce").fillna(0).astype(int)

        return df

    except Exception as e:
        logging.exception(f"⚠️ Error syncing or fetching Customer_Tracking: {e}")
        return pd.DataFrame()

def sync_customer_tracking_unified():
    """
    Unified sync for Customer_Tracking with verbose debug logging.
    - Groups by Visitor_ID to capture add_to_cart before login.
    - Detects assisted purchases and credits +0.5 to the add_to_cart campaign/source.
    - Merges with existing Customer_Tracking by customer_id or any visitor_id overlap.
    - Preserves customer_info unless new non-empty info is present.
    Extensive logging added after each step for debugging.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Get a logger instance
    logger = logging.getLogger(__name__)
    dubai_tz = pytz.timezone("Asia/Dubai")

    def now_dubai():
        return datetime.now(dubai_tz).strftime("%Y-%m-%d %H:%M:%S")

    def parse_event_details(event_details):
        """
        Parse Event_Details that might be single-quoted (Python dict style)
        or valid JSON. Returns dict or {}.
        """
        if not event_details:
            return {}
        if isinstance(event_details, dict):
            return event_details
        try:
            # First try valid JSON
            return json.loads(event_details)
        except Exception:
            try:
                # Fallback: try to safely evaluate single-quoted dict
                return ast.literal_eval(event_details)
            except Exception:
                logger.info("[WARN] Could not parse Event_Details: %s", event_details)
                return {}

    def extract_product_name(event_details, event_type):
        """Extract product name(s) from Event_Details JSON or dict."""
        try:
            details = parse_event_details(event_details)
            if not details:
                return []

            if event_type == "purchase":
                if "products_name" in details:
                    return [str(details["products_name"]).strip()]
                if "products" in details and isinstance(details["products"], list):
                    names = [str(p["product_name"]).strip() for p in details["products"] if isinstance(p, dict) and p.get("product_name")]
                    return names
            if event_type == "add_to_cart":
                if "name" in details:
                    return [str(details["name"]).strip()]
                if "product_name" in details:
                    return [str(details["product_name"]).strip()]
            return []
        except Exception as ex:
            logger.info("extract_product_name error: %s | raw=%s", ex, event_details)
            return []

    try:
        logger.info("START sync_customer_tracking_unified()")

        # 1️⃣ last synced distinct_id
        existing_latest = (
            supabase.table("Customer_Tracking")
            .select("distinct_id")
            .order("distinct_id", desc=True)
            .limit(1)
            .execute()
            .data or []
        )
        last_distinct_id = int(existing_latest[0].get("distinct_id") or 0) if existing_latest else 0
        logger.info("Last synced distinct_id = %s", last_distinct_id)

        # 2️⃣ Fetch new visitor events
        visitors = fetch_data_from_supabase_specific(
            "Tracking_Visitors",
            columns=[
                "Distinct_ID", "Customer_ID", "Visitor_ID", "Event_Type",
                "Visited_at", "UTM_Campaign", "UTM_Source", "Event_Details",
                "Customer_Name", "Customer_Email", "Customer_Mobile"
            ],
            filters={
                "Distinct_ID": ("gt", last_distinct_id),
                "Event_Type": ("in", ["purchase", "add_to_cart"])
            }
        )

        logger.info("Fetched visitors shape: %s", None if visitors is None else getattr(visitors, "shape", str(visitors)))
        if visitors is None or visitors.empty:
            logger.info("No new visitors to sync. Exiting.")
            return {"synced_customers": 0, "last_sync": now_dubai(), "last_distinct_id": last_distinct_id}

        logger.info("Sample visitors (first 5 rows):\n%s", visitors.head(5).to_dict(orient="records"))

        # 3️⃣ Normalize timestamps and extract product names
        visitors["Visited_at"] = pd.to_datetime(visitors["Visited_at"], errors="coerce", utc=True).dt.tz_convert(dubai_tz)
        visitors["Customer_ID"] = visitors["Customer_ID"].replace("", None)
        visitors["Visitor_ID"] = visitors["Visitor_ID"].astype(str)
        visitors["product_names"] = visitors.apply(lambda r: extract_product_name(r.get("Event_Details"), r.get("Event_Type")), axis=1)

        logger.info("After normalization - sample with product_names:\n%s", visitors[["Distinct_ID", "Visitor_ID", "Customer_ID", "UTM_Campaign", "Event_Type", "product_names"]].to_dict(orient="records"))

        # 4️⃣ Group by Visitor_ID
        grouped_data = []
        for visitor_id, group in visitors.groupby("Visitor_ID"):
            customer_id = group["Customer_ID"].dropna().iloc[0] if group["Customer_ID"].notna().any() else None
            visitor_ids = list(group["Visitor_ID"].unique())
            campaigns_summary = {}  # campaign -> dict
            sources_summary = {}    # source -> dict

            purchases = group[group["Event_Type"] == "purchase"]
            add_to_carts = group[group["Event_Type"] == "add_to_cart"]

            # Normal counts (use floats for purchases so +0.5 is allowed)
            for _, row in group.iterrows():
                evt = row["Event_Type"]
                camp = str(row.get("UTM_Campaign") or "Direct")
                src = str(row.get("UTM_Source") or "Direct")
                key = "purchases" if evt == "purchase" else "add_to_cart"

                campaigns_summary.setdefault(camp, {"campaign": camp, "purchases": 0.0, "add_to_cart": 0})
                sources_summary.setdefault(src, {"source": src, "purchases": 0.0, "add_to_cart": 0})

                # increment as float for purchases (so assisted +0.5 works)
                if key == "purchases":
                    campaigns_summary[camp]["purchases"] += 1.0
                    sources_summary[src]["purchases"] += 1.0
                else:
                    campaigns_summary[camp]["add_to_cart"] += 1
                    sources_summary[src]["add_to_cart"] += 1

            logger.info(
                "Visitor group %s: purchases=%s add_to_cart=%s campaigns=%s",
                visitor_id,
                int(purchases.shape[0]),
                int(add_to_carts.shape[0]),
                list(campaigns_summary.keys())
            )

            # Assisted purchase logic: find purchases and prior add_to_cart with same product but different campaign
            assisted_count = 0.0
            for _, p_row in purchases.iterrows():
                purchase_products = set([pn for pn in (p_row.get("product_names") or []) if pn])
                purchase_campaign = str(p_row.get("UTM_Campaign") or "Direct")
                purchase_source = str(p_row.get("UTM_Source") or "Direct")

                logger.info("Evaluating purchase distinct_id=%s products=%s campaign=%s",
                             p_row.get("Distinct_ID"), purchase_products, purchase_campaign)

                # skip if purchase_products empty
                if not purchase_products:
                    logger.info("No product names found for purchase row %s — skipping assisted logic for this purchase.", p_row.get("Distinct_ID"))
                    continue

                for _, a_row in add_to_carts.iterrows():
                    add_campaign = str(a_row.get("UTM_Campaign") or "Direct")
                    add_source = str(a_row.get("UTM_Source") or "Direct")
                    add_products = set([pn for pn in (a_row.get("product_names") or []) if pn])

                    if not add_products:
                        continue

                    # same product(s) and different campaign => assisted
                    common = purchase_products & add_products
                    logger.info("common :: -----------",common)
                    if common and add_campaign != purchase_campaign:
                        product_match = ", ".join(common)
                        assisted_count += 0.5
                        logger.info(
                            "[ASSISTED_PURCHASE] visitor=%s purchase_distinct=%s product='%s' add_campaign='%s' -> purchase_campaign='%s'",
                            visitor_id, p_row.get("Distinct_ID"), product_match, add_campaign, purchase_campaign
                        )
                        # credit +0.5 to the add_campaign purchases (assisted)
                        campaigns_summary.setdefault(add_campaign, {"campaign": add_campaign, "purchases": 0.0, "add_to_cart": 0})
                        sources_summary.setdefault(add_source, {"source": add_source, "purchases": 0.0, "add_to_cart": 0})

                        campaigns_summary[add_campaign]["purchases"] += 0.5
                        sources_summary[add_source]["purchases"] += 0.5

            if assisted_count:
                logger.info("Applied assisted credit %.2f for visitor %s", assisted_count, visitor_id)

            grouped_data.append({
                "distinct_id": int(last_distinct_id),
                "customer_id": customer_id,
                "visitor_ids": visitor_ids,
                "campaigns": list(campaigns_summary.values()),
                "campaign_source": list(sources_summary.values()),
                "purchases": int(group["Event_Type"].eq("purchase").sum()),   # overall integer count for purchases
                "add_to_cart": int(group["Event_Type"].eq("add_to_cart").sum()),
                "last_visit": group["Visited_at"].max().strftime("%Y-%m-%dT%H:%M:%S"),
                "updated_at": now_dubai(),
                "customer_info": {
                    "name": group["Customer_Name"].dropna().iloc[0] if group["Customer_Name"].notna().any() else "",
                    "email": group["Customer_Email"].dropna().iloc[0] if group["Customer_Email"].notna().any() else "",
                    "mobile": int(group["Customer_Mobile"].dropna().iloc[0]) if group["Customer_Mobile"].notna().any() else ""
                }
            })

        logger.info("Grouped data count: %s", len(grouped_data))
        logger.info("Sample grouped_data (first 5): %s", grouped_data[:5])

        new_df = pd.DataFrame(grouped_data)
        if new_df.empty:
            logger.info("No grouped rows created — exiting.")
            return {"synced_customers": 0, "last_sync": now_dubai(), "last_distinct_id": last_distinct_id}

        # 5️⃣ Find existing matching customers by customer_id or visitor overlap
        all_customer_ids = [cid for cid in new_df["customer_id"].dropna().unique() if cid]
        all_visitor_ids = list(itertools.chain.from_iterable(new_df["visitor_ids"].tolist()))
        logger.info("All customer_ids to lookup: %s", all_customer_ids)
        logger.info("All visitor_ids to lookup (count): %s", len(all_visitor_ids))

        filters = {}
        if all_customer_ids:
            filters["customer_id"] = ("in", all_customer_ids)
        if all_visitor_ids:
            # 'overlaps' may be Postgres array operator — adapt to your fetch function implementation.
            filters["visitor_ids"] = ("overlaps", list(set(all_visitor_ids)))

        existing = fetch_data_from_supabase_specific("Customer_Tracking", filters=filters) if filters else pd.DataFrame()
        logger.info("Existing lookup rows fetched: %s", 0 if existing is None else (existing.shape if hasattr(existing, "shape") else "Direct"))

        # Build lookup maps
        customer_lookup = {}
        visitor_lookup = {}
        if existing is not None and not existing.empty:
            for _, row in existing.iterrows():
                cid = row.get("customer_id")
                row_visitors = json.loads(row["visitor_ids"]) if isinstance(row["visitor_ids"], str) else row["visitor_ids"]
                if cid:
                    customer_lookup[cid] = row
                for v in row_visitors:
                    visitor_lookup[v] = row
            logger.info("customer_lookup size=%s visitor_lookup size=%s", len(customer_lookup), len(visitor_lookup))
        else:
            logger.info("No existing matching rows found.")

        # 6️⃣ Merge new rows with existing rows
        merged_records_dict = {}
        for _, new_row in new_df.iterrows():
            new_visitors = set(new_row["visitor_ids"])
            customer_id = new_row.get("customer_id")

            matched_row = None
            if customer_id and customer_id in customer_lookup:
                matched_row = customer_lookup[customer_id]
                logger.info("Matched by customer_id=%s", customer_id)
            else:
                # Try any visitor match
                for v in new_visitors:
                    if v in visitor_lookup:
                        matched_row = visitor_lookup[v]
                        logger.info("Matched by visitor_id=%s (customer_id=%s)", v, matched_row.get("customer_id"))
                        break

            # Use key based on customer_id if present, otherwise first visitor_id
            key = customer_id if customer_id else next(iter(new_visitors), None)

            if key in merged_records_dict:
                # incremental merge into already accumulated merged record
                rec = merged_records_dict[key]
                rec_visitors_before = set(rec["visitor_ids"])
                rec["visitor_ids"] = sorted(set(rec["visitor_ids"]) | new_visitors)

                rec["purchases"] = int(rec.get("purchases", 0)) + int(new_row.get("purchases", 0))
                rec["add_to_cart"] = int(rec.get("add_to_cart", 0)) + int(new_row.get("add_to_cart", 0))

                # merge campaign/source dicts
                for c in new_row.get("campaigns", []):
                    cd = rec["campaigns_dict"].setdefault(c["campaign"], {"campaign": c["campaign"], "purchases": 0.0, "add_to_cart": 0})
                    cd["purchases"] += float(c.get("purchases", 0) or 0.0)
                    cd["add_to_cart"] += int(c.get("add_to_cart", 0) or 0)

                for s in new_row.get("campaign_source", []):
                    sd = rec["sources_dict"].setdefault(s["source"], {"source": s["source"], "purchases": 0.0, "add_to_cart": 0})
                    sd["purchases"] += float(s.get("purchases", 0) or 0.0)
                    sd["add_to_cart"] += int(s.get("add_to_cart", 0) or 0)

                # keep latest last_visit
                rec["last_visit"] = max(rec["last_visit"], new_row["last_visit"])
                rec["updated_at"] = now_dubai()
                logger.info("Merged into existing merged_records_dict[%s]: visitors before=%s after=%s",
                             key, rec_visitors_before, rec["visitor_ids"])
            else:
                # start a new merged record (seed from matched_row if exists)
                if matched_row is not None:
                    # seed from existing DB row
                    seed_visitors = json.loads(matched_row["visitor_ids"]) if isinstance(matched_row["visitor_ids"], str) else matched_row["visitor_ids"]
                    seed_campaigns = json.loads(matched_row["campaigns"]) if isinstance(matched_row.get("campaigns"), str) else matched_row.get("campaigns") or []
                    seed_sources = json.loads(matched_row["campaign_source"]) if isinstance(matched_row.get("campaign_source"), str) else matched_row.get("campaign_source") or []

                    seed_purchases = int(matched_row.get("purchases", 0) or 0)
                    seed_add_to_cart = int(matched_row.get("add_to_cart", 0) or 0)
                    seed_last_visit = str(matched_row.get("last_visit") or new_row["last_visit"])

                    campaigns_dict = {c["campaign"]: {"campaign": c["campaign"], "purchases": float(c.get("purchases", 0) or 0.0), "add_to_cart": int(c.get("add_to_cart", 0) or 0)} for c in seed_campaigns}
                    sources_dict = {s["source"]: {"source": s["source"], "purchases": float(s.get("purchases", 0) or 0.0), "add_to_cart": int(s.get("add_to_cart", 0) or 0)} for s in seed_sources}

                    # add new_row campaign/source values into dicts
                    for c in new_row.get("campaigns", []):
                        cd = campaigns_dict.setdefault(c["campaign"], {"campaign": c["campaign"], "purchases": 0.0, "add_to_cart": 0})
                        cd["purchases"] += float(c.get("purchases", 0) or 0.0)
                        cd["add_to_cart"] += int(c.get("add_to_cart", 0) or 0)

                    for s in new_row.get("campaign_source", []):
                        sd = sources_dict.setdefault(s["source"], {"source": s["source"], "purchases": 0.0, "add_to_cart": 0})
                        sd["purchases"] += float(s.get("purchases", 0) or 0.0)
                        sd["add_to_cart"] += int(s.get("add_to_cart", 0) or 0)

                    merged_records_dict[key] = {
                        "distinct_id": int(max(new_row.get("distinct_id", 0), int(matched_row.get("distinct_id", 0) or 0))),
                        "customer_id": matched_row.get("customer_id") or customer_id,
                        "visitor_ids": sorted(set(seed_visitors) | new_visitors),
                        "campaigns_dict": campaigns_dict,
                        "sources_dict": sources_dict,
                        "purchases": seed_purchases + int(new_row.get("purchases", 0)),
                        "add_to_cart": seed_add_to_cart + int(new_row.get("add_to_cart", 0)),
                        "last_visit": max(seed_last_visit, new_row["last_visit"]),
                        "updated_at": now_dubai(),
                        "customer_info": json.loads(matched_row["customer_info"]) if matched_row.get("customer_info") else new_row.get("customer_info") or {"name": "", "email": "", "mobile": ""}
                    }
                    logger.info("Seeded merged record from existing matched_row for key=%s", key)
                else:
                    # brand new record (no matched_row)
                    campaigns_dict = {c["campaign"]: {"campaign": c["campaign"], "purchases": float(c.get("purchases", 0) or 0.0), "add_to_cart": int(c.get("add_to_cart", 0) or 0)} for c in new_row.get("campaigns", [])}
                    sources_dict = {s["source"]: {"source": s["source"], "purchases": float(s.get("purchases", 0) or 0.0), "add_to_cart": int(s.get("add_to_cart", 0) or 0)} for s in new_row.get("campaign_source", [])}

                    merged_records_dict[key] = {
                        "distinct_id": int(new_row.get("distinct_id", 0)),
                        "customer_id": customer_id,
                        "visitor_ids": sorted(new_row["visitor_ids"]),
                        "campaigns_dict": campaigns_dict,
                        "sources_dict": sources_dict,
                        "purchases": int(new_row.get("purchases", 0)),
                        "add_to_cart": int(new_row.get("add_to_cart", 0)),
                        "last_visit": new_row["last_visit"],
                        "updated_at": now_dubai(),
                        "customer_info": new_row.get("customer_info") or {"name": "", "email": "", "mobile": ""}
                    }
                    logger.info("Created new merged record for key=%s", key)

        logger.info("Before finalize merged_records_dict size=%s sample keys=%s", len(merged_records_dict), list(merged_records_dict.keys())[:5])

        # 7️⃣ Finalize merged records (convert dicts to lists)
        final_rows = []
        for key, rec in merged_records_dict.items():
            campaigns_list = list(rec.pop("campaigns_dict").values())
            sources_list = list(rec.pop("sources_dict").values())

            final = {
                "distinct_id": int(rec.get("distinct_id", 0)),
                "customer_id": rec.get("customer_id"),
                "visitor_ids": rec.get("visitor_ids"),
                "campaigns": campaigns_list,
                "campaign_source": sources_list,
                "purchases": int(rec.get("purchases", 0)),
                "add_to_cart": int(rec.get("add_to_cart", 0)),
                "last_visit": rec.get("last_visit"),
                "updated_at": rec.get("updated_at"),
                "customer_info": rec.get("customer_info") or {"name": "", "email": "", "mobile": ""}
            }
            final_rows.append(final)

        merged_df = pd.DataFrame(final_rows)
        logger.info("After finalize merged_df shape=%s", merged_df.shape)
        logger.info("Merged sample (first row): %s", merged_df.head(1).to_dict(orient="records"))

        # 8️⃣ JSON-serialize columns safely
        def safe_dump(x):
            try:
                return json.dumps(x)
            except Exception:
                return json.dumps(str(x))

        for col in ["visitor_ids", "campaigns", "campaign_source", "customer_info"]:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].apply(safe_dump)

        # 9️⃣ Upsert to Supabase (by customer_id)
        if not merged_df.empty:
            records = merged_df.to_dict("records")
            logger.info("Upserting %s records to Customer_Tracking", len(records))
            supabase.table("Customer_Tracking").upsert(records, on_conflict="visitor_ids").execute()
            logger.info("✅ Upsert complete for %s records", len(records))
        else:
            logger.info("No merged rows to upsert.")

        return {"synced_customers": len(merged_df), "last_sync": now_dubai(), "last_distinct_id": int(new_df["distinct_id"].max())}

    except Exception as e:
        logger.exception("[SYNC][ERROR] %s", e)
        return None
