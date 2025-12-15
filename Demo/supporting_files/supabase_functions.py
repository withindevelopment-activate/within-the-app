import os, json, ast, logging, pandas as pd, pytz, uuid, re
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from django.core.cache import cache
import itertools
from Demo.supporting_files.supporting_functions import get_uae_current_date
import requests
from django.contrib import messages

# Import keys
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)

## yet to be created but to avoid import error
def sync_customer_tracking_incremental():
    return

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
        "tiktok_org": token_row.get("Tiktok_Org"),
        "meta_access": token_row.get("Meta_Access"),
        "long_snapchat_token": token_row.get("Snapchat_long_term_Access")
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
                df['Quantity'].fillna(0, inplace=True)

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
            'All_Uploaded_Orders': 'Distinct_ID', 'All_Customers': 'Distinct_ID', 'Write_Inventory': 'Distinct_ID'
        }

        # Convert ID columns to integer type if applicable
        if table_name in int_columns and int_columns[table_name] in df.columns:
            df[int_columns[table_name]] = pd.to_numeric(df[int_columns[table_name]], errors='coerce').astype('Int64')

        # Fill missing values for numeric and non-numeric columns
        for column in df.columns:
            if df[column].dtype.kind in 'biufc':  # Numeric columns
                df[column].fillna(0, inplace=True)
            else:  # Non-numeric columns (strings, objects)
                df[column].fillna("", inplace=True)

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
            df['Quantity'].fillna(0, inplace=True)

        return df
    else:
        raise Exception("Error fetching data: " + response.error_message)

# Write back to the database - Sometimes we're passing a whole df, sometimes we are passing a single entry. This function is written such that  it handles both cases.
def write_table_to_supabase(df, table_name): # - Handles the DF passing

        # Ensure 'id' column is converted to int8 if it exists
    if table_name == 'Report_Num_Track' and 'Current_ID' in df.columns:
        df['Current_ID'] = df['Current_ID'].astype('int64', errors='ignore')

    if table_name == 'Funding_Requests' and 'Distinct_ID' in df.columns:
        df['Distinct_ID'] = df['Distinct_ID'].astype('int64', errors='ignore')

    # Fill numeric columns with 0 and non-numeric columns with empty string,
    # but exclude the 'id' column
    for column in df.columns:
        if (table_name == 'Report_Num_Track' and column == 'Current_ID') or (table_name == 'Funding_Requests' and column == 'Distinct_ID'):
            continue  # Skip the 'id' column
        if df[column].dtype.kind in 'biufc':  # If the column is numeric (b: boolean, i: integer, u: unsigned integer, f: float, c: complex float)
            df[column].fillna(0, inplace=True)  # Replace NaN with 0
        else:
            df[column].fillna("", inplace=True)  # Replace NaN with an empty string

    data = df.to_dict(orient='records')
    response = supabase.table(table_name).upsert(data).execute()

    if response.data is not None:
        logging.info(f"Successfully wrote DataFrame to '{table_name}' table.")
    else:
        raise Exception(f"Error writing DataFrame to Supabase table '{table_name}': {response.error_message}")

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

'''
#### The old syncing fucntion -----

def sync_customer_tracking_unified():
    """
    Unified sync for Customer_Tracking with verbose debug logging.
    - Groups by Visitor_ID to capture add_to_cart before login.
    - Detects assisted purchases and credits +0.5 to the add_to_cart campaign/source.
    - Merges with existing Customer_Tracking by customer_id or any visitor_id overlap.
    - Preserves customer_info unless new non-empty info is present.
    Extensive logging added after each step for debugging.
    """

    # 1️ Fetch existing Customer_Tracking
    existing_customers = supabase.table("Customer_Tracking").select("*").execute().data
    existing_df = pd.DataFrame(existing_customers) if existing_customers else pd.DataFrame()

    last_updated = None
    if not existing_df.empty and "updated_at" in existing_df.columns:
        last_updated = pd.to_datetime(existing_df["updated_at"]).max()

    # 2️ Fetch new or all Tracking_Visitors rows
    filters = {"Visited_at": ("gt", last_updated.isoformat())} if last_updated else None
    df = fetch_data_from_supabase_specific(
        "Tracking_Visitors",
        columns=[
            "Customer_ID", "Customer_Email", "Customer_Mobile", "Customer_Name",
            "Visitor_ID", "Session_ID", "Event_Type", "UTM_Campaign",
            "UTM_Source", "Visited_at",
        ],
        filters=filters,
    )

    if df.empty:
        print("No new tracking data to sync.")
        return

    # 3️ Normalize identifiers
    for col in ["Customer_ID", "Customer_Email", "Customer_Mobile", "Visitor_ID", "Session_ID", "UTM_Campaign", "UTM_Source"]:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").str.strip().replace("nan", "")

    df["Customer_Email"] = df["Customer_Email"].str.lower()
    df["UTM_Campaign"] = df["UTM_Campaign"].str.title().replace("+", " ", regex=False)
    df["UTM_Source"] = df["UTM_Source"].str.title().replace("+", " ", regex=False)
    df["Visited_at"] = pd.to_datetime(df["Visited_at"], errors="coerce")

    # 4️ Create unified customer_key
    df["customer_key"] = (
        df["Customer_ID"].replace("", pd.NA)
        .fillna(df["Customer_Email"])
        .replace("", pd.NA)
        .fillna(df["Customer_Mobile"])
    )
    df = df.dropna(subset=["customer_key"])

    # 5️ Build visitor → session mapping
    visitor_session_map = df.groupby("Visitor_ID")["Session_ID"].unique().apply(list).to_dict()

    # 6️ Aggregate new data
    agg_df = (
        df.groupby("customer_key")
        .agg(
            customer_name=("Customer_Name", lambda x: x.dropna().iloc[-1] if len(x.dropna()) else ""),
            visitor_ids=("Visitor_ID", lambda v: {vid: visitor_session_map.get(vid, []) for vid in v.dropna().unique()}),
            add_to_cart=("Event_Type", lambda x: (x == "add_to_cart").sum()),
            purchases=("Event_Type", lambda x: (x == "purchase").sum()),
            campaigns=("UTM_Campaign", lambda x: list(set(x.dropna()) - {""})),
            sources=("UTM_Source", lambda x: list(set(x.dropna()) - {""})),
            updated_at=("Visited_at", "max"),
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
 
        # Normalize timestamps and extract product names
        visitors["Visited_at"] = pd.to_datetime(visitors["Visited_at"], errors="coerce", utc=True).dt.tz_convert(dubai_tz)
        visitors["Customer_ID"] = visitors["Customer_ID"].replace("", None)
        visitors["Visitor_ID"] = visitors["Visitor_ID"].str.strip().astype(str)
        visitors["product_names"] = visitors.apply(lambda r: extract_product_name(r.get("Event_Details"), r.get("Event_Type")), axis=1)
 
        logger.info("After normalization - sample with product_names:\n%s", visitors[["Distinct_ID", "Visitor_ID", "Customer_ID", "UTM_Campaign", "Event_Type", "product_names"]].to_dict(orient="records"))
 
        # 4 -- Group by Visitor_ID
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
            # Assisted purchase logic with percentage normalization
            assisted_summary = {}  # to track contribution percentages per campaign

            for _,addto_row in add_to_carts.iterrows():
                add_to_cart_products = set([an for an in (addto_row.get("product_names") or []) if an])
                add_to_cart_campaign = str(addto_row.get("UTM_Campaign") or "Direct")
                add_to_cart_source = str(addto_row.get("UTM_Source") or "Direct")

    # 7️ Merge with existing data (incremental)
    if not existing_df.empty:
        existing_df.set_index("customer_key", inplace=True)
        agg_df.set_index("customer_key", inplace=True)

                # skip if product_names empty
                if not purchase_products:
                    logger.info("No product names found for purchase row %s — skipping.", p_row.get("Distinct_ID"))
                    continue

                # find all add_to_cart rows with same product(s)
                logger.info("purchase_products = %s add_to_cart_products = %s",purchase_products,add_to_cart_products)
                add_rows = add_to_carts[
                    add_to_carts["product_names"].apply(
                        lambda x: bool(
                            (set(p.strip() for p in x.split(",") if p.strip()) 
                            if isinstance(x, str) 
                            else set(p for p in (x or []) if p)
                            ) 
                            & purchase_products
                        )
                    )
                ]
                add_campaigns = set(add_rows["UTM_Campaign"].fillna("Direct"))
                logger.info("add_campaigns = %s add_rows = %s" , add_campaigns,add_rows)

                # CASE 1 -- same campaign (direct purchase from same source)
                if purchase_campaign in add_campaigns or not add_campaigns:
                    # 100% credit to this campaign
                    # assisted_summary[purchase_campaign] = assisted_summary.get(purchase_campaign, 0) + 1.0
                    logger.info("[FULL CREDIT] %s got 100%% for purchase %s", purchase_campaign, p_row.get("Distinct_ID"))
                else:
                    # CASE 2 -- assisted by other campaigns
                    num_assists = len(add_campaigns)
                    assist_share = 0.5  # 50% split across assist campaigns
                    purchase_share = 0.5              # 50% to the final campaign

                    # distribute shares
                    assisted_summary[purchase_campaign] = assisted_summary.get(purchase_campaign, 0) + purchase_share
                    for ac in add_campaigns:
                        assisted_summary[ac] = assisted_summary.get(ac, 0) + assist_share

                    logger.info(
                        "[ASSISTED] %s got %.2f%% (final) and %s got %.2f%% each (assist) for purchase %s",
                        purchase_campaign, purchase_share * 100, add_campaigns, assist_share * 100, p_row.get("Distinct_ID")
                    )

            # Store normalized contributions into campaigns_summary
            for camp, share in assisted_summary.items():
                campaigns_summary.setdefault(camp, {"campaign": camp, "purchases": 0.0, "add_to_cart": 0})
                campaigns_summary[camp]["purchases"] += share  # share sums up to 1.0 per purchase

 
            grouped_data.append({
                "distinct_id": int(group["Distinct_ID"].max()),
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

    # 8️ Generate new distinct_ids for new customers
    if "distinct_id" not in agg_df.columns:
        agg_df["distinct_id"] = None
    agg_df["distinct_id"] = agg_df["distinct_id"].fillna([str(uuid.uuid4()) for _ in range(len(agg_df))])

    # 9️ Upsert in chunks
    records = agg_df.to_dict(orient="records")
    BATCH_SIZE = 1000
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            supabase.table("Customer_Tracking").upsert(batch, on_conflict="customer_key").execute()
        except Exception as e:
            print(f"Batch {i//BATCH_SIZE+1} failed: {e}")

    print(f"✅ Synced {len(agg_df)} customers into Customer_Tracking (unified incremental).")
    

def get_tracking_customers_df():
    """
    Runs an incremental sync (sync_customer_tracking_unified) 
    and then fetches fresh data from 'Customer_Tracking'.
    Returns a cleaned DataFrame ready for dashboard use.
    """
    try:
        # 1️⃣ Run incremental sync first
         #sync_customer_tracking_unified()

        # 2️⃣ Fetch fresh customer tracking data
        data = supabase.table("Customer_Tracking").select("*").execute().data
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # 3️⃣ Clean and normalize
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
            uae_tz = pytz.timezone("Asia/Dubai")
            df["updated_at"] = df["updated_at"].dt.tz_localize("UTC", nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert(uae_tz)

        for col in ["customer_key", "customer_name"]:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna("").replace("nan", "")

        # JSON-like fields normalization
        if "visitor_ids" in df.columns:
            df["visitor_ids"] = df["visitor_ids"].apply(lambda x: x if isinstance(x, dict) else {})

        if "campaigns" in df.columns:
            df["campaigns"] = df["campaigns"].apply(lambda x: x if isinstance(x, list) else [])
        
        if "sources" in df.columns:
            df["sources"] = df["sources"].apply(lambda x: x if isinstance(x, list) else [])

        # Numeric fallback
        for num_col in ["add_to_cart", "purchases"]:
            if num_col not in df.columns:
                df[num_col] = 0
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

def sync_customer_tracking_unified_sarah():
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
 
        # Normalize timestamps and extract product names
        visitors["Visited_at"] = pd.to_datetime(visitors["Visited_at"], errors="coerce", utc=True).dt.tz_convert(dubai_tz)
        visitors["Customer_ID"] = visitors["Customer_ID"].replace("", None)
        visitors["Visitor_ID"] = visitors["Visitor_ID"].str.strip().astype(str)
        visitors["product_names"] = visitors.apply(lambda r: extract_product_name(r.get("Event_Details"), r.get("Event_Type")), axis=1)
 
        logger.info("After normalization - sample with product_names:\n%s", visitors[["Distinct_ID", "Visitor_ID", "Customer_ID", "UTM_Campaign", "Event_Type", "product_names"]].to_dict(orient="records"))
 
        # 4 -- Group by Visitor_ID
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
            # Assisted purchase logic with percentage normalization
            assisted_summary = {}  # to track contribution percentages per campaign

            for _, p_row in purchases.iterrows():
                purchase_products = set([pn for pn in (p_row.get("product_names") or []) if pn])
                purchase_campaign = str(p_row.get("UTM_Campaign") or "Direct")
                purchase_source = str(p_row.get("UTM_Source") or "Direct")

                # skip if product_names empty
                if not purchase_products:
                    logger.info("No product names found for purchase row %s — skipping.", p_row.get("Distinct_ID"))
                    continue

                # find all add_to_cart rows with same product(s)
                add_rows = add_to_carts[add_to_carts["product_names"].apply(lambda x: bool(set(x or []) & purchase_products))]
                add_campaigns = set(add_rows["UTM_Campaign"].fillna("Direct"))

                # CASE 1 -- same campaign (direct purchase from same source)
                if purchase_campaign in add_campaigns or not add_campaigns:
                    # 100% credit to this campaign
                    assisted_summary[purchase_campaign] = assisted_summary.get(purchase_campaign, 0) + 1.0
                    logger.info("[FULL CREDIT] %s got 100%% for purchase %s", purchase_campaign, p_row.get("Distinct_ID"))
                else:
                    # CASE 2 -- assisted by other campaigns
                    num_assists = len(add_campaigns)
                    assist_share = 0.5 / num_assists  # 50% split across assist campaigns
                    purchase_share = 0.5              # 50% to the final campaign

                    # distribute shares
                    assisted_summary[purchase_campaign] = assisted_summary.get(purchase_campaign, 0) + purchase_share
                    for ac in add_campaigns:
                        assisted_summary[ac] = assisted_summary.get(ac, 0) + assist_share

                    logger.info(
                        "[ASSISTED] %s got %.2f%% (final) and %s got %.2f%% each (assist) for purchase %s",
                        purchase_campaign, purchase_share * 100, add_campaigns, assist_share * 100, p_row.get("Distinct_ID")
                    )

            # Store normalized contributions into campaigns_summary
            for camp, share in assisted_summary.items():
                campaigns_summary.setdefault(camp, {"campaign": camp, "purchases": 0.0, "add_to_cart": 0})
                campaigns_summary[camp]["purchases"] += share  # share sums up to 1.0 per purchase

 
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
        return None'''

########################################
######### Syncing both the main tracking_db and creating the sub customers db
## Function 1 -- basic syncing and updating sessions

def update_customer_tracking(tracking_df, customer_tracking_df):
    # Decalre list to store entries
    customer_entries = []
    # A list for changed ids to only send those for session percentage computation
    changed_ids = []


    # ---------------------------------------------------------
    # FAST LOOKUP SETS 
    existing_customer_ids = set(customer_tracking_df["Customer_ID"].dropna().astype(str))
    existing_visitor_ids  = set(customer_tracking_df["Visitor_ID"].dropna().astype(str))

    # record checkpoint --- greatest Distinct_ID in the main tracking df
    tracking_df['Distinct_ID'] = tracking_df['Distinct_ID'].astype(int)
    tracking_df['Visitor_ID'] = tracking_df['Visitor_ID'].str.strip().astype(str)

    last_distinct_checkpoint = (
        tracking_df["Distinct_ID"].iloc[-1]
        if len(tracking_df) > 0
        else None
    )

    # Get the latest distinct id from the customer tracking df
    distinct_id = int(get_next_id_from_supabase_compatible_all(name='Customer_Tracking', column='Distinct_ID'))

    # ---------------------------------------------------------
    # Group by Visitor_IDs and record events
    for visitor_id, group in tracking_df.groupby("Visitor_ID"):
        customer_id = group["Customer_ID"].dropna().iloc[0] if group["Customer_ID"].notna().any() else 00000
        identifier = str(customer_id or visitor_id)
        ### Now check if the visitor id or the customer_id exists in the customer_tracking_df or not
        entry_exists_in_df = (
            (identifier in existing_customer_ids) or
            (identifier in existing_visitor_ids)
        )

        # ----------------------------
        # Update sessions / totals
        # ----------------------------
        # Get the row that exists in the customers df
        if entry_exists_in_df:
            mask = (
                (customer_tracking_df["Distinct_ID"] == identifier) |
                (customer_tracking_df["Customer_ID"] == identifier)
            )
            row_index = customer_tracking_df.index[mask][0]

            # Visitor_IDs
            vis_list = customer_tracking_df.at[row_index, "Visitor_IDs"] or []
            if visitor_id not in vis_list:
                vis_list.append(visitor_id)
            customer_tracking_df.at[row_index, "Visitor_IDs"] = vis_list

            # Sessions dictionary
            sessions = customer_tracking_df.at[row_index, "Sessions"] or {}

            ### improvement
            # Here, let's call the entry from supabase -- update it, and then push it right back.

        else:
            # New entry 
            row0 = group.iloc[0]
            entry = {
                "Distinct_ID": distinct_id,
                "Customer_ID": customer_id,
                "Customer_Info": {
                    "name": row0.get("Customer_Name"),
                    "email": row0.get("Customer_Email"),
                    "mobile": row0.get("Customer_Mobile")
                },
                "Visitor_IDs": [visitor_id],
                "Add_to_Cart": 0,
                "Purchases": 0,
                "Sessions": {},
                "Updated_at": None,
                "Last_Visit": None,
                "Last_ID_Map": None,
                "Distinct_Checkpoint": 0,
                "Campaign_Contributions": ''
            }

            sessions = entry["Sessions"]

            # update lookup
            existing_customer_ids.add(identifier)
            existing_visitor_ids.add(identifier)
            identifier += 1

        # ----------------------------
        # Iterate events in this visitor group -- updating the 'sessions' dict
        # ----------------------------
        for _, row in group.iterrows():
            session_id = row["Session_ID"]
            evt = row["Event_Type"]
            details = row.get("Event_Details")
            visited_at = row["Visited_at"]
            camp = str(row.get("UTM_Campaign") or "Direct")
            src = str(row.get("UTM_Source") or "Direct")

            # Session creation
            if session_id not in sessions: ## as in, if the session id in the row for the said group does not exist in the session dict for the said entry in the customers df -- update it.
                sessions[session_id] = {
                    "date_first_seen": visited_at,
                    "date_last_seen": visited_at,
                    "campaign": camp,
                    "source": src,
                    "pageviews": 0,
                    "add_to_cart": 0,
                    "purchases": 0
                }

            s = sessions[session_id]

            # Update timestamps
            if visited_at > s["date_last_seen"]:
                s["date_last_seen"] = visited_at

            # Update session counters
            if evt == "pageview":
                s["pageviews"] += 1
            elif evt == "add_to_cart":
                s["add_to_cart"] += 1

                # --- ADD TO CART DETAILS ---
                if isinstance(details, dict):
                    s.setdefault("add_to_cart_details", []).append({
                        "id": details.get("id"),
                        "name": details.get("name"),
                        "sku": details.get("sku"),
                        "quantity": details.get("quantity"),
                    })

            elif evt == "purchase":
                s["purchases"] += 1

                # --- PURCHASE DETAILS ---
                if isinstance(details, dict):
                    order_total_raw = details.get("order_total_string")

                    # extract numeric part only (e.g. "130.00 SAR" → 130.00)
                    order_total_num = None
                    if isinstance(order_total_raw, str):
                        match = re.search(r"[\d\.]+", order_total_raw)
                        if match:
                            order_total_num = float(match.group())

                    s.setdefault("purchase_details", []).append({
                        "order_id": details.get("order_id"),
                        "order_total": order_total_num,
                        "products_name": details.get("products_name"),
                    })

            # Update total counters
            if entry_exists_in_df:
                if evt == "add_to_cart":
                    customer_tracking_df.at[row_index, "Add_to_Cart"] += 1
                elif evt == "purchase":
                    customer_tracking_df.at[row_index, "Purchases"] += 1
            else:
                if evt == "add_to_cart":
                    entry["Add_to_Cart"] += 1
                elif evt == "purchase":
                    entry["Purchases"] += 1


        # ----------------------------
        # Save back sessions n other details --
        # ----------------------------
        if entry_exists_in_df:
            customer_tracking_df.at[row_index, "Sessions"] = sessions
            customer_tracking_df.at[row_index, "Updated_at"] = get_uae_current_date()
            customer_tracking_df.at[row_index, "Last_Visit"] = visited_at
            customer_tracking_df.at[row_index, "Last_ID_Map"] = visitor_id

            ## put this ditinct id in the list
            existing_distinct = customer_tracking_df.at[row_index, "Distinct_ID"]
            if existing_distinct not in changed_ids:
                changed_ids.append(existing_distinct)
        else:
            entry["Sessions"] = sessions
            entry["Updated_at"] = get_uae_current_date()
            entry["Last_Visit"] = visited_at
            entry["Last_ID_Map"] = visitor_id
            entry["Distinct_Checkpoint"] = last_distinct_checkpoint

            # Append the entry now
            customer_entries.append(entry)
            # Add the distinct id to the changed list
            new_distinct = entry["Distinct_ID"]
            if new_distinct not in changed_ids:
                changed_ids.append(new_distinct)

    # Upsert partial the changed rows
    upsert_partial(customer_tracking_df, 'Customer_Tracking', 'Distinct_ID')
    
    if customer_entries:
        # Convert entries into a df
        entries_df = pd.DataFrame(customer_entries)
        # Batch insert the new entries.
        batch_insert_to_supabase(entries_df, 'Customer_Tracking')

    return True, changed_ids

### Function to get the tokens
def get_latest_token():
    """Fetch the most recent token from the database by highest Distinct_ID."""
    res = supabase.table("tokens") \
        .select("*") \
        .order("Distinct_ID", desc=True) \
        .limit(1) \
        .execute()

    if not res.data:
        raise ValueError("No tokens found in database")
    
    token_row = res.data[0]
    return {
        "access_token": token_row["Access"],
        "authorization_token": token_row["Authorization"],
        "refresh_token": token_row["Refresh"],
        "store_id": token_row["Store_ID"]
    }

### Supporting function to get the product sku from the name in the final purchase.
def get_item_skus(order_id):
    # Get the order from zid and get the skus, put them in a list and then return the list.
    # Fetch access token
    tokens = get_latest_token()
    authorization = tokens["authorization_token"]
    access_token = tokens['access_token']
    
    # Create the headers
    headers = {
            'Authorization': f'Bearer {authorization}',
            'X-MANAGER-TOKEN': access_token,
        }
    
    # A function to get the order -- 
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
    
    order_data = get_order_from_zid(order_id, headers)
    if not order_data:
        print("COULD NOT GET ORDER FROM ZID")
        return 
    
    products = pd.DataFrame(order_data.get("products", [])) ### Access the products in the order from the products -- 
    products['sku'] = products['sku'].str.strip().astype(str)
    sku_list = products['sku'].tolist()

    return sku_list


#### Function 2 -- the function that would calculate the contributing percentages per each product in the purchase, atc, or a mere pageview process.
def compute_campaign_percentages(sessions):
    """
    Compute campaign contribution percentages for a customer based on sessions.
    
    Args:
        sessions (dict): Customer sessions dictionary with structure:
            {
                session_id: {
                    'pageviews': int,
                    'add_to_cart': int,
                    'add_to_cart_details': [{ 'id','name','sku','quantity','campaign'}],
                    'purchases': int,
                    'purchase_details': [{ 'order_id','products_name','campaign'}],
                    ...
                },
                ...
            }

    Returns:
        dict: campaign contribution percentages per product.
    """
    campaign_contrib = {}

    # Keep track of previous purchases
    last_purchase_index = -1
    session_keys = list(sessions.keys())
    
    # Sort sessions chronologically by date_first_seen
    session_keys.sort(key=lambda s: sessions[s]["date_first_seen"])

    for idx, session_id in enumerate(session_keys):
        session = sessions[session_id]

        # Skip sessions with only pageviews if there is no ATC or purchase yet
        has_purchase = session.get("purchases", 0) > 0
        has_atc = len(session.get("add_to_cart_details", [])) > 0
        if not has_purchase and not has_atc:
            continue

        # Gather pageview campaigns
        pv_campaigns = []
        if "pageviews" in session and session["pageviews"] > 0:
            if "campaign" in session:
                pv_campaigns = [session["campaign"]]  # assume single campaign per session pageviews

        # Gather ATC campaigns
        atc_campaigns = []
        for atc in session.get("add_to_cart_details", []):
            atc_campaigns.append(atc.get("campaign"))

        # Process purchase if exists
        purchases = session.get("purchase_details", [])
        for purchase in purchases:
            purchase_campaign = purchase.get("campaign")
            order_id = purchase.get('order_id')
            #product_names = [p.strip() for p in purchase.get("products_name", "").split(",")]
            purchase_skus_list = get_item_skus(order_id)

            # Filter ATCs to only those that contributed to purchased products
            relevant_atc_campaigns = []
            for s_idx in range(last_purchase_index + 1, idx + 1):
                s = sessions[session_keys[s_idx]]
                for atc in s.get("add_to_cart_details", []):
                    # Use lookup_sku helper to match product
                    sku_of_product = str(atc["sku"].strip())
                    if sku_of_product in purchase_skus_list:
                        relevant_atc_campaigns.append(atc.get("campaign"))

            # Remove duplicates
            relevant_atc_campaigns = list(set(relevant_atc_campaigns))
            pv_campaigns_unique = list(set(pv_campaigns))

            # Assign percentages
            # 50% → purchase, 25% → ATC, 25% → pageview
            if purchase_campaign:
                campaign_contrib[purchase_campaign] = campaign_contrib.get(purchase_campaign, 0) + 50

            if relevant_atc_campaigns:
                share = 25 / len(relevant_atc_campaigns)
                for c in relevant_atc_campaigns:
                    campaign_contrib[c] = campaign_contrib.get(c, 0) + share

            if pv_campaigns_unique:
                share = 25 / len(pv_campaigns_unique)
                for c in pv_campaigns_unique:
                    campaign_contrib[c] = campaign_contrib.get(c, 0) + share

            # Update last purchase index
            last_purchase_index = idx

        # Handle sessions with ATCs but no purchase (percentage-only)
        if not purchases and has_atc:
            relevant_atc_campaigns = list(set(atc_campaigns))
            pv_campaigns_unique = list(set(pv_campaigns))

            # 50% → ATC, 50% → pageview
            if relevant_atc_campaigns:
                share = 50 / len(relevant_atc_campaigns)
                for c in relevant_atc_campaigns:
                    campaign_contrib[c] = campaign_contrib.get(c, 0) + share
            if pv_campaigns_unique:
                share = 50 / len(pv_campaigns_unique)
                for c in pv_campaigns_unique:
                    campaign_contrib[c] = campaign_contrib.get(c, 0) + share

    # Normalize contributions to sum to 100% if needed
    total = sum(campaign_contrib.values())
    if total > 0:
        for c in campaign_contrib:
            campaign_contrib[c] = round((campaign_contrib[c] / total) * 100, 2)

    return campaign_contrib


##########################
##########################
##### Main Syncing Function -- 

def sync_customers():
    ## Get the databases
    ## Retrieve the checkpoint for the last distinct id processed from the customers_tracking db
    customers = fetch_data_from_supabase_specific("Customer_Tracking")
    customers['Distinct_Checkpoint'] = customers['Distinct_Checkpoint'].astype(int)

    checkpoint_value = int(customers['Distinct_Checkpoint'].max())


    # Retrieve the tracking values beyong the checkpoint_value
    new_tracking = fetch_data_from_supabase_specific("Tracking_Visitors",
                                                        filters={
                                                            'Distinct_ID': ('gt', checkpoint_value)
                                                        })
                                                        
    
    ### Send the data over to the first step
    response, changed_ids = update_customer_tracking(new_tracking, customers)

    print("successfully synced customers")
    return

    '''
    ## Commenting this section for now to test the bare customer tracking --- 

    ## Get the changed id entries + do the percetnage computation
    if response and len(changed_ids) > 0:
        # Call the database with the distinct ids
        changed = fetch_data_from_supabase_specific("Customer_Tracking",
                                                        filters = {
                                                            'Distinct_ID': ('in', changed_ids)
                                                        }
                                                    )
        ## Do the sessions percentage computation -- 
        if not changed.empty:
            for i, row in changed.iterrows():
                cid = row["Distinct_ID"]

                sessions = row["Sessions"]
                pct = compute_campaign_percentages(sessions)

                # Write back into entries_df so it's inserted with contributions
                changed.at[i, "Campaign_Contributions"] = pct

            ## After updates, upsert the changed rows
            upsert_partial(changed, 'Customer_Tracking', 'Distinct_ID')'''
        
    
def update_database_after_filter(request, df):
    updated_rows = 0

    for _, row in df.iterrows():
        supabase.table("Tracking_Visitors").update({
            "UTM_Source": row.get("UTM_Source"),
            "Referrer_Platform": row.get("Referrer_Platform"),
        }).eq("Distinct_ID", row["Distinct_ID"]).execute()

        updated_rows += 1

    return messages.success(
        request,
        f"{updated_rows} records updated successfully."
    )