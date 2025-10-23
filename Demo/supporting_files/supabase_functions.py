import os,json,ast,logging,pandas as pd, pytz
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from django.core.cache import cache

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

def sync_customer_tracking_unified():
    """
    Unified sync for Customer_Tracking:
    - Performs full sync if Customer_Tracking is empty.
    - Performs incremental sync based on new visits per distinct_id.
    - Aggregates visitor/session/event data efficiently.
    """
    # 1️⃣ Fetch the last updated timestamp from Customer_Tracking
    existing_customers = supabase.table("Customer_Tracking").select("*").execute().data
    existing_df = pd.DataFrame(existing_customers) if existing_customers else pd.DataFrame()

    last_updated = None
    if not existing_df.empty and "updated_at" in existing_df.columns:
        last_updated = pd.to_datetime(existing_df["updated_at"]).max()

    # 2️⃣ Fetch only relevant tracking rows
    query_filter = f"Visited_at > '{last_updated.isoformat()}'" if last_updated else None
    df = fetch_data_from_supabase_specific(
        "Tracking_Visitors",
        columns=[
            "distinct_id",  # main key
            "Customer_Name",
            "Visitor_ID",
            "Session_ID",
            "Event_Type",
            "UTM_Campaign",
            "UTM_Source",
            "Visited_at",
        ],
        filter=query_filter
    )

    if df.empty:
        print("No new tracking data to sync.")
        return

    # 3️⃣ Normalize columns
    for col in ["distinct_id", "Visitor_ID", "Session_ID", "UTM_Campaign", "UTM_Source"]:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").str.strip().replace("nan", "")

    df["UTM_Campaign"] = df["UTM_Campaign"].str.title().replace("+", " ", regex=False)
    df["UTM_Source"] = df["UTM_Source"].str.title().replace("+", " ", regex=False)
    df["Visited_at"] = pd.to_datetime(df["Visited_at"], errors="coerce")
    df = df[df["distinct_id"] != ""]

    # 4️⃣ Build visitor → sessions mapping
    visitor_session_map = df.groupby("Visitor_ID")["Session_ID"].unique().apply(list).to_dict()

    # 5️⃣ Aggregate by distinct_id
    agg_df = (
        df.groupby("distinct_id")
        .agg(
            customer_name=("Customer_Name", lambda x: x.dropna().iloc[-1] if len(x.dropna()) else ""),
            visitor_ids=("Visitor_ID", lambda vlist: {vid: visitor_session_map.get(vid, []) for vid in vlist.dropna().unique()}),
            add_to_cart=("Event_Type", lambda x: (x == "add_to_cart").sum()),
            purchases=("Event_Type", lambda x: (x == "purchase").sum()),
            campaigns=("UTM_Campaign", lambda x: list(set(x.dropna()) - {""})),
            sources=("UTM_Source", lambda x: list(set(x.dropna()) - {""})),
            updated_at=("Visited_at", "max"),
        )
        .reset_index()
        .rename(columns={"distinct_id": "customer_key"})
    )

    if agg_df.empty:
        print("No customer summaries to update.")
        return

    # 6️⃣ Merge with existing Customer_Tracking if exists
    if not existing_df.empty:
        existing_df.set_index("customer_key", inplace=True)
        agg_df.set_index("customer_key", inplace=True)

        for key in agg_df.index:
            if key in existing_df.index:
                # Merge visitor_ids
                existing_visitors = existing_df.at[key, "visitor_ids"] or {}
                new_visitors = agg_df.at[key, "visitor_ids"]
                merged_visitors = {**existing_visitors, **new_visitors}

                agg_df.at[key, "visitor_ids"] = merged_visitors

                # Merge add_to_cart and purchases
                agg_df.at[key, "add_to_cart"] += existing_df.at[key, "add_to_cart"]
                agg_df.at[key, "purchases"] += existing_df.at[key, "purchases"]

                # Merge campaigns
                existing_campaigns = set(existing_df.at[key, "campaigns"] or [])
                new_campaigns = set(agg_df.at[key, "campaigns"])
                agg_df.at[key, "campaigns"] = list(existing_campaigns | new_campaigns)

                # Merge sources
                existing_sources = set(existing_df.at[key, "sources"] or [])
                new_sources = set(agg_df.at[key, "sources"])
                agg_df.at[key, "sources"] = list(existing_sources | new_sources)

                # Take max updated_at
                agg_df.at[key, "updated_at"] = max(existing_df.at[key, "updated_at"], agg_df.at[key, "updated_at"])

        agg_df.reset_index(inplace=True)

    # 7️⃣ Bulk upsert in chunks
    records = agg_df.to_dict(orient="records")
    BATCH_SIZE = 1000
    total = len(records)

    for i in range(0, total, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            supabase.table("Customer_Tracking").upsert(batch, on_conflict="customer_key").execute()
        except Exception as e:
            print(f"Batch {i//BATCH_SIZE + 1} failed: {e}")

    print(f"✅ Synced {len(agg_df)} customers into Customer_Tracking (unified incremental).")

def get_tracking_customers_df():
    """
    Runs an incremental sync (sync_customer_tracking_unified) 
    and then fetches fresh data from 'Customer_Tracking'.
    Returns a cleaned DataFrame ready for dashboard use.
    """
    try:
        # 1️⃣ Run incremental sync first
        sync_customer_tracking_unified()

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
                df[num_col] = pd.to_numeric(df[num_col], errors="coerce").fillna(0)

        return df

    except Exception as e:
        print(f"⚠️ Error syncing or fetching Customer_Tracking: {e}")
        return pd.DataFrame()
