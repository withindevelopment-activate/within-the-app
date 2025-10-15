import os,json,ast,logging,pandas as pd, pytz
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# Import keys
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)

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
    

def get_tracking_df():
    """
    Fetch all tracking data from the database and return as a DataFrame.
    """
    df = fetch_data_from_supabase("Tracking_Visitors")

    # Convert dates properly
    if "Visited_at" in df.columns:
        df["Visited_at"] = pd.to_datetime(df["Visited_at"], errors="coerce")
    return df


def build_customer_dictionary(df: pd.DataFrame) -> dict:
    """
    Build a dictionary of customers with all related visitor_ids, session_ids,
    campaigns, and event stats (pageviews, add_to_cart, purchases).
    """
    df = df.copy()

    # Normalize and prepare columns
    df["Customer_ID"] = df["Customer_ID"].fillna("").astype(str).str.strip()
    df["Customer_Email"] = df["Customer_Email"].fillna("").astype(str).str.strip()
    df["Customer_Mobile"] = df["Customer_Mobile"].fillna("").astype(str).str.strip()
    df["Visitor_ID"] = df["Visitor_ID"].fillna("").astype(str).str.strip()
    df["Session_ID"] = df["Session_ID"].fillna("").astype(str).str.strip()
    df["UTM_Campaign"] = df["UTM_Campaign"].fillna("").astype(str).str.replace("+", " ", regex=False).str.strip()
    df["Visited_at"] = pd.to_datetime(df["Visited_at"])

    def get_customer_key(row):
        if row["Customer_ID"]:
            return row["Customer_ID"]
        elif row["Customer_Email"]:
            return row["Customer_Email"].lower()
        elif row["Customer_Mobile"]:
            return row["Customer_Mobile"]
        return None

    df["customer_key"] = df.apply(get_customer_key, axis=1)

    customer_dict = {}

    for key, group in df.groupby("customer_key"):
        if not key:
            continue

        # Collect all identifiers for this customer
        customer_visitors = set(group["Visitor_ID"]) - {""}
        customer_sessions = set(group["Session_ID"]) - {""}
        customer_ids = {key}

        # Find *all rows in df* that match any of these identifiers
        mask = (
            df["Visitor_ID"].isin(customer_visitors) |
            df["Session_ID"].isin(customer_sessions) |
            df["Customer_ID"].isin(customer_ids) |
            df["Customer_Email"].isin(customer_ids) |
            df["Customer_Mobile"].isin(customer_ids)
        )
        full_rows = df[mask].copy()

        # Campaigns this customer interacted with
        campaigns = set(full_rows["UTM_Campaign"]) - {""}

        # Count event types
        pageviews = (full_rows["Event_Type"] == "pageview").sum()
        add_to_cart = (full_rows["Event_Type"] == "add_to_cart").sum()
        purchases = (full_rows["Event_Type"] == "purchase").sum()

        # Latest info for display
        latest_row = full_rows.sort_values("Visited_at").iloc[-1]

        customer_dict[key] = {
            "visitor_ids": customer_visitors,
            "sessions": customer_sessions,
            "campaigns": campaigns,
            "stats": {
                "pageviews": int(pageviews),
                "add_to_cart": int(add_to_cart),
                "purchases": int(purchases),
            },
            "user_info": {
                "name": latest_row.get("Customer_Name"),
                "email": latest_row.get("Customer_Email"),
                "mobile": latest_row.get("Customer_Mobile"),
                "customer_id": latest_row.get("Customer_ID"),
            }
        }

    return customer_dict


def calculate_campaign_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate campaign-level performance directly from tracking data
    without linking to individual customers.

    Returns a DataFrame with columns:
    [campaign, pageviews, add_to_cart, purchases, total_value, events_count]
    """

    # Keep only rows with a campaign
    df = df[df["UTM_Campaign"].notna() & (df["UTM_Campaign"].str.strip() != "")]
    if df.empty:
        return pd.DataFrame(columns=["campaign", "pageviews", "add_to_cart", "purchases", "total_value", "events_count"])

    # Normalize campaign names
    df["UTM_Campaign"] = df["UTM_Campaign"].str.replace("+", " ", regex=False).str.strip()

    # Prepare event value column
    def get_value(row):
        details = row.get("Event_Details")
        if pd.notna(details):
            try:
                details_dict = json.loads(details.replace("'", '"'))  # ensure valid JSON
                if row["Event_Type"] in ["purchase", "add_to_cart"]:
                    # Use price * quantity if available
                    price = float(details_dict.get("price", 1.0))
                    quantity = int(details_dict.get("quantity", 1))
                    return price * quantity
            except Exception:
                return 1.0
        return 0.0

    df["event_value"] = df.apply(get_value, axis=1)

    # Group by campaign and aggregate metrics
    summary = (
        df.groupby("UTM_Campaign")
        .agg(
            pageviews=pd.NamedAgg(column="Event_Type", aggfunc=lambda x: (x == "pageview").sum()),
            add_to_cart=pd.NamedAgg(column="Event_Type", aggfunc=lambda x: (x == "add_to_cart").sum()),
            purchases=pd.NamedAgg(column="Event_Type", aggfunc=lambda x: (x == "purchase").sum()),
            total_value=pd.NamedAgg(column="event_value", aggfunc="sum"),
        )
        .reset_index()
        .rename(columns={"UTM_Campaign": "campaign"})
    )

    # Total events count per campaign
    summary["events_count"] = summary["pageviews"] + summary["add_to_cart"] + summary["purchases"]

    # Sort by total_value descending
    summary = summary.sort_values("total_value", ascending=False)

    return summary

def get_visitor_last_day_activity(df: pd.DataFrame, visitor_id: str) -> pd.DataFrame:
    """
    Get activity for a specific visitor in the last 24 hours.
    Returns a DataFrame with event counts and campaigns visited.
    """
    df["Visited_at"] = pd.to_datetime(df["Visited_at"])
    uae_timezone = pytz.timezone('Asia/Dubai')

    # Filter for this visitor
    visitor_df = df[df["Visitor_ID"] == visitor_id].copy()
    if visitor_df.empty:
        return pd.DataFrame(columns=["visitor_id", "campaign", "pageviews", "add_to_cart", "purchases", "events_count"])

    # Filter for last 24 hours
    last_day = datetime.now(uae_timezone) - timedelta(days=1)
    visitor_df = visitor_df[visitor_df["Visited_at"] >= last_day]

    if visitor_df.empty:
        return pd.DataFrame(columns=["visitor_id", "campaign", "pageviews", "add_to_cart", "purchases", "events_count"])

    # Normalize campaign names
    visitor_df["UTM_Campaign"] = visitor_df["UTM_Campaign"].fillna("").str.replace("+", " ", regex=False).str.strip()

    # Prepare event value column
    def get_value(row):
        details = row.get("Event_Details")
        if pd.notna(details):
            try:
                details_dict = json.loads(details.replace("'", '"'))
                if row["Event_Type"] in ["purchase", "add_to_cart"]:
                    price = float(details_dict.get("price", 1.0))
                    quantity = int(details_dict.get("quantity", 1))
                    return price * quantity
            except Exception:
                return 1.0
        return 0.0

    visitor_df["event_value"] = visitor_df.apply(get_value, axis=1)

    # Group by campaign
    summary = (
        visitor_df.groupby("UTM_Campaign")
        .agg(
            pageviews=pd.NamedAgg(column="Event_Type", aggfunc=lambda x: (x == "pageview").sum()),
            add_to_cart=pd.NamedAgg(column="Event_Type", aggfunc=lambda x: (x == "add_to_cart").sum()),
            purchases=pd.NamedAgg(column="Event_Type", aggfunc=lambda x: (x == "purchase").sum()),
            total_value=pd.NamedAgg(column="event_value", aggfunc="sum"),
        )
        .reset_index()
        .rename(columns={"UTM_Campaign": "campaign"})
    )

    # Total events count
    summary["events_count"] = summary["pageviews"] + summary["add_to_cart"] + summary["purchases"]
    summary["visitor_id"] = visitor_id

    # Sort by total_value descending
    summary = summary.sort_values("total_value", ascending=False)

    return summary

def attribute_purchases_to_campaigns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a campaign-level summary with purchases attributed to the most relevant campaign
    using session_id and visit order.
    """
    df = df.copy()
    df["Visited_at"] = pd.to_datetime(df["Visited_at"])
    df["UTM_Campaign"] = df["UTM_Campaign"].fillna("").astype(str).str.replace("+", " ", regex=False).str.strip()

    # Helper: get event value (purchase/add_to_cart amount)
    def get_value(row):
        details = row.get("Event_Details")
        if pd.notna(details):
            try:
                d = json.loads(details.replace("'", '"'))
                price = float(d.get("price", 1.0))
                qty = int(d.get("quantity", 1))
                return price * qty
            except Exception:
                return 0.0
        return 0.0

    df["event_value"] = df.apply(get_value, axis=1)

    # Separate purchases to attribute them
    purchases = df[df["Event_Type"] == "purchase"].copy()
    campaign_attribution = []

    for _, purchase in purchases.iterrows():
        session_id = purchase["Session_ID"]
        purchase_time = purchase["Visited_at"]

        # Direct campaign
        if purchase["UTM_Campaign"]:
            campaign_attribution.append({
                "campaign": purchase["UTM_Campaign"],
                "value": purchase["event_value"],
            })
            continue

        # Look back in the same session for the latest campaign
        session_events = df[
            (df["Session_ID"] == session_id) &
            (df["Visited_at"] < purchase_time) &
            (df["UTM_Campaign"] != "")
        ].sort_values("Visited_at")

        if not session_events.empty:
            last_campaign = session_events.iloc[-1]["UTM_Campaign"]
            campaign_attribution.append({
                "campaign": last_campaign,
                "value": purchase["event_value"],
            })
        else:
            campaign_attribution.append({
                "campaign": "(no campaign)",
                "value": purchase["event_value"],
            })

    attribution_df = pd.DataFrame(campaign_attribution)
    if attribution_df.empty:
        return pd.DataFrame(columns=["campaign", "purchases", "total_value"])

    summary = (
        attribution_df.groupby("campaign")
        .agg(
            purchases=pd.NamedAgg(column="campaign", aggfunc="count"),
            total_value=pd.NamedAgg(column="value", aggfunc="sum")
        )
        .reset_index()
    )

    return summary.sort_values("total_value", ascending=False)


# def summarize_campaign_performance(df: pd.DataFrame, attribution_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Aggregate campaign performance including event counts, pageviews, add_to_cart, and purchases.
#     """
#     # --- Count events per campaign ---
#     events_summary = (
#         df[df["UTM_Campaign"].notna()]
#         .groupby("UTM_Campaign")["Event_Type"]
#         .value_counts()
#         .unstack(fill_value=0)
#         .reset_index()
#         .rename(columns={"UTM_Campaign": "campaign"})
#     )

#     # Rename for consistency
#     for col in ["pageview", "add_to_cart", "purchase"]:
#         if col not in events_summary.columns:
#             events_summary[col] = 0

#     events_summary["events_count"] = events_summary.drop(columns=["campaign"]).sum(axis=1)

#     # --- Purchase attribution summary ---
#     if not attribution_df.empty:
#         attribution_summary = (
#             attribution_df.groupby("campaign")
#             .agg(
#                 total_credit=pd.NamedAgg(column="credit_value", aggfunc="sum"),
#                 conversions=pd.NamedAgg(column="customer_key", aggfunc="count"),
#             )
#             .reset_index()
#         )
#     else:
#         attribution_summary = pd.DataFrame(columns=["campaign", "total_credit", "conversions"])

#     # --- Merge event counts with attribution ---
#     campaign_summary = pd.merge(
#         events_summary,
#         attribution_summary,
#         on="campaign",
#         how="outer"
#     ).fillna(0)

#     # Final ordering
#     campaign_summary = campaign_summary.sort_values("total_credit", ascending=False)

#     return campaign_summary

# def calculate_campaign_attribution(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Calculate partial attribution for campaigns per purchase.
#     Returns a DataFrame with columns: [customer_key, purchase_date, campaign, credit_value]
#     """
#     def get_customer_key(row):
#         if pd.notna(row.get("Customer_Email")) and row["Customer_Email"].strip():
#             return row["Customer_Email"].lower().strip()
#         elif pd.notna(row.get("Customer_Mobile")) and row["Customer_Mobile"].strip():
#             return row["Customer_Mobile"].strip()
#         elif pd.notna(row.get("Customer_ID")) and str(row["Customer_ID"]).strip():
#             return str(row["Customer_ID"])
#         return None

#     df["customer_key"] = df.apply(get_customer_key, axis=1)

#     # Extract purchases
#     purchases = df[df["Event_Type"] == "purchase"].copy()
#     if purchases.empty:
#         return pd.DataFrame(columns=["customer_key", "purchase_date", "campaign", "credit_value"])

#     purchases["purchase_date"] = purchases["Visited_at"].dt.date

#     attribution_rows = []

#     for _, purchase in purchases.iterrows():
#         key = purchase["customer_key"]
#         if not key:
#             continue

#         purchase_date = purchase["purchase_date"]
#         purchase_value = 1.0

#         # Optional: read value from Event_Details JSON if exists
#         details = purchase.get("Event_Details")
#         if pd.notna(details):
#             try:
#                 data = json.loads(details)
#                 purchase_value = float(data.get("value", 1.0))
#             except Exception:
#                 pass

#         # Find campaigns visited the same day
#         campaigns_today = df[
#             (df["customer_key"] == key) &
#             (df["UTM_Campaign"].notna()) &
#             (df["Visited_at"].dt.date == purchase_date)
#         ]["UTM_Campaign"].unique()

#         if len(campaigns_today) == 0:
#             continue

#         credit_per_campaign = purchase_value / len(campaigns_today)

#         for campaign in campaigns_today:
#             attribution_rows.append({
#                 "customer_key": key,
#                 "purchase_date": purchase_date,
#                 "campaign": campaign,
#                 "credit_value": credit_per_campaign
#             })

#     return pd.DataFrame(attribution_rows)