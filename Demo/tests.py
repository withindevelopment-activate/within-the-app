from django.test import TestCase

# <div class="data-wrap">
#     <div class="data-viewport">
#       <div class="data-scroller" role="region" aria-label="Scrollable data table" tabindex="0">
#         <table class="data-table" role="table" aria-label="Tracking dataset">
#           <thead>
#             <tr>
#               <th>Distinct ID</th>
#               <th class="sticky-col">Visitor ID</th>
#               <th>Session ID</th>
#               <th>Store URL</th>
#               <th>Event Type</th>
#               <th>Event Details</th>
#               <th>Page URL</th>
#               <th>Visited At</th>
#               <th>UTM Source</th>
#               <th>UTM Medium</th>
#               <th>UTM Campaign</th>
#               <th>UTM Term</th>
#               <th>UTM Content</th>
#               <th>Referrer</th>
#               <th>Traffic Source</th>
#               <th>Traffic Medium</th>
#               <th>Traffic Campaign</th>
#               <th>Customer ID</th>
#               <th>Customer Name</th>
#               <th>Customer Email</th>
#               <th>Customer Mobile</th>
#               <th>User Agent</th>
#               <th>Language</th>
#               <th>Timezone</th>
#               <th>Platform</th>
#               <th>Screen Resolution</th>
#               <th>Device Memory</th>
#               <th>IP Address</th>
#             </tr>
#           </thead>
#           <tbody>
#             {% for r in rows %}
#             <tr>
#               <td>{{ r.Distinct_ID }}</td>
#               <td class="sticky-col">{{ r.Visitor_ID }}</td>
#               <td>{{ r.Session_ID }}</td>
#               <td>{{ r.Store_URL }}</td>
#               <td>{{ r.Event_Type }}</td>
#               <td>{{ r.Event_Details }}</td>
#               <td>{{ r.Page_URL }}</td>
#               <td>{{ r.Visited_at }}</td>
#               <td>{{ r.UTM_Source }}</td>
#               <td>{{ r.UTM_Medium }}</td>
#               <td>{{ r.UTM_Campaign }}</td>
#               <td>{{ r.UTM_Term }}</td>
#               <td>{{ r.UTM_Content }}</td>
#               <td>{{ r.Referrer_Platform }}</td>
#               <td>{{ r.Traffic_Source }}</td>
#               <td>{{ r.Traffic_Medium }}</td>
#               <td>{{ r.Traffic_Campaign }}</td>
#               <td>{{ r.Customer_ID }}</td>
#               <td>{{ r.Customer_Name }}</td>
#               <td>{{ r.Customer_Email }}</td>
#               <td>{{ r.Customer_Mobile }}</td>
#               <td>{{ r.User_Agent }}</td>
#               <td>{{ r.Language }}</td>
#               <td>{{ r.Timezone }}</td>
#               <td>{{ r.Platform }}</td>
#               <td>{{ r.Screen_Resolution }}</td>
#               <td>{{ r.Device_Memory }}</td>
#               <td>{{ r.IP_Address }}</td>
#             </tr>
#             {% empty %}
#             <tr>
#               <td colspan="28" class="text-center text-muted">No tracking data found</td>
#             </tr>
#             {% endfor %}
#           </tbody>
#         </table>
#       </div>
#     </div>
#   </div>

# def build_customer_dictionary(df: pd.DataFrame) -> dict:
#     """
#     Build a dictionary of customers:
#     - Tracks add_to_cart, purchase, and campaigns used
#     - Includes latest customer name
#     - Includes all events from any of their visitor IDs
#     """
#     if df.empty:
#         return {}

#     df = df.copy()

#     # Normalize columns
#     for col in ["Customer_ID", "Customer_Email", "Customer_Mobile", "Customer_Name", "Visitor_ID"]:
#         df[col] = df.get(col, "").fillna("").astype(str).str.strip()

#     df["UTM_Campaign"] = (
#         df.get("UTM_Campaign", "")
#         .fillna("")
#         .astype(str)
#         .str.replace("+", " ", regex=False)
#         .str.strip()
#     )
#     df["Visited_at"] = pd.to_datetime(df["Visited_at"], errors="coerce")

#     # Extract Customer_ID from Event_Details if missing
#     def extract_customer_id(row):
#         if row["Customer_ID"]:
#             return row["Customer_ID"]
#         details = row.get("Event_Details")
#         if pd.isna(details) or not details:
#             return ""
#         try:
#             d = json.loads(details.replace("'", '"'))
#             return str(d.get("customer_id", "")) or ""
#         except Exception:
#             return ""

#     df["Customer_ID"] = df.apply(extract_customer_id, axis=1)

#     # Define unified key
#     def get_customer_key(row):
#         if row["Customer_ID"]:
#             return row["Customer_ID"]
#         elif row["Customer_Email"]:
#             return row["Customer_Email"].lower()
#         elif row["Customer_Mobile"]:
#             return row["Customer_Mobile"]
#         return None

#     df["customer_key"] = df.apply(get_customer_key, axis=1)
#     df = df[df["customer_key"].notna()]

#     customer_dict = {}

#     # First, get visitor IDs for each customer
#     customer_visitors_map = (
#         df.groupby("customer_key")["Visitor_ID"]
#         .apply(lambda x: set([v for v in x if v]))
#         .to_dict()
#     )

#     for key, visitor_ids in customer_visitors_map.items():
#         # Filter all rows where customer_key matches OR visitor_id is in this set
#         mask = (df["customer_key"] == key) | (df["Visitor_ID"].isin(visitor_ids))
#         customer_rows = df[mask]

#         # Only relevant events
#         stats_rows = customer_rows[customer_rows["Event_Type"].isin(["add_to_cart", "purchase"])]

#         # Stats
#         add_to_cart_count = (stats_rows["Event_Type"] == "add_to_cart").sum()
#         purchase_count = (stats_rows["Event_Type"] == "purchase").sum()
#         campaigns = list(set(stats_rows["UTM_Campaign"]) - {""})

#         # Latest name
#         name_rows = customer_rows[customer_rows["Customer_Name"] != ""]
#         latest_name = ""
#         if not name_rows.empty:
#             latest_name = name_rows.sort_values("Visited_at").iloc[-1]["Customer_Name"]

#         customer_dict[key] = {
#             "customer_name": latest_name,
#             "visitor_ids": visitor_ids,
#             "add_to_cart": int(add_to_cart_count),
#             "purchases": int(purchase_count),
#             "campaigns": campaigns,
#         }

#     return customer_dict


# def update_customer_tracking(df: pd.DataFrame):
#     """
#     Update the Customer_Tracking table incrementally.
#     Only process new events per customer after their last updated_at.
#     """
#     if df.empty:
#         return {}

#     # Fetch existing customer tracking table
#     customer_tracking_df = fetch_data_from_supabase("Customer_Tracking")

#     # Normalize customer_tracking_df
#     if customer_tracking_df.empty:
#         customer_tracking_df = pd.DataFrame(columns=[
#             "customer_key", "customer_name", "visitor_ids",
#             "add_to_cart", "purchases", "campaigns", "updated_at"
#         ])
#     else:
#         # Convert JSON columns back to Python objects
#         customer_tracking_df["visitor_ids"] = customer_tracking_df["visitor_ids"].apply(lambda x: set(json.loads(x) if x else []))
#         customer_tracking_df["campaigns"] = customer_tracking_df["campaigns"].apply(lambda x: list(json.loads(x) if x else []))
#         customer_tracking_df["updated_at"] = pd.to_datetime(customer_tracking_df["updated_at"], errors="coerce")

#     # Normalize incoming df
#     for col in ["Customer_ID", "Customer_Email", "Customer_Mobile", "Customer_Name", "Visitor_ID"]:
#         df[col] = df.get(col, "").fillna("").astype(str).str.strip()
#     df["UTM_Campaign"] = df.get("UTM_Campaign", "").fillna("").astype(str).str.replace("+", " ", regex=False).str.strip()
#     df["Visited_at"] = pd.to_datetime(df["Visited_at"], errors="coerce")

#     # Extract customer_key
#     def get_customer_key(row):
#         if row["Customer_ID"]:
#             return row["Customer_ID"]
#         elif row["Customer_Email"]:
#             return row["Customer_Email"].lower()
#         elif row["Customer_Mobile"]:
#             return row["Customer_Mobile"]
#         return None
#     df["customer_key"] = df.apply(get_customer_key, axis=1)
#     df = df[df["customer_key"].notna()]

#     # Build incremental updates
#     updated_rows = []
#     for key, group in df.groupby("customer_key"):
#         last_updated = customer_tracking_df.loc[customer_tracking_df["customer_key"] == key, "updated_at"]
#         if not last_updated.empty:
#             last_updated = last_updated.iloc[0]
#             group = group[group["Visited_at"] > last_updated]

#         if group.empty:
#             continue  # nothing new to process

#         # Stats
#         add_to_cart_count = (group["Event_Type"] == "add_to_cart").sum()
#         purchase_count = (group["Event_Type"] == "purchase").sum()
#         campaigns = list(set(group["UTM_Campaign"]) - {""})
#         visitor_ids = set(group["Visitor_ID"])

#         # Merge with existing data
#         if key in customer_tracking_df["customer_key"].values:
#             existing = customer_tracking_df.loc[customer_tracking_df["customer_key"] == key].iloc[0]
#             add_to_cart_count += int(existing["add_to_cart"])
#             purchase_count += int(existing["purchases"])
#             campaigns = list(set(existing["campaigns"]) | set(campaigns))
#             visitor_ids = visitor_ids | set(existing["visitor_ids"])
#             latest_name = group.loc[group["Customer_Name"] != "", "Customer_Name"].sort_values().iloc[-1] if not group.loc[group["Customer_Name"] != ""].empty else existing["customer_name"]
#         else:
#             latest_name = group.loc[group["Customer_Name"] != "", "Customer_Name"].sort_values().iloc[-1] if not group.loc[group["Customer_Name"] != ""].empty else ""

#         # Update timestamp
#         updated_at = group["Visited_at"].max()

#         updated_rows.append({
#             "customer_key": key,
#             "customer_name": latest_name,
#             "visitor_ids": json.dumps(list(visitor_ids)),
#             "add_to_cart": int(add_to_cart_count),
#             "purchases": int(purchase_count),
#             "campaigns": json.dumps(campaigns),
#             "updated_at": updated_at
#         })

#     # Upsert back to Supabase
#     for row in updated_rows:
#         supabase.table("Customer_Tracking").upsert(row, on_conflict="customer_key").execute()

#     # Return as dictionary for immediate use
#     return {row["customer_key"]: {
#         "customer_name": row["customer_name"],
#         "visitor_ids": set(json.loads(row["visitor_ids"])),
#         "add_to_cart": row["add_to_cart"],
#         "purchases": row["purchases"],
#         "campaigns": json.loads(row["campaigns"])
#     } for row in updated_rows}

# def sync_customer_tracking_unified():
#     """
#     Unified sync for Customer_Tracking:
#     - Performs full sync if Customer_Tracking is empty.
#     - Performs incremental sync based on new visits per distinct_id.
#     - Aggregates visitor/session/event data efficiently.
#     """
#     # 1️⃣ Fetch the last updated timestamp from Customer_Tracking
#     existing_customers = supabase.table("Customer_Tracking").select("*").execute().data
#     existing_df = pd.DataFrame(existing_customers) if existing_customers else pd.DataFrame()

#     last_updated = None
#     if not existing_df.empty and "updated_at" in existing_df.columns:
#         last_updated = pd.to_datetime(existing_df["updated_at"]).max()

#     # 2️⃣ Fetch only relevant tracking rows
#     query_filter = f"Visited_at > '{last_updated.isoformat()}'" if last_updated else None
#     df = fetch_data_from_supabase_specific(
#         "Tracking_Visitors",
#         columns=[
#             "distinct_id",  # main key
#             "Customer_Name",
#             "Visitor_ID",
#             "Session_ID",
#             "Event_Type",
#             "UTM_Campaign",
#             "Visited_at",
#         ],
#         filter=query_filter
#     )

#     if df.empty:
#         print("No new tracking data to sync.")
#         return

#     # 3️⃣ Normalize columns
#     for col in ["distinct_id", "Visitor_ID", "Session_ID", "UTM_Campaign"]:
#         if col in df.columns:
#             df[col] = df[col].astype(str).fillna("").str.strip().replace("nan", "")

#     df["UTM_Campaign"] = df["UTM_Campaign"].str.replace("+", " ", regex=False)
#     df["Visited_at"] = pd.to_datetime(df["Visited_at"], errors="coerce")
#     df = df[df["distinct_id"] != ""]

#     # 4️⃣ Build visitor → sessions mapping
#     visitor_session_map = df.groupby("Visitor_ID")["Session_ID"].unique().apply(list).to_dict()

#     # 5️⃣ Aggregate by distinct_id
#     agg_df = (
#         df.groupby("distinct_id")
#         .agg(
#             customer_name=("Customer_Name", lambda x: x.dropna().iloc[-1] if len(x.dropna()) else ""),
#             visitor_ids=("Visitor_ID", lambda vlist: {vid: visitor_session_map.get(vid, []) for vid in vlist.dropna().unique()}),
#             add_to_cart=("Event_Type", lambda x: (x == "add_to_cart").sum()),
#             purchases=("Event_Type", lambda x: (x == "purchase").sum()),
#             campaigns=("UTM_Campaign", lambda x: list(set(x.dropna()) - {""})),
#             updated_at=("Visited_at", "max"),
#         )
#         .reset_index()
#         .rename(columns={"distinct_id": "customer_key"})
#     )

#     if agg_df.empty:
#         print("No customer summaries to update.")
#         return

#     # 6️⃣ Merge with existing Customer_Tracking if exists
#     if not existing_df.empty:
#         existing_df.set_index("customer_key", inplace=True)
#         agg_df.set_index("customer_key", inplace=True)

#         for key in agg_df.index:
#             if key in existing_df.index:
#                 # Merge visitor_ids
#                 existing_visitors = existing_df.at[key, "visitor_ids"] or {}
#                 new_visitors = agg_df.at[key, "visitor_ids"]
#                 merged_visitors = {**existing_visitors, **new_visitors}

#                 agg_df.at[key, "visitor_ids"] = merged_visitors

#                 # Merge add_to_cart and purchases
#                 agg_df.at[key, "add_to_cart"] += existing_df.at[key, "add_to_cart"]
#                 agg_df.at[key, "purchases"] += existing_df.at[key, "purchases"]

#                 # Merge campaigns
#                 existing_campaigns = set(existing_df.at[key, "campaigns"] or [])
#                 new_campaigns = set(agg_df.at[key, "campaigns"])
#                 agg_df.at[key, "campaigns"] = list(existing_campaigns | new_campaigns)

#                 # Take max updated_at
#                 agg_df.at[key, "updated_at"] = max(existing_df.at[key, "updated_at"], agg_df.at[key, "updated_at"])

#         agg_df.reset_index(inplace=True)

#     # 7️⃣ Bulk upsert in chunks
#     records = agg_df.to_dict(orient="records")
#     BATCH_SIZE = 1000
#     total = len(records)

#     for i in range(0, total, BATCH_SIZE):
#         batch = records[i:i + BATCH_SIZE]
#         try:
#             supabase.table("Customer_Tracking").upsert(batch, on_conflict="customer_key").execute()
#         except Exception as e:
#             print(f"Batch {i//BATCH_SIZE + 1} failed: {e}")

#     print(f"✅ Synced {len(agg_df)} customers into Customer_Tracking (unified incremental).")
