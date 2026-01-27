from datetime import datetime, timedelta, timezone
import requests, pandas as pd, json, re, asyncio, traceback, logging, pytz, uuid, os, http.client
from django.conf import settings
from django.shortcuts import redirect, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.contrib import messages
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponse
from django.template.loader import render_to_string
from rapidfuzz import fuzz
from urllib.parse import urlencode
from django.core.files.storage import FileSystemStorage
from io import BytesIO
from supabase import create_client, Client
from dateutil import parser
import threading


## Custom Imports ------------------
# Supabase & Supporting imports
from Demo.supporting_files.supabase_functions import get_next_id_from_supabase_compatible_all, batch_insert_to_supabase, sync_customers, fetch_data_from_supabase_specific, update_database_after_filter, get_last_non_direct_utm

from Demo.supporting_files.supporting_functions import get_uae_current_date, detect_source_from_url_or_domain, detect_source_from_row, detect_source_from_user_agent
# Marketing Report functions
from Demo.supporting_files.marketing_report import create_general_analysis, create_product_percentage_amount_spent, landing_performance_5_async, column_check
# Webhook function imports
from Demo.supporting_files.hook_tasks import track_price_changes, process_zid_order_logic, initial_fetch_products

# Constructing the marketing files
from Demo.supporting_files.constructing_marketing_files import create_tiktok_file, create_snapchat_file
# ------------------------------------
# initialize database client
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)


# Redirect user to Zid OAuth page
'''def zid_login(request):
    params = {
            'client_id': settings.ZID_CLIENT_ID,
            'redirect_uri': settings.ZID_REDIRECT_URI,
            'response_type': 'code',
        }
    
    # Add optional parameters if they exist
    query_string = '&'.join([f'{k}={v}' for k, v in params.items()])
    return redirect(f'{settings.ZID_AUTH_URL}?{query_string}')'''

def zid_login(request):
    params = {
        "client_id": settings.ZID_CLIENT_ID,
        "redirect_uri": settings.ZID_REDIRECT_URI,
        "response_type": "code",
    }
    url = settings.ZID_AUTH_URL
    auth_url = f"{url}?{urlencode(params)}"
    return redirect(auth_url)

# Handle callback and exchange code for access_token
def zid_callback(request):
    code = request.GET.get('code')
    if not code:
        messages.error(request, "No code returned from Zid.")
        return redirect('Demo:zid_login')

    data = {
        'grant_type': 'authorization_code',
        'client_id': settings.ZID_CLIENT_ID,
        'client_secret': settings.ZID_CLIENT_SECRET,
        'redirect_uri': settings.ZID_REDIRECT_URI,
        'code': code,
    }

    try:
        # Exchange code for access token
        response = requests.post(settings.ZID_TOKEN_URL, data=data)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get('access_token')
        refresh_token = token_data.get('refresh_token')
        authorization_token = token_data.get('authorization')

        print("Retrieved Token data:", token_data)

        # Save tokens to session
        request.session['access_token'] = access_token
        request.session['refresh_token'] = refresh_token
        request.session['authorization_token'] = authorization_token

        # Fetch user profile to get store ID
        headers = {
            'Authorization': f'Bearer {authorization_token}',
            'X-MANAGER-TOKEN': access_token
        }

        # Fetch user profile to get store ID
        profile_response = requests.get(f"{settings.ZID_API_BASE}/managers/account/profile", headers=headers)
        profile = profile_response.json() if profile_response.status_code == 200 else {}
        store_id = profile.get('user', {}).get('store', {}).get('id')

        if store_id:
            request.session['store_id'] = store_id
        else:
            print("Store ID not found in profile response.")

        ## Add an entry with the tokens into the database
        tokens = {
            "Distinct_ID": int(get_next_id_from_supabase_compatible_all(name='tokens', column='Distinct_ID')),
            'Access': access_token,
            'Authorization': authorization_token,
            'Refresh': refresh_token,
            'Store_ID': store_id if store_id else 'No Store ID',
            'Tiktok_Access': '',
            'Tiktok_Org': '',
            'Snapchat_Access': '',
            'Snapchat_Refresh': ''
        }

        tokens_df = pd.DataFrame([tokens])
        batch_insert_to_supabase(tokens_df, 'tokens')

        ### Subscribe to the products webhook --
        print("Creating the product webhook")
        subscribe_store_to_product_update(authorization_token, access_token)
        ## Subscribe to the order webhook
        print("creating the new order webhook")
        #subscribe_store_to_order_create(authorization_token, access_token)

        ##### Initial fetch for the products and orders
        threading.Thread(
            target=initial_fetch_products,
            args=(authorization_token, access_token, store_id)
        ).start()

        return redirect('Demo:home')  # go to the home view

    except requests.RequestException as e:
        messages.error(request, f"Token error: {str(e)}")
        return redirect('Demo:zid_login')

def home(request):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')
    store_id = request.session.get('store_id')

    '''print("The access token is:", token)
    print("The authorization token is:", auth_token)
    print("The retrieved store id is:", store_id)
'''
    if not token:
        return redirect('Demo:zid_login')

    headers = {
    'Authorization': f'Bearer {auth_token}',
    'X-MANAGER-TOKEN': token,
    }

    # Fetch products
    # Define the headers to retrieve the products
    headers_product = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token, 
        'accept': 'application/json',
        'Accept-Language': 'all-languages',
        'Store-Id': f'{store_id}',
        'Role': 'Manager',
    }

    profile = {}
    orders = []
    products = []
    total_orders = 0
    total_products = 0
    total_revenue = 0.0
    user_name = ""

    try:
        # Fetch profile information
        profile_res = requests.get(f"{settings.ZID_API_BASE}/managers/account/profile", headers=headers)
        profile_res.raise_for_status()
        profile = profile_res.json()
        user_name = profile.get('user', {}).get('name') or profile.get('username')
        store_title = profile.get('user', {}).get('store', {}).get('title', 'Unknown Store')
        store_uuid = profile.get('user', {}).get('store', {}).get('uuid')
        
        request.session['store_uuid'] = store_uuid

        if not user_name:
            return redirect('Demo:zid_login')
        
        # Fetch orders
        orders_res = requests.get(f"{settings.ZID_API_BASE}/managers/store/orders", headers=headers)
        orders_res.raise_for_status()
        orders_data = orders_res.json()

        # Extract orders and calculate totals
        orders = orders_data.get('orders', [])
        #print("The orders payload is:", orders)
        '''for order in orders[:5]:
            print(order)'''
        total_orders = round(orders_data.get('total_order_count', len(orders)), 2)
        total_revenue = sum(float(order['transaction_amount']) for order in orders)
        total_revenue = round(total_revenue, 2)

        # Process orders to extract total and status
        for order in orders:
            created_str = order.get("created_at")
            updated_str = order.get("updated_at")
            if created_str:
                try:
                    order["created_at"] = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                    order["updated_at"] = datetime.strptime(updated_str, "%Y-%m-%d %H:%M:%S") if updated_str else order["created_at"]
                except ValueError:
                    order["created_at"] = None
                    order["updated_at"] = None

            order['order_total'] = float(order.get('order_total', 0))
            order['config_logo'] = order.get('printed_invoice_settings', {}).get('config_logo', '')
            status_obj = order.get("order_status", {})
            order["display_status"] = status_obj.get("name") or status_obj.get("code") or "unknown"

        # Call the products --
        products_res = requests.get(f"{settings.ZID_API_BASE}/products", headers=headers_product)
        products_res.raise_for_status()
        products_data = products_res.json()

        products = products_data.get('results', [])
        total_products = products_data.get('count', len(products))
        # Process products to extract price and display name
        for product in products:
            product['price'] = float(product.get('price', 0))
            for image in product.get('images', []):
                product['display_image'] = image.get('image', {}).get('thumbnail', '')
            name_obj = product.get("name", {})
            product["display_name"] = name_obj.get("ar") or name_obj.get("en") or "Unnamed"

        # Sort orders and products by total and price respectively
        orders = sorted(orders, key=lambda o: o['order_total'], reverse=True)
        products = sorted(products, key=lambda p: p['price'], reverse=True)

    # Handle any request errors       
    except requests.RequestException as e:
        traceback.print_exc()
        messages.error(request, f"Something went wrong: {str(e)} + {store_id}")
    
    context= {
        'profile': profile,
        'user_name': user_name,
        'store_title': store_title,
        'total_orders': total_orders,
        'total_products': total_products,
        'total_revenue': total_revenue,
        'orders': orders[:10],
        'products': products[:10],
    }

    return render(request, 'Demo/home.html', context)

# Logout and clear session
def zid_logout(request):
    request.session.flush()
    messages.success(request, "You have been logged out.")
    return redirect('Demo:zid_login')

# Refresh access token using stored refresh token
def zid_refresh_token(request):
    """Refresh the access token using session-stored refresh token"""
    refresh_token = request.session.get('refresh_token')
    if not refresh_token:
        return HttpResponseBadRequest("No refresh token found in session.")

    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': settings.ZID_CLIENT_ID,
        'client_secret': settings.ZID_CLIENT_SECRET,
        'redirect_uri': settings.ZID_REDIRECT_URI,
    }

    response = requests.post(settings.ZID_TOKEN_URL, data=payload)

    if response.status_code != 200:
        return JsonResponse({'error': 'Token refresh failed', 'details': response.text}, status=400)

    new_tokens = response.json()

    # Update tokens in session
    request.session['access_token'] = new_tokens.get('access_token')
    request.session['refresh_token'] = new_tokens.get('refresh_token')

    return JsonResponse({'status': 'refreshed', 'new_access_token': new_tokens.get('access_token')})

###############################################################################
################# ZID ORDERS + GOOGLE ANALYTICS ###############################
'''
This section takes in the data from Google Analytics and:
1- Extracts the Order ID from the Analytics data, locates this order in the store through the API connection.
2- Performs analysis on pulled information. 
'''
def normalize_campaign(name):
    if not isinstance(name, str):
        return ""
    # Remove + and multiple spaces, lowercase
    return name.replace("+", " ").lower().strip()

def group_similar_campaigns(campaign_list, threshold=90):
    """Group similar campaign names into one representative name."""
    groups = {}
    for c in campaign_list:
        norm_c = normalize_campaign(c)
        found_group = None
        for key in groups.keys():
            if fuzz.ratio(norm_c, key) >= threshold:
                found_group = key
                break
        if found_group:
            groups[found_group].append(c)
        else:
            groups[norm_c] = [c]
    return groups

def clean_source(source):
    source = str(source).lower()
    if "tiktok" in source:
        return "TikTok"
    elif "google" in source:
        return "Google"
    elif "instagram" in source:
        return "Instagram"
    elif "snapchat" in source:
        return "Snapchat"
    elif "facebook" in source:
        return "Facebook"
    elif "direct" in source or "(direct)" in source:
        return "Direct"
    else:
        return source.title()

def get_order_from_zid(order_id , headers):
    """
    Fetch a single order from Zid API.
    Returns dict with {total, status, source, products_list} or None on error.
    """
    url = f"https://api.zid.sa/v1/managers/store/orders/{order_id}/view"

    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        order = data.get("order", {})
        products_list = []
        for p in order.get("products", []):
            products_list.append({
                "product name": p.get("name", ""),
                "quantity": int(p.get("quantity", 1) or 1)
            })
        return {
            "total": float(order.get("order_total", 0) or 0),
            "status": order.get("order_status", {}).get("name", ""),
            "source": order.get("source", ""),
            "products": products_list
        }
    except Exception as e:
        print(f"Error fetching order {order_id}: {e}")
        return None

def match_orders_with_analytics(request):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')

    if not token:
        return redirect('Demo:zid_login')

    headers = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
    }

    matched = []
    unmatched_orders = []
    unmatched_analytics = []
    total_order_value = 0.0
    total_purchase_revenue = 0.0
    unmatched_order_total = 0.0
    unmatched_merchant_admin_total = 0.0
    unmatched_analytics_total = 0.0
    source_metrics = {}
    campaign_product_sales = {}

    try:
        if request.method == "POST" and request.FILES.get("analytics_file"):
            analytics_file = request.FILES["analytics_file"]

            # Read Analytics CSV
            analytics_df = pd.read_csv(analytics_file, skiprows=9)
            analytics_df['Transaction ID'] = analytics_df['Transaction ID'].astype(str)
            analytics_df['clean_campaign'] = analytics_df['Session campaign'].fillna('').apply(clean_source)

            # Build campaign-level metrics
            source_metrics_df = analytics_df.groupby('clean_campaign', dropna=False).agg({
                'Purchase revenue': 'sum',
                'Ecommerce purchases': 'sum',
                'Ecommerce purchase quantity': 'sum'
            }).reset_index()
            source_metrics = source_metrics_df.set_index('clean_campaign').to_dict(orient='index')

            analytics_ids = set(analytics_df['Transaction ID'])

            for oid in analytics_ids:
                # Call Zid API to get order details
                order_data = get_order_from_zid(oid , headers)
                if not order_data:
                    # If order not found in Zid → unmatched analytics
                    arow = analytics_df.loc[analytics_df['Transaction ID'] == oid].iloc[0]
                    unmatched_analytics.append({
                        "Transaction ID": oid,
                        "Purchase revenue": round(float(arow.get('Purchase revenue', 0) or 0), 2),
                        "date": "N/A"
                    })
                    unmatched_analytics_total += float(arow.get('Purchase revenue', 0) or 0)
                    continue

                # Found order → match
                arow = analytics_df.loc[analytics_df['Transaction ID'] == oid].iloc[0]
                purchase_revenue = float(arow.get('Purchase revenue', 0) or 0)
                raw_campaign = str(arow.get('Session campaign', '') or '')
                campaign_name = normalize_campaign(raw_campaign)

                # Aggregate campaign product sales
                if campaign_name not in campaign_product_sales:
                    campaign_product_sales[campaign_name] = {}
                for p in order_data["products"]:
                    pname = p["product name"]
                    qty = p.get("quantity", 1)
                    campaign_product_sales[campaign_name][pname] = campaign_product_sales[campaign_name].get(pname, 0) + qty

                # Merge similar campaigns
                merged_groups = group_similar_campaigns(list(campaign_product_sales.keys()), threshold=90)
                merged_sales = {}
                for rep_name, variants in merged_groups.items():
                    merged_sales[rep_name] = {}
                    for var in variants:
                        for product, qty in campaign_product_sales[var].items():
                            merged_sales[rep_name][product] = merged_sales[rep_name].get(product, 0) + qty
                campaign_product_sales = dict(
                    sorted(merged_sales.items(), key=lambda x: len(x[1]), reverse=True)
                )

                matched.append({
                    "id": oid,
                    "status": order_data["status"],
                    "campaign": campaign_name,
                    "order_total": round(order_data["total"], 2),
                    "purchase_revenue": round(purchase_revenue, 2),
                    "products": order_data["products"],
                    "products_str": ", ".join(f"{p['product name']} (x{p.get('quantity', 1)})" for p in order_data["products"])
                })

                total_order_value += order_data["total"]
                total_purchase_revenue += purchase_revenue

            # Any order found in Zid but not in analytics → unmatched_orders

    except Exception as e:
        messages.error(request, f"Error: {str(e)}")

    return render(request, "Demo/google-zid.html", {
        "matched": matched,
        "total_order_value": round(total_order_value, 2),
        "total_purchase_revenue": round(total_purchase_revenue, 2),
        "unmatched_orders": unmatched_orders,
        "unmatched_analytics": unmatched_analytics,
        "unmatched_order_total": round(unmatched_order_total, 2),
        "unmatched_analytics_total": round(unmatched_analytics_total, 2),
        "unmatched_merchant_admin_total": round(unmatched_merchant_admin_total, 2),
        "source_metrics": source_metrics,
        "campaign_product_sales": campaign_product_sales,
    })

#############################################################################################################
################################## The Visitor Tracking Section #############################################
@csrf_exempt
@require_POST
def save_tracking_old(request):
    try:
        # ---- Parse JSON ----
        try:
            data = json.loads(request.body)
        except Exception:
            return JsonResponse({"status": "error", "message": "Invalid JSON payload"}, status=400)

        if not data:
            return JsonResponse({"status": "error", "message": "No JSON payload received"}, status=400)

        session_id = data.get("session_id")
        if not session_id:
            return JsonResponse({"status": "error", "message": "session_id is required"}, status=400)

        visitor_info = data.get('visitor_info', {}) or {}
        client_info = data.get('client_info', {}) or {}
        utm_params = data.get('utm_params', {}) or {}
        traffic_source = data.get('traffic_source', {}) or {}
        referrer = data.get("referrer") or ""
        page_url = data.get('page_url') or ""
        agent = client_info.get('user_agent') or ""
        crawlers = ["crawler","bingbot","Googlebot","GoogleOther","Applebot","AdsBot","AhrefsBot"]

        is_crawler = False
        for word in crawlers:
            if word.lower() in agent.lower():
                is_crawler = True
                break

        if is_crawler:
            return JsonResponse({"status": "skipped", "message": "Crawler detected"})

        # If UA source detected, override utm_source
        
        # --------------- NEW: Detect source from referrer URL ------------------

        # 2) If detected_source exists → override UTM_Source
        
        # -----------------------------------------------------------------------

        # ---- Fetch existing session rows for UTM merge ----
        existing_session_data = []
        try:
            existing_session = (
                supabase.table("Tracking_Visitors")
                .select(
                    "Distinct_ID,UTM_Source,UTM_Medium,UTM_Campaign,UTM_Term,UTM_Content,"
                    "Customer_ID,Customer_Name,Customer_Email,Customer_Mobile"
                )
                .eq("Session_ID", session_id)
                .execute()
            )
            if existing_session and existing_session.data:
                existing_session_data = existing_session.data
        except Exception:
            traceback.print_exc()

        # ---- Propagate UTM from existing session rows ----

        for field in ["UTM_Source","UTM_Medium","UTM_Campaign","UTM_Term","UTM_Content"]:
            for r in existing_session_data:
                if r.get(field):
                    utm_params[field.lower()] = r[field]
                    break

        ua_detected_source = detect_source_from_user_agent(agent)
        if referrer:
            referrer_detected_source = detect_source_from_url_or_domain(referrer)
        if not referrer:
            referrer_detected_source = detect_source_from_url_or_domain(page_url)

        if not utm_params.get("utm_source") or utm_params.get("utm_source") == "direct":
            if ua_detected_source:
                utm_params["utm_source"] = ua_detected_source
            elif referrer_detected_source:
                utm_params["utm_source"] = referrer_detected_source
                
        # ---- Fallback: if no UTM source + no detected → direct ----
        if not utm_params.get("utm_source"):
            utm_params["utm_source"] = "direct"

        # ---- Determine customer info ----
        session_customer_info = {}

        if visitor_info.get("customer_id"):
            session_customer_info.update({
                "Customer_ID": int(visitor_info.get("customer_id")),
                "Customer_Name": visitor_info.get("name"),
                "Customer_Email": visitor_info.get("email"),
                "Customer_Mobile": int(visitor_info.get("mobile")) if visitor_info.get("mobile") else None
            })
        else:
            for r in existing_session_data:
                if r.get("Customer_ID"):
                    session_customer_info.update({
                        "Customer_ID": r.get("Customer_ID"),
                        "Customer_Name": r.get("Customer_Name"),
                        "Customer_Email": r.get("Customer_Email"),
                        "Customer_Mobile": r.get("Customer_Mobile"),
                    })
                    break

        # ---- Build tracking row ----
        distinct_id = int(get_next_id_from_supabase_compatible_all(name='Tracking_Visitors', column='Distinct_ID'))

        # ---- Prevent duplicate purchases ----
        event_type = data.get("event_type")
        event_details = data.get("event_details", {})
        visitor_id = data.get('visitor_id'),
        if event_details and event_type == "purchase":
            order_id = event_details.get("order_id")

            try:
                df = fetch_data_from_supabase_specific(
                    "Tracking_Visitors",
                    columns=[
                        "Customer_ID", "Visitor_ID", "Event_Type", "Event_Details"
                    ],
                    filters={
                        "Visitor_ID": ("eq", visitor_id),
                        "Event_Type": ("eq", ["purchase"]),
                        "Event_Details": ("eq", event_details),
                    },
                    limit=1
                )

                print("DEBUG | Supabase raw response:", df)

                if not df.empty:
                    print("DEBUG | DUPLICATE FOUND:", df)

                    return JsonResponse({
                        "status": "skipped",
                        "message": "Duplicate purchase detected",
                        "debug": "purchase already exists"
                    })

                print("DEBUG | No duplicate found, proceeding to insert")

            except Exception as e:
                print("DEBUG | Supabase query failed:", str(e))
                messages.error(request, f"Supabase query error: {str(e)}")
                traceback.print_exc()


        tracking_entry = {
            'Distinct_ID': distinct_id,
            'Visitor_ID': data.get('visitor_id'),
            'Session_ID': session_id,
            'Store_URL': data.get('store_url'),
            'Event_Type': data.get('event_type'),
            'Event_Details': str(data.get('event_details', {})),
            'Page_URL': data.get('page_url'),
            'Visited_at': get_uae_current_date(),

            # UTM values
            'UTM_Source': utm_params.get('utm_source'),
            'UTM_Medium': utm_params.get('utm_medium'),
            'UTM_Campaign': utm_params.get('utm_campaign'),
            'UTM_Term': utm_params.get('utm_term'),
            'UTM_Content': utm_params.get('utm_content'),

            # Referrer
            'Referrer_Platform': referrer,
            'Traffic_Source': traffic_source.get('source'),
            'Traffic_Medium': traffic_source.get('medium'),
            'Traffic_Campaign': traffic_source.get('campaign'),

            # Customer
            'Customer_ID': session_customer_info.get('Customer_ID'),
            'Customer_Name': session_customer_info.get('Customer_Name'),
            'Customer_Email': session_customer_info.get('Customer_Email'),
            'Customer_Mobile': session_customer_info.get('Customer_Mobile'),

            # Client info
            'User_Agent': client_info.get('user_agent'),
            'Language': client_info.get('language'),
            'Timezone': client_info.get('timezone'),
            'Platform': client_info.get('platform'),
            'Screen_Resolution': client_info.get('screen_resolution'),
            'Device_Memory': int(client_info.get('device_memory')) if client_info.get('device_memory') else None,
        }

        # ---- Insert ----
        tracking_entry_df = pd.DataFrame([tracking_entry])
        batch_insert_to_supabase(tracking_entry_df, "Tracking_Visitors")

        # ---- Update customer info for entire session ----
        if session_customer_info.get("Customer_ID"):
            supabase.table("Tracking_Visitors").update({
                "Customer_ID": session_customer_info["Customer_ID"],
                "Customer_Name": session_customer_info.get("Customer_Name"),
                "Customer_Email": session_customer_info.get("Customer_Email"),
                "Customer_Mobile": session_customer_info.get("Customer_Mobile")
            }).eq("Session_ID", session_id).execute()

        return JsonResponse({"status": "success"})

    except Exception as e:
        traceback.print_exc()
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

@csrf_exempt
@require_POST
def save_tracking_no_debug(request):
    try:
        # -------------------------
        # Parse JSON
        # -------------------------
        try:
            data = json.loads(request.body)
        except Exception:
            return JsonResponse({"status": "error", "message": "Invalid JSON payload"}, status=400)

        if not data:
            return JsonResponse({"status": "error", "message": "No JSON payload received"}, status=400)

        session_id = data.get("session_id")
        if not session_id:
            return JsonResponse({"status": "error", "message": "session_id is required"}, status=400)

        visitor_info = data.get("visitor_info", {}) or {}
        client_info = data.get("client_info", {}) or {}
        utm_params = data.get("utm_params", {}) or {}
        traffic_source = data.get("traffic_source", {}) or {}
        referrer = data.get("referrer") or ""
        page_url = data.get('page_url') or ""
        agent = client_info.get("user_agent") or ""

        # -------------------------
        # Block crawlers
        crawlers = [
            "crawler", "bingbot", "googlebot", "googleother",
            "applebot", "adsbot", "ahrefsbot"
        ]

        if any(bot in agent.lower() for bot in crawlers):
            return JsonResponse({"status": "skipped", "message": "Crawler detected"})

        # -------------------------
        # Fetch existing session rows
        existing_session_data = []
        try:
            res = (
                supabase.table("Tracking_Visitors")
                .select(
                    "UTM_Source,UTM_Medium,UTM_Campaign,UTM_Term,UTM_Content,"
                    "Customer_ID,Customer_Name,Customer_Email,Customer_Mobile"
                )
                .eq("Session_ID", session_id)
                .execute()
            )
            if res and res.data:
                existing_session_data = res.data
        except Exception:
            traceback.print_exc()

        # -------------------------
        # Fill existing UTM values (session-level wise)
        # -------------------------
        for field in ["UTM_Source", "UTM_Medium", "UTM_Campaign", "UTM_Term", "UTM_Content"]:
            for row in existing_session_data:
                if row.get(field):
                    utm_params[field.lower()] = row[field]
                    break

        # -------------------------
        # Detecting source
        ua_detected_source = detect_source_from_user_agent(agent)
        if referrer:
            referrer_detected_source = detect_source_from_url_or_domain(referrer)
        if not referrer:
            referrer_detected_source = detect_source_from_url_or_domain(page_url)

        if not utm_params.get("utm_source") or utm_params.get("utm_source") == "direct":
            if ua_detected_source:
                utm_params["utm_source"] = ua_detected_source
            elif referrer_detected_source:
                utm_params["utm_source"] = referrer_detected_source

        if not utm_params.get("utm_source"):
            utm_params["utm_source"] = "direct"

        # -------------------------
        # Determine customer info
        session_customer_info = {}

        if visitor_info.get("customer_id"):
            session_customer_info = {
                "Customer_ID": int(visitor_info.get("customer_id")),
                "Customer_Name": visitor_info.get("name"),
                "Customer_Email": visitor_info.get("email"),
                "Customer_Mobile": int(visitor_info.get("mobile"))
                if visitor_info.get("mobile") else None
            }
        else:
            for row in existing_session_data:
                if row.get("Customer_ID"):
                    session_customer_info = {
                        "Customer_ID": row.get("Customer_ID"),
                        "Customer_Name": row.get("Customer_Name"),
                        "Customer_Email": row.get("Customer_Email"),
                        "Customer_Mobile": row.get("Customer_Mobile"),
                    }
                    break

        # -------------------------
        # Prevent duplicate purchases
        event_type = data.get("event_type")
        event_details = data.get("event_details", {}) or {}
        visitor_id = data.get("visitor_id")

        if event_type == "purchase" and event_details:
            order_id = event_details.get("order_id")

            if order_id:
                try:
                    df = fetch_data_from_supabase_specific(
                        "Tracking_Visitors",
                        filters={
                            "Event_Type": ("eq", "purchase"),
                            "Event_Details": ("like", f"%{order_id}%"),
                        },
                        limit=1
                    )

                    if df is not None and not df.empty:
                        return JsonResponse({
                            "status": "skipped",
                            "message": "Duplicate purchase detected",
                            "order_id": order_id
                        })
                except Exception:
                    traceback.print_exc()

        # -------------------------
        # Build tracking entry
        distinct_id = int(
            get_next_id_from_supabase_compatible_all(
                name="Tracking_Visitors",
                column="Distinct_ID"
            )
        )

        tracking_entry = {
            "Distinct_ID": distinct_id,
            "Visitor_ID": visitor_id,
            "Session_ID": session_id,
            "Store_URL": data.get("store_url"),
            "Event_Type": event_type,
            "Event_Details": str(event_details),
            "Page_URL": data.get("page_url"),
            "Visited_at": get_uae_current_date(),

            # UTM
            "UTM_Source": utm_params.get("utm_source"),
            "UTM_Medium": utm_params.get("utm_medium"),
            "UTM_Campaign": utm_params.get("utm_campaign"),
            "UTM_Term": utm_params.get("utm_term"),
            "UTM_Content": utm_params.get("utm_content"),

            # Referrer & traffic
            "Referrer_Platform": referrer,
            "Traffic_Source": traffic_source.get("source"),
            "Traffic_Medium": traffic_source.get("medium"),
            "Traffic_Campaign": traffic_source.get("campaign"),

            # Customer
            **session_customer_info,

            # Client
            "User_Agent": agent,
            "Language": client_info.get("language"),
            "Timezone": client_info.get("timezone"),
            "Platform": client_info.get("platform"),
            "Screen_Resolution": client_info.get("screen_resolution"),
            "Device_Memory": int(client_info.get("device_memory"))
            if client_info.get("device_memory") else None,
        }

        # -------------------------
        # Insert to db
        batch_insert_to_supabase(
            pd.DataFrame([tracking_entry]),
            "Tracking_Visitors"
        )

        # -------------------------
        # Update customer info across session
        if session_customer_info.get("Customer_ID"):
            supabase.table("Tracking_Visitors").update(session_customer_info)\
                .eq("Session_ID", session_id).execute()

        return JsonResponse({"status": "success"})

    except Exception as e:
        traceback.print_exc()
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

'''@csrf_exempt
@require_POST
def save_tracking(request):
    def dprint(msg):
        print(f"[SAVE_TRACKING DEBUG] {msg}")

    dprint("===== START save_tracking =====")

    try:
        # -------------------------
        # STEP 1: Parse JSON
        # -------------------------
        dprint("Parsing request.body")

        try:
            raw_body = request.body.decode("utf-8")
            dprint(f"Raw body: {raw_body}")
            data = json.loads(raw_body)
        except Exception as e:
            dprint(f"[ERROR] JSON parse failed: {e}")
            return JsonResponse({"status": "error", "message": "Invalid JSON payload"}, status=400)

        if not data:
            dprint("[ERROR] Empty JSON payload")
            return JsonResponse({"status": "error", "message": "No JSON payload received"}, status=400)

        dprint(f"Parsed JSON keys: {list(data.keys())}")

        # -------------------------
        # STEP 2: Required fields
        # -------------------------
        session_id = data.get("session_id")
        dprint(f"session_id = {session_id}")

        if not session_id:
            dprint("[ERROR] session_id missing")
            return JsonResponse({"status": "error", "message": "session_id is required"}, status=400)

        def clean_dict(d):
            return {k: (v.strip() if isinstance(v, str) else v) for k, v in (d or {}).items()}

        visitor_info   = clean_dict(data.get("visitor_info"))
        client_info    = clean_dict(data.get("client_info"))
        utm_params     = clean_dict(data.get("utm_params"))
        traffic_source = clean_dict(data.get("traffic_source"))
        event_type    = (data.get("event_type") or "").strip()
        visitor_id    = (data.get("visitor_id") or "").strip()
        event_details = clean_dict(data.get("event_details"))

        referrer = (data.get("referrer") or "").strip()
        page_url = (data.get("page_url") or "").strip()
        agent    = (client_info.get("user_agent") or "").strip()

        dprint(f"visitor_info: {visitor_info}")
        dprint(f"client_info: {client_info}")
        dprint(f"utm_params (initial): {utm_params}")
        dprint(f"traffic_source: {traffic_source}")
        dprint(f"referrer: {referrer}")
        dprint(f"page_url: {page_url}")
        dprint(f"user_agent: {agent}")

        # -------------------------
        # STEP 3: Block crawlers
        # -------------------------
        crawlers = [
            "crawler", "bingbot", "googlebot", "googleother",
            "applebot", "adsbot", "ahrefsbot"
        ]

        if any(bot in agent.lower() for bot in crawlers):
            dprint("[SKIPPED] Crawler detected via user agent")
            return JsonResponse({"status": "skipped", "message": "Crawler detected"})

        # -------------------------
        # STEP 4: Fetch existing session rows
        # -------------------------
        dprint("Fetching existing session data from Supabase")

        existing_session_data = []

        try:
            res = (
                supabase.table("Tracking_Visitors")
                .select(
                    "UTM_Source,UTM_Medium,UTM_Campaign,UTM_Term,UTM_Content,"
                    "Customer_ID,Customer_Name,Customer_Email,Customer_Mobile"
                )
                .eq("Session_ID", session_id)
                .execute()
            )

            if res and res.data:
                existing_session_data = res.data
                dprint(f"Existing rows found: {len(existing_session_data)}")
            else:
                dprint("No existing rows for session")

        except Exception:
            dprint("[ERROR] Failed fetching existing session data")
            traceback.print_exc()

        # -------------------------
        # STEP 5: Backfill UTM values
        # -------------------------
        dprint("Backfilling UTM values from session history")

        for field in ["UTM_Source", "UTM_Medium", "UTM_Campaign", "UTM_Term", "UTM_Content"]:
            for row in existing_session_data:
                if row.get(field):
                    utm_params[field.lower()] = row[field]
                    dprint(f"Backfilled {field} = {row[field]}")
                    break

        dprint(f"utm_params (after backfill): {utm_params}")

        # -------------------------
        # STEP 6: Detect traffic source
        # -------------------------
        dprint("Detecting traffic source")

        ua_detected_source = detect_source_from_user_agent(agent)
        dprint(f"UA detected source: {ua_detected_source}")

        if referrer:
            referrer_detected_source = detect_source_from_url_or_domain(referrer)
            dprint(f"Referrer detected source: {referrer_detected_source}")
        else:
            referrer_detected_source = detect_source_from_url_or_domain(page_url)
            dprint(f"Page URL detected source: {referrer_detected_source}")

        if not utm_params.get("utm_source") or utm_params.get("utm_source") == "direct":
            if ua_detected_source:
                utm_params["utm_source"] = ua_detected_source
                dprint(f"utm_source set from UA: {ua_detected_source}")
            elif referrer_detected_source:
                utm_params["utm_source"] = referrer_detected_source
                dprint(f"utm_source set from referrer/page: {referrer_detected_source}")

        # ----------------------------------
        # Attribution persistence (anti-direct overwrite)
        # ----------------------------------
        if not utm_params.get("utm_source") or utm_params.get("utm_source") == "direct":

            last_utm = get_last_non_direct_utm(visitor_id, agent)

            if last_utm:
                dprint(f"Reusing last non-direct attribution: {last_utm}")

                utm_params["utm_source"]   = last_utm.get("UTM_Source")
                utm_params["utm_medium"]   = last_utm.get("UTM_Medium")
                utm_params["utm_campaign"] = last_utm.get("UTM_Campaign")
                utm_params["utm_term"]     = last_utm.get("UTM_Term")
                utm_params["utm_content"]  = last_utm.get("UTM_Content")

            else:
                utm_params["utm_source"] = "direct"
                dprint("No previous attribution found → direct")


        # -------------------------
        # STEP 7: Determine customer info
        # -------------------------
        dprint("Resolving customer info")

        session_customer_info = {}

        if visitor_info.get("customer_id"):
            session_customer_info = {
                "Customer_ID": int(visitor_info.get("customer_id")),
                "Customer_Name": visitor_info.get("name"),
                "Customer_Email": visitor_info.get("email"),
                "Customer_Mobile": int(visitor_info.get("mobile"))
                if visitor_info.get("mobile") else None
            }
            dprint(f"Customer info from visitor_info: {session_customer_info}")
        else:
            for row in existing_session_data:
                if row.get("Customer_ID"):
                    session_customer_info = {
                        "Customer_ID": row.get("Customer_ID"),
                        "Customer_Name": row.get("Customer_Name"),
                        "Customer_Email": row.get("Customer_Email"),
                        "Customer_Mobile": row.get("Customer_Mobile"),
                    }
                    dprint(f"Customer info inherited from session: {session_customer_info}")
                    break

        # -------------------------
        # STEP 8: Prevent duplicate purchases
        # -------------------------
        

        dprint(f"event_type: {event_type}")
        dprint(f"event_details: {event_details}")

        if event_type == "purchase" and event_details:
            order_id = event_details.get("order_id")
            dprint(f"Checking duplicate purchase for order_id: {order_id}")

            if order_id:
                try:
                    df = fetch_data_from_supabase_specific(
                    "Tracking_Visitors",
                    filters={'Event_Details': ('eq', str(event_details))},
                )

                    if df is not None and not df.empty:
                        dprint("[SKIPPED] Duplicate purchase detected")
                        return JsonResponse({
                            "status": "skipped",
                            "message": "Duplicate purchase detected",
                            "order_id": order_id
                        })

                except Exception:
                    dprint("[ERROR] Duplicate purchase check failed")
                    traceback.print_exc()

        # -------------------------
        # STEP 9: Build tracking entry
        # -------------------------
        dprint("Building tracking entry")

        distinct_id = int(
            get_next_id_from_supabase_compatible_all(
                name="Tracking_Visitors",
                column="Distinct_ID"
            )
        )

        dprint(f"Generated Distinct_ID: {distinct_id}")

        tracking_entry = {
            "Distinct_ID": distinct_id,
            "Visitor_ID": visitor_id,
            "Session_ID": session_id,
            "Store_URL": data.get("store_url"),
            "Event_Type": event_type,
            "Event_Details": str(event_details),
            "Page_URL": page_url,
            "Visited_at": get_uae_current_date(),

            "UTM_Source": utm_params.get("utm_source"),
            "UTM_Medium": utm_params.get("utm_medium"),
            "UTM_Campaign": utm_params.get("utm_campaign"),
            "UTM_Term": utm_params.get("utm_term"),
            "UTM_Content": utm_params.get("utm_content"),

            "Referrer_Platform": referrer, 
            "Traffic_Source": traffic_source.get("source"),
            "Traffic_Medium": traffic_source.get("medium"),
            "Traffic_Campaign": traffic_source.get("campaign"),

            **session_customer_info,

            "User_Agent": agent,
            "Language": client_info.get("language"),
            "Timezone": client_info.get("timezone"),
            "Platform": client_info.get("platform"),
            "Screen_Resolution": client_info.get("screen_resolution"),
            "Device_Memory": int(client_info.get("device_memory"))
            if client_info.get("device_memory") else None,
        }

        dprint(f"Final tracking entry:\n{tracking_entry}")

        # -------------------------
        # STEP 10: Insert into DB
        # -------------------------
        dprint("Inserting tracking entry into Supabase")

        batch_insert_to_supabase(
            pd.DataFrame([tracking_entry]),
            "Tracking_Visitors"
        )

        dprint("Insert successful")

        # -------------------------
        # STEP 11: Update customer info across session
        # -------------------------
        if session_customer_info.get("Customer_ID"):
            dprint("Updating customer info across session rows")

            supabase.table("Tracking_Visitors") \
                .update(session_customer_info) \
                .eq("Session_ID", session_id) \
                .execute()

        dprint("===== END save_tracking (SUCCESS) =====")
        return JsonResponse({"status": "success"})

    except Exception as e:
        dprint("[FATAL ERROR] save_tracking crashed")
        traceback.print_exc()
        return JsonResponse({"status": "error", "message": str(e)}, status=500)'''




#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
##############################################################################################################################
##############################################################################################################################
##################### SAVE TRACKING UPDATE AS OF THE 25TH OF JAN - SARAH #############################################################
def source_weight(source):
    """
    A function that gives each source weight to decide on what the actual source is for the said mobile_number or the visitor_id.

    """
    weights = {
        "instagram": 100,
        "facebook": 100,
        "tiktok": 100,
        "snapchat": 100,

        "google": 70,
        "bing": 60,

        "referral": 50,
        "email": 40,

        "direct": 20,
        "unknown": 10,
    }
    return weights.get((source or "").lower(), 10) ## Any source not listed gets a 10

def pick_stronger_source(incoming, existing):
    """
    Which source wins based on weight.
    """
    if not existing:
        return incoming
    if source_weight(incoming) > source_weight(existing):
        return incoming
    return existing

def is_confirmed_direct(incoming_source, page_url, store_url, referrer):
    """
    Decide whether this hit deserves to be called 'direct'
    or whether it's just 'unknown'.
    it's direct only if the user explicity typed in our url in the search, otherwise, unknown.
    """
    if incoming_source != "direct":
        return False

    if referrer:
        return False

    if not page_url or not store_url:
        return False

    # Must be homepage or intentional entry
    page = page_url.rstrip("/")
    store = store_url.rstrip("/")

    return page == store



@csrf_exempt
@require_POST
def save_tracking_mobile_priority(request):
    """
    My function with heavy debugging, this function prioritizes mobile sources above all which is wrong, hence why it is replaced by the one below.
    """

    def dprint(msg):
        print(f"[SAVE_TRACKING] {msg}")

    dprint("========== START save_tracking ==========")

    try:
        # --------------------------------------------------
        # Get the data incoming from the tracking-snippet.
        try:
            raw_body = request.body.decode("utf-8")
            dprint(f"[RAW PAYLOAD] {raw_body[:600]}")
            data = json.loads(raw_body)
        except Exception as e:
            dprint(f"[FATAL] JSON parse failed: {e}")
            return JsonResponse({"status": "error"}, status=400)

        # --------------------------------------------------
        # Helper with cleaning the dicts
        def clean_dict(d):
            cleaned = {k: (v.strip() if isinstance(v, str) else v) for k, v in (d or {}).items()}
            dprint(f"[CLEAN_DICT] input={d} → output={cleaned}")
            return cleaned

        # --------------------------------------------------
        # Extract raw inputs (NO LOGIC YET)
        visitor_id = str(data.get("visitor_id") or "").strip()
        session_id = str(data.get("session_id") or "").strip()
        event_type = str(data.get("event_type") or "").strip()

        dprint(f"[IDS] visitor_id={visitor_id} | session_id={session_id} | event_type={event_type}")

        event_details = clean_dict(data.get("event_details"))
        utm_params = clean_dict(data.get("utm_params"))
        traffic_source = clean_dict(data.get("traffic_source"))
        visitor_info = clean_dict(data.get("visitor_info"))
        client_info = clean_dict(data.get("client_info"))

        # --------------------------------------------------
        # Extract extra customer
        session_customer_info = {}
        for key in ["Customer_ID", "Customer_Name", "Customer_Email", "Customer_Mobile"]:
            if visitor_info.get(key) is not None:
                session_customer_info[key] = visitor_info[key]
                dprint(f"[CUSTOMER FIELD] {key}={visitor_info.get(key)}")

        page_url = data.get("page_url")
        store_url = data.get("store_url")
        referrer = data.get("referrer")
        agent = (client_info.get("user_agent") or "").strip().lower()

        dprint(f"[CONTEXT] page_url={page_url}")
        dprint(f"[CONTEXT] referrer={referrer}")
        dprint(f"[CONTEXT] user_agent={agent}")

        # --------------------------------------------------
        # Kill crawlers
        crawlers = ["googlebot", "bingbot", "crawler", "adsbot", "ahrefs"]
        if any(bot in agent for bot in crawlers):
            dprint("[SKIPPED] crawler detected")
            return JsonResponse({"status": "skipped"})

        # --------------------------------------------------
        # Resolve INCOMING source
        incoming_source = (
            utm_params.get("utm_source")
            or traffic_source.get("source")
            or "unknown"
        ).lower()

        if incoming_source in ["", "(not set)", "(none)", None]:
            incoming_source = "unknown"

        final_source = incoming_source
        attribution_type = "explicit_utm" if utm_params.get("utm_source") else "unknown_first_touch"

        dprint(f"[INCOMING SOURCE] {incoming_source}")
        dprint(f"[ATTRIBUTION INIT] {attribution_type}")

        # --------------------------------------------------
        # If the incoming source == unknown, check if we can get the source from either the user agent or the referrer url
        ## Prior source cleaning to handle later on based on identifier_type
        if incoming_source == "unknown":
            dprint("[INFER SOURCE] incoming_source is unknown → attempting inference")

            inferred_source = None

            # Try getting the source from the referrer / page URL
            inferred_source = (
                detect_source_from_url_or_domain(referrer)
                or detect_source_from_url_or_domain(page_url)
            )

            # If URL inference failed, try user agent
            if not inferred_source:
                inferred_source = detect_source_from_user_agent(agent)

                if inferred_source:
                    attribution_type = "inferred_user_agent"
                    dprint(f"[INFERRED FROM UA] {inferred_source}")

            else:
                attribution_type = "inferred_referrer"
                dprint(f"[INFERRED FROM URL] {inferred_source}")

            # If we successfully got an inferred source, that is the incoming_source now.
            if inferred_source:
                incoming_source = inferred_source.lower()
                dprint(f"[INCOMING SOURCE SET] {incoming_source}")
            else:
                incoming_source = "unknown"
                dprint("[INFER RESULT] still unknown — no inference applied")

        # --------------------------------------------------
        # DIRECT CONFIRMATION  --- if it's an actual direct, modify the incoming source to point for direct, then the rest of the logic applies if we're gonna need to change that according to previous recorded history.
        # Having it in the section underneath would have the resutls from the 2 next blocks overwritten.
        ## Also part of cleaning prior to update based on identity_type.
        if incoming_source == "direct":
            is_homepage = page_url and store_url and page_url.rstrip("/") == store_url.rstrip("/")
            dprint(f"[DIRECT CHECK] homepage={is_homepage} referrer={referrer}")

            if not referrer and is_homepage:
                incoming_source = "direct"
                attribution_type = "direct_confirmed"
                dprint("[DIRECT CONFIRMED] incoming_source set to 'direct'")
            else:
                incoming_source = "unknown"
                attribution_type = "unknown_first_touch"
                dprint("[DIRECT DOWNGRADED] incoming_source set to 'unknown'")

        # --------------------------------------------------
        # MOBILE NUMBER = First to check
        mobile = str(visitor_info.get("mobile") or "").strip()

        if mobile:
            dprint(f"[MOBILE DETECTED] {mobile}")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source, Session_ID")
                .eq("Customer_Mobile", mobile)
                .execute()
            )

            strongest_source = incoming_source
            existing_social_found = False
            existing_social_value = None
            session_social_map = {}

            dprint(f"[MOBILE HISTORY COUNT] {len(res.data or [])}")

            for row in res.data or []:
                existing = str(row.get("UTM_Source") or "").strip().lower()
                sid = str(row.get("Session_ID")).strip()

                dprint(f"[MOBILE ROW] session={sid} source={existing}")

                if source_weight(existing) >= 100:
                    existing_social_found = True
                    existing_social_value = existing
                    session_social_map[sid] = existing
                    dprint(f"[SOCIAL FOUND] {existing} (session {sid})")

                strongest_source = pick_stronger_source(strongest_source, existing)
                dprint(f"[STRONGEST UPDATE] now={strongest_source}")

            # Determine final source for current session -- update source for session in db if incoming session source holds greater weight.
            if session_id in session_social_map:
                recorded_source = session_social_map[session_id]
                if source_weight(incoming_source) > source_weight(recorded_source):
                    final_source = incoming_source
                    attribution_type = "incoming_higher_weight"
                    dprint(f"[SESSION OVERRIDE] {recorded_source} -- {incoming_source}")

                    # Persist stronger source across all session rows
                    supabase.table("Tracking_Visitors_duplicate") \
                        .update({"UTM_Source": final_source}) \
                        .eq("Session_ID", session_id) \
                        .execute()
                    dprint(f"[SESSION UPDATE] persisted stronger source to all rows")
                else:
                    final_source = recorded_source
                    attribution_type = "persisted_mobile_session"
                    dprint(f"[SESSION SOCIAL INHERITED] {final_source}")

            elif existing_social_found:
                if source_weight(incoming_source) >= 100:
                    final_source = incoming_source
                    dprint(f"[NEW SOCIAL EVENT] keeping {final_source}")
                else:
                    final_source = existing_social_value
                    dprint(f"[SOCIAL PRESERVED] using {final_source}")

                attribution_type = "persisted_mobile"

            else:
                final_source = strongest_source
                if final_source != incoming_source:
                    attribution_type = "persisted_mobile"
                    dprint(f"[MOBILE OVERRIDE] {incoming_source} → {final_source}")

            session_customer_info["Customer_Mobile"] = mobile

        # --------------------------------------------------
        # VISITOR ID logic if mobile missing
        if not mobile and visitor_id:
            dprint("[VISITOR FALLBACK] no mobile, checking visitor_id")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("Customer_Mobile, UTM_Source, Session_ID")
                .eq("Visitor_ID", visitor_id)
                .execute()
            )

            strongest_source = incoming_source
            discovered_mobiles = set()
            existing_social_value = None
            session_social_map = {}

            dprint(f"[VISITOR HISTORY COUNT] {len(res.data or [])}")

            for row in res.data or []:
                existing = (row.get("UTM_Source") or "").lower()
                sid = row.get("Session_ID")

                dprint(f"[VISITOR ROW] session={sid} source={existing}")

                if row.get("Customer_Mobile"):
                    discovered_mobiles.add(row["Customer_Mobile"])

                if source_weight(existing) >= 100:
                    existing_social_value = existing
                    session_social_map[sid] = existing
                    dprint(f"[SOCIAL FOUND VISITOR] {existing}")

                strongest_source = pick_stronger_source(strongest_source, existing)

            # Determine final source for current session
            if session_id in session_social_map:
                recorded_source = session_social_map[session_id]
                if source_weight(incoming_source) > source_weight(recorded_source):
                    final_source = incoming_source
                    attribution_type = "incoming_higher_weight_visitor"
                    dprint(f"[SESSION OVERRIDE VISITOR] {recorded_source} → {incoming_source}")

                    # Persist stronger source across all session rows
                    supabase.table("Tracking_Visitors_duplicate") \
                        .update({"UTM_Source": final_source}) \
                        .eq("Session_ID", session_id) \
                        .execute()
                    dprint(f"[SESSION UPDATE VISITOR] persisted stronger source to all rows")
                else:
                    final_source = recorded_source
                    attribution_type = "persisted_visitor_session"
                    dprint(f"[SESSION SOCIAL VISITOR INHERITED] {final_source}")

            elif existing_social_value:
                if source_weight(incoming_source) > source_weight(existing_social_value):
                    final_source = incoming_source
                    dprint(f"[VISITOR NEW SOCIAL EVENT] keeping {final_source}")
                else:
                    final_source = existing_social_value
                    dprint(f"[VISITOR SOCIAL PRESERVED] using {final_source}")

                attribution_type = "persisted_visitor"

            else:
                final_source = strongest_source

            if discovered_mobiles:
                resolved_mobile = list(discovered_mobiles)[0]
                session_customer_info["Customer_Mobile"] = resolved_mobile
                dprint(f"[VISITOR>>MOBILE LINK] {resolved_mobile}")

        '''# --------------------------------------------------
        # FIRST / LAST / ASSISTED ATTRIBUTION
        identity_column = None
        identity_value = None

        if session_customer_info.get("Customer_Mobile"):
            identity_column = "Customer_Mobile"
            identity_value = session_customer_info["Customer_Mobile"]
        elif visitor_id:
            identity_column = "Visitor_ID"
            identity_value = visitor_id

        dprint(f"[ATTRIBUTION IDENTITY] {identity_column}={identity_value}")

        first_touch_source = None
        first_touch_medium = None
        first_touch_timestamp = None
        assisted_sources = []

        if identity_column and identity_value:
            history = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source, UTM_Medium, Visited_at")
                .eq(identity_column, identity_value)
                .order("Visited_at", desc=False)
                .execute()
            )

            dprint(f"[HISTORY ROWS] {len(history.data or [])}")

            if history.data:
                first_row = history.data[0]
                first_touch_source = first_row.get("UTM_Source")
                first_touch_medium = first_row.get("UTM_Medium")
                first_touch_timestamp = first_row.get("Visited_at")

                dprint(f"[FIRST TOUCH] source={first_touch_source} medium={first_touch_medium}")

                for row in history.data:
                    src = row.get("UTM_Source")
                    if src and src not in [first_touch_source, final_source]:
                        assisted_sources.append(src)

        assisted_sources = list(set(assisted_sources))
        dprint(f"[ASSISTED SOURCES] {assisted_sources}")'''

        # --------------------------------------------------
        # Build tracking row
        tracking_entry = {
            "Distinct_ID": int(get_next_id_from_supabase_compatible_all(name="Tracking_Visitors_duplicate", column="Distinct_ID")),
            "Visitor_ID": visitor_id,
            "Session_ID": session_id,
            "Event_Type": event_type,
            "Event_Details": str(event_details),
            "Page_URL": page_url,
            "Referrer_Platform": referrer,
            "Visited_at": get_uae_current_date(),

            "UTM_Source": final_source,
            "UTM_Medium": utm_params.get("utm_medium"),
            "UTM_Campaign": utm_params.get("utm_campaign"),
            "UTM_Term": utm_params.get("utm_term"),
            "UTM_Content": utm_params.get("utm_content"),

            "Attribution_Type": attribution_type,

            #"First_Touch_Source": first_touch_source or final_source,
            "First_Touch_Source": "PLACEHOLDER",
            #"First_Touch_Medium": first_touch_medium,
            "First_Touch_Medium": "PLACEHOLDER",
            #"First_Touch_Timestamp": first_touch_timestamp,
            "First_Touch_Timestamp": "PLACEHOLDER",

            "Last_Touch_Source": final_source,
            #"Assisted_Sources": assisted_sources,
            "Assisted_Sources": "PLACEHOLDER",

            "User_Agent": agent,
            "Language": client_info.get("language"),
            "Timezone": client_info.get("timezone"),
            "Platform": client_info.get("platform"),
            "Screen_Resolution": client_info.get("screen_resolution"),
            "Device_Memory": client_info.get("device_memory"),
            "Last_Updated": get_uae_current_date(),

            **session_customer_info,
        }

        dprint(f"[INSERT PAYLOAD PREVIEW] {tracking_entry}")

        batch_insert_to_supabase(pd.DataFrame([tracking_entry]), "Tracking_Visitors_duplicate")

        # --------------------------------------------------
        # Backfill ONLY non-social sources across same mobile
        if session_customer_info.get("Customer_Mobile") and source_weight(final_source) < 100:
            dprint(f"[BACKFILL] non-social → {final_source}")
            supabase.table("Tracking_Visitors") \
                .update({"UTM_Source": final_source}) \
                .eq("Customer_Mobile", session_customer_info["Customer_Mobile"]) \
                .execute()

        dprint("========== END save_tracking (SUCCESS) ==========")
        return JsonResponse({"status": "success"})

    except Exception as e:
        dprint(f"[CRASH] save_tracking failed: {e}")
        traceback.print_exc()
        return JsonResponse({"status": "error"}, status=500)


@csrf_exempt
@require_POST
def save_tracking_with_commented_sections_for_future_reference(request):
    """
    - This function takes in the incoming source, makes sure that it is an actual direct, if the incoming source is deemed 'unknown' it attempts to extract the source from the UA, the page URL or the referrer URL.
    - When processing sources, it gives the visitor_id priority, looks for the incoming session if visitor_id found in history, takes in session-level source if session is found for the said visitor_id. If no session match found for the said visitor_id but visitor_id is found it takes in the source with the greatest weight and gives it to the said entry.
    """

    def dprint(msg):
        print(f"[SAVE_TRACKING] {msg}")

    dprint("========== START save_tracking ==========")

    try:
        # --------------------------------------------------
        # Get the data incoming from the tracking-snippet.
        try:
            raw_body = request.body.decode("utf-8")
            dprint(f"[RAW PAYLOAD] {raw_body[:600]}")
            data = json.loads(raw_body)
        except Exception as e:
            dprint(f"[FATAL] JSON parse failed: {e}")
            return JsonResponse({"status": "error"}, status=400)

        # --------------------------------------------------
        # Helper with cleaning the dicts
        def clean_dict(d):
            cleaned = {k: (v.strip() if isinstance(v, str) else v) for k, v in (d or {}).items()}
            dprint(f"[CLEAN_DICT] input={d} → output={cleaned}")
            return cleaned

        # --------------------------------------------------
        # Extract raw inputs (NO LOGIC YET)
        visitor_id = str(data.get("visitor_id") or "").strip()
        session_id = str(data.get("session_id") or "").strip()
        event_type = str(data.get("event_type") or "").strip()

        dprint(f"[IDS] visitor_id={visitor_id} | session_id={session_id} | event_type={event_type}")

        event_details = clean_dict(data.get("event_details"))
        utm_params = clean_dict(data.get("utm_params"))
        traffic_source = clean_dict(data.get("traffic_source"))
        visitor_info = clean_dict(data.get("visitor_info"))
        client_info = clean_dict(data.get("client_info"))

        # --------------------------------------------------
        # Extract extra customer
        session_customer_info = {}
        for key in ["Customer_ID", "Customer_Name", "Customer_Email", "Customer_Mobile"]:
            if visitor_info.get(key) is not None:
                session_customer_info[key] = visitor_info[key]
                dprint(f"[CUSTOMER FIELD] {key}={visitor_info.get(key)}")

        page_url = data.get("page_url")
        store_url = data.get("store_url")
        referrer = data.get("referrer")
        agent = (client_info.get("user_agent") or "").strip().lower()

        dprint(f"[CONTEXT] page_url={page_url}")
        dprint(f"[CONTEXT] referrer={referrer}")
        dprint(f"[CONTEXT] user_agent={agent}")

        # --------------------------------------------------
        # Kill crawlers
        crawlers = ["googlebot", "bingbot", "crawler", "adsbot", "ahrefs"]
        if any(bot in agent for bot in crawlers):
            dprint("[SKIPPED] crawler detected")
            return JsonResponse({"status": "skipped"})

        # --------------------------------------------------
        # Get INCOMING source
        '''incoming_source = (
            (utm_params.get("utm_source"))
            .strip()
            .lower()
            or "unknown"
        )

        if incoming_source in ["", "(not set)", "(none)", None]:
            incoming_source = "unknown"

        final_source = incoming_source
        attribution_type = "explicit_utm" if str(utm_params.get("utm_source")).strip() else "unknown_first_touch"

        dprint(f"[INCOMING SOURCE] {incoming_source}")
        dprint(f"[ATTRIBUTION INIT] {attribution_type}")'''

        # --------------------------------------------------
        # If the incoming source == unknown, check if we can get the source from either the user agent or the referrer url
        ## Prior source cleaning to handle later on based on identifier_type
        '''if incoming_source == "unknown":
            dprint("[INFER SOURCE] incoming_source is unknown → attempting inference")

            inferred_source = None

            # Try getting the source from the referrer / page URL
            inferred_source = (
                detect_source_from_url_or_domain(referrer)
                or detect_source_from_url_or_domain(page_url)
            )

            # If URL inference failed, try user agent
            if not inferred_source:
                inferred_source = detect_source_from_user_agent(agent)

                if inferred_source:
                    attribution_type = "inferred_user_agent"
                    dprint(f"[INFERRED FROM UA] {inferred_source}")

            else:
                attribution_type = "inferred_referrer"
                dprint(f"[INFERRED FROM URL] {inferred_source}")

            # If we successfully got an inferred source, that is the incoming_source now.
            if inferred_source:
                incoming_source = inferred_source.lower()
                dprint(f"[INCOMING SOURCE SET] {incoming_source}")
            else:
                incoming_source = "unknown"
                dprint("[INFER RESULT] still unknown — no inference applied")'''
         ## This section was commented and replaced with a block where we always infer the source from any referrer, user agent, or traffic source.

        ### LOG THE RAW UTM 
        raw_utm_source = str(utm_params.get("utm_source") or "").strip().lower()
        dprint(f"[RAW UTM SOURCE] {raw_utm_source or 'none'}")

        # Intialize a list to store possible candidates to act as the incoming source (NO DECISIONS YET)
        candidates = []

        # Explicit UTM (strongest)
        if raw_utm_source:
            candidates.append({
                "source": raw_utm_source,
                "type": "explicit_utm"
            })

        # Referrer inference (outweighs UA)
        ref_source = (
            detect_source_from_url_or_domain(referrer)
            or detect_source_from_url_or_domain(page_url)
        )
        if ref_source:
            candidates.append({
                "source": ref_source.lower(),
                "type": "inferred_referrer"
            })

        # User agent inference
        ua_source = detect_source_from_user_agent(agent)
        if ua_source:
            candidates.append({
                "source": ua_source.lower(),
                "type": "inferred_user_agent"
            })

        # Traffic source fallback (weakest)
        traffic_fallback = (traffic_source.get("source") or "").strip().lower()
        if traffic_fallback:
            candidates.append({
                "source": traffic_fallback,
                "type": "traffic_source"
            })

        if not candidates:
            candidates.append({
                "source": "unknown",
                "type": "unknown"
            })

        # --------------------------------------------------
        # PICK STRONGEST CANDIDATE FROM THE LIST
        winner = candidates[0]
        for c in candidates[1:]:
            if source_weight(c["source"]) > source_weight(winner["source"]):
                winner = c

        incoming_source = winner["source"]
        attribution_type = winner["type"]

        dprint(f"[SOURCE WINNER] {incoming_source} ({attribution_type})")

        ### MOVING ON WITH DIRECT CONFIRMATION AND HISTORY OUTLOOK. --- 
        # --------------------------------------------------
        # DIRECT CONFIRMATION  --- if it's an actual direct, modify the incoming source to point for direct, then the rest of the logic applies if we're gonna need to change that according to previous recorded history.
        # Having it in the section underneath would have the resutls from the 2 next blocks overwritten.
        ## Also part of cleaning prior to update based on identity_type.
        if incoming_source == "direct":
            is_homepage = page_url and store_url and page_url.rstrip("/") == store_url.rstrip("/")
            dprint(f"[DIRECT CHECK] homepage={is_homepage} referrer={referrer}")

            if not referrer and is_homepage:
                incoming_source = "direct"
                attribution_type = "direct_confirmed"
                dprint("[DIRECT CONFIRMED] incoming_source set to 'direct'")
            else:
                incoming_source = "unknown"
                attribution_type = "unknown_first_touch"
                dprint("[DIRECT DOWNGRADED] incoming_source set to 'unknown'")

        # --------------------------------------------------
        ### THE BLOCK TO DECIDE ON WEHTHER INCOMING SOURCE IS UPDATED OR NOT --- 
        # SESSION = AUTHORITATIVE SOURCE -- LOOK FOR THE INCOMING SESSION -- IF SESSION FOUND COMPARE ICNOMING SOURCE WITH EXISTING SOURCE GIVE IN WEIGHTS AND UPDATE ACCORDINGLY.
        session_source = None
        session_rows = (
            supabase.table("Tracking_Visitors_duplicate")
            .select("UTM_Source")
            .eq("Session_ID", session_id)
            .execute()
        ).data or []

        if session_rows:
            recorded_source = (
                (session_rows[0].get("UTM_Source") or "").strip().lower()
                or "unknown"
            )
            dprint(f"[SESSION FOUND] recorded={recorded_source}")

            if recorded_source != "unknown":
                session_source = recorded_source

            '''if source_weight(incoming_source) > source_weight(recorded_source):
                final_source = incoming_source
                attribution_type = "session_upgraded"

                supabase.table("Tracking_Visitors_duplicate") \
                    .update({"UTM_Source": final_source}) \
                    .eq("Session_ID", session_id) \
                    .execute()

                dprint(f"[SESSION UPGRADE] {recorded_source} >> {final_source}")
            else:
                final_source = recorded_source
                attribution_type = "session_persisted"

                dprint(f"[SESSION PERSISTED] using {final_source}")'''

            # --------------------------------------------------
            # CASE 1 -- VALID SESSION SOURCE EXISTS
            if session_source:
                if source_weight(incoming_source) > source_weight(session_source):
                    final_source = incoming_source
                    attribution_type = "session_upgraded"

                    supabase.table("Tracking_Visitors_duplicate") \
                        .update({"UTM_Source": final_source}) \
                        .eq("Session_ID", session_id) \
                        .execute()

                    dprint(f"[SESSION UPGRADE] {session_source} >> {final_source}")
                else:
                    final_source = session_source
                    attribution_type = "session_persisted"
                    dprint(f"[SESSION PERSISTED] using {final_source}")

        # --------------------------------------------------
        # VISITOR ID = ONLY IF SESSION UNRESOLVED --- If no records founded for the said session to pull'em sources from, we resort to the visitor_id, get the greates weighing source.
        # --------------------------------------------------
        # CASE 2: SESSION UNKNOWN >>> CHECK USING VISITOR_ID

        elif visitor_id:
            dprint("[VISITOR CHECK] session unknown >> checking visitor history")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source, Session_ID, Customer_Mobile")
                .eq("Visitor_ID", visitor_id)
                .execute()
            )

            strongest_source = incoming_source
            discovered_mobile = None

            for row in res.data or []:
                existing = (str((row.get("UTM_Source") or "")).strip().lower() or "unknown")
                strongest_source = pick_stronger_source(strongest_source, existing)

                if row.get("Customer_Mobile"):
                    discovered_mobile = row["Customer_Mobile"]

            if strongest_source != "unknown":
                final_source = strongest_source
                attribution_type = "visitor_inferred"
                dprint(f"[VISITOR INFERRED] {final_source}")

                # Backfill the UNKNOWN session using the source found in the visitor_id
                if session_rows:
                    supabase.table("Tracking_Visitors_duplicate") \
                        .update({"UTM_Source": final_source}) \
                        .eq("Session_ID", session_id) \
                        .execute()
                    dprint(f"[SESSION BACKFILLED FROM VISITOR] {final_source}")

            if discovered_mobile:
                session_customer_info["Customer_Mobile"] = discovered_mobile
                dprint(f"[VISITOR>>MOBILE LINK] {discovered_mobile}")


        ''''elif visitor_id:
            dprint("[VISITOR CHECK] session unresolved >> checking visitor history")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source, Session_ID, Customer_Mobile")
                .eq("Visitor_ID", visitor_id)
                .execute()
            )

            strongest_source = incoming_source
            discovered_mobile = None

            for row in res.data or []:
                existing = ((row.get("UTM_Source") or "").strip().lower() or "unknown")
                strongest_source = pick_stronger_source(strongest_source, existing)

                if row.get("Customer_Mobile"):
                    discovered_mobile = row["Customer_Mobile"]

            if strongest_source != incoming_source:
                final_source = strongest_source
                attribution_type = "visitor_inferred"
                dprint(f"[VISITOR INFERRED] {final_source}")

            if discovered_mobile:
                session_customer_info["Customer_Mobile"] = discovered_mobile
                dprint(f"[VISITOR>>MOBILE LINK] {discovered_mobile}")'''

        # --------------------------------------------------
        '''# MOBILE = LAST RESORT RECONCILIATION
        mobile = str(visitor_info.get("mobile") or "").strip()

        if final_source == incoming_source and mobile:
            dprint(f"[MOBILE RECONCILE] unresolved >> checking mobile {mobile}")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source")
                .eq("Customer_Mobile", mobile)
                .execute()
            )

            strongest_source = incoming_source

            for row in res.data or []:
                existing = ((row.get("UTM_Source") or "").strip().lower() or "unknown")
                strongest_source = pick_stronger_source(strongest_source, existing)

            if strongest_source != incoming_source:
                final_source = strongest_source
                attribution_type = "mobile_unified"
                dprint(f"[MOBILE UNIFIED] {final_source}")

            session_customer_info["Customer_Mobile"] = mobile'''

        # --------------------------------------------------
        # CASE 3: BOTH SESSION + VISITOR UNKNOWN RESORT TO MOBILE SOURCE 
        mobile = str(visitor_info.get("mobile") or "").strip()

        if final_source == "unknown" and mobile:
            dprint(f"[MOBILE RECONCILE] unresolved >> checking mobile {mobile}")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source")
                .eq("Customer_Mobile", mobile)
                .execute()
            )

            strongest_source = incoming_source

            for row in res.data or []:
                existing = (str((row.get("UTM_Source") or "")).strip().lower() or "unknown")
                strongest_source = pick_stronger_source(strongest_source, existing)

            if strongest_source != "unknown":
                final_source = strongest_source
                attribution_type = "mobile_unified"
                dprint(f"[MOBILE UNIFIED] {final_source}")

            session_customer_info["Customer_Mobile"] = mobile


        '''# --------------------------------------------------
        # FIRST / LAST / ASSISTED ATTRIBUTION
        identity_column = None
        identity_value = None

        if session_customer_info.get("Customer_Mobile"):
            identity_column = "Customer_Mobile"
            identity_value = session_customer_info["Customer_Mobile"]
        elif visitor_id:
            identity_column = "Visitor_ID"
            identity_value = visitor_id

        dprint(f"[ATTRIBUTION IDENTITY] {identity_column}={identity_value}")

        first_touch_source = None
        first_touch_medium = None
        first_touch_timestamp = None
        assisted_sources = []

        if identity_column and identity_value:
            history = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source, UTM_Medium, Visited_at")
                .eq(identity_column, identity_value)
                .order("Visited_at", desc=False)
                .execute()
            )

            dprint(f"[HISTORY ROWS] {len(history.data or [])}")

            if history.data:
                first_row = history.data[0]
                first_touch_source = first_row.get("UTM_Source")
                first_touch_medium = first_row.get("UTM_Medium")
                first_touch_timestamp = first_row.get("Visited_at")

                dprint(f"[FIRST TOUCH] source={first_touch_source} medium={first_touch_medium}")

                for row in history.data:
                    src = row.get("UTM_Source")
                    if src and src not in [first_touch_source, final_source]:
                        assisted_sources.append(src)

        assisted_sources = list(set(assisted_sources))
        dprint(f"[ASSISTED SOURCES] {assisted_sources}")'''

        # --------------------------------------------------
        # Build tracking row
        tracking_entry = {
            "Distinct_ID": int(get_next_id_from_supabase_compatible_all(name="Tracking_Visitors_duplicate", column="Distinct_ID")),
            "Visitor_ID": visitor_id,
            "Session_ID": session_id,
            "Event_Type": event_type,
            "Event_Details": str(event_details),
            "Page_URL": page_url,
            "Referrer_Platform": referrer,
            "Visited_at": get_uae_current_date(),

            "UTM_Source": final_source,
            "UTM_Medium": utm_params.get("utm_medium"),
            "UTM_Campaign": utm_params.get("utm_campaign"),
            "UTM_Term": utm_params.get("utm_term"),
            "UTM_Content": utm_params.get("utm_content"),

            "Attribution_Type": attribution_type,

            #"First_Touch_Source": first_touch_source or final_source,
            "First_Touch_Source": "PLACEHOLDER",
            #"First_Touch_Medium": first_touch_medium,
            "First_Touch_Medium": "PLACEHOLDER",
            #"First_Touch_Timestamp": first_touch_timestamp,
            "First_Touch_Timestamp": "PLACEHOLDER",

            "Last_Touch_Source": final_source,
            #"Assisted_Sources": assisted_sources,
            "Assisted_Sources": "PLACEHOLDER",

            "User_Agent": agent,
            "Language": client_info.get("language"),
            "Timezone": client_info.get("timezone"),
            "Platform": client_info.get("platform"),
            "Screen_Resolution": client_info.get("screen_resolution"),
            "Device_Memory": client_info.get("device_memory"),
            "Last_Updated": get_uae_current_date(),
            "RAW_UTM_SOURCE": raw_utm_source,
            "Which_Update": "270126 1155"

            **session_customer_info,
        }

        dprint(f"[INSERT PAYLOAD PREVIEW] {tracking_entry}")

        batch_insert_to_supabase(pd.DataFrame([tracking_entry]), "Tracking_Visitors_duplicate")

        # --------------------------------------------------
        # Backfill ONLY non-social sources across same mobile
        if session_customer_info.get("Customer_Mobile") and source_weight(final_source) < 100:
            dprint(f"[BACKFILL] non-social → {final_source}")
            supabase.table("Tracking_Visitors") \
                .update({"UTM_Source": final_source}) \
                .eq("Customer_Mobile", session_customer_info["Customer_Mobile"]) \
                .execute()

        dprint("========== END save_tracking (SUCCESS) ==========")
        return JsonResponse({"status": "success"})

    except Exception as e:
        dprint(f"[CRASH] save_tracking failed: {e}")
        traceback.print_exc()
        return JsonResponse({"status": "error"}, status=500)


@csrf_exempt
@require_POST
def save_tracking(request):
    """
    - This function takes in the incoming source, makes sure that it is an actual direct, 
      if the incoming source is deemed 'unknown' it attempts to extract the source from 
      the UA, the page URL or the referrer URL.
    - When processing sources, it gives the visitor_id priority, looks for the incoming session 
      if visitor_id found in history, takes in session-level source if session is found for the said visitor_id. 
      If no session match found for the said visitor_id but visitor_id is found it takes in the source 
      with the greatest weight and gives it to the said entry.
    """

    def dprint(msg):
        print(f"[SAVE_TRACKING] {msg}")

    dprint("========== START save_tracking ==========")

    try:
        # --------------------------------------------------
        # Get the data incoming from the tracking-snippet.
        try:
            raw_body = request.body.decode("utf-8")
            dprint(f"[RAW PAYLOAD] {raw_body[:600]}")
            data = json.loads(raw_body)
        except Exception as e:
            dprint(f"[FATAL] JSON parse failed: {e}")
            return JsonResponse({"status": "error"}, status=400)

        # --------------------------------------------------
        # Helper with cleaning the dicts
        def clean_dict(d):
            cleaned = {k: (v.strip() if isinstance(v, str) else v) for k, v in (d or {}).items()}
            dprint(f"[CLEAN_DICT] input={d} → output={cleaned}")
            return cleaned

        # --------------------------------------------------
        # Extract raw inputs (NO LOGIC YET)
        visitor_id = str(data.get("visitor_id") or "").strip()
        session_id = str(data.get("session_id") or "").strip()
        event_type = str(data.get("event_type") or "").strip()

        dprint(f"[IDS] visitor_id={visitor_id} | session_id={session_id} | event_type={event_type}")

        event_details = clean_dict(data.get("event_details"))
        utm_params = clean_dict(data.get("utm_params"))
        traffic_source = clean_dict(data.get("traffic_source"))
        visitor_info = clean_dict(data.get("visitor_info"))
        client_info = clean_dict(data.get("client_info"))

        # --------------------------------------------------
        # Extract extra customer data
        session_customer_info = {}
        for key in ["Customer_ID", "Customer_Name", "Customer_Email", "Customer_Mobile"]:
            if visitor_info.get(key) is not None:
                session_customer_info[key] = visitor_info[key]
                dprint(f"[CUSTOMER FIELD] {key}={visitor_info.get(key)}")

        page_url = str(data.get("page_url") or "").strip()
        store_url = str(data.get("store_url") or "").strip()
        referrer = str(data.get("referrer") or "").strip()
        agent = str(client_info.get("user_agent") or "").strip().lower()


        dprint(f"[CONTEXT] page_url={page_url}")
        dprint(f"[CONTEXT] referrer={referrer}")
        dprint(f"[CONTEXT] user_agent={agent}")

        # --------------------------------------------------
        # Kill crawlers
        crawlers = ["googlebot", "bingbot", "crawler", "adsbot", "ahrefs"]
        if any(bot in agent for bot in crawlers):
            dprint("[SKIPPED] crawler detected")
            return JsonResponse({"status": "skipped"})

        # --------------------------------------------------
        ### LOG THE RAW UTM 
        raw_utm_source = str(utm_params.get("utm_source") or "").strip().lower()
        dprint(f"[RAW UTM SOURCE] {raw_utm_source or 'none'}")

        # Intialize a list to store possible candidates to act as the incoming source (NO DECISIONS YET)
        candidates = []

        # Explicit UTM (strongest)
        if raw_utm_source:
            candidates.append({"source": raw_utm_source, "type": "explicit_utm"})

        # Referrer inference (outweighs UA) -- with existential checks to prevent errors
        ref_source = None
        if referrer or page_url:
            ref_source = detect_source_from_url_or_domain(referrer) or detect_source_from_url_or_domain(page_url)
        if ref_source:
            candidates.append({"source": ref_source.lower(), "type": "inferred_referrer"})

        # User agent inference -- with existential checks to prevent errors
        ua_source = None
        if agent:
            ua_source = detect_source_from_user_agent(agent)
        if ua_source:
            candidates.append({"source": ua_source.lower(), "type": "inferred_user_agent"})

        # Traffic source fallback (weakest)
        traffic_fallback = str((traffic_source.get("source") or "")).strip().lower()
        if traffic_fallback:
            candidates.append({"source": traffic_fallback, "type": "traffic_source"})

        if not candidates:
            candidates.append({"source": "unknown", "type": "unknown"})

        # --------------------------------------------------
        # PICK STRONGEST CANDIDATE FROM THE LIST
        winner = candidates[0]
        for c in candidates[1:]:
            if source_weight(c["source"]) > source_weight(winner["source"]):
                winner = c

        incoming_source = winner["source"]
        attribution_type = winner["type"]

        dprint(f"[SOURCE WINNER] {incoming_source} ({attribution_type})")

        # --------------------------------------------------
        # DIRECT CONFIRMATION
        if incoming_source == "direct":
            is_homepage = page_url and store_url and page_url.rstrip("/") == store_url.rstrip("/")
            dprint(f"[DIRECT CHECK] homepage={is_homepage} referrer={referrer}")

            if not referrer and is_homepage:
                incoming_source = "direct"
                attribution_type = "direct_confirmed"
                dprint("[DIRECT CONFIRMED] incoming_source set to 'direct'")
            else:
                incoming_source = "unknown"
                attribution_type = "unknown_first_touch"
                dprint("[DIRECT DOWNGRADED] incoming_source set to 'unknown'")

        # --------------------------------------------------
        ### THE BLOCK TO DECIDE ON WHETHER INCOMING SOURCE IS UPDATED OR NOT
        session_source = None
        session_rows = (
            supabase.table("Tracking_Visitors_duplicate")
            .select("UTM_Source")
            .eq("Session_ID", session_id)
            .execute()
        ).data or []

        if session_rows:
            recorded_source = ((session_rows[0].get("UTM_Source") or "").strip().lower() or "unknown")
            dprint(f"[SESSION FOUND] recorded={recorded_source}")

            if recorded_source != "unknown":
                session_source = recorded_source

            # CASE 1 -- VALID SESSION SOURCE EXISTS
            if session_source:
                if source_weight(incoming_source) > source_weight(session_source):
                    final_source = incoming_source
                    attribution_type = "session_upgraded"

                    supabase.table("Tracking_Visitors_duplicate").update({"UTM_Source": final_source}).eq("Session_ID", session_id).execute()
                    dprint(f"[SESSION UPGRADE] {session_source} >> {final_source}")
                else:
                    final_source = session_source
                    attribution_type = "session_persisted"
                    dprint(f"[SESSION PERSISTED] using {final_source}")

        # CASE 2: SESSION UNKNOWN >>> CHECK USING VISITOR_ID
        elif visitor_id:
            dprint("[VISITOR CHECK] session unknown >> checking visitor history")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source, Session_ID, Customer_Mobile")
                .eq("Visitor_ID", visitor_id)
                .execute()
            )

            strongest_source = incoming_source
            discovered_mobile = None

            for row in res.data or []:
                existing = (row.get("UTM_Source") or "").strip().lower() or "unknown"
                strongest_source = pick_stronger_source(strongest_source, existing)

                if row.get("Customer_Mobile"):
                    discovered_mobile = row["Customer_Mobile"]

            if strongest_source != "unknown":
                final_source = strongest_source
                attribution_type = "visitor_inferred"
                dprint(f"[VISITOR INFERRED] {final_source}")

                # Backfill the UNKNOWN session using the source found in the visitor_id
                if session_rows:
                    supabase.table("Tracking_Visitors_duplicate").update({"UTM_Source": final_source}).eq("Session_ID", session_id).execute()
                    dprint(f"[SESSION BACKFILLED FROM VISITOR] {final_source}")

            if discovered_mobile:
                session_customer_info["Customer_Mobile"] = discovered_mobile
                dprint(f"[VISITOR>>MOBILE LINK] {discovered_mobile}")

        # CASE 3: BOTH SESSION + VISITOR UNKNOWN >>> RESORT TO MOBILE SOURCE 
        mobile = str(visitor_info.get("mobile") or "").strip()

        if final_source == "unknown" and mobile:
            dprint(f"[MOBILE RECONCILE] unresolved >> checking mobile {mobile}")

            res = (
                supabase.table("Tracking_Visitors_duplicate")
                .select("UTM_Source")
                .eq("Customer_Mobile", mobile)
                .execute()
            )

            strongest_source = incoming_source

            for row in res.data or []:
                existing = (row.get("UTM_Source") or "").strip().lower() or "unknown"
                strongest_source = pick_stronger_source(strongest_source, existing)

            if strongest_source != "unknown":
                final_source = strongest_source
                attribution_type = "mobile_unified"
                dprint(f"[MOBILE UNIFIED] {final_source}")

            session_customer_info["Customer_Mobile"] = mobile

        # --------------------------------------------------
        # Build tracking row
        tracking_entry = {
            "Distinct_ID": int(get_next_id_from_supabase_compatible_all(name="Tracking_Visitors_duplicate", column="Distinct_ID")),
            "Visitor_ID": visitor_id,
            "Session_ID": session_id,
            "Event_Type": event_type,
            "Event_Details": str(event_details),
            "Page_URL": page_url,
            "Referrer_Platform": referrer,
            "Visited_at": get_uae_current_date(),

            "UTM_Source": final_source,
            "UTM_Medium": utm_params.get("utm_medium"),
            "UTM_Campaign": utm_params.get("utm_campaign"),
            "UTM_Term": utm_params.get("utm_term"),
            "UTM_Content": utm_params.get("utm_content"),

            "Attribution_Type": attribution_type,
            "First_Touch_Source": "PLACEHOLDER",
            "First_Touch_Medium": "PLACEHOLDER",
            "First_Touch_Timestamp": "PLACEHOLDER",
            "Last_Touch_Source": final_source,
            "Assisted_Sources": "PLACEHOLDER",

            "User_Agent": agent,
            "Language": client_info.get("language"),
            "Timezone": client_info.get("timezone"),
            "Platform": client_info.get("platform"),
            "Screen_Resolution": client_info.get("screen_resolution"),
            "Device_Memory": client_info.get("device_memory"),
            "Last_Updated": get_uae_current_date(),
            "RAW_UTM_SOURCE": raw_utm_source,
            "Which_Update": "270126 1218",

            **session_customer_info,
        }

        dprint(f"[INSERT PAYLOAD PREVIEW] {tracking_entry}")

        batch_insert_to_supabase(pd.DataFrame([tracking_entry]), "Tracking_Visitors_duplicate")

        # --------------------------------------------------
        # Backfill ONLY non-social sources across same mobile
        if session_customer_info.get("Customer_Mobile") and source_weight(final_source) < 100:
            dprint(f"[BACKFILL] non-social → {final_source}")
            supabase.table("Tracking_Visitors").update({"UTM_Source": final_source}).eq("Customer_Mobile", session_customer_info["Customer_Mobile"]).execute()

        dprint("========== END save_tracking (SUCCESS) ==========")
        return JsonResponse({"status": "success"})

    except Exception as e:
        dprint(f"[CRASH] save_tracking failed: {e}")
        traceback.print_exc()
        return JsonResponse({"status": "error"}, status=500)

    
# The route to render the tracking javascript -- 
def tracking_snippet(request):
    js_content = render_to_string("tracking-snippet.js")
    return HttpResponse(js_content, content_type="application/javascript")
###################################################################################################################3
###########################################

def get_client_ip(request):
    """
    Returns the real client IP address, even behind proxies (e.g., Cloudflare, Nginx).
    """
    # 1. Cloudflare specific header (if you're using it)
    cf_ip = request.META.get('HTTP_CF_CONNECTING_IP')
    if cf_ip:
        return cf_ip

    # 2. Standard X-Forwarded-For header (can contain multiple IPs)
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        # X-Forwarded-For can contain multiple IPs, the first is the real one
        ip = x_forwarded_for.split(',')[0].strip()
        if ip:
            return ip

    # 3. Standard Django fallback
    real_ip = request.META.get('REMOTE_ADDR')
    if real_ip:
        return real_ip

    return "0.0.0.0"

def search_view(request):
    query = request.GET.get("q", "").lower()
    results = []

    if query:
        # Example: search in predefined pages
        pages = [
            {"title": "Dashboard", "url": "/"},
            {"title": "Analytics", "url": "/zid_orders/"},
            {"title": "Logout", "url": "/zid_logout/"},
            {"title": "Marketing", "url": "/marketing/"},
            {"title": "Products", "url": "/zid/products//"},
            {"title": "Orders", "url": "/zid/orders//"},
            {"title": "Analytics", "url": "/zid/match_google//"},
        ]
        results = [p for p in pages if query in p["title"].lower()]

    return JsonResponse(results, safe=False)

def orders_page(request):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')
    store_id = request.session.get('store_id')

    if not token:
        return redirect('Demo:zid_login')

    headers = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
    }

    try:
        orders_res = requests.get(f"{settings.ZID_API_BASE}/managers/store/orders", headers=headers)
        orders_res.raise_for_status()
        orders_data = orders_res.json()
        orders = orders_data.get('orders', [])

        # process dates + totals
        for order in orders:
            order['order_total'] = float(order.get('order_total', 0))
            order['display_status'] = order.get('order_status', {}).get('name', 'unknown')

            created_str = order.get("created_at")
            updated_str = order.get("updated_at")
            if created_str:
                try:
                    order["created_at"] = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                    order["updated_at"] = datetime.strptime(updated_str, "%Y-%m-%d %H:%M:%S") if updated_str else order["created_at"]
                except ValueError:
                    order["created_at"] = None
                    order["updated_at"] = None

    except requests.RequestException as e:
        traceback.print_exc()
        messages.error(request, f"⚠️ Error fetching orders: {str(e)}")
        orders = []

    return render(request, 'Demo/orders.html', {
        'orders': orders,
        'total_orders': len(orders),
        'total_revenue': sum(float(order['transaction_amount']) for order in orders)
    })

def products_page(request):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')
    store_id = request.session.get('store_id')

    if not token:
        return redirect('Demo:zid_login')

    headers_product = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
        'accept': 'application/json',
        'Accept-Language': 'all-languages',
        'Store-Id': f'{store_id}',
        'Role': 'Manager',
    }

    products_res = requests.get(f"{settings.ZID_API_BASE}/products", headers=headers_product)
    products_res.raise_for_status()
    products_data = products_res.json()
    print("Products data fetched successfully:", products_data)

    # Extract product list safely
    products_list = products_data.get("results", [])  # ✅ correct key
    total_products = products_data.get("total_products_count", len(products_list))

    # KPIs
    published_count = sum(1 for p in products_list if p.get("is_published"))
    avg_rating = (
        round(sum(p.get("rating", {}).get("average", 0) for p in products_list) / total_products, 2)
        if total_products > 0 else 0
    )
    on_sale_count = sum(
        1 for p in products_list if p.get("sale_price") and p.get("price") and p["sale_price"] < p["price"]
    )

    context = {
        "products": products_list,  # send all products
        "total_products": total_products,
        "published_count": published_count,
        "avg_rating": avg_rating,
        "on_sale_count": on_sale_count,
    }

    return render(request, "Demo/products_page.html", context)

def safe_name(name):
    return re.sub(r'[^0-9a-zA-Z_]', '_', str(name))

def safe_numeric(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0  # fallback if non-numeric

#############################################################################
############################ Marketing Report Section #######################
def marketing_page_ready(request):
    # This function is made to process the ready reporting file since the system cannot perform the full operation
    context = {}
    if request.method == "POST" and request.FILES.get("excel_file"):
        excel_file = request.FILES["excel_file"]

        # Save uploaded file temporarily
        fs = FileSystemStorage()
        filename = fs.save(excel_file.name, excel_file)
        file_path = fs.path(filename)

        # Read Excel with pandas (all sheets)
        xls = pd.ExcelFile(file_path)
        sheets_data = {}

        for sheet_name in xls.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            safe_sheet = safe_name(sheet_name)

            # Extract labels (first column, may be Arabic)
            labels = df.iloc[:, 0].fillna("").astype(str).tolist()

            # Extract numeric columns after the first one
            charts = []
            for col in df.columns[1:]:
                # Convert all values safely to numeric
                values = [safe_numeric(v) for v in df[col].tolist()]
                charts.append({
                    "column": col,
                    "values": values
                })

            sheets_data[sheet_name] = {
                "columns": df.columns.tolist(),
                "safe_name": safe_sheet,
                "rows": df.values.tolist(),
                "labels": json.dumps(labels, ensure_ascii=False),  # keep Arabic intact
                "charts": charts,
            }

        context["sheets"] = sheets_data

    return render(request, "Demo/marketing_ready.html", context)

####### The marketing report creation from scratch #############
################################################################
### A function just to view
def marketing_page(request):
    return render(request, "Demo/marketing.html", {})

@csrf_exempt
def process_marketing_report_file_uploads(request):
    # A dictionary to store the data
    context = {}
    # Mehtod check
    if request.method != "POST":
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

    uploaded_files = []
    missing_files = []
    dataframes = {}

    # Get start and end dates
    start_time = request.POST.get("start_time")
    end_time = request.POST.get("end_time")

    # Check uploaded files
    for i in range(1, 7):
        file = request.FILES.get(f'file{i}')
        if file:
            message, response, file_1, file_2, file_3, file_4, file_5, file_6 = column_check(file, i)
            if not response:
                return JsonResponse({'status': 'error', 'message': message}, status=400)

            if i == 1:
                dataframes[i] = file_1
            elif i == 2:
                dataframes[i] = file_2
            elif i == 3:
                dataframes[i] = file_3
            elif i == 4:
                dataframes[i] = file_4
            elif i == 5:
                dataframes[i] = file_5
            elif i == 6:
                dataframes[i] = file_6

            uploaded_files.append(file.name)
        else:
            missing_files.append(f'file{i}')

    if missing_files:
        return JsonResponse({'status': 'error', 'message': f'Missing files: {", ".join(missing_files)}'}, status=400)

    # Process data
    subsheet_one, subsheet_two, facebook, tiktok, snapchat, google, zid, analytics, orders_breakdown, zid_unfiltered = create_general_analysis(dataframes, start_time, end_time)
    subsheet_three, subsheet_four, facebook_detailed, snapchat_detailed, tiktok_detailed, full_detailed, facebook, tiktok, snapchat, zid, analytics, vanilla_db, advertised_prods, orders_count_unfiltered, advertised_prods_copied = create_product_percentage_amount_spent(facebook, tiktok, snapchat, zid, analytics, zid_unfiltered)
    landing = asyncio.run(landing_performance_5_async(analytics, vanilla_db, advertised_prods_copied, full_detailed, orders_count_unfiltered))

    # Collect all sheets into a dictionary for template
    sheets_data = {
            "General_Analysis": subsheet_one,
            "Orders Analysis": orders_breakdown,
            "Platforms_Summary": subsheet_two,
            "Percentages(Orders)": subsheet_three,
            "AD Amount Spent": subsheet_four,
            "Facebook Detailed": facebook_detailed,
            "Snapchat Detailed": snapchat_detailed,
            "Tiktok Detailed": tiktok_detailed,
            "Full Detailed": full_detailed,
            "Landing Performance Main_Vars": landing
    }

    template_sheets = {}

    for sheet_name, df in sheets_data.items():
            safe_sheet = safe_name(sheet_name)

            # Extract labels (first column)
            labels = df.iloc[:, 0].fillna("").astype(str).tolist()

            # Extract numeric columns after the first one
            charts = []
            for col in df.columns[1:]:
                values = [safe_numeric(v) for v in df[col].tolist()]
                charts.append({
                    "column": col,
                    "values": values
                })

            template_sheets[sheet_name] = {
                "columns": df.columns.tolist(),
                "safe_name": safe_sheet,
                "rows": df.values.tolist(),
                "labels": json.dumps(labels, ensure_ascii=False),
                "charts": charts
            }

    context["sheets"] = template_sheets
    context["start_time"] = start_time
    context["end_time"] = end_time

    return render(request, "Demo/marketing.html", context)

#### Refined process_marketing_Report that creates files from API
def process_marketing_report(request):
    ## This function creates both the tiktok and snapchat files from the API
    context = {}

    # Validate request
    if request.method != "POST":
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

    uploaded_files = []
    missing_files = []
    dataframes = {}

    # Get date range
    start_time = request.POST.get("start_time")
    end_time = request.POST.get("end_time")
    # Retrieve the store id from session
    store_id = request.session.get("store_id")

    # Loop through expected files 1–6
    for i in range(1, 7):

        # --- AUTO-CREATE FILE 2 (TIKTOK) ---
        if i == 2:
            try:
                print("[INFO] Creating File 2...")
                file_2 = create_tiktok_file(start_time, end_time, store_id)
                dataframes[i] = file_2
                uploaded_files.append("auto_created_file2")
                print("[SUCCESS] File 2 created successfully.")
            except Exception as e:
                return JsonResponse({'status': 'error', 'message': f'Error creating file2: {e}'}, status=500)
            continue

        # --- AUTO-CREATE FILE 3 ---
        if i == 3:
            try:
                print("[INFO] Creating File 3...")
                file_3 = create_snapchat_file(start_time, end_time, store_id)
                dataframes[i] = file_3
                uploaded_files.append("auto_created_file3")
                print("[SUCCESS] File 3 created successfully.")
            except Exception as e:
                return JsonResponse({'status': 'error', 'message': f'Error creating file3: {e}'}, status=500)
            continue

        # --- HANDLE UPLOADED FILES (1, 4, 5, 6) ---
        file = request.FILES.get(f'file{i}')
        if file:
            message, response, file_1, file_2, file_3, file_4, file_5, file_6 = column_check(file, i)
            if not response:
                return JsonResponse({'status': 'error', 'message': message}, status=400)

            if i == 1:
                dataframes[i] = file_1
            elif i == 4:
                dataframes[i] = file_4
            elif i == 5:
                dataframes[i] = file_5
            elif i == 6:
                dataframes[i] = file_6

            uploaded_files.append(file.name)
        else:
            missing_files.append(f'file{i}')

    # Only consider truly missing files (not 2 & 3)
    missing_files = [f for f in missing_files if f not in ('file2', 'file3')]
    if missing_files:
        return JsonResponse({'status': 'error', 'message': f'Missing files: {", ".join(missing_files)}'}, status=400)

    # --- MAIN DATA PROCESSING ---
    subsheet_one, subsheet_two, facebook, tiktok, snapchat, google, zid, analytics, orders_breakdown, zid_unfiltered = create_general_analysis(
        dataframes, start_time, end_time
    )
    subsheet_three, subsheet_four, facebook_detailed, snapchat_detailed, tiktok_detailed, full_detailed, facebook, tiktok, snapchat, zid, analytics, vanilla_db, advertised_prods, orders_count_unfiltered, advertised_prods_copied = create_product_percentage_amount_spent(
        facebook, tiktok, snapchat, zid, analytics, zid_unfiltered
    )
    landing = asyncio.run(
        landing_performance_5_async(analytics, vanilla_db, advertised_prods_copied, full_detailed, orders_count_unfiltered)
    )

    # --- PREPARE SHEETS FOR TEMPLATE ---
    sheets_data = {
        "General_Analysis": subsheet_one,
        "Orders Analysis": orders_breakdown,
        "Platforms_Summary": subsheet_two,
        "Percentages(Orders)": subsheet_three,
        "AD Amount Spent": subsheet_four,
        "Facebook Detailed": facebook_detailed,
        "Snapchat Detailed": snapchat_detailed,
        "Tiktok Detailed": tiktok_detailed,
        "Full Detailed": full_detailed,
        "Landing Performance Main_Vars": landing,
    }

    template_sheets = {}
    for sheet_name, df in sheets_data.items():
        safe_sheet = safe_name(sheet_name)
        labels = df.iloc[:, 0].fillna("").astype(str).tolist()
        charts = []
        for col in df.columns[1:]:
            values = [safe_numeric(v) for v in df[col].tolist()]
            charts.append({"column": col, "values": values})
        template_sheets[sheet_name] = {
            "columns": df.columns.tolist(),
            "safe_name": safe_sheet,
            "rows": df.values.tolist(),
            "labels": json.dumps(labels, ensure_ascii=False),
            "charts": charts,
        }

    context["sheets"] = template_sheets
    context["start_time"] = start_time
    context["end_time"] = end_time

    return render(request, "Demo/marketing.html", context)

#############################################################################################
################################# Viewing the Tracking ######################################

'''
def view_tracking(request):
    """
    Displays synced Customer_Tracking data with campaign search.
    Automatically triggers incremental sync before rendering.
    """
    store_id = request.GET.get("store_id") or request.session.get("store_uuid")
    if not store_id:
        return redirect("Demo:home")

    uae_timezone = pytz.timezone("Asia/Dubai")
    rows = []
    total_visitors = total_sessions = total_pageviews = 0
    context = {}

    try:
        sync_customer_tracking_unified()
        # --- Fetch synced customer tracking data ---
        df = get_tracking_customers_df()
        if df.empty:
            messages.warning(request, "No customer tracking data available.")
            return redirect("Demo:home")

        # --- Filters ---
        campaign_filter = request.GET.get("campaign")
        source_filter = request.GET.get("source")
        from_date = request.GET.get("from")
        to_date = request.GET.get("to")

        # Filter by campaign name (case-insensitive)
        if campaign_filter:
            df = df[df["campaigns"].apply(
                lambda c: any(campaign_filter.lower() in str(x).lower() for x in c)
            )]
        
        # Filter by source
        if source_filter:
            df = df[df["source"].apply(
                lambda c: any(source_filter.lower() in str(x).lower() for x in c)
            )]

        # Filter by date range
        if from_date:
            df = df[df["updated_at"] >= pd.to_datetime(from_date)]
        if to_date:
            df = df[df["updated_at"] <= pd.to_datetime(to_date)]

        # --- Last 30 minutes stats ---
        thirty_minutes_ago = datetime.now(uae_timezone) - timedelta(minutes=30)
        df_recent = df[df["updated_at"] >= thirty_minutes_ago]

        total_visitors = len(df_recent)
        total_sessions = df_recent["visitor_ids"].apply(lambda x: sum(len(v) for v in x.values())).sum()
        total_pageviews = df_recent["add_to_cart"].sum() + df_recent["purchases"].sum()

        # --- Campaign summary for chart ---
        all_campaigns = []
        for c_list in df["campaigns"]:
            all_campaigns.extend(c_list)
        campaign_counts = pd.Series(all_campaigns).value_counts().head(10)

        campaign_labels = campaign_counts.index.tolist()
        campaign_data = campaign_counts.values.tolist()


        all_sources = []
        for s_list in df["source"]:
            all_sources.extend(s_list)
        source_counts = pd.Series(all_sources).value_counts().head(10)
        source_labels = source_counts.index.tolist()
        source_data = source_counts.values.tolist()
        # --- Top 50 customers ---
        rows = (
            df.sort_values(by="updated_at", ascending=False)
            .head(50)
            .to_dict(orient="records")
        )

        # --- Build context ---
        context = {
            "track_id": store_id,
            "rows": rows,
            "total_visitors": total_visitors,
            "total_sessions": total_sessions,
            "total_pageviews": total_pageviews,
            "campaign_labels": campaign_labels,
            "campaign_data": campaign_data,
            "source_labels": source_labels,
            "source_data": source_data,
            "request": request,
        }

    except Exception as e:
        logging.error(f"Error in view_tracking: {str(e)}")
        messages.error(request, f"❌ Error fetching tracking data: {str(e)}")
        return redirect("Demo:home")

    return render(request, "Demo/tracking_view.html", context=context)
'''


def view_tracking(request):
    """
    Displays summarized Customer_Tracking data.
    Handles multiple visitor_ids per customer and merged customer records.
    """
    store_id = request.GET.get("store_id") or request.session.get("store_uuid")
    if not store_id:
        return redirect("Demo:home")

    dubai_tz = pytz.timezone("Asia/Dubai")

    try:
        # Updating the customers db
        ##### --------------> commented for the time being --/ sync_customers()

        # Fetch from Supabase
        data = supabase.table("Customer_Tracking").select("*").execute().data or []
        if not data:
            return render(request, "Demo/tracking_view.html", {"rows": [], "track_id": store_id})

        df = pd.DataFrame(data)

        # Normalize timestamps
        if "updated_at" in df.columns:
            df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
            if df["updated_at"].dt.tz is None:
                df["updated_at"] = df["updated_at"].dt.tz_localize("UTC")
            df["updated_at"] = df["updated_at"].dt.tz_convert(dubai_tz)

        if "last_visit" in df.columns:
            df["last_visit"] = pd.to_datetime(df["last_visit"], errors="coerce")
            if df["updated_at"].dt.tz is None:
                df["updated_at"] = df["updated_at"].dt.tz_localize("UTC")
            df["updated_at"] = df["updated_at"].dt.tz_convert(dubai_tz)
            

        # Ensure visitor_ids is always a list
        def normalize_visitors(v):
            if isinstance(v, list):
                return v
            if isinstance(v, str):
                try:
                    parsed = json.loads(v)
                    return parsed if isinstance(parsed, list) else [parsed]
                except Exception:
                    return [v]
            return []
        df["visitor_ids"] = df.get("visitor_ids", [[]]).apply(normalize_visitors)

        # Parse JSON fields if still strings
        json_cols = ["customer_info", "campaigns", "campaign_source"]
        for col in json_cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.loads(x)
                    if isinstance(x, str) and (x.strip().startswith("[") or x.strip().startswith("{"))
                    else x
                )

        # Calculate Totals
        total_customers = len(df)
        total_purchases = df["purchases"].sum() if "purchases" in df else 0

        # Campaign & Source summaries
        campaign_counts, source_counts = {}, {}

        if "campaigns" in df.columns:
            for row in df["campaigns"].dropna():
                if isinstance(row, list):
                    for c in row:
                        name = c.get("campaign") or "Unknown"
                        campaign_counts[name] = campaign_counts.get(name, 0) + c.get("purchases", 0)

        if "campaign_source" in df.columns:
            for row in df["campaign_source"].dropna():
                if isinstance(row, list):
                    for s in row:
                        name = s.get("source") or "Unknown"
                        source_counts[name] = source_counts.get(name, 0) + s.get("purchases", 0)

        campaign_labels = list(campaign_counts.keys())
        campaign_purchases = list(campaign_counts.values())
        source_labels = list(source_counts.keys())
        source_purchases = list(source_counts.values())

        # Apply filters (optional)
        campaign_filter = request.GET.get("campaign")
        date_from = request.GET.get("from")
        date_to = request.GET.get("to")

        if campaign_filter:
            df = df[df["campaigns"].apply(lambda lst: any(campaign_filter.lower() in (c.get("campaign", "").lower()) for c in (lst or [])))]
        if date_from:
            df = df[df["updated_at"] >= pd.to_datetime(date_from)]
        if date_to:
            df = df[df["updated_at"] <= pd.to_datetime(date_to) + pd.Timedelta(days=1)]

        # Convert timestamps to readable strings for display
        for col in ["updated_at", "last_visit"]:
            if col in df.columns:
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M")

        # Build Context
        context = {
            "track_id": store_id,
            "rows": df.to_dict("records"),
            "total_customers": total_customers,
            "total_purchases": total_purchases,
            "campaign_labels": json.dumps(campaign_labels or []),
            "campaign_purchases": json.dumps(campaign_purchases or []),
            "source_labels": json.dumps(source_labels or []),
            "source_purchases": json.dumps(source_purchases or []),
            "last_sync": datetime.now(dubai_tz).strftime("%Y-%m-%d %H:%M"),
        }

        return render(request, "Demo/tracking_view.html", context)

    except Exception as e:
        logging.error("Error in view_tracking()", exc_info=True)
        return render(request, "Demo/tracking_view.html", {
            "rows": [],
            "error": str(e),
            "track_id": store_id
        })


##############################################################################
###############################################################################
### Abdandoned Carts
def abandoned_carts_page(request):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')
    store_id = request.session.get('store_id')

    if not token:
        return redirect('Demo:zid_login')

    headers_cart = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
        'accept': 'application/json',
        'Accept-Language': 'all-languages',
        'Store-Id': f'{store_id}',
        'Role': 'Manager',
    }

    all_carts = []
    page = 1
    per_page = 100
    try:
        while True:
            params = {'page': page, 'page_size': per_page}
            carts_res = requests.get(f"{settings.ZID_API_BASE}/managers/store/abandoned-carts", headers=headers_cart, params=params)
            carts_res.raise_for_status()
            carts_data = carts_res.json()
            carts_list = carts_data.get('results', [])
            if not carts_list:
                break
            all_carts.extend(carts_list)
            if len(all_carts) >= carts_data.get('count', 0):  # Zid uses 'count'
                break
            page += 1
    except requests.RequestException as e:
        traceback.print_exc()
        messages.error(request, f"⚠️ Error fetching abandoned carts: {str(e)}")
        all_carts = []

    total_carts = len(all_carts)

    # KPIs
    recovered_count = sum(1 for c in all_carts if c.get("is_recovered"))
    total_value = sum(float(c.get("total", 0)) for c in all_carts)
    avg_cart_value = round(total_value / total_carts, 2) if total_carts > 0 else 0

    context = {
        "carts": all_carts,
        "total_carts": total_carts,
        "recovered_count": recovered_count,
        "avg_cart_value": avg_cart_value,
        "total_value": total_value,
    }

    return render(request, "Demo/abandoned_carts_page.html", context)

########################################################################################
########################################################################################
### Customers page -
def customers_page(request):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')
    store_id = request.session.get('store_id')

    if not token:
        return redirect('Demo:zid_login')

    headers_customer = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
        'accept': 'application/json',
        'Accept-Language': 'all-languages',
        'Store-Id': f'{store_id}',
        'Role': 'Manager',
    }

    all_customers = []
    page = 1
    per_page = 10
    try:
        while True:
            params = {'page': page, 'page_size': per_page}
            res = requests.get(f"{settings.ZID_API_BASE}/managers/store/customers", headers=headers_customer, params=params)
            res.raise_for_status()
            data = res.json()
            customers_list = data.get('customers', [])  # ✅ FIXED
            if not customers_list:
                break

            all_customers.extend(customers_list)

            if len(all_customers) >= data.get('total_customers_count', 0):  # ✅ FIXED
                break
            page += 1
    except requests.RequestException as e:
        traceback.print_exc()
        messages.error(request, f"⚠️ Error fetching customers: {str(e)}")
        all_customers = []

    total_customers = len(all_customers)

    # KPIs (note: Zid uses `verified` not `is_verified`)
    verified_count = sum(1 for c in all_customers if c.get("verified"))
    blocked_count = sum(1 for c in all_customers if not c.get("is_active"))  # inactive means blocked
    avg_orders = round(
        sum(c.get("order_counts", 0) for c in all_customers) / total_customers, 2
    ) if total_customers > 0 else 0

    context = {
        "customers": all_customers,
        "total_customers": data.get("total_customers_count", total_customers),
        "verified_count": verified_count,
        "blocked_count": blocked_count,
        "avg_orders": avg_orders,
    }
    return render(request, "Demo/customers_page.html", context)

# AJAX endpoint for customer details popup
def customer_detail_api(request, customer_id):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')
    store_id = request.session.get('store_id')

    headers_customer = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
        'accept': 'application/json',
        'Accept-Language': 'all-languages',
        'Store-Id': f'{store_id}',
        'Role': 'Manager',
    }

    try:
        res = requests.get(f"{settings.ZID_API_BASE}/managers/store/customers/{customer_id}", headers=headers_customer)
        res.raise_for_status()
        data = res.json()
        return JsonResponse(data.get("customer", {}), safe=False)
    except requests.RequestException as e:
        return JsonResponse({"error": str(e)}, status=400)

#########################################################################
##################### THE PRODUCTS WEBHOOK SECTION --- SARAH 16TH OF OCTOBER
logger = logging.getLogger(__name__)

def subscribe_store_to_product_update(authorization_token, access_token):

    conn = http.client.HTTPSConnection(settings.ZID_API_HOST)

    payload = json.dumps({
        "event": "product.update",
        "target_url": settings.TARGET_URL_PRODUCT_HOOK,
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
    
### THE FUNCITON THAT DOES THE UPDATES AFTER THE WEBHOOK IS TRIGGERED (A PRODUCT IS UPDATED)
def product_update(request):
    """
    This view receives data from Zid whenever a product is updated.
    Zid sends a POST request with the updated product data.
    """
    if request.method == "GET":
        # Only send an ok reponse
        return JsonResponse({"status": "ok"}, status=200)
    
    try:
        data = json.loads(request.body.decode("utf-8"))
        print("The raw data gotten is:", data)

        # Extracting key details
        product_id = data.get("id")
        name = data.get("name")
        sku = data.get("sku")
        sale_price = data.get("sale_price")
        price = data.get("price")
        updated_at = data.get("updated_at")

        print(f"Product Update Received — ID: {product_id}, Name: {name}, Price: {price}, SKU: {sku}, update occurred at {updated_at}")

        return JsonResponse({"status": "success"}, status=200)

    except Exception as e:
        print(f"Error in product_update: {e}")
        return JsonResponse({"error": "Invalid payload"}, status=400)

################################################################################################
######################################## SNAPCHAT's API ----

def snapchat_login(request):
    """
    Initiates the Snapchat Marketing API OAuth 2.0 flow by creating a unique state
    and storing it in the user's session.
    """
    # Generate a unique state to prevent CSRF attacks.
    state = str(uuid.uuid4())
    # Store the state in the Django session. It's automatically saved
    # and tied to the user's browser session.
    request.session['snapchat_oauth_state'] = state
    
    # Construct the authorization URL
    auth_url = 'https://accounts.snapchat.com/login/oauth2/authorize'
    params = {
        'response_type': 'code',
        'client_id': settings.SNAPCHAT_CLIENT_ID,
        'redirect_uri': settings.SNAPCHAT_REDIRECT_URI,
        'scope': settings.SNAPCHAT_OAUTH_SCOPE,
        'state': state
    }
    
    auth_request_url = requests.Request('GET', auth_url, params=params).prepare().url
    return redirect(auth_request_url)

def snapchat_callback(request):
    """
    Handles the redirect from Snapchat, validates the state, and exchanges
    the authorization code for an access token.
    """
    code = request.GET.get('code')
    state_from_snapchat = request.GET.get('state')

    # 1. Retrieve the state from the session.
    state_from_session = request.session.get('snapchat_oauth_state')

    # 2. Validate the state to prevent CSRF attacks.
    if not state_from_session or state_from_snapchat != state_from_session:
        return HttpResponse('State mismatch: Potential CSRF attack.', status=400)

    # Delete the state from the session after successful validation.
    del request.session['snapchat_oauth_state']

    # 3. Exchange the authorization code for an access token
    token_url = "https://accounts.snapchat.com/login/oauth2/access_token"

    data = {
        "grant_type": "authorization_code",
        "code": code,
        'client_id': settings.SNAPCHAT_CLIENT_ID,
        "client_secret": settings.SNAPCHAT_CLIENT_SECRET,
        "redirect_uri": settings.SNAPCHAT_REDIRECT_URI,
    }

    try:
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        token_data = response.json()
        print("Snapchat's Token Data is:", token_data)

        # Store the tokens securely in the session.
        request.session["snapchat_access_token"] = token_data["access_token"]
        request.session["snapchat_refresh_token"] = token_data.get("refresh_token")

        expires_in_seconds = token_data.get("expires_in", 3600)
        expiry_datetime = datetime.now() + timedelta(seconds=expires_in_seconds)
        request.session["snapchat_token_expires_at"] = expiry_datetime.isoformat()

        ## Add them to database
        store_id = request.session.get("store_id")
        if not store_id:
            return HttpResponse("No store_id found in session", status=400)

        # Check if row exists first
        existing = supabase.table("tokens").select("Store_ID").eq("Store_ID", store_id).execute()

        if not existing.data:
            # No row found 
            return HttpResponse(
                f"No entry found in 'tokens' for Store_ID: {store_id}. Please create it first.",
                status=404
            )
        long_term_access_token = refresh_snapchat_token(request)
        print("Long term access token is:", long_term_access_token)
        # Row exists, update it
        update_data = {
            "Snapchat_Access": token_data["access_token"],
            "Snapchat_Refresh": token_data.get("refresh_token"),
            "Snapchat_long_term_Access": long_term_access_token,
        }

        response = supabase.table("tokens").update(update_data).eq("Store_ID", store_id).execute()
        print(f"TikTok tokens updated for Store_ID {store_id}")

        # Redirect the user to the campaigns overview page.
        return redirect("Demo:campaigns_overview")

    except requests.RequestException as e:
        return HttpResponse(f"Failed to get access token: {e}", status=500)

def refresh_snapchat_token(request):
    """
    Refreshes the Snapchat access token using the refresh token.
    """
    refresh_token = request.session.get("snapchat_refresh_token")
    if not refresh_token:
        return None

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": settings.SNAPCHAT_CLIENT_ID,
        "client_secret": settings.SNAPCHAT_CLIENT_SECRET,
    }

    try:
        resp = requests.post("https://accounts.snapchat.com/login/oauth2/access_token", data=data)
        resp.raise_for_status()
        token_data = resp.json()

        # Update session with new tokens
        request.session["snapchat_access_token"] = token_data["access_token"]
        request.session["snapchat_refresh_token"] = token_data.get("refresh_token", refresh_token)

        expires_in = token_data.get("expires_in", 3600)
        expiry_datetime = datetime.now() + timedelta(seconds=expires_in)
        request.session["snapchat_token_expires_at"] = expiry_datetime.isoformat()

        return token_data["access_token"]
    except requests.RequestException as e:
        print(f"Token refresh failed: {e}")
        return None
    
from dateutil import parser

def snapchat_api_call(request, endpoint, method="GET", params=None, data=None, json_data=None):
    """
    Generic Snapchat API helper supporting GET, POST, PUT, DELETE
    - Handles token refresh automatically
    - params = query string dict
    - data/json_data = request body
    """
    access_token = request.session.get("snapchat_access_token")

    # Parse expiry from ISO string
    expires_at_str = request.session.get("snapchat_token_expires_at")
    expires_at = parser.isoparse(expires_at_str) if expires_at_str else None

    if not access_token or (expires_at and datetime.now() > expires_at - timedelta(minutes=5)):
        access_token = refresh_snapchat_token(request)
        if not access_token:
            return None

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    url = f"https://adsapi.snapchat.com/v1/{endpoint}"

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, params=params, json=json_data or data)
        elif method.upper() == "PUT":
            response = requests.put(url, headers=headers, params=params, json=json_data or data)
        elif method.upper() == "DELETE":
            response = requests.delete(url, headers=headers, params=params)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        messages.error(request, f"API request failed to {endpoint}: {e}")
        try:
            return response.json()  # return error response if available
        except:
            return None

def snapchat_select_organization(request, org_id=None):
    # If an org is selected, save it and redirect to ad accounts
    if org_id:
        request.session["snap_org_id"] = org_id
        return redirect("Demo:snapchat_select_account", org_id=org_id)

    # Fetch organizations from Snapchat API
    orgs_data = snapchat_api_call(request, "me/organizations")
    if not orgs_data or "organizations" not in orgs_data:
        messages.error(request, "No organizations found.")
        return JsonResponse(orgs_data or {}, status=404)

    organizations = [
        {"id": org["organization"]["id"], "name": org["organization"]["name"]}
        for org in orgs_data["organizations"]
    ]

    return render(request, "Demo/snapchat_select_org.html", {"organizations": organizations})


def snapchat_select_account(request, org_id, ad_account_id=None):
    # If an ad account is selected, save it and redirect to campaigns
    if ad_account_id:
        request.session["snap_ad_account_id"] = ad_account_id
        return redirect("Demo:campaigns_overview")

    # Fetch ad accounts for the organization
    accounts_data = snapchat_api_call(request, f"organizations/{org_id}/adaccounts")
    if not accounts_data or "adaccounts" not in accounts_data:
        messages.error(request, "No ad accounts found.")
        return JsonResponse(accounts_data or {}, status=404)

    ad_accounts = [
        {"id": acc["adaccount"]["id"], "name": acc["adaccount"]["name"]}
        for acc in accounts_data["adaccounts"]
    ]

    return render(request, "Demo/snapchat_select_account.html", {"ad_accounts": ad_accounts,"org_id": org_id})

def campaigns_overview(request):
    status = request.GET.get("status") or "ACTIVE"   # default = ACTIVE
    from_date = request.GET.get("from")  # yyyy-mm-dd
    to_date = request.GET.get("to") 
    ZeroSpent = request.GET.get("ZeroSpent")
    access_token = request.session.get('snapchat_access_token')

    tz_offset_minutes = int(request.GET.get("tz_offset", 0))  # in minutes

    from_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else datetime.now() - timedelta(days=8)
    to_dt = datetime.strptime(to_date, "%Y-%m-%d") if to_date else (datetime.now())

    # Adjust start/end for user's timezone
    from_dt = from_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=tz_offset_minutes)
    to_dt = to_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=tz_offset_minutes)

    if from_dt > to_dt:
        from_dt, to_dt = to_dt, from_dt

    # Attach timezone offset instead of Z
    start_time = from_dt.isoformat(timespec="milliseconds") + 'Z'
    end_time = to_dt.isoformat(timespec="milliseconds") + 'Z'

    if not access_token:
        return redirect('Demo:snapchat_login')
    
    params = {
        "fields": "impressions,spend,conversion_purchases,conversion_purchases_value",
        "granularity": "TOTAL",
        "start_time": start_time,
        "end_time": end_time,
    }

    # Step 1: Get org & ad account
    organization_id = request.session.get("snap_org_id")
    if not organization_id:
        return redirect("Demo:snapchat_select_organization")

    ad_account_id = request.session.get("snap_ad_account_id")
    if not ad_account_id:
        return redirect("Demo:snapchat_select_account", org_id=organization_id)

    # Step 2: Get campaigns
    campaigns_data = snapchat_api_call(request, f"adaccounts/{ad_account_id}/campaigns")
    raw_campaigns = campaigns_data.get("campaigns", [])
    processed_campaigns, total_spend, total_revenue = [], 0, 0


    if status and status != "all":
        raw_campaigns = [
            c for c in raw_campaigns
            if c["campaign"]["status"] == status
        ]
    else:
        raw_campaigns = campaigns_data.get("campaigns", [])
    
    # Step 3: Loop per campaign for stats
    for c in raw_campaigns:
        campaign = c["campaign"]
        stats_resp = snapchat_api_call(request, f"campaigns/{campaign['id']}/stats", params=params)
        adsquads_data = snapchat_api_call(request, f"campaigns/{campaign['id']}/adsquads")
        adsquads_raw = adsquads_data.get("adsquads", [])
        daily_budget = 0
        if adsquads_raw:
            for adsquad in adsquads_raw:
                daily_budget += adsquad["adsquad"].get("daily_budget_micro", 0) / 1_000_000.0

        stats = {}
        if stats_resp and "total_stats" in stats_resp:
            stats = stats_resp["total_stats"][0]["total_stat"].get("stats", {})

        spend = stats.get("spend", 0) / 1_000_000.0
        purchases = stats.get("conversion_purchases", 0)
        revenue = stats.get("conversion_purchases_value", 0) / 1_000_000.0
        roas = (revenue / spend) if spend > 0 else 0

        total_spend += spend
        total_revenue += revenue

        if ZeroSpent and int(spend) > 0:
            processed_campaigns.append({
                "id": campaign["id"],
                "name": campaign["name"],
                "status": campaign["status"],
                "spend": spend,
                "daily_budget": daily_budget,
                "impressions": stats.get("impressions", 0),
                "purchases": purchases,
                "revenue": revenue,
                "roas": roas,
                "start_time": campaign["start_time"],
            })

        elif not ZeroSpent:
            processed_campaigns.append({
                "id": campaign["id"],
                "name": campaign["name"],
                "status": campaign["status"],
                "spend": spend,
                "daily_budget": daily_budget,
                "impressions": stats.get("impressions", 0),
                "purchases": purchases,
                "revenue": revenue,
                "roas": roas,
                "start_time": campaign["start_time"],
            })

    # Step 4: Summary
    total_campaigns = len(processed_campaigns)
    avg_spend = total_spend / total_campaigns if total_campaigns else 0
    avg_roas = total_revenue / total_spend if total_spend > 0 else 0

    return render(request, "Demo/campaigns_overview.html", {
        "campaigns": processed_campaigns,
        "total_campaigns": total_campaigns,
        "total_spend": total_spend,
        "total_revenue": total_revenue,
        "avg_spend": avg_spend,
        "avg_roas": avg_roas,
        "from_date": from_dt.strftime("%Y-%m-%d"),
        "to_date": to_dt.strftime("%Y-%m-%d"),
        "ZeroSpent": ZeroSpent,
    })

################################################################################################
######################################## TIKTOK's API ----

# TikTok API constants
API_BASE = "https://business-api.tiktok.com/open_api/v1.3"
OAUTH_URL = "https://business-api.tiktok.com/portal/auth"
TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"

# --- LOGIN VIEW ---
def tiktok_login(request):
    scope = ",".join([
        "ad_account",
        "ad_campaign",
        "ad_group",
        "ad",
        "audience",
        "report",
        "measurement",
        "creative",
        "app",
        "pixel",
        "dpa_catalog",
        "reach_frequency",
        "lead",
        "tiktok_creator_marketplace",
        "ad_comment",
        "business_plugin",
        "automated_rules",
        "tiktok_account",
        "onsite_commerce_store",
        "offline_event",
    ])

    auth_url = (
        "https://business-api.tiktok.com/portal/auth"
        f"?app_id={settings.TIKTOK_CLIENT_KEY}"
        f"&state=xyz123"
        f"&redirect_uri={settings.TIKTOK_REDIRECT_URI}"
        f"&scope={scope}"
    )
    return redirect(auth_url)

# --- CALLBACK VIEW ---
def tiktok_callback(request):
    code = request.GET.get("auth_code")
    state = request.GET.get("state")

    if not code:
        return HttpResponse("No auth_code returned", status=400)

    # Exchange auth_code for access token
    data = {
        "app_id": settings.TIKTOK_CLIENT_KEY,
        "secret": settings.TIKTOK_CLIENT_SECRET,
        "auth_code": code,
        "grant_type": "authorized_code"
    }

    resp = requests.post(TOKEN_URL, json=data, headers={"Content-Type": "application/json"}, timeout=10)
    tokens = resp.json()
    print("Tiktok's token data is:", tokens)

    if not tokens or "data" not in tokens:
        return redirect("Demo:tiktok_login")

    access_token = tokens["data"]["access_token"]
    advertiser_ids = tokens["data"].get("advertiser_ids", [])
    refresh_token = tokens["data"].get("refresh_token")
    expiry_seconds = tokens["data"].get("expires_in", 86400)
    expiry_time = (datetime.now() + timedelta(seconds=expiry_seconds)).isoformat()

    # Store in session
    request.session["tiktok_access_token"] = access_token
    request.session["tiktok_advertiser_ids"] = advertiser_ids
    if refresh_token:
        request.session["tiktok_refresh_token"] = refresh_token
    request.session["tiktok_token_expiry"] = expiry_time

    # --- Fetch advertiser accounts ---
    url = f"{API_BASE}/oauth2/advertiser/get/"
    headers = {"Access-Token": access_token}
    params  = {
        "app_id": settings.TIKTOK_CLIENT_KEY,
        "secret": settings.TIKTOK_CLIENT_SECRET,
    }
    resp = requests.get(url, params=params, headers=headers, timeout=10)
    advertisers = resp.json()
    print("Advertisers data:", advertisers)

    if "data" not in advertisers or "list" not in advertisers["data"]:
        return JsonResponse(advertisers, status=400)

    advertiser_list = advertisers["data"]["list"]
    if advertiser_list:
        advertiser_id = advertiser_list[0]["advertiser_id"]
        request.session["tiktok_advertiser_id"] = advertiser_id
    else:
        advertiser_id = None

    # --- Store tokens in Supabase ---
    try:
        store_id = request.session.get("store_id")
        if not store_id:
            return HttpResponse("No store_id found in session", status=400)

        # Check if row exists first
        existing = supabase.table("tokens").select("Store_ID").eq("Store_ID", store_id).execute()

        if not existing.data:
            # No row found 
            return HttpResponse(
                f"No entry found in 'tokens' for Store_ID: {store_id}. Please create it first.",
                status=404
            )

        # Row exists, update it
        update_data = {
            "Tiktok_Access": access_token,
            "Tiktok_Org": advertiser_id
        }

        response = supabase.table("tokens").update(update_data).eq("Store_ID", store_id).execute()
        print(f"TikTok tokens updated for Store_ID {store_id}")

    except Exception as e:
        print(f"Error saving TikTok tokens to table: {e}")
        return HttpResponse(f"Error saving tokens: {e}", status=500)

    # Redirect or render advertiser selection
    if len(advertiser_list) == 1:
        return redirect("Demo:tiktok_login")
    return render(request, "Demo/tiktok_select_advertiser.html", {"advertisers": advertiser_list})


# --- SAVE SELECTED ADVERTISER (if multiple) ---
def tiktok_select_advertiser(request, advertiser_id=None):
    if not request.session.get("tiktok_access_token"):
        return redirect("Demo:tiktok_login")
    
    # If an advertiser is selected, save it to the session and redirect to campaigns
    if advertiser_id:
        request.session["tiktok_advertiser_id"] = advertiser_id
        return redirect("Demo:tiktok_campaigns")
    
    # Fetch the list of advertisers again
    access_token = request.session["tiktok_access_token"]
    url = f"{API_BASE}/oauth2/advertiser/get/"
    headers = {"Access-Token": access_token}
    params = {
        "app_id": settings.TIKTOK_CLIENT_KEY,
        "secret": settings.TIKTOK_CLIENT_SECRET,
    }
    resp = requests.get(url, headers=headers, params=params)
    advertisers = resp.json()

    if "data" not in advertisers or "list" not in advertisers["data"]:
        messages.error(request, "Failed to fetch advertisers.")
        return JsonResponse(advertisers, status=400)

    advertiser_list = advertisers["data"]["list"]
    return render(request, "Demo/tiktok_select_advertiser.html", {"advertisers": advertiser_list})

# --- FETCH CAMPAIGNS ---
def tiktok_campaigns(request):
    access_token = request.session.get("tiktok_access_token")
    advertiser_id = request.session.get("tiktok_advertiser_id")

    if not access_token:
        return redirect("Demo:tiktok_login")
    if not advertiser_id:
        return HttpResponse("No advertiser selected", status=400)

    # Filters
    start_date = request.GET.get("start_date", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
    end_date = request.GET.get("end_date", datetime.now().strftime("%Y-%m-%d"))
    page_size = int(request.GET.get("page_size", 100))
    status_filter = request.GET.get("status", "CAMPAIGN_STATUS_ENABLE")  # Default to enabled campaigns

    # Validate the status filter
    valid_statuses = ["CAMPAIGN_STATUS_ENABLE", "CAMPAIGN_STATUS_DISABLE", "CAMPAIGN_STATUS_DELETE", "ALL"]
    if status_filter not in valid_statuses:
        status_filter = "CAMPAIGN_STATUS_ENABLE"  # Fallback to default if invalid

    # --- Step 1: Get campaigns ---
    url = f"{API_BASE}/campaign/get/"
    params = {"advertiser_id": advertiser_id, "page_size": page_size, "page": 1}
    if status_filter != "ALL":
        params["filtering"] = json.dumps({"secondary_status": status_filter})  # Add status filter if not "ALL"

    headers = {"Access-Token": access_token}

    resp = requests.get(url, headers=headers, params=params)
    campaigns_data = resp.json()
    if "data" not in campaigns_data or "list" not in campaigns_data["data"]:
        return JsonResponse(campaigns_data, status=400)

    campaigns = campaigns_data["data"]["list"]

    # --- Step 2: Get ad groups once for budget mapping ---
    ad_group_url = f"{API_BASE}/adgroup/get/"
    ad_group_params = {"advertiser_id": advertiser_id, "page_size": 100, "page": 1}
    ad_group_resp = requests.get(ad_group_url, headers=headers, params=ad_group_params)
    ad_group_data = ad_group_resp.json()
    
    ad_group_details = {}  # Dictionary to store ad group budgets and statuses
    if "data" in ad_group_data and "list" in ad_group_data["data"]:
        ad_group_details = {
            ad["campaign_id"]: {
                "budget": ad.get("budget", 0),
                "status": ad.get("secondary_status", "UNKNOWN")  # Extract status
            }
            for ad in ad_group_data["data"]["list"]
        }
    # --- Step 3: Get campaign stats ---
    stats_url = f"{API_BASE}/report/integrated/get/"
    stats_params = {
        "advertiser_id": advertiser_id,
        "service_type": "AUCTION",
        "report_type": "BASIC",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": json.dumps(["campaign_id"]),
        "metrics": json.dumps([
            "spend", "clicks", "impressions", "complete_payment", "total_complete_payment_rate",
            "cost_per_complete_payment", "complete_payment_roas"
        ]),
        "start_date": start_date,
        "end_date": end_date,
        "page": 1,
        "page_size": page_size,
    }
    stats_resp = requests.get(stats_url, headers=headers, params=stats_params)
    stats_data = stats_resp.json()
    print("Stats data:", stats_data)

    stats_lookup = {}
    if "data" in stats_data and "list" in stats_data["data"]:
        for stat in stats_data["data"]["list"]:
            stats_lookup[stat["dimensions"]["campaign_id"]] = stat["metrics"]

    print("Stats lookup:", stats_lookup)
    # --- Step 4: Merge data ---
    enriched_campaigns = []
    for campaign in campaigns:
        cid = campaign["campaign_id"]
        metrics = stats_lookup.get(cid, {})
        ad_group_info = ad_group_details.get(cid, {"budget": 0, "status": "UNKNOWN"})

        # Transform the ad group status
        raw_status = ad_group_info["status"]
        readable_status = raw_status.replace("ADGROUP_STATUS_", "").replace("_", " ").title()
        enriched_campaigns.append({
            "campaign_id": cid,
            "campaign_name": campaign["campaign_name"],
            "campaign_status": campaign["secondary_status"],
            "spend": float(metrics.get("spend", 0)),
            "clicks": int(metrics.get("clicks", 0)),
            "impressions": int(metrics.get("impressions", 0)),
            "purchases": int(metrics.get("complete_payment", 0)),
            "purchases_value": float(metrics.get("total_complete_payment_rate", 0)),
            "roas": float(metrics.get("complete_payment_roas", 0)),
            "cost_per_conversion": float(metrics.get("cost_per_complete_payment", 0)),
            "budget": ad_group_info.get("budget", 0),
            "status": readable_status  # Use the ad group status
        })

    print("Enriched campaigns:", enriched_campaigns)
    return render(request, "Demo/tiktok_campaigns.html", {
        "campaigns": enriched_campaigns,
        "start_date": start_date,
        "end_date": end_date,
        "page_size": page_size,
        "status_filter": status_filter, 
    })

################################################################################################
######################################## META's API ----
# --- LOGIN VIEW ---
def meta_login(request):
    """
    Redirects user to Meta OAuth page.
    """
    state = str(uuid.uuid4())
    request.session['meta_oauth_state'] = state

    scope = ",".join([
        "ads_read", "ads_management", "business_management"
    ])

    auth_url = (
        f"{settings.OAUTH_PROVIDERS['meta']['auth_url']}"
        f"?client_id={settings.META_APP_ID}"
        f"&redirect_uri={settings.META_REDIRECT_URI}"
        f"&state={state}"
        f"&scope={scope}"
        f"&response_type=code"
    )
    return redirect(auth_url)

# --- CALLBACK VIEW ---
def meta_callback(request):
    """
    Handles Meta redirect, validates state, exchanges code for token, and stores in session.
    """
    code = request.GET.get("code")
    state = request.GET.get("state")
    state_session = request.session.get("meta_oauth_state")

    if not state_session or state_session != state:
        return HttpResponse("Invalid state — potential CSRF attack", status=400)

    del request.session["meta_oauth_state"]

    data = {
        "client_id": settings.META_APP_ID,
        "redirect_uri": settings.META_REDIRECT_URI,
        "client_secret": settings.META_APP_SECRET,
        "code": code
    }

    try:
        resp = requests.get(settings.OAUTH_PROVIDERS['meta']['token_url'], params=data)
        resp.raise_for_status()
        token_data = resp.json()
        print("Meta token data:", token_data)
        short_lived_token = token_data["access_token"]

        # Optional: exchange for long-lived token
        long_lived_token = exchange_long_lived_token(short_lived_token)

        # Save token and expiry in session
        expires_in = token_data.get("expires_in", 3600)
        expiry = datetime.now() + timedelta(seconds=expires_in)
        request.session["meta_access_token"] = long_lived_token or short_lived_token
        request.session["meta_token_expires_at"] = expiry.isoformat()

        ## Save the access token to the db
        ## Add them to database
        store_id = request.session.get("store_id")
        if not store_id:
            return HttpResponse("No store_id found in session", status=400)

        # Check if row exists first
        existing = supabase.table("tokens").select("Store_ID").eq("Store_ID", store_id).execute()

        if not existing.data:
            # No row found 
            return HttpResponse(
                f"No entry found in 'tokens' for Store_ID: {store_id}. Please create it first.",
                status=404
            )

        # Row exists, update it
        update_data = {
            "Meta_Access": request.session.get("meta_access_token")
        }

        response = supabase.table("tokens").update(update_data).eq("Store_ID", store_id).execute()
        print(f"TikTok tokens updated for Store_ID {store_id}")

        # your mom

        return redirect("Demo:meta_select_ad_account")

    except requests.RequestException as e:
        messages.error(request, f"Token exchange failed: {e}")
        return redirect("Demo:home")

# --- EXCHANGE LONG-LIVED TOKEN ---
def exchange_long_lived_token(short_token):
    url = settings.OAUTH_PROVIDERS['meta']['token_url']
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": settings.META_APP_ID,
        "client_secret": settings.META_APP_SECRET,
        "fb_exchange_token": short_token
    }
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        print("Long-lived token response:", resp.json())
        return resp.json().get("access_token")
    except requests.RequestException as e:
        print("Failed to get long-lived token:", e)
        return None

# --- SELECT ACCOUNT (if multiple) ---
def meta_select_ad_account(request, account_id=None):
    if not request.session.get("meta_access_token"):
        return redirect("Demo:meta_login")

    if account_id:
        request.session["meta_ad_account_id"] = account_id
        return redirect("Demo:meta_campaigns")

    # Fetch accounts again
    token = request.session["meta_access_token"]
    url = f"{settings.OAUTH_PROVIDERS['meta']['api_base_url']}/me/adaccounts"
    params = {"access_token": token}
    resp = requests.get(url, params=params)
    accounts = resp.json().get("data", [])
    messages.info(request, "Please select an ad account to proceed.")
    return render(request, "Demo/meta_select_ad_account.html", {"accounts": accounts})

def meta_campaigns(request):
    token = request.session.get("meta_access_token")
    account_id = request.session.get("meta_ad_account_id")

    if not token:
        return redirect("Demo:meta_login")
    if not account_id:
        return redirect("Demo:meta_select_ad_account")

    # Filters
    date_preset = request.GET.get("date_preset", "last_7d")
    start_date = request.GET.get("start_date")
    end_date = request.GET.get("end_date")
    effective_status = request.GET.getlist("status") or ["ACTIVE", "PAUSED"]

    # Build API params
    params = {
        "fields": "campaign_name,results,campaign_id,purchase_roas,impressions,clicks,spend",
        "level": "campaign",
    }

    if start_date and end_date:
        params["time_range"] = json.dumps({"since": start_date, "until": end_date})
    else:
        params["date_preset"] = date_preset

    if token:
        params["access_token"] = token

    url = f"{settings.OAUTH_PROVIDERS['meta']['api_base_url']}/{account_id}/insights"

    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        campaigns = resp.json().get("data", [])
    except Exception as e:
        messages.error(request, f"Failed to fetch campaigns: {e}")
        campaigns = []

    # Process campaigns for template
    table_rows = []
    for camp in campaigns:
        campaign_name = camp.get("campaign_name")
        campaign_id = camp.get("campaign_id")
        results = camp.get("results", [])
        purchase_roas_list = camp.get("purchase_roas", [])

        # Get purchase count
        purchases = 0
        if results:
            for r in results:
                if r.get("indicator") == "actions:offsite_conversion.fb_pixel_purchase":
                    values = r.get("values", [])
                    if values:
                        purchases = int(values[0].get("value", 0))

        # Get ROAS
        roas = 0
        if purchase_roas_list:
            roas = float(purchase_roas_list[0].get("value", 0))

        table_rows.append({
            "campaign_name": campaign_name,
            "campaign_id": campaign_id,
            "purchases": purchases,
            "roas": roas,
            "clicks": int(camp.get("clicks", 0)),
            "impressions": int(camp.get("impressions", 0)),
            "spend": float(camp.get("spend", 0)),
        })

    return render(request, "Demo/meta_campaigns.html", {
        "table_rows": table_rows,
        "date_preset": date_preset,
        "start_date": start_date,
        "end_date": end_date,
        "status_filter": effective_status,
    })


#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# VIEWING THE PRICE MONITOR UPON PRODUCT PRICE UPDATE AND THEIR ORDER COUNTS

def view_price_monitor(request):
    """
    Reads the Price_Change_Monitor table from Supabase,
    unpacks Order_Count_History and Price_Updates JSON fields,
    and sends a clean, flat list to the HTML template.
    """
    try:
        # --- Fetch table data
        res = supabase.table("Price_Change_Monitor").select("*").execute()
        data = res.data or []

        clean_rows = []
        for record in data:
            sku = record.get("SKU", "").strip()
            product_name = record.get("Product_Name", "").strip()
            is_variant = record.get("Is_Variant")
            current_price = record.get("Current_Price")
            product_id = record.get("Product_ID")
            last_updated = record.get("Last_Update_Date")  

            # get json records
            order_history = record.get("Order_Count_History")
            price_updates = record.get("Price_Updates")

            if isinstance(order_history, str):
                order_history = json.loads(order_history or "{}")
            if isinstance(price_updates, str):
                price_updates = json.loads(price_updates or "{}")
                # Removing white space
                price_updates = {k.strip(): v for k, v in price_updates.items()}

            # Get latest order count entry (highest key)
            latest_update_key = None
            latest_order_count = None
            latest_date = None
            if order_history:
                try:
                    latest_update_key = max(order_history.keys(), key=int)
                    latest_order_entry = order_history[latest_update_key]
                    latest_order_count = latest_order_entry.get("orders_after_update")
                    latest_date = latest_order_entry.get("date")
                except Exception:
                    pass

            # Get new/old price info if available
            new_price = None
            old_price = None
            if price_updates and sku in price_updates:
                try:
                    latest_price_key = max(price_updates[sku].keys(), key=int)
                    price_entry = price_updates[sku][latest_price_key]
                    new_price = price_entry.get("new_price")
                    old_price = price_entry.get("old_price")
                except Exception:
                    pass

            clean_rows.append({
                "SKU": sku,
                "Product_Name": product_name,
                "Is_Variant": is_variant,
                "Product_ID": product_id,
                "Old_Price": old_price,
                "New_Price": new_price,
                "Current_Price": current_price,
                "Last_Update_Date": latest_date,
                "Orders_After_Update": latest_order_count,
                "Last_Updated": last_updated,
            })

        # Sort by most recent date
        clean_rows.sort(key=lambda x: (x["Last_Update_Date"] or ""), reverse=True)

        return render(request, "Demo/price_monitor_view.html", {"records": clean_rows})

    except Exception as e:
        print(f"[ERROR] view_price_monitor: {e}")
        import traceback
        traceback.print_exc()
        return render(request, "Demo/price_monitor_view.html", {"records": []})


###### Privacy policy & Data Deletion for meta
def privacy_policy(request):
    return render(request, "Demo/privacy_policy.html")

def data_deletion(request):
    return render(request, "Demo/data_deletion.html")

#Database pageview - Remaz
def events_table_view(request):
    from urllib.parse import unquote

    event_type = request.GET.get("event_type")
    limit = min(int(request.GET.get("limit", 100)), 5000)
    date_after = request.GET.get("date_after")
    if date_after:
        date_after = date_after.replace("T", " ")
    date_end = request.GET.get("date_end")
    if date_end:
        date_end = date_end.replace("T", " ")
    session_search = request.GET.get("session_id", "")
    visitor_search = request.GET.get("visitor_id", "")
    sort_field = request.GET.get("sort_by", "Distinct_ID")
    timezone_search = request.GET.get("timezone", "")
    number_search = request.GET.get("number_search", "")
    action = request.GET.get("action", "filter")

    filters = {}

    if event_type:
        filters["Event_Type"] = ("eq", event_type)

    if session_search and session_search != "None":
        filters["Session_ID"] = ("eq", session_search)

    if visitor_search and visitor_search != "None":
        filters["Visitor_ID"] = ("eq", visitor_search)

    if number_search and number_search != "None":
        filters["Customer_Mobile"] = ("eq", number_search)

    if timezone_search and timezone_search != "None":
        filters["Timezone"] = ("eq", timezone_search)

    if date_after and not date_end:
        if len(date_after) == 10:
            date_after += "T00:00:00"
        filters["Visited_at"] = ("gte", date_after)

    if date_end and not date_after:
        if len(date_end) == 10:
            date_end += "T23:59:59"
        filters["Visited_at"] = ("lte", date_end)

    # ---- Proper date range handling ----
    if date_after and date_end:
        if len(date_after) == 10:
            date_after += "T00:00:00"
        elif len(date_after) == 16:
            date_after += ":00"
        if len(date_end) == 10:
            date_end += "T23:59:59"
        elif len(date_end) == 16:
            date_end += ":00"

        filters["Visited_at"] = ("between", date_after, date_end)
    
    # ---- Last 30 minutes unique visitors ----
    # last_30_min = (get_uae_current_date() - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")

    # last_30_filters = {
    #     "Visited_at": ("gte", last_30_min)
    # }

    # df_last_30 = fetch_data_from_supabase_specific(
    #     table_name="Tracking_Visitors",
    #     columns=["Visitor_ID","Visited_at"],
    #     filters=last_30_filters,
    #     limit=1000,  # high enough to capture traffic
    # )

    # if df_last_30 is None or df_last_30.empty:
    #     last_30_unique_visitors = 0
    # else:
    #     last_30_unique_visitors = df_last_30["Visitor_ID"].nunique()


    df = fetch_data_from_supabase_specific(
        table_name="Tracking_Visitors_duplicate",
        filters=filters,
        order_by=sort_field,
        limit=limit,
    )

    data = [] if df is None or df.empty else df.to_dict(orient="records")

    from collections import Counter

    utm_sources = [r["UTM_Source"] for r in data if r.get("UTM_Source")]
    utm_campaigns = [r["UTM_Campaign"] for r in data if r.get("UTM_Campaign")]

    def to_pct(counter):
        total = sum(counter.values())
        return {k: round(v / total * 100, 2) for k, v in counter.items()} if total else {}

    if action == "export_excel":
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Tracking Sheet"

        headers = list(df.columns)
        ws.append(headers)

        for _, row in df.iterrows():
            ws.append([str(row[h]) if row[h] is not None else "" for h in headers])

        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = "attachment; filename=tracking_sheet.xlsx"
        wb.save(response)
        return response
    
    row_count = 0 if df is None else len(df)

    context = {
        "data": data,
        "row_count": row_count,
        "selected_event_type": event_type,
        "selected_limit": limit,
        "selected_date": date_after,
        "selected_date_end": date_end,
        "session_search": session_search,
        "visitor_search": visitor_search,
        "number_search": number_search,
        "sort_field": sort_field,
        "timezone_search": timezone_search,
        # "last_30_unique_visitors": last_30_unique_visitors,
        "utm_source_labels": json.dumps(list(to_pct(Counter(utm_sources)).keys())),
        "utm_source_values": json.dumps(list(to_pct(Counter(utm_sources)).values())),
        "utm_campaign_labels": json.dumps(list(to_pct(Counter(utm_campaigns)).keys())),
        "utm_campaign_values": json.dumps(list(to_pct(Counter(utm_campaigns)).values())),
    }

    return render(request, "Demo/events_table.html", context)
