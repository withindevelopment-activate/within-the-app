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


# Supabase & Supporting imports
from .supabase_functions import batch_insert_to_supabase, get_next_id_from_supabase_compatible_all, get_tracking_df, build_customer_dictionary, attribute_purchases_to_campaigns, update_customer_tracking
from .supporting_functions import get_uae_current_date
# Marketing Report functions
from .marketing_report import create_general_analysis, create_product_percentage_amount_spent, landing_performance_5_async, column_check

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
            'X-MANAGER-TOKEN': access_token,  # Sometimes needed depending on endpoint
        }

        # Fetch user profile to get store ID
        profile_response = requests.get(f"{settings.ZID_API_BASE}/managers/account/profile", headers=headers)
        profile = profile_response.json() if profile_response.status_code == 200 else {}
        store_id = profile.get('user', {}).get('store', {}).get('id')

        if store_id:
            request.session['store_id'] = store_id
        else:
            print("Store ID not found in profile response.")

        ### Subscribe to the products webhook --
        subscribe_store_to_product_update(authorization_token, access_token)
        ##
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
@csrf_exempt  # This is added because we are adding the tracking javascript to the app but the store pages likely do not have a <meta name="csrf-token">
@require_POST
def save_tracking(request):
    try:

        import json
        try:
            data = json.loads(request.body)
        except Exception:
            return JsonResponse({"status": "error", "message": "Invalid JSON payload"}, status=400)

        if not data:
            return JsonResponse({"status": "error", "message": "No JSON payload received"}, status=400)

        # Generate unique ID
        distinct_id = int(get_next_id_from_supabase_compatible_all(name='Tracking_Visitors', column='Distinct_ID'))

        # Flatten visitor info
        visitor_info = data.get('visitor_info', {}) or {}
        client_info = data.get('client_info', {}) or {}
        utm_params = data.get('utm_params', {}) or {}
        traffic_source = data.get('traffic_source', {}) or {}
        client_ip = get_client_ip(request)

        tracking_entry = {
            'Distinct_ID': distinct_id,
            'Visitor_ID': data.get('visitor_id'),
            'Session_ID': data.get('session_id'),
            'Store_URL': data.get('store_url'),
            'Event_Type': data.get('event_type'),
            'Event_Details': str(data.get('event_details', {})),
            'Page_URL': data.get('page_url'),
            'Visited_at': get_uae_current_date(),

            # UTM Parameters
            'UTM_Source': utm_params.get('utm_source'),
            'UTM_Medium': utm_params.get('utm_medium'),
            'UTM_Campaign': utm_params.get('utm_campaign'),
            'UTM_Term': utm_params.get('utm_term'),
            'UTM_Content': utm_params.get('utm_content'),

            # Referrer
            'Referrer_Platform': data.get('referrer'),

            # Traffic Source
            'Traffic_Source': traffic_source.get('source'),
            'Traffic_Medium': traffic_source.get('medium'),
            'Traffic_Campaign': traffic_source.get('campaign'),

            # Visitor Info
            'Customer_ID': visitor_info.get('customer_id'),
            'Customer_Name': visitor_info.get('name'),
            'Customer_Email': visitor_info.get('email'),
            'Customer_Mobile': visitor_info.get('mobile'),

            # Client Info
            'User_Agent': client_info.get('user_agent'),
            'Language': client_info.get('language'),
            'Timezone': client_info.get('timezone'),
            'Platform': client_info.get('platform'),
            'Screen_Resolution': client_info.get('screen_resolution'),
            'Device_Memory': client_info.get('device_memory'),
            'Client_IP': client_ip,
        }

        print("ABOUT TO BATCH INSERT")

        try:
            tracking_entry_df = pd.DataFrame([tracking_entry])
            batch_insert_to_supabase(tracking_entry_df, 'Tracking_Visitors')
        except Exception as e:
            print("Failed to insert tracking entry into Supabase:", e)
            traceback.print_exc()

        return JsonResponse({"status": "success"})
    
    except Exception as e:
        print("TRACKING FUNCTION ERROR:", e)
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

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
def process_marketing_report(request):
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

def view_tracking(request):
    store_id = request.GET.get("store_id") or request.session.get("store_uuid")
    if not store_id:
        return redirect("Demo:home")
    
    uae_timezone = pytz.timezone('Asia/Dubai')
    
    rows = []
    total_visitors = total_sessions = total_pageviews = 0
    campaigns_summary = []
    sources_summary = []
    context = {}

    try:
        # --- Fetch tracking events ---
        df = get_tracking_df()
        if df.empty:
            messages.warning(request, "No tracking data available.")
            return redirect("Demo:home")

        # --- Apply filters ---
        visitor_filter = request.GET.get("visitor")
        session_filter = request.GET.get("session")
        from_date = request.GET.get("from")
        to_date = request.GET.get("to")

        df["Visited_at"] = pd.to_datetime(df["Visited_at"], errors="coerce")
        if visitor_filter:
            df = df[df["Visitor_ID"] == visitor_filter]
        if session_filter:
            df = df[df["Session_ID"] == session_filter]
        if from_date:
            df = df[df["Visited_at"] >= from_date]
        if to_date:
            df = df[df["Visited_at"] <= to_date]

        # --- Last 30 minutes stats ---
        thirty_minutes_ago = datetime.now(uae_timezone) - timedelta(minutes=30)
        df_last_30min = df[df["Visited_at"].dt.tz_localize("Asia/Dubai", ambiguous='NaT', nonexistent='shift_forward') >= thirty_minutes_ago]
        total_visitors = df_last_30min["Visitor_ID"].nunique()
        total_sessions = df_last_30min["Session_ID"].nunique()
        total_pageviews = len(df_last_30min)

        # --- Top 50 rows for display ---
        rows = df.sort_values(by="Visited_at", ascending=False).head(50).to_dict(orient="records")

        # --- Incremental customer tracking ---
        customer_dict = update_customer_tracking(df)

        # --- Campaign attribution ---
        campaigns_summary_df, sources_summary_df = attribute_purchases_to_campaigns(df)
        campaigns_summary = campaigns_summary_df.to_dict(orient="records")
        sources_summary = sources_summary_df.to_dict(orient="records")

        # --- Build context ---
        context = {
            "store_id": store_id,
            "rows": rows,
            "total_visitors": total_visitors,
            "total_sessions": total_sessions,
            "total_pageviews": total_pageviews,
            "customer_dict": customer_dict,
            "campaigns": campaigns_summary,
            "sources": sources_summary,
            "campaign_labels": campaigns_summary_df["campaign"].tolist(),
            "campaign_data": campaigns_summary_df["conversion_credit"].tolist(),
            "source_labels": sources_summary_df["source"].tolist(),
            "source_data": sources_summary_df["conversion_credit"].tolist(),
            "request": request,
        }

    except Exception as e:
        logging.error(f"Error fetching tracking data: {str(e)}")
        messages.error(request, f"❌ Error fetching tracking data: {str(e)}")
        return redirect("Demo:home")

    return render(request, "Demo/tracking_view.html", context=context)


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

        # 4. Store the tokens securely in the session.
        request.session["snapchat_access_token"] = token_data["access_token"]
        request.session["snapchat_refresh_token"] = token_data.get("refresh_token")

        expires_in_seconds = token_data.get("expires_in", 3600)
        expiry_datetime = datetime.now() + timedelta(seconds=expires_in_seconds)
        request.session["snapchat_token_expires_at"] = expiry_datetime.isoformat()

        # 5. Redirect the user to the campaigns overview page.
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

def select_organization(request):
    # Call Snapchat to list organizations
    orgs_data = snapchat_api_call(request, "me/organizations")

    if not orgs_data or "organizations" not in orgs_data:
        return HttpResponse("No organizations found", status=404)

    organizations = [
        {
            "id": org["organization"]["id"],
            "name": org["organization"]["name"],
        }
        for org in orgs_data["organizations"]
    ]

    if request.method == "POST":
        selected_org = request.POST.get("organization_id")
        if selected_org:
            request.session["snap_org_id"] = selected_org
            return redirect("Demo:select_account", org_id=selected_org)

    return render(request, "Demo/snapchat_select_org.html", {"organizations": organizations})

def select_ad_account(request, org_id):
    # Call Snapchat to list ad accounts for the chosen org
    accounts_data = snapchat_api_call(request, f"organizations/{org_id}/adaccounts")

    if not accounts_data or "adaccounts" not in accounts_data:
        return HttpResponse("No ad accounts found", status=404)

    ad_accounts = [
        {
            "id": acc["adaccount"]["id"],
            "name": acc["adaccount"]["name"],
        }
        for acc in accounts_data["adaccounts"]
    ]

    if request.method == "POST":
        selected_account = request.POST.get("ad_account_id")
        if selected_account:
            request.session["snap_ad_account_id"] = selected_account
            return redirect("Demo:campaigns_overview") 

    return render(request, "Demo/snapchat_select_account.html", {"ad_accounts": ad_accounts})

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
        return redirect("Demo:select_org")

    ad_account_id = request.session.get("snap_ad_account_id")
    if not ad_account_id:
        return redirect("Demo:select_account", org_id=organization_id)

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
        print("Stats response:", stats_resp)
        adsquads_data = snapchat_api_call(request, f"campaigns/{campaign['id']}/adsquads")
        adsquads_raw = adsquads_data.get("adsquads", [])
        daily_budget = 0
        if adsquads_raw:
            for adsquad in adsquads_raw:
                daily_budget += adsquad["adsquad"].get("daily_budget_micro", 0) / 1_000_000.0

        # print("adsquads_data -----------",adsquads_data)
        # print("stats_resp -----------",stats_resp)
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

    data = {
        "app_id": settings.TIKTOK_CLIENT_KEY,
        "secret": settings.TIKTOK_CLIENT_SECRET,
        "auth_code": code,
        "grant_type": "authorized_code"
    }

    resp = requests.post(TOKEN_URL, json=data, headers={"Content-Type": "application/json"}, timeout=10)
    tokens = resp.json()
    print("Tiktok's token data is:", tokens)

    if not tokens:
        return redirect("Demo:tiktok_login")

    # Always save access_token
    request.session["tiktok_access_token"] = tokens["data"]["access_token"]

    # Save advertiser IDs
    request.session["tiktok_advertiser_ids"] = tokens["data"].get("advertiser_ids", [])

    # Refresh token may not exist
    if "refresh_token" in tokens["data"]:
        request.session["tiktok_refresh_token"] = tokens["data"]["refresh_token"]

    # TikTok didn’t return expires_in, so assume default short expiry (e.g. 24h)
    expiry_seconds = tokens["data"].get("expires_in", 86400)
    request.session["tiktok_token_expiry"] = (
        datetime.now() + timedelta(seconds=expiry_seconds)
    ).isoformat()

    # --- Fetch advertiser accounts ---
    url = f"{API_BASE}/oauth2/advertiser/get/"
    headers = {"Access-Token": request.session["tiktok_access_token"]}
    params  = {
        "app_id": settings.TIKTOK_CLIENT_KEY,
        "secret": settings.TIKTOK_CLIENT_SECRET,
    }
    resp = requests.get(url, params=params, headers=headers, timeout=10)
    advertisers = resp.json()

    if "data" not in advertisers or "list" not in advertisers["data"]:
        return JsonResponse(advertisers, status=400)

    # For demo: auto-pick the first advertiser
    advertiser_list = advertisers["data"]["list"]
    if advertiser_list:
        request.session["tiktok_advertiser_id"] = advertiser_list[0]["advertiser_id"]

    # Or render a selection page for user if multiple accounts
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
        return JsonResponse(advertisers, status=400)

    advertiser_list = advertisers["data"]["list"]
    print("Advertiser list:", advertiser_list)
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
        short_lived_token = token_data["access_token"]

        # Optional: exchange for long-lived token
        long_lived_token = exchange_long_lived_token(short_lived_token)

        # Save token and expiry in session
        expires_in = token_data.get("expires_in", 3600)
        expiry = datetime.now() + timedelta(seconds=expires_in)
        request.session["meta_access_token"] = long_lived_token or short_lived_token
        request.session["meta_token_expires_at"] = expiry.isoformat()

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
        return resp.json().get("access_token")
    except requests.RequestException as e:
        print("Failed to get long-lived token:", e)
        return None


# --- FETCH AD ACCOUNTS ---
# def meta_ad_accounts(request):
#     """
#     Fetch the user's Meta ad accounts and store one advertiser/account in session.
#     """
#     token = request.session.get("meta_access_token")
#     if not token:
#         return redirect("Demo:meta_login")

#     url = f"{settings.OAUTH_PROVIDERS['meta']['api_base_url']}/me/adaccounts"
#     params = {"access_token": token}

#     try:
#         resp = requests.get(url, params=params)
#         resp.raise_for_status()
#         data = resp.json()

#         accounts = data.get("data", [])
#         if not accounts:
#             return HttpResponse("No ad accounts found", status=400)

#         # Save the first account by default
#         request.session["meta_ad_account_id"] = accounts[0]["id"]

#         # If multiple accounts, render selection
#         if len(accounts) > 1:
#             return render(request, "Demo/meta_select_ad_account.html", {"accounts": accounts})

#         return redirect("Demo:meta_campaigns")

#     except requests.RequestException as e:
#         return HttpResponse(f"Failed to fetch ad accounts: {e}", status=500)


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


# --- FETCH CAMPAIGNS ---
def meta_campaigns(request):
    token = request.session.get("meta_access_token")
    account_id = request.session.get("meta_ad_account_id")

    if not token:
        return redirect("Demo:meta_login")
    if not account_id:
        return redirect("Demo:meta_select_ad_account")

    # Fetch campaigns
    url = f"{settings.OAUTH_PROVIDERS['meta']['api_base_url']}/{account_id}/campaigns"
    params = {"access_token": token, "fields": "id,name,status,daily_budget"}
    resp = requests.get(url, params=params)
    campaigns = resp.json().get("data", [])

    # Optional: enrich with insights
    for camp in campaigns:
        insights_url = f"{settings.OAUTH_PROVIDERS['meta']['api_base_url']}/{camp['id']}/insights"
        insights_params = {"access_token": token, "fields": "spend,impressions,clicks,actions"}
        try:
            r = requests.get(insights_url, params=insights_params)
            r.raise_for_status()
            camp["insights"] = r.json().get("data", [{}])[0]
        except requests.RequestException as e:
            camp["insights"] = {}
            messages.error(request, f"Failed to fetch insights for campaign {camp['id']}: {e}")
    return render(request, "Demo/meta_campaigns.html", {"campaigns": campaigns})

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# VIEWING THE PRICE MONITOR UPON PRODUCT PRICE UPDATE AND THEIR ORDER COUNTS
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)

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
            sku = record.get("SKU")
            product_name = record.get("Product_Name")
            is_variant = record.get("Is_Variant")
            current_price = record.get("Current_Price")
            product_id = record.get("Product_ID")
            last_updated = record.get("Last_Updated")

            # Parse JSON fields safely
            order_history = record.get("Order_Count_History")
            price_updates = record.get("Price_Updates")

            if isinstance(order_history, str):
                order_history = json.loads(order_history or "{}")
            if isinstance(price_updates, str):
                price_updates = json.loads(price_updates or "{}")

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

def privacy_policy(request):
    return render(request, "Demo/privacy_policy.html")

def data_deletion(request):
    return render(request, "Demo/data_deletion.html")