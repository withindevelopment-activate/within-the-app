from datetime import datetime, timedelta, timezone
import requests, pandas as pd, json, re, asyncio, traceback, logging, pytz
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
import logging

# Supabase & Supporting imports
from .supabase_functions import batch_insert_to_supabase, get_next_id_from_supabase_compatible_all, get_tracking_df, build_customer_dictionary, attribute_purchases_to_campaigns
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
        messages.error(request, "❌ No code returned from Zid.")
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
            print("⚠️ Store ID not found in profile response.")
        return redirect('Demo:home')  # go to the home view

    except requests.RequestException as e:
        messages.error(request, f"Token error: {str(e)}")
        return redirect('Demo:zid_login')

# Step 3: Render analytics dashboard using Zid API
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
        print("ENTERED THE TRACKING FUNCTION")

        import json
        try:
            data = json.loads(request.body)
        except Exception:
            return JsonResponse({"status": "error", "message": "Invalid JSON payload"}, status=400)

        print("The data received is:", data)
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
    customer_dict = {}
    campaigns_summary = []
    chart_labels = []
    chart_data = []
    context = {}

    try:
        df = get_tracking_df()
        df["Visited_at"] = pd.to_datetime(df["Visited_at"])

        # --- Apply filters ---
        visitor_filter = request.GET.get("visitor")
        session_filter = request.GET.get("session")
        from_date = request.GET.get("from")
        to_date = request.GET.get("to")

        if visitor_filter:
            df = df[df["Visitor_ID"] == visitor_filter]
        if session_filter:
            df = df[df["Session_ID"] == session_filter]
        if from_date:
            df = df[df["Visited_at"] >= from_date]
        if to_date:
            df = df[df["Visited_at"] <= to_date]

        # --- Last 30 min stats ---
        thirty_minutes_ago = datetime.now(uae_timezone) - timedelta(minutes=30)
        df_last_30min = df[df["Visited_at"].dt.tz_localize("Asia/Dubai", ambiguous='NaT', nonexistent='shift_forward') >= thirty_minutes_ago]
        total_visitors = df_last_30min["Visitor_ID"].nunique()
        total_sessions = df_last_30min["Session_ID"].nunique()
        total_pageviews = len(df_last_30min)

        # --- Top 50 rows for display ---
        rows = df.sort_values(by="Visited_at", ascending=False).head(50).to_dict(orient="records")

        # --- Customer dictionary ---
        customer_dict = build_customer_dictionary(df)

        # --- Campaign results ---
        campaigns_summary_df, sources_summary_df = attribute_purchases_to_campaigns(df)

        campaigns_summary = campaigns_summary_df.to_dict(orient="records")
        sources_summary = sources_summary_df.to_dict(orient="records")

        context = {
            "store_id": store_id,
            "rows": rows,
            "total_visitors": total_visitors,
            "total_sessions": total_sessions,
            "total_pageviews": total_pageviews,
            "customer_dict": customer_dict,
            "request": request,
        }

        context["campaigns"] = campaigns_summary
        context["sources"] = sources_summary

        # For charts
        context["campaign_labels"] = campaigns_summary_df["campaign"].tolist()
        context["campaign_data"] = campaigns_summary_df["conversion_credit"].tolist()

        context["source_labels"] = sources_summary_df["source"].tolist()
        context["source_data"] = sources_summary_df["conversion_credit"].tolist()

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
##################### THE PRODUCTS WEBHOOK SECTION 
logger = logging.getLogger(__name__)

def zid_product_update(request):
    if request.method != 'POST':
        return JsonResponse({'error': 'Invalid method'}, status=405)

    try:
        # Parse JSON payload
        body_unicode = request.body.decode('utf-8')
        data = json.loads(body_unicode)

        if not data:
            return JsonResponse({'error': 'No JSON payload received'}, status=400)

        product_id = data.get('id')
        product_name = data.get('name')
        price = data.get('price')

        logger.info(f"Received product update from Zid — ID: {product_id}, Name: {product_name}, Price: {price}")


        return JsonResponse({'status': 'success'}, status=200)

    except json.JSONDecodeError:
        logger.error("Invalid JSON payload received.")
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    except Exception as e:
        logger.exception(f"Error processing Zid webhook: {e}")
        return JsonResponse({'error': 'Internal server error'}, status=500)
