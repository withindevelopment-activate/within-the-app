from datetime import datetime
import pandas as pd
import requests
from django.conf import settings
from django.shortcuts import redirect, render
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
import traceback
from django.http import JsonResponse, HttpResponseBadRequest
from rapidfuzz import fuzz
import json


# Step 1: Redirect user to Zid OAuth page
def zid_login(request):
    params = {
            'client_id': settings.ZID_CLIENT_ID,
            'redirect_uri': settings.ZID_REDIRECT_URI,
            'response_type': 'code',
        }
    
    # Add optional parameters if they exist
    query_string = '&'.join([f'{k}={v}' for k, v in params.items()])
    return redirect(f'{settings.ZID_AUTH_URL}?{query_string}')


# Step 2: Handle callback and exchange code for access_token
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

        # ✅ Save token to session
        request.session['access_token'] = token_data.get('access_token')
        request.session['refresh_token'] = token_data.get('refresh_token')
        request.session['authorization_token'] = token_data.get('authorization')

        # # Log the tokens for debugging
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
            print("Store ID saved to session: - views.py:61", store_id)
        else:
            print("⚠️ Store ID not found in profile response. - views.py:63")
        return redirect('Demo:home')  # go to the home view

    except requests.RequestException as e:
        messages.error(request, f"Token error: {str(e)}")
        return redirect('Demo:zid_login')

# Step 3: Render analytics dashboard using Zid API
def home(request):
    token = request.session.get('access_token')
    auth_token = request.session.get('authorization_token')
    store_id = request.session.get('store_id')

    if not token:
        return redirect('Demo:zid_login')

    headers = {
        'Authorization': f'Bearer {auth_token}',
        'X-MANAGER-TOKEN': token,
    }

    headers_product = {
            'Authorization': f'Bearer {auth_token}',
            'X-MANAGER-TOKEN': token,  # Sometimes needed depending on endpoint
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

        # Fetch orders
        orders_res = requests.get(f"{settings.ZID_API_BASE}/managers/store/orders", headers=headers)
        orders_res.raise_for_status()
        orders_data = orders_res.json()
        print("Orders data fetched successfully:", orders_data)

        # Extract orders and calculate totals
        orders = orders_data.get('orders', [])
        total_orders = round(orders_data.get('total_order_count', len(orders)), 2)
        total_revenue = sum(float(o.get('order_total' , 0)) for o in orders)
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

        # Fetch products
        products_res = requests.get(f"{settings.ZID_API_BASE}/products", headers=headers_product)
        products_res.raise_for_status()
        products_data = products_res.json()
        print("Products data fetched successfully:", products_data)
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
        messages.error(request, f"⚠️ Something went wrong: {str(e)}")
    
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

# Step 4: Logout and clear session
def zid_logout(request):
    request.session.flush()
    messages.success(request, "You have been logged out.")
    return redirect('Demo:zid_login')

# Step 5: Refresh access token using stored refresh token
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

    return render(request, "ads_api/match_orders.html", {
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
def save_tracking(request):
    if request.method == "POST":
        data = json.loads(request.body)
        print("Tracking data:", data)

        return JsonResponse({"status": "success"})
    
    return JsonResponse({"error": "Invalid request method"}, status=405)