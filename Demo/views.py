from datetime import datetime, timedelta, timezone
from django.conf import settings
from django.shortcuts import redirect, render
from django.contrib import messages
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponse
import pandas as pd
from rapidfuzz import fuzz
import json, re, os, sqlite3, pytz, requests, traceback, uuid
from django.core.files.storage import FileSystemStorage
from .supporting_functions import get_uae_current_date
from django.template.loader import render_to_string
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from pathlib import Path
from django.db import connection
from urllib.parse import urlencode


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
        # print(f"✅ Profile fetched successfully: {profile}")
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

def safe_name(name):
    return re.sub(r'[^0-9a-zA-Z_]', '_', str(name))

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
        "source_metrics": json.dumps(source_metrics),
        "campaign_product_sales": campaign_product_sales,
    })

def search_view(request):
    query = request.GET.get("q", "").lower()
    results = []

    if query:
        # Example: search in predefined pages
        pages = [
            {"title": "Dashboard", "url": "/"},
            {"title": "Analytics", "url": "/zid_orders/"},
            {"title": "Logout", "url": "/zid_logout/"},
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

    all_orders = []
    page = 1
    per_page = 100

    try:
        while True:
            params = {"page": page, "page_size": per_page}
            orders_res = requests.get(f"{settings.ZID_API_BASE}/managers/store/orders",
                               headers=headers, params=params)
            orders_res.raise_for_status()
            orders_data = orders_res.json()

            orders_list = orders_data.get("orders", [])
            if not orders_list:
                break

            # Process orders (dates + totals)
            for order in orders_list:
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

            all_orders.extend(orders_list)

            # Stop if we already got everything
            if len(all_orders) >= orders_data.get("total_orders_count", 0):
                break

            page += 1

    except requests.RequestException as e:
        traceback.print_exc()
        messages.error(request, f"⚠️ Error fetching orders: {str(e)}")
        all_orders = []

    return render(request, 'Demo/orders.html', {
        'orders': all_orders,
        'total_orders': len(all_orders),
        'total_revenue': sum(float(order.get('transaction_amount', 2)) for order in all_orders),
        'avg_order': (sum(float(order.get('transaction_amount', 2)) for order in all_orders) / len(all_orders) if all_orders else 0),
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

    all_products = []
    page = 1
    per_page = 50
    try:
        while True:
            params = {'page': page, 'page_size': per_page}
            products_res = requests.get(f"{settings.ZID_API_BASE}/products", headers=headers_product, params=params)
            products_res.raise_for_status()
            products_data = products_res.json()
            products_list = products_data.get('results', [])
            if not products_list:
                break
            all_products.extend(products_list)
            # Optional: break if you've fetched as many as reported by the API
            if len(all_products) >= products_data.get('total_products_count', 0):
                break
            page += 1
    except requests.RequestException as e:
        traceback.print_exc()
        messages.error(request, f"⚠️ Error fetching products: {str(e)}")
        all_products = []

    # Extract product list safely
    total_products = products_data.get("total_products_count", len(all_products))

    # KPIs
    published_count = sum(1 for p in all_products if p.get("is_published"))
    avg_rating = (
        round(sum(p.get("rating", {}).get("average", 0) for p in all_products) / total_products, 2)
        if total_products > 0 else 0
    )
    on_sale_count = sum(
        1 for p in all_products if p.get("sale_price") and p.get("price") and p["sale_price"] < p["price"]
    )

    context = {
        "products": all_products,  # send all products
        "total_products": total_products,
        "published_count": published_count,
        "avg_rating": avg_rating,
        "on_sale_count": on_sale_count,
    }

    return render(request, "Demo/products_page.html", context)

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

def safe_numeric(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0  # fallback if non-numeric

def marketing_page(request):
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

    return render(request, "Demo/marketing.html", context)

def utm_builder(request):
    return render(request, "Demo/utm_builder.html")

# Path to Django default SQLite DB
DB_PATH = os.path.join(settings.BASE_DIR, "db.sqlite3")

# helper to make safe SQL table name
def get_table_name(store_id: str) -> str:
    safe_id = re.sub(r'[^0-9a-zA-Z_]', '_', str(store_id))  # sanitize
    return f"Tracking_Visitors_{safe_id}"

def init_user_table(store_id: str):
    table = get_table_name(store_id)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                Distinct_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                Visitor_ID TEXT,
                Session_ID TEXT,
                Store_URL TEXT,
                Event_Type TEXT,
                Event_Details TEXT,
                Page_URL TEXT,
                Visited_at TEXT,
                UTM_Source TEXT,
                UTM_Medium TEXT,
                UTM_Campaign TEXT,
                UTM_Term TEXT,
                UTM_Content TEXT,
                Referrer_Platform TEXT,
                Traffic_Source TEXT,
                Traffic_Medium TEXT,
                Traffic_Campaign TEXT,
                Customer_ID TEXT,
                Customer_Name TEXT,
                Customer_Email TEXT,
                Customer_Mobile TEXT,
                User_Agent TEXT,
                Language TEXT,
                Timezone TEXT,
                Platform TEXT,
                Screen_Resolution TEXT,
                IP_Address TEXT,
                Device_Memory TEXT
            )
        """)
        conn.commit()
    return table

@csrf_exempt
@require_POST
def save_tracking(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return JsonResponse({"status": "error", "message": "Invalid JSON"}, status=400)

    store_id = data.get("store_id")
    if not store_id:
        return JsonResponse({"status": "error", "message": "No store_id provided"}, status=403)

    # ensure table exists
    table = init_user_table(store_id)
    client_ip = get_client_ip(request)

    tracking_entry = (
        data.get('visitor_id'),
        data.get('session_id'),
        data.get('store_url'),
        data.get('event_type'),
        json.dumps(data.get('event_details', {}), ensure_ascii=False),
        data.get('page_url'),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        data.get('utm_params', {}).get('utm_source'),
        data.get('utm_params', {}).get('utm_medium'),
        data.get('utm_params', {}).get('utm_campaign'),
        data.get('utm_params', {}).get('utm_term'),
        data.get('utm_params', {}).get('utm_content'),
        data.get('referrer'),
        data.get('traffic_source', {}).get('source'),
        data.get('traffic_source', {}).get('medium'),
        data.get('traffic_source', {}).get('campaign'),
        data.get('visitor_info', {}).get('customer_id'),
        data.get('visitor_info', {}).get('name'),
        data.get('visitor_info', {}).get('email'),
        data.get('visitor_info', {}).get('mobile'),
        data.get('client_info', {}).get('user_agent'),
        data.get('client_info', {}).get('language'),
        data.get('client_info', {}).get('timezone'),
        data.get('client_info', {}).get('platform'),
        data.get('client_info', {}).get('screen_resolution'),
        client_ip,
        data.get('client_info', {}).get('device_memory'),
    )

    columns = """Visitor_ID, Session_ID, Store_URL, Event_Type, Event_Details, Page_URL, Visited_at,
                 UTM_Source, UTM_Medium, UTM_Campaign, UTM_Term, UTM_Content,
                 Referrer_Platform, Traffic_Source, Traffic_Medium, Traffic_Campaign,
                 Customer_ID, Customer_Name, Customer_Email, Customer_Mobile,
                 User_Agent, Language, Timezone, Platform, Screen_Resolution, IP_Address, Device_Memory"""

    placeholders = ",".join(["?"] * 27)

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                tracking_entry
            )
            conn.commit()
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JsonResponse({"status": "error", "message": str(e)}, status=500)

    return JsonResponse({"status": "success"})

def view_tracking(request):
    # get store_id from querystring or session
    store_id = request.GET.get("store_id") or request.session.get("store_uuid")
    if not store_id:
        return HttpResponse("❌ store_id is required", status=400)

    # path to the default Django DB
    db_path = os.path.join(settings.BASE_DIR, "db.sqlite3")

    # get table name
    table = get_table_name(store_id)

    # ensure table exists
    init_user_table(store_id)

    rows = []
    total_visitors = total_sessions = total_pageviews = 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # build query with filters
        query = f"SELECT * FROM {table} WHERE 1=1"
        params = []

        if request.GET.get("visitor"):
            query += " AND Visitor_ID = ?"
            params.append(request.GET["visitor"])

        if request.GET.get("session"):
            query += " AND Session_ID = ?"
            params.append(request.GET["session"])

        if request.GET.get("from"):
            query += " AND date(Visited_at) >= date(?)"
            params.append(request.GET["from"])

        if request.GET.get("to"):
            query += " AND date(Visited_at) <= date(?)"
            params.append(request.GET["to"])

        query += " ORDER BY Visited_at DESC LIMIT 200"
        cur.execute(query, params)
        rows = cur.fetchall()

        # stats
        cur.execute(f"SELECT COUNT(DISTINCT Visitor_ID) FROM {table}")
        total_visitors = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(DISTINCT Session_ID) FROM {table}")
        total_sessions = cur.fetchone()[0]

        cur.execute(f"SELECT COUNT(*) FROM {table}")
        total_pageviews = cur.fetchone()[0]

    return render(request, "Demo/tracking_view.html", {
        "store_id": store_id,
        "rows": rows,
        "total_visitors": total_visitors,
        "total_sessions": total_sessions,
        "total_pageviews": total_pageviews,
    })

# The route to render the tracking javascript -- 
def tracking_snippet(request):
    store_id = request.GET.get("store_id")
    if not store_id:
        return HttpResponse("❌ store_id is required", status=400)

    # Ensure the store table exists before serving snippet
    init_user_table(store_id)

    js_content = render_to_string("tracking-snippet.js", {
        "store_id": store_id,
        "backend_url": settings.BACKEND_URL.rstrip("/"),
    })
    return HttpResponse(js_content, content_type="application/javascript")

def get_client_ip(request):
    x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")

def snapchat_login(request):
    """
    Initiates the Snapchat Marketing API OAuth 2.0 flow by creating a unique state
    and storing it in the user's session.
    """
    # Generate a unique state to prevent CSRF attacks.
    state = str(uuid.uuid4())
    print(f"Generated OAuth state: {state}")
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
    print(f"Redirecting to Snapchat OAuth URL: {auth_request_url}")
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
    cfg = settings.OAUTH_PROVIDERS["snapchat"]
    token_url = cfg["token_url"]

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

    cfg = settings.OAUTH_PROVIDERS["snapchat"]
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": settings.SNAPCHAT_CLIENT_ID,
        "client_secret": settings.SNAPCHAT_CLIENT_SECRET,
    }

    try:
        resp = requests.post(cfg["token_url"], data=data)
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

def snapchat_api_call(request, endpoint, params=None):
    """
    Helper function to make an authenticated API call, handling token refresh.
    """
    access_token = request.session.get('snapchat_access_token')

    # Parse expiry from ISO string
    expires_at_str = request.session.get("snapchat_token_expires_at")
    expires_at = parser.isoparse(expires_at_str) if expires_at_str else None

    if not access_token or (expires_at and datetime.now() > expires_at - timedelta(minutes=5)):
        access_token = refresh_snapchat_token(request)
        if not access_token:
            return None

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
    }
    
    url = f'https://adsapi.snapchat.com/v1/{endpoint}'
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed to {endpoint}: {e}")
        return None

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
    orgs = snapchat_api_call(request, "me/organizations")
    if not orgs or not orgs.get("organizations"):
        return HttpResponse("No organizations found", status=404)
    organization_id = orgs["organizations"][0]["organization"]["id"]

    accounts = snapchat_api_call(request, f"organizations/{organization_id}/adaccounts")
    if not accounts or not accounts.get("adaccounts"):
        return HttpResponse("No ad accounts found", status=404)
    ad_account_id = accounts["adaccounts"][0]["adaccount"]["id"]

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

# TikTok API constants
CLIENT_KEY = settings.TIKTOK_CLIENT_KEY
CLIENT_SECRET = settings.TIKTOK_CLIENT_SECRET
REDIRECT_URI = settings.TIKTOK_REDIRECT_URI   # must match TikTok app settings

AUTH_BASE = "https://business-api.tiktokglobalshop.com/open_api/v1.3"  # Marketing API base
OAUTH_URL = "https://business-api.tiktokglobalshop.com/oauth"
TOKEN_URL = "https://business-api.tiktokglobalshop.com/oauth2/access_token/"

# --- LOGIN VIEW ---
def tiktok_login(request):
    auth_url = (
        f"{OAUTH_URL}/authorize?"
        f"app_id={settings.TIKTOK_CLIENT_KEY}"
        f"&redirect_uri={settings.TIKTOK_REDIRECT_URI}"
        f"&state=xyz123"
        f"&scope={settings.TIKTOK_OAUTH_SCOPE}"   # add required scopes
    )
    return redirect(auth_url)

# --- CALLBACK VIEW ---
def tiktok_callback(request):
    code = request.GET.get("auth_code")
    if not code:
        return HttpResponse("No auth_code returned", status=400)

    data = {
        "app_id": settings.TIKTOK_CLIENT_KEY,
        "secret": settings.TIKTOK_CLIENT_SECRET,
        "auth_code": code,
    }
    resp = requests.post(TOKEN_URL, data=data)
    tokens = resp.json()

    if "data" not in tokens:
        return JsonResponse(tokens, status=400)

    # Save tokens in session or DB
    request.session["tiktok_access_token"] = tokens["data"]["access_token"]
    request.session["tiktok_refresh_token"] = tokens["data"]["refresh_token"]
    request.session["tiktok_token_expiry"] = (
        datetime.datetime.utcnow()
        + datetime.timedelta(seconds=tokens["data"]["expires_in"])
    ).isoformat()

    return redirect("tiktok_campaigns")

# --- REFRESH TOKEN ---
def tiktok_refresh_token(request):
    refresh_token = request.session.get("tiktok_refresh_token")
    if not refresh_token:
        return HttpResponse("No refresh token", status=400)

    data = {
        "app_id": settings.TIKTOK_CLIENT_KEY,
        "secret": settings.TIKTOK_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    resp = requests.post(TOKEN_URL, data=data)
    tokens = resp.json()

    if "data" not in tokens:
        return JsonResponse(tokens, status=400)

    request.session["tiktok_access_token"] = tokens["data"]["access_token"]
    request.session["tiktok_refresh_token"] = tokens["data"]["refresh_token"]
    request.session["tiktok_token_expiry"] = (
        datetime.datetime.utcnow()
        + datetime.timedelta(seconds=tokens["data"]["expires_in"])
    ).isoformat()

    return JsonResponse({"status": "refreshed"})

# --- FETCH CAMPAIGNS ---
def tiktok_campaigns(request):
    access_token = request.session.get("tiktok_access_token")
    if not access_token:
        return redirect("tiktok_login")

    # You need advertiser_id (from your TikTok Ads Manager account)
    advertiser_id = settings.TIKTOK_ADVERTISER_ID

    url = f"{AUTH_BASE}/campaign/get/"
    params = {
        "advertiser_id": advertiser_id,
        "page_size": 10,
        "page": 1,
    }
    headers = {"Access-Token": access_token}
    resp = requests.get(url, headers=headers, params=params)
    data = resp.json()

    # Render campaigns in a template
    return render(request, "tiktok/campaigns.html", {"campaigns": data})
