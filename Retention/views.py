from django.shortcuts import render
import json
from io import BytesIO
import logging
from Demo.supporting_files.supabase_functions import get_next_id_from_supabase_compatible_all
from Demo.supporting_files.supporting_functions import get_uae_current_date
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

## Import the database functions
from Retention.supporting_pys.database_functions import *
from Retention.supporting_pys.supporting_functions import *
from Retention.supporting_pys.retention_functions import * 
from datetime import datetime, timedelta

import pandas as pd
import pytz

logger = logging.getLogger('Retention')
########################################################################### Retention webhooks 
@csrf_exempt
@require_POST
############
#######
# To get all the None values, I can create a seperate API call for the order
#######
###########
def adding_order_to_db(payload):
    """
    Take the incoming payload from the webhook and adding it to the All ZID orders db --- 
    """

    dubai = pytz.timezone("Asia/Dubai")

    def dubai_time(value):
        if not value:
            return None

        try:
            dt = pd.to_datetime(value, utc=True)
            return dt.tz_convert(dubai).strftime("%Y-%m-%d %I:%M %p")
        except Exception:
            return value

    def get_nested(data, *keys):
        current = data
        for key in keys:
            if current is None:
                return None

            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current
    
    print("=" * 80)
    print("NEW WEBHOOK RECEIVED")
    print(f"Order ID: {payload.get('id')}")
    print(f"Payload Keys: {list(payload.keys())}")

    rows = []

    products = payload.get("products", [])
    print(f"Number of products: {len(products)}")

    customer = payload.get("customer", {})
    shipping = payload.get("shipping", {})
    payment = payload.get("payment", {})
    coupon = payload.get("coupon")
    address = get_nested(payload, "shipping", "address") or {}
    utm = payload.get("utm", {})
    analytics = payload.get("analytics", {})
    split_payments = payload.get("split_payment_methods", [])

    split1 = split_payments[0] if len(split_payments) > 0 else {}
    split2 = split_payments[1] if len(split_payments) > 1 else {}

    ## Extract order level info
    order_id = payload.get("id")

    customer_id = customer.get("id")
    customer_name = customer.get("name")
    customer_mobile = customer.get("mobile")
    customer_email = customer.get("email")

    print(f"Customer ID: {customer_id}")
    print(f"Customer Name: {customer_name}")
    print(f"Customer Mobile: {customer_mobile}")

    customer_note = payload.get("customer_note")

    utm_source = utm.get("source")

    ## Get the distinct_id
    distinct_id = int(get_next_id_from_supabase_compatible_all(
                name='All_ZID_Orders', column='Distinct_ID'
            ))
    
    print(f"Starting Distinct_ID: {distinct_id}")
    
    ## Initialize a dictionary to store the products + their quans
    products_dict = {}
    ## We loop thru the products because an order has multiple orders = multiple rows
    for product in products:

        print(f"\nProcessing product {product}")
        print(f"SKU: {product.get('sku')}")
        print(f"Name: {product.get('name')}")
        print(f"Quantity: {product.get('quantity')}")

        row = {
            ## The Distinct ID
            "Distinct_ID": distinct_id,

            ## Product
            "product name": product.get("name"),
            "sku": product.get("sku"),
            "quantity": int(product.get("quantity") or 0),
            "order_products_cost": None,
            "unit_price": product.get("price_with_additions"),
        }

        rows.append(row)
        distinct_id += 1

        ## Add the products and their quantity to a dict
        sku = product.get("sku")
        product_name = product.get("name")
        quantity = int(product.get("quantity") or 0)

        if sku not in products_dict:
            products_dict[sku] = {
                "product_name": product_name,
                "quantity": quantity
            }
        else:
            products_dict[sku]["quantity"] += quantity


    ## Convert to a df 
    rows_df = pd.DataFrame(rows)
    ## Add the row level info to the row after looping -- this populates all the fields for the said order id for easier future access -- 
    ## Order Information
    rows_df["id"] = order_id
    rows_df["order_status"] = get_nested(payload, "order_status", "name")
    rows_df["source"] = payload.get("source")

    ## Customer info
    rows_df["customer_note"] = customer_note
    rows_df["customer_name"] = customer_name
    rows_df["customer_email"] = customer_email
    rows_df["customer_mobile"] = customer_mobile
    rows_df["customer_id"] = customer_id

    ## Payment
    rows_df["payment_method"] = get_nested(payment, "method", "name")
    rows_df["payment_status"] = payload.get("payment_status")
    rows_df["transaction_reference"] = payload.get("transaction_reference")

    ## Shipping
    rows_df["shipping_method"] = None
    rows_df["shipping_address"] = address.get("address")
    rows_df["shipping_city"] = address.get("city")
    rows_df["shipping_company_tracking_id"] = shipping.get("tracking_id")
    rows_df["googlemaps_location"] = address.get("googlemaps_location")
    rows_df["shipping_short_address"] = None

    ## Coupon
    rows_df["coupon_code"] = coupon
    rows_df["coupon_name"] = None

    ## Totals
    order_total = float(payload.get("order_total") or 0)
    rows_df["sub_totals"] = None
    rows_df["vat"] = None
    rows_df["shipping"] = None
    rows_df["cod"] = None
    rows_df["discount"] = None
    rows_df["total"] = payload.get("total")
    rows_df["currency"] = payload.get("currency_code")

    ## Get the region
    currency_code = payload.get("currency_code")

    if currency_code == "OMR":
        region = "OM"
    elif currency_code == "AED":
        region = "UAE"
    elif currency_code == "QAR":
        region = "Qatar"
    else:
        region = None

    print(f"Currency: {currency_code}")
    print(f"Region: {region}")

    rows_df["region"] = region

    ## Dates
    added_at = dubai_time(payload.get("created_at"))
    rows_df["added_at (Asia/Dubai)"] = added_at
    rows_df["last_update_at (Asia/Dubai)"] = dubai_time(payload.get("updated_at"))

    print(f"Added At: {added_at}")

    ## POS
    rows_df["pos_inventory_location"] = None
    rows_df["pos_cashier_user_name"] = None

    ## Split Payments
    rows_df["split_payment_method_1_name"] = split1.get("name")
    rows_df["split_payment_method_1_total"] = split1.get("total")
    rows_df["split_payment_method_2_name"] = split2.get("name")
    rows_df["split_payment_method_2_total"] = split2.get("total")

    ## UTMs
    rows_df["utm_source"] = None
    rows_df["utm_medium"] = None
    rows_df["utm_campaign"] = None
    rows_df["utm_term"] = None
    rows_df["utm_content"] = None

    ## Analytics
    rows_df["referer"] = None
    rows_df["platform"] = None
    rows_df["device_type"] = None

    rows_df["ip_country"] = None
    rows_df["ip_city"] = None
    rows_df["ip_region"] = None
    rows_df["ip_timezone"] = None

    ## Markings
    rows_df['order_source'] = 'Webhook'
    rows_df['last_updated'] = get_uae_current_date()

    ## Append to the database
    print("About to insert into All_ZID_Orders...")
    print(rows_df.to_string())
    batch_insert_to_supabase(rows_df, "All_ZID_Orders")

    print("Successfully inserted into All_ZID_Orders.")

    ## Update the customers_db
    print("Updating customer database...")
    print(products_dict)
    verdict = update_customers_db(customer_id, customer_name, customer_mobile, order_id, products_dict, added_at, utm_source, order_total)
    print(f"Customer DB update verdict: {verdict}")

    print(f"Finished processing Order {order_id}")
    print("=" * 80)
    return True

def retention_dashboard(request):
    """
    Displays customer data with filters for retention analysis.
    """
    # Fetch filter parameters from request
    limit = request.GET.get("limit") or "20"
    order_count_filter = request.GET.get("order_count")
    order_date_filter = request.GET.get("order_date")
    not_ordered_since_months = request.GET.get("not_ordered_since")
    phone_filter = request.GET.get("phone")
    action = request.GET.get("action")
    contacted_filter = request.GET.get("contacted")
    tags_filter = request.GET.getlist("tags") # Get list of selected tags

    try:
        limit = int(limit)
    except (ValueError, TypeError):
        limit = 20

    phone_numbers_from_tags = []
    filters_for_tags = {}
    if tags_filter:
        filters_for_tags["Tag"] = ("in", tags_filter)
        # Fetch phone numbers from the Customer_Tags table based on the selected tags.
        tags_df, _ = fetch_data_from_supabase_specific(
            table_name="Customer_Tags",
            filters=filters_for_tags
        )
        if not tags_df.empty:
            # Get a unique list of phone numbers.
            phone_numbers_from_tags = tags_df["Customer_Mobile"].unique().tolist()[:limit]

    # Build filters conditionally
    filters = {}
    if phone_filter:
        filters["Customer_Mobile"] = ("eq", phone_filter)

    if contacted_filter:
        filters["Contacted"] = ("eq", True)

    # If we have phone numbers from tags, use them to filter.
    if phone_numbers_from_tags:
        filters["Customer_Mobile"] = ("in", phone_numbers_from_tags)

    if order_count_filter:
        try:
            filters["Order_Count"] = ("eq", int(order_count_filter))
        except (ValueError, TypeError):
            pass # Ignore if not a valid integer

    if not_ordered_since_months:
        try:
            months = int(not_ordered_since_months)
            if months > 0:
                cutoff_date = datetime.now() - timedelta(days=months * 30)
                filters["Last_Updated"] = ("lt", cutoff_date.isoformat())
        except (ValueError, TypeError):
            pass

    # First, get the total count of customers matching the filters without any limit.
    _, total_customers = fetch_data_from_supabase_specific(
        table_name="Store_Customers", columns=["Distinct_ID"], filters=filters, count='exact')
    
    # If the action is to download, fetch all data, otherwise fetch the paginated view.
    if action == "download_excel":
        df, _ = fetch_data_from_supabase_specific(
            table_name="Store_Customers", filters=filters, order_by="Last_Updated")
    else:
        df, _ = fetch_data_from_supabase_specific(
            table_name="Store_Customers", limit=limit, filters=filters, order_by="Last_Updated")

    # --- Data Processing and Cleaning ---
    # This block is now common for both display and download
    required_cols = [
        "Order_Count", "Customer_Lifetime_Value", "Orders", 
        "Last_Visit", "Customer_Name", "Customer_Mobile", "Customer_ID"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["Order_Count"] = pd.to_numeric(df["Order_Count"], errors='coerce').fillna(0).astype(int)
    df["Customer_Lifetime_Value"] = pd.to_numeric(df["Customer_Lifetime_Value"], errors='coerce').fillna(0)

    def get_last_visit(order_json):
        if pd.isna(order_json): return pd.NaT
        try:
            if isinstance(order_json, str): order_json = json.loads(order_json)
            if not isinstance(order_json, dict): return pd.NaT
            dates = [pd.to_datetime(order.get("added_at"), errors='coerce') for order in order_json.values() if order.get("added_at")]
            return max(d for d in dates if pd.notna(d)) if dates else pd.NaT
        except (json.JSONDecodeError, TypeError): return pd.NaT
    df['Last_Visit'] = df['Orders'].apply(get_last_visit)

    # --- Data Processing and Cleaning ---
    # Ensure required columns exist, even if df is empty, to prevent KeyErrors
    required_cols = [
        "Order_Count", "Customer_Lifetime_Value", 
        "Orders", "Last_Visit", "Customer_Name", "Customer_Mobile"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    if order_date_filter:
        filter_date = pd.to_datetime(order_date_filter).date()
        df = df[df['Last_Visit'].notna() & (df['Last_Visit'].dt.date == filter_date)]

    is_filtered = any([order_count_filter, order_date_filter, not_ordered_since_months, phone_filter, tags_filter, contacted_filter])

        # Default limit if no filters are applied
    if not is_filtered and limit:
        df = df.head(int(limit))
    elif not is_filtered:
        df = df.head(20) # Default to 20 if no limit and no filters

    # --- Handle Excel Download ---
    if action == "download_excel":
        from django.http import HttpResponse

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Prepare data for export
            df_export = df[[
                "Customer_ID", "Customer_Name", "Customer_Mobile",
                "Order_Count", "Last_Visit", "Customer_Lifetime_Value"
            ]].copy()
            df_export.rename(columns={
                "Customer_ID": "Customer ID", "Customer_Name": "Name", "Customer_Mobile": "Mobile",
                "Order_Count": "Orders", "Last_Visit": "Last Order", "Customer_Lifetime_Value": "Total Spent (AED)"
            }, inplace=True)
            df_export['Last Order'] = pd.to_datetime(df_export['Last Order']).dt.strftime('%Y-%m-%d %H:%M')
            df_export.to_excel(writer, index=False, sheet_name='Customers')
        
        output.seek(0)
        response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = 'attachment; filename="filtered_customers.xlsx"'
        return response

    # --- Calculate KPIs ---
    total_customers_filtered = len(df)
    total_ltv = df["Customer_Lifetime_Value"].sum()
    avg_ltv = total_ltv / total_customers_filtered if total_customers_filtered > 0 else 0

    customers = df.to_dict(orient="records")

    # Clean up data for template
    for customer in customers:
        customer['Customer_Name'] = customer.get('Customer_Name') or 'N/A'
        customer['Customer_Mobile'] = customer.get('Customer_Mobile') or ''

        if pd.isna(customer.get('Last_Visit')):
            customer['Last_Visit'] = None

    # Mapping from English tag values (database) to Arabic display names (UI)
    tag_display_mapping = {
        "active": "نشط",
        "inactive": "غير نشط",
        "at_risk": "في خطر",
        "lost": "مفقود",
        "vip": "عميل مميز",
        "First-time customer": "عميل لأول مرة",
        "orders_1": "طلب واحد",
        "orders_2": "طلبان",
        "orders_3": "3 طلبات",
        "orders_4": "4 طلبات",
        "orders_5_plus": "5 طلبات أو أكثر",
        "aov_100_300": "متوسط قيمة الطلب 100-300",
        "aov_400_600": "متوسط قيمة الطلب 400-600",
        "aov_700_900": "متوسط قيمة الطلب 700-900",
        "aov_1000_2000": "متوسط قيمة الطلب 1000-2000",
        "aov_2000_plus": "متوسط قيمة الطلب +2000",
    }

    tag_descriptions = {
        "active": "العملاء الذين لديهم أكثر من طلب واحد وقاموا بالشراء خلال الـ 90 يومًا الماضية.",
        "inactive": "العملاء الذين لم يقوموا بالشراء منذ أكثر من 90 يومًا.",
        "at_risk": "العملاء الذين لم يقوموا بالشراء منذ أكثر من 180 يومًا.",
        "lost": "العملاء الذين لم يقوموا بالشراء منذ أكثر من 365 يومًا.",
        "vip": "العملاء ذوو القيمة العالية بناءً على إجمالي إنفاقهم.",
        "First-time customer": "العملاء الذين أجروا عملية شراء واحدة فقط.",
        "orders_1": "العملاء الذين لديهم طلب واحد بالضبط.",
        "orders_2": "العملاء الذين لديهم طلبان بالضبط.",
        "orders_3": "العملاء الذين لديهم 3 طلبات بالضبط.",
        "orders_4": "العملاء الذين لديهم 4 طلبات بالضبط.",
        "orders_5_plus": "العملاء الذين لديهم 5 طلبات أو أكثر.",
        "aov_100_300": "العملاء الذين يتراوح متوسط قيمة طلبهم بين 100 و 300.",
        "aov_400_600": "العملاء الذين يتراوح متوسط قيمة طلبهم بين 400 و 600.",
        "aov_700_900": "العملاء الذين يتراوح متوسط قيمة طلبهم بين 700 و 900.",
        "aov_1000_2000": "العملاء الذين يتراوح متوسط قيمة طلبهم بين 1000 و 2000.",
        "aov_2000_plus": "العملاء الذين يزيد متوسط قيمة طلبهم عن 2000.",
    }

    context = {
        "customers": customers,
        "row_count": len(customers),
        "is_filtered": is_filtered,
        "filters": {
            "limit": limit or "20",
            "order_count": order_count_filter or "",
            "order_date": order_date_filter or "",
            "not_ordered_since": not_ordered_since_months or "",
            "phone": phone_filter or "",
            "tags": tags_filter,
            "contacted": contacted_filter,
        },
        "all_tags": tag_display_mapping,
        "tag_descriptions": tag_descriptions,
        "kpis": {
            "total_customers": f"{total_customers:,.0f}",
            "avg_ltv": f"{avg_ltv:,.2f}",
            "total_customers_filtered": total_customers_filtered,
            "total_ltv": f"{total_ltv:,.2f}",
        },
    }

    return render(request, "Retention/retention_dashboard.html", context)


'''@csrf_exempt
@require_POST
def order_update_webhook(request):
    try:
        payload = json.loads(request.body)
        order = payload.get("order", {})
        order_status = order.get("order_status", {}).get("name", "").lower()
        logger.info(f"[Webhook Customer] Payload: {payload}")
        logger.info(f"[Webhook Customer] Received order.update webhook for Order Status: {order_status}")


        # We only care about cancellations
        if "cancel" not in order_status and "مسترجع" not in order_status and "تم الإلغاء" not in order_status:
            return JsonResponse({'status': 'skipped', 'message': 'Order status is not cancellation.'})

        customer_data = order.get("customer", {})
        customer_id = customer_data.get("id")
        order_total = float(order.get("order_total", 0.0))

        if not customer_id:
            return JsonResponse({'status': 'error', 'message': 'Customer ID missing in payload'}, status=400)

        # Fetch the customer
        # customer_res = supabase.table("____").select("*").eq("Customer_ID", customer_id).execute()
        customer_res = {}

        if not customer_res.data:
            return JsonResponse({'status': 'skipped', 'message': f'Customer {customer_id} not found'}, status=200)

        customer_record = customer_res.data[0]

        # Safely decrement values, ensuring they don't go below zero
        current_purchases = int(customer_record.get("Purchases") or 0)
        current_orders_total = float(customer_record.get("Customer_Orders_Total") or 0.0)

        update_payload = {
            "Purchases": max(0, current_purchases - 1),
            "Customer_Orders_Total": max(0.0, current_orders_total - order_total),
            "Last_Updated": get_uae_current_date()
        }

        # Update the record in Supabase
        # supabase.table("____").update(update_payload).eq("Customer_ID", customer_id).execute()

        return JsonResponse({'status': 'success', 'message': f'Customer {customer_id} updated for cancelled order.'})

    except Exception as e:
        traceback.print_exc()
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)

@csrf_exempt
@require_POST
def customer_create_webhook(request):
    # This webhook can be used in the future to create a new entry
    # in your Customer_Tracking_duplicate table if one doesn't exist.
    # For now, we'll just acknowledge it.
    try:
        payload = json.loads(request.body)
        customer_id = payload.get("customer", {}).get("id")
        logger.info(f"[Webhook Customer] Payload: {payload}")
        logger.info(f"[Webhook Customer] Received customer.create event for Customer ID: {customer_id}")
        return JsonResponse({'status': 'received'})
    except Exception as e:
        traceback.print_exc()
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)'''
