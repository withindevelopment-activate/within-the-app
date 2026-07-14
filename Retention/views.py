from django.shortcuts import render
import json
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

    rows = []

    products = payload.get("products", [])

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

    customer_note = payload.get("customer_note")

    utm_source = utm.get("source")

    ## Get the distinct_id
    distinct_id = int(get_next_id_from_supabase_compatible_all(
                name='All_ZID_Orders', column='Distinct_ID'
            ))
    
    ## Initialize a dictionary to store the products + their quans
    products_dict = {}
    ## We loop thru the products because an order has multiple orders = multiple rows
    for product in products:
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

    rows_df["region"] = region

    ## Dates
    added_at = dubai_time(payload.get("created_at"))
    rows_df["added_at (Asia/Dubai)"] = added_at
    rows_df["last_update_at (Asia/Dubai)"] = dubai_time(payload.get("updated_at"))

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
    batch_insert_to_supabase(rows_df, "All_ZID_Orders")

    ## Update the customers_db
    verdict = update_customers_db(customer_id, customer_name, customer_mobile, order_id, products_dict, added_at, utm_source, order_total)

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

    # Base query from the new customer tracking table
    query = supabase.table("Store_Customers").select("*").order("Last_Updated", desc=True)

    # Apply Supabase-level filters for efficiency
    if phone_filter:
        query = query.like("Customer_Mobile", f"%{phone_filter}%")
    
    if order_count_filter:
        query = query.eq("Order_Count", int(order_count_filter))

    # Execute query to get all data for filtering in pandas
    response = query.execute()
    if not response.data:
        return render(request, "Retention/retention_dashboard.html", {"customers": [], "row_count": 0})

    df = pd.DataFrame(response.data)

    # Ensure numeric types for calculations and display
    numeric_cols = ["Order_Count", "Customer_Lifetime_Value"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df["Order_Count"] = df["Order_Count"].astype(int)

    # Convert date columns for filtering
    # Convert Last_Visit to datetime for filtering
    df['Last_Visit'] = pd.to_datetime(df['Last_Visit'], errors='coerce')

    if order_date_filter:
        filter_date = pd.to_datetime(order_date_filter).date()
        df = df[df["Last_Visit"].dt.date == filter_date]

    if not_ordered_since_months:
        try:
            months = int(not_ordered_since_months)
            if months > 0:
                cutoff_date = datetime.now() - timedelta(days=months * 30)
                # Filter out customers who have a last visit date more recent than the cutoff
                df = df[df['Last_Visit'] < cutoff_date]
        except (ValueError, TypeError):
            pass # Ignore if not a valid number

    is_filtered = any([order_count_filter, order_date_filter, not_ordered_since_months, phone_filter])

    # Default limit if no filters are applied
    if not is_filtered and limit:
        df = df.head(int(limit))
    elif not is_filtered:
        df = df.head(20) # Default to 20 if no limit and no filters

    customers = df.to_dict(orient="records")

    # Clean up data for template
    for customer in customers:
        customer['Customer_Name'] = customer.get('Customer_Name') or 'N/A'
        customer['Customer_Mobile'] = customer.get('Customer_Mobile') or ''

    context = {
        "customers": customers,
        "row_count": len(customers),
        "is_filtered": is_filtered,
        "filters": {
            "limit": limit or "20",
            "order_count": order_count_filter,
            "order_date": order_date_filter,
            "not_ordered_since": not_ordered_since_months,
            "phone": phone_filter,
        }
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
