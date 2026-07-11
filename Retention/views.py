from django.shortcuts import render
from django.http import JsonResponse
import json, os, traceback
from supabase import create_client, Client
import logging
from Demo.supporting_files.supabase_functions import get_next_id_from_supabase_compatible_all
from Demo.supporting_files.supporting_functions import get_uae_current_date
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

# Create your views here.
url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)

logger = logging.getLogger('Retention')
########################################################################### Retention webhooks 
@csrf_exempt
@require_POST
def order_create_webhook(request):
    try:
        payload = json.loads(request.body)
        order = payload.get("order", {})
        customer_data = order.get("customer", {})
        customer_id = customer_data.get("id")
        order_total = float(order.get("total", 0.0))
        logger.info(f"[Webhook Customer] Payload: {payload}")
        logger.info(f"[Webhook Customer] Received order.create webhook for Customer ID: {customer_id}, Order Total: {order_total}")

        if not customer_id:
            return JsonResponse({'status': 'error', 'message': 'Customer ID missing in payload'}, status=400)

        # Fetch the customer from your tracking table
        # customer_res = supabase.table("____").select("*").eq("Customer_ID", customer_id).execute()
        customer_res = {}
        if not customer_res.data:
            # Customer does not exist, create a new one.
            new_customer_payload = {
                "Distinct_ID": int(get_next_id_from_supabase_compatible_all(name='____', column='Distinct_ID')),
                "Customer_ID": customer_id,
                "Customer_Info": {
                    "name": customer_data.get("name"),
                    "email": customer_data.get("email"),
                    "mobile": customer_data.get("mobile")
                },
                "Purchases": 1,
                "Customer_Orders_Total": order_total,
                "Last_Updated": get_uae_current_date(),
                "Is_Anonymous": False,
            }
            # supabase.table("____").insert(new_customer_payload).execute()
            return JsonResponse({'status': 'success', 'message': f'New customer {customer_id} created and order recorded.'})

        customer_record = customer_res.data[0]
        
        # Safely increment purchases and LTV
        current_purchases = int(customer_record.get("Purchases") or 0)
        current_orders_total = float(customer_record.get("Customer_Orders_Total") or 0.0)

        update_payload = {
            "Purchases": current_purchases + 1,
            "Customer_Orders_Total": current_orders_total + order_total,
            "Last_Updated": get_uae_current_date()
        }

        # Update the record in Supabase
        # supabase.table("____").update(update_payload).eq("Customer_ID", customer_id).execute()

        return JsonResponse({'status': 'success', 'message': f'Customer {customer_id} updated for new order.'})

    except Exception as e:
        traceback.print_exc()
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


@csrf_exempt
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
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
