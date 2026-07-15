import pandas as pd
from datetime import datetime
from Retention.supporting_pys.database_functions import *
from Retention.supporting_pys.supporting_functions import *
# from Retention.supporting_pys.retention_functions import * 

def get_customer_tags(order_count, ltv, ltv_averaged, orders_dict):
    """
    Generates a list of tags for a customer based on their metrics.
    """
    tags = []

    # --- Recency Tags ---
    last_purchase_date = None
    from datetime import datetime

    # Initialize to prevent NameError if dict is empty or parsing fails
    last_purchase_date = None

    if orders_dict:
        try:
            latest_order = max(
                orders_dict.values(), 
                key=lambda o: o.get('added_at') or '1970-01-01 00:00:00'
            )
            
            added_at = latest_order.get('added_at')
            if added_at:
                date_str = added_at.replace('T', ' ').split()[0]
                last_purchase_date = datetime.fromisoformat(date_str)
                
        except (ValueError, TypeError, IndexError):
            last_purchase_date = None

    if last_purchase_date:
        days_since_last_purchase = (datetime.now() - last_purchase_date).days

        if order_count > 1:
            if days_since_last_purchase <= 90:
                tags.append('active')
        
        if days_since_last_purchase > 90:
            tags.append('inactive')
        if days_since_last_purchase > 180:
            tags.append('at_risk')
        if days_since_last_purchase > 365:
            tags.append('lost')

    # --- Order Count Tags ---
    if order_count == 1:
        tags.append('orders_1')
        tags.append('First-time customer')
    elif order_count == 2:
        tags.append('orders_2')
    elif order_count == 3:
        tags.append('orders_3')
    elif order_count == 4:
        tags.append('orders_4')
    elif order_count >= 5:
        tags.append('orders_5_plus')

    # --- LTV Tag ---
    if ltv > 1000:
        tags.append('vip')

    # --- AOV Tags ---
    if 100 <= ltv_averaged <= 300:
        tags.append('aov_100_300')
    elif 400 <= ltv_averaged <= 600:
        tags.append('aov_400_600')
    elif 700 <= ltv_averaged <= 900:
        tags.append('aov_700_900')
    elif 1000 <= ltv_averaged <= 2000:
        tags.append('aov_1000_2000')
    elif ltv_averaged > 2000:
        tags.append('aov_2000_plus')

    return list(set(tags))

## Next is the funciton to call whenever an order is coming in to update the customers database as well -- Match on the phone number if it exists update the orders + the order count and other relevant fields
## If the customer doesnt exist, create an entry --- 
def update_customers_db(customer_id, customer_name, customer_mobile, 
                        order_id, products_dict, added_at, utm_source, order_total):

    """
    This funciton takes in the necessary info to either create an entry
    of a customer or update an existing one --
    """

    customer_df = pd.DataFrame()
    # Only search by customer_id if it's a valid, non-zero ID
    if customer_id and int(customer_id) != 0:
        customer_df = fetch_data_from_supabase_specific(
            "Store_Customers",
            filters={"Customer_ID": ("eq", customer_id)}
        )

    ## If not found, try fetching using the customer mobile
    if customer_df.empty and customer_mobile:

        customer_df = fetch_data_from_supabase_specific(
            "Store_Customers",
            filters={
                "Customer_Mobile": ("eq", customer_mobile)
            }
        )


    ## Customer already exists
    if not customer_df.empty:

        row = customer_df.iloc[0]

        distinct_id = row["Distinct_ID"]

        products = row["Products"]
        orders = row["Orders"]

        ## Safety functions
        if not isinstance(products, dict):
            products = {}

        if not isinstance(orders, dict):
            orders = {}


        ## Update Products
        for sku, details in products_dict.items():

            product_name = details["product_name"]
            quantity = int(details["quantity"])

            if sku not in products:

                products[sku] = {
                    "product_name": product_name,
                    "quantity": quantity
                }

            else:

                products[sku]["quantity"] += quantity


        ## Update Orders
        orders[str(order_id)] = {
            "added_at": added_at,
            "products": products_dict
        }

        order_count = len(orders)

        ## Update the customer's LTV
        current_ltv = float(row["Customer_Lifetime_Value"] or 0)
        new_ltv = current_ltv + float(order_total or 0)
        ltv_averaged = new_ltv / order_count if order_count else 0

        ## Update the existing row
        customer_df.at[customer_df.index[0], "Customer_Name"] = customer_name
        customer_df.at[customer_df.index[0], "Customer_Mobile"] = customer_mobile

        customer_df.at[customer_df.index[0], "Order_Count"] = order_count

        customer_df.at[customer_df.index[0], "Customer_Lifetime_Value"] = new_ltv
        customer_df.at[customer_df.index[0], "LTV_Averaged"] = ltv_averaged

        customer_df.at[customer_df.index[0], "Products"] = products
        
        tags = get_customer_tags(order_count, new_ltv, ltv_averaged, orders)
        customer_df.at[customer_df.index[0], "Tags_List"] = tags
        customer_df.at[customer_df.index[0], "Orders"] = orders

        customer_df.at[customer_df.index[0], "Hook_Source"] = utm_source
        customer_df.at[customer_df.index[0], "Last_Updated"] = get_uae_current_date()

        ## Partially upsert the updated row
        upsert_partial(customer_df, "Store_Customers", "Distinct_ID")

        return True


    ## New Customer
    else:

        distinct_id = int(
            get_next_id_from_supabase_compatible_all(
                name="Store_Customers",
                column="Distinct_ID"
            )
        )

        products = {}

        for sku, details in products_dict.items():

            products[sku] = {
                "product_name": details["product_name"],
                "quantity": int(details["quantity"])
            }

        orders = {

            str(order_id): {

                "added_at": added_at,
                "products": products_dict

            }

        }

        row = pd.DataFrame([{

            "Distinct_ID": distinct_id,

            "Customer_ID": customer_id,
            "Customer_Name": customer_name,
            "Customer_Mobile": customer_mobile,

            "Order_Count": 1,

            "Customer_Lifetime_Value": float(order_total or 0),

            "Products": products,
            "Orders": orders,

            "LTV_Averaged": float(order_total or 0),
            "Hook_Source": utm_source,            
            "Tags_List": get_customer_tags(1, float(order_total or 0), float(order_total or 0), orders),
            "Last_Updated": get_uae_current_date()

        }])

        batch_insert_to_supabase(
            row,
            "Store_Customers"
        )

        return True