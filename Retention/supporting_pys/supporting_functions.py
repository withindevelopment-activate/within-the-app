from datetime import datetime
import pytz
import pandas as pd

## Import the database functions
from Retention.supporting_pys.database_functions import *


def get_uae_current_date():
    # Define the UAE timezone
    uae_timezone = pytz.timezone('Asia/Dubai')
    
    # Get the current time in the UAE timezone
    now_uae = datetime.now(uae_timezone)
    
    # Format the current date and time
    current_date = now_uae.strftime('%Y-%m-%d %H:%M:%S')
    
    return current_date

def lifetime_repurchase_rate():
    """
    This function finds the lifetime repurchase rate by calling the database of all the orders + The customers database
    then, performs operations and returns the final rate. 

    """
    '''
    ## Note -- Actually, we don't need the all orders because the current 'Store_Customers' table's structure should be enough to get the repurchase rate

    ## Calling all the orders database
    ## Since the database is desgined to have the status on all order rows, the filtering is now easier -- as in, call only orders with relevant statuses
    all_orders = fetch_data_from_supabase_specific("All_ZID_Orders", 
                                                   filters = {
                                                       'order_status': ('not_in', ['مسترجع', 'قيد الاسترجاع', 'تم الإلغاء']),
                                                       'source': ('not_in', ['نقاط بيع زد']),
                                                       'currency': ('not_in', ['OMR', 'QAR']),
                                                       'pos_inventory_location': ('not_in', ['OM'])
                                                   },
                                                   columns = ['customer_note', 'id', 'customer_mobile', 'added_at (Asia/Dubai)', 'source']
                                                   )

    ## Filter for only the CS New Orders -- Now that every order row has the order's customer_note, we can just directly filter for the new CS orders -- Simpler version than the one in the finding_repurchse_rate.py
    all_orders = all_orders[
    (all_orders['source'] != 'لوحة التحكم') |
    (
        (all_orders['source'] == 'لوحة التحكم') &
        (all_orders['customer_note'].str.contains('جديد -', na=False))
    )]

    ## Now onto processing the customers and finding the rate
    store_customers = fetch_data_from_supabase_specific("Store_Customers", 
                                                        filters = {
                                                            ''
                                                        })'''

    ## Finding the repurchase rate by solely relying on the 'Store_Customers'

    ## Get the store customers
    customers_df = fetch_data_from_supabase("Store_Customers")


    customers = customers_df.copy()

    customers["Order_Count"] = (
        pd.to_numeric(customers["Order_Count"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    total_customers = len(customers)

    repeat_customers = (
        customers["Order_Count"] >= 2
    ).sum()

    new_customers = (
        customers["Order_Count"] == 1
    ).sum()

    repurchase_rate = (
        repeat_customers / total_customers * 100
        if total_customers else 0
    )

    print("-------- LIFETIME REPURCHASE RATE --------")
    print(f"Total customers: {total_customers:,}")
    print(f"New customers (1 order): {new_customers:,}")
    print(f"Repeat customers (2+ orders): {repeat_customers:,}")
    print(f"Repurchase Rate: {repurchase_rate:.2f}%")

    return {
        "total_customers": total_customers,
        "new_customers": new_customers,
        "repeat_customers": repeat_customers,
        "repurchase_rate": repurchase_rate
    }
