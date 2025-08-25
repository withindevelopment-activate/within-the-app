import pandas as pd
from .supabase_functions import fetch_data_from_supabase
import ast
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from io import BytesIO


##### Supporting Functions
def column_check(file, file_index):
    # This function stores the files in a variable is well and returns them
    response = False
    message = None
    file_1, file_2, file_3, file_4, file_5, file_6 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        file_data = file.read()
        if file_index == 4:  # Google file case
            # Use the third row (index 2) as the column names
            df = pd.read_excel(BytesIO(file_data), engine='openpyxl', header=2)  # Set header row to the 3rd row
        elif file_index == 6: # Google Analytics
            # start from row 10 (index 9)
            df = pd.read_excel(BytesIO(file_data), engine='openpyxl', header=9)  # Set header row to the 10th row
        else:
            df = pd.read_excel(BytesIO(file_data), engine='openpyxl')  # Default for other files

        if file_index == 1:  # Facebook
            required_column = "Purchase ROAS (return on ad spend)"
        elif file_index == 2:  # Tiktok
            required_column = "Payment completion ROAS (website)"
        elif file_index == 3:  # Snapchat
            required_column = "Amount Spent"
        elif file_index == 4: # Google ADS
            required_column = 'Cost / conv.'
        elif file_index == 5:  # Zid
            required_column = "id"
        elif file_index == 6: # Google Analytics
            required_column = "Key events"

        # checking for required columns
        if required_column not in df.columns:
            message = f"Please upload a valid file for {required_column}."
            return message, response, file_1, file_2, file_3, file_4, file_5, file_6
        
        # Save the dfs
        if file_index == 1:
            file_1 = df
        elif file_index == 2:
            file_2 = df
        elif file_index == 3:
            file_3 = df
        elif file_index == 4:
            file_4 = df
        elif file_index == 5:
            file_5 = df
        elif file_index == 6:
            file_6 = df

        print(f"{required_column} found and file processed successfully.")
        response = True
        return message, response, file_1, file_2, file_3, file_4, file_5, file_6
    
    except Exception as e:
        message = f"Error processing {file.filename}: {str(e)}"
        return message, response, file_1, file_2, file_3, file_4, file_5, file_6
    

def create_entry(amount_spent, overall_roas, orders_num, cpa, sales, platform_df, platform):
    row = {
        'Platform': platform,
        'Amount_Spent': amount_spent,
        'ROAS': overall_roas,
        'Number_of_Purchases': orders_num,
        'CPA': cpa,
        'Total_Sales': sales
    }

    row_df = pd.DataFrame([row])

    # Add to the df
    platform_df = pd.concat([platform_df, row_df], ignore_index=True)
    # Return the df
    return platform_df

def get_influencer_budget(start_time, end_time):
    # Convert the dates to a date object
    start_date = pd.to_datetime(start_time)
    end_date = pd.to_datetime(end_time)
    # Get the funding requests
    requests_table = fetch_data_from_supabase("Funding_Requests")
    requests_table['Purpose'] = requests_table['Purpose'].str.strip().astype(str)
    requests_table['Type'] = requests_table['Type'].str.strip().astype(str)
    requests_table['Total_Amount'] = requests_table['Total_Amount'].replace('', 0).fillna(0).astype(int)
    requests_table['Distinct_ID'] = requests_table['Distinct_ID'].astype(int)
    requests_table['Status'] = requests_table['Status'].str.strip().astype(str)
    requests_table['Influencer_Region'] = requests_table['Influencer_Region'].str.strip().astype(str)
    # Split the 'Last_Updated' column to separate the date and time
    requests_table['AD_Date'] = requests_table['AD_Date'].str.split('T').str[0]

    # Fill in empties before converting to a datetime
    requests_table['AD_Date'] = requests_table['AD_Date'].replace('', pd.NaT)

    # Convert to a date format
    requests_table['AD_Date'] = pd.to_datetime(requests_table['AD_Date'], errors='coerce')
    # Filter for the influencers
    '''requests_table = requests_table[(requests_table['Purpose'] == 'الاعلانات') & 
                                    (requests_table['Type'] == 'مشاهير') &
                                    (requests_table['Status'] == 'Approved')]'''

    requests_table = requests_table[((requests_table['Purpose'] == 'الاعلانات') & 
                                    (requests_table['Type'] == 'مشاهير') &
                                    (requests_table['Influencer_Region'] != 'OM'))]
    # Now filter that for the date
    filtered_df = requests_table[(requests_table['AD_Date'] >= start_date) & (requests_table['AD_Date'] <= end_date)]

    # Now if the filtered df is empty, send in the amount to be 0 if not sum
    if filtered_df.empty:
        influencers = 0
    else:
        influencers = filtered_df['Total_Amount'].sum()
    return influencers

def breakage(sku, vanilla_df, zid, name_in_ad):
    """
    Handles both a single SKU (string) or multiple SKUs (list).
    Returns:
        total_variations (int): sum of all quantities
        all_variations (list): combined list of all variation SKUs
        total_occurrences_variation (int): sum of all orders across variations
    """

    # --- Case 1: sku is a list ---
    if isinstance(sku, list):
        grand_total_variations = 0
        grand_variations_list = []
        grand_total_occurrences = 0

        for single_sku in sku:
            print("The single sku is:", single_sku)
            total_variations, variations_list, total_occurrences_variation = breakage(single_sku, vanilla_df, zid, name_in_ad)
            
            grand_total_variations += total_variations
            grand_variations_list.extend(variations_list)
            grand_total_occurrences += total_occurrences_variation

        return grand_total_variations, grand_variations_list, grand_total_occurrences

    # --- Case 2: sku is a single value ---
    product_row = vanilla_df.loc[vanilla_df['SKU'] == sku]
    if product_row.empty:
        return 0, [], 0  # nothing to process if sku not found

    # Extract variations list
    try:
        variations_list = ast.literal_eval(product_row['Variations'].values[0])
    except Exception:
        return 0, [], 0  # safe fallback if variations are malformed

    # Normalize variation SKUs
    variations_list = [str(v).strip().strip("'") for v in variations_list]

    # Initialize counters
    total_variations = 0
    total_occurrences_variation = 0

    # Loop through variations
    for variation in variations_list:
        variation_df = zid[zid['sku'] == variation]
        if variation_df.empty:
            continue

        # Handle multiple product names
        distinct_names = variation_df['product name'].str.strip().astype(str).dropna().unique()
        if len(distinct_names) > 1:
            filtered_df = variation_df[variation_df['product name'].str.contains(name_in_ad, case=False, na=False)]
            if filtered_df.empty:
                print(f"No entries contain '{name_in_ad}' in product name for variation {variation}. Skipping.")
                continue
            else:
                variation_df = filtered_df

        # Quantities + occurrences
        total_quantity_variation = variation_df['quantity'].sum()
        occurrence_count = len(variation_df)

        total_variations += total_quantity_variation
        total_occurrences_variation += occurrence_count

    return total_variations, variations_list, total_occurrences_variation

async def fetch_all_h1_tags(urls, analytics, sleepy_website):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url_2(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

    # Map H1 tags to URLs (filtering out None values)
    h1_tag_map = {url: h1_text for url, h1_text in results if h1_text != "404"}
    if h1_tag_map:
        # Create the DataFrame with the list of URLs and corresponding H1 tags
        url_h1_df = pd.DataFrame({
            'URL': list(h1_tag_map.keys()),  # List of URLs
            'H1 Tag': list(h1_tag_map.values()),  # Corresponding H1 Tags
            'Active Users': [
                analytics.loc[analytics['Page path and screen class'] == url.replace(sleepy_website, ''), 'Active users'].values[0]
                if len(analytics.loc[analytics['Page path and screen class'] == url.replace(sleepy_website, '')]) > 0
                else 0
                for url in h1_tag_map.keys()
            ],
            'Adds to Cart': [
                analytics.loc[analytics['Page path and screen class'] == url.replace(sleepy_website, ''), 'Add to carts'].values[0]
                if len(analytics.loc[analytics['Page path and screen class'] == url.replace(sleepy_website, '')]) > 0
                else 0
                for url in h1_tag_map.keys()
            ]
        })


    return url_h1_df

async def fetch_url_2(session, url):
    """Fetch URL asynchronously and return H1 tag or '404' on failure"""
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                soup = BeautifulSoup(await response.text(), 'html.parser')
                h1_tag = soup.find('h1')
                return url, h1_tag.text.strip() if h1_tag else None
            else:
                print(f"Skipping {url} - Received status {response.status}")
                return url, "404"  # Return '404' for failed URLs
    except Exception as e:
        print(f"Exception fetching {url}: {e}")
        return url, "404"


def match_variations_with_h1(vanilla_db, h1_tag_map, combined_products_df, package_skus):
    result_data = []
    all_names = []
    name_variations = {}
    # Track used rows
    used_rows = set()

    # A bit of cleaning
    h1_tag_map['H1 Tag'] = h1_tag_map['H1 Tag'].str.strip().astype(str)
    # vanilla_db
    vanilla_db['Product Name'] = vanilla_db['Product Name'].str.strip().astype(str)
    vanilla_db['Name_in_AD'] = vanilla_db['Name_in_AD'].str.strip().astype(str)
    # Loop through the vanilla products to get product names
    for _, row in vanilla_db.iterrows():
        total_active_users = 0
        total_adds_to_cart = 0
        variation_names = []
        prod_name = row['Name_in_AD']

        variations_list = ast.literal_eval(row['Variations'])
        variations_list = [str(sku).strip().strip("'") for sku in variations_list]

        # Extract variation names
        for variation in variations_list:
            if variation.startswith('P'):
                var_name = package_skus.loc[package_skus['Indication'] == variation, 'Package Name'].values
            else:
                var_name = combined_products_df.loc[combined_products_df['SKU'] == variation, 'Product Name'].values

            variation_names.append(var_name[0] if var_name.size > 0 else 'Error Fetching Name')
            # Add it to the dictionary with its product name in the vanilla db
            name_variations[prod_name] = variation_names

        # Adding to the mega list
        all_names.extend(variation_names)

        # Match variation names with H1 tags from the pre-fetched map
        for url, h1_row in h1_tag_map.iterrows():
            # Get the row index
            row_index = h1_row.name
            # Check if its used
            if row_index in used_rows:
                continue

            h1_text = str(h1_row['H1 Tag']).strip()
            # Make it lower for unbiased search
            h1_lower = h1_text.lower()
            h1_words = h1_text.lower().split()

            ###
            for name in variation_names:
                clean_name = str(name).strip()
                name_lower = clean_name.lower()
                name_words = name_lower.split()
                # Look for exact matches
                if clean_name == h1_text:
                    print(f"Exact match: {h1_text} == {clean_name}")
                    total_active_users += h1_row['Active Users']
                    total_adds_to_cart += h1_row['Adds to Cart']
                    used_rows.add(row_index)
                    break  # -- stop checking for other vairaitons for this H1 Tag
                    
                # Partial match (all words in clean_name appear in h1_text) -- this is made because we might have the name of the product without any specifications so the words in the h1 text is going to be in the name.
                # لباد سليبي كلاود 14 سم قطن 100% id going to be in لباد سليبي كلاود 14 سم قطن 100% - 140x200
                '''elif all(word in name_lower for word in h1_words):
                    print(f"Partial match: All words in '{h1_text}' found in variation '{clean_name}'")
                    total_active_users += h1_row['Active Users']
                    total_adds_to_cart += h1_row['Adds to Cart']
                    used_rows.add(row_index)
                    continue'''

        result_data.append({
            'Product Name': row['Name_in_AD'],
            'Active Users': total_active_users,
            'Adds to Cart': total_adds_to_cart
        })

        # PART 2 -- CHCEKING FOR THE PAGES WHERE THE H1 TAG IS THE MAIN NAME OF THE PRODUCT AND NOT ONE OF THE VARIATIONS
        # EXAMPLE لباد سليبي كلاود 14 سم قطن 100% id going to be in لباد سليبي كلاود 14 سم قطن 100% - 140x200
        # Step 2: Fallback (match unused H1s to first variation that contains all H1 words)
        unused_h1s = h1_tag_map[~h1_tag_map.index.isin(used_rows)].copy()
        # Further filter
        # Further filter
        filtered_df = unused_h1s[
            ~unused_h1s['H1 Tag'].str.contains('200', na=False) &
            ~unused_h1s['H1 Tag'].str.contains('المقاسات المفردة', na=False) &
            ~unused_h1s['URL'].str.contains('reviews', case=False, na=False)
        ]
        fallback_results = []

        for _, row in filtered_df.iterrows():
            h1_text = str(row['H1 Tag']).strip().lower()
            matched = False

            for product_name in name_variations.keys():
                product_name_lower = product_name.lower().strip()
                # To filter out the memory foam MT
                if product_name_lower == 'لباد':
                    product_name_lower = 'لباد سليبي كلاود 14 سم قطن 100%'

                # Exact match
                if h1_text == product_name_lower:
                    print(f"Exact match: H1 '{h1_text}' == Product Name '{product_name}'")
                    fallback_results.append({
                        'Product Name': product_name,
                        'H1 Tag': row['H1 Tag'],
                        'Active Users': row['Active Users'],
                        'Adds to Cart': row['Adds to Cart']
                    })
                    matched = True
                    break

                # Sub match
                elif product_name_lower in h1_text:
                    print(f"Partial match: Product Name '{product_name}' found in H1 '{h1_text}'")
                    fallback_results.append({
                        'Product Name': product_name,
                        'H1 Tag': row['H1 Tag'],
                        'Active Users': row['Active Users'],
                        'Adds to Cart': row['Adds to Cart']
                    })
                    matched = True
                    break

    # The df from the result
    final_df = pd.DataFrame(result_data)
    # The fallbacks -- 
    fallback_df = pd.DataFrame(fallback_results)
    # Concat 
    final = pd.concat([final_df, fallback_df])
    
    return fallback_df



### Main Functions ######
#########################
#########################
def create_general_analysis(file_dictionary, start_time, end_time):
    facebook = file_dictionary.get(1, pd.DataFrame())
    tiktok = file_dictionary.get(2, pd.DataFrame())
    snapchat = file_dictionary.get(3, pd.DataFrame())
    google = file_dictionary.get(4, pd.DataFrame())
    zid = file_dictionary.get(5, pd.DataFrame())
    analytics =  file_dictionary.get(6, pd.DataFrame())

    # Removing the last row since its a total row - miss this and its all broken lolz
    tiktok = tiktok.drop(tiktok.index[-1])

    total_amount_spent = 0

    ################# Shway data cleaning
    # Snapchat
    # Let's just go an extra step and filter out the Campaigns where عمان is present
    snapchat = snapchat[~snapchat['Campaign Name'].str.contains('عمان', na=False)]

    # Zid
    zid['id'] = zid['id'].astype(int)
    zid['sku'] = zid['sku'].str.strip().astype(str)
    zid['product name'] = zid['product name'].str.strip().astype(str)
    zid['customer_note'] = zid['customer_note'].str.strip().astype(str)
    zid['payment_method'] = zid['payment_method'].str.strip().astype(str)
    zid['order_status'] = zid['order_status'].str.strip().astype(str)
    zid['source'] = zid['source'].str.strip().astype(str)

    # Remove the exceptions from the product name
    # Remove the cancelled orders and other exceptions
    exceptions = ['رسوم استبدال', 'رسوم مندوب', 'رسوم مندوب خاص']
    zid = zid[~((zid['product name'].str.contains('رسوم', na=False)) & (zid['customer_name'].isna()))]

    # Make a copy of the unfiltered zid to send back
    to_remove = ['قيد الاسترجاع', 'مسترجع']
    source_to_remove = ['لوحة التحكم']
    zid_with_cancelled_only = zid[zid['order_status'].isin(to_remove)]['id']
    zid_cs_orders_ids = zid[zid['source'].isin(source_to_remove)]['id']
    zid_unfiltered = zid[~(zid['id'].isin(zid_with_cancelled_only) | 
                       zid['id'].isin(zid_cs_orders_ids))]
    # Only keep the orders from the website and not by the cusotmer service
    # Let's get the product orders through the website and the conversion rates

    # ZID orders
    # Cancelled orders
    exceptions_status = ['تم الإلغاء', 'قيد الاسترجاع', 'مسترجع']
    order_ids_to_remove = zid[zid['order_status'].isin(exceptions_status)]['id']
    # Filter out these order IDs from the DataFrame
    zid = zid[~zid['id'].isin(order_ids_to_remove)]
    ############## Cleaning fin~


    # Get the whole breakdown of the order sources
    def get_source_specifications(zid, total_sales):
        # Here I want to drop the dupes but keep the ones where the payment_method != nan since first is not necessarily the one with all the fields populated
        zid['payment_method_filled'] = zid['payment_method'] != 'nan'
        # sort based on the newly created row
        zid = zid.sort_values(by=['payment_method_filled'], ascending=False)
        # drop and keep first occrrences
        unique_orders = zid.drop_duplicates(subset='id', keep='first')
        unique_orders = unique_orders.drop(columns=['payment_method_filled'])
        # Find the cash on delivery
        cash_on_delivery_orders = unique_orders[unique_orders['payment_method'] == 'دفع عند الاستلام']
        # Get their number
        num_cash_on_delivery_orders = cash_on_delivery_orders.shape[0]

        #### Get number breakdowns based on source
        # OOOkaaaay let's go. Here is where all the shit goes down - season 1.
        # First if the source was 'الموقع الالكتروني' just normally take the number of tap and tabby. However, if the source is 'لوحة التحكم' then to get the breakdown of tap and tabby I need to refer to the customer notes.
        # Part 1 -> get the payment method breakdown if the source == 'الموقع الالكتروني' --> use the df with only the unique entries 'id' wise
        website = unique_orders[unique_orders['source'] == 'المتجر الإلكتروني']
        # Get the value count for each unique entry in the 'payment_method' column
        payment_method_counts_website = website['payment_method'].value_counts()
        # Sum up the counts for 'بطاقة إئتمانية' and 'تحويل بنكي' --> all under tap payments
        tap_payment_count = payment_method_counts_website.get('بطاقة إئتمانية', 0) + payment_method_counts_website.get('تحويل بنكي', 0)
        # Remove 'بطاقة إئتمانية' and 'تحويل بنكي' from the Series
        payment_method_counts_website = payment_method_counts_website.drop(['بطاقة إئتمانية', 'تحويل بنكي'], errors='ignore')
        # Add the combined count as 'Tap_Payment_Website'
        payment_method_counts_website['Tap_Payment_Website'] = tap_payment_count
        # Tabby count
        tabby_website = website[website['payment_method'] == 'items.tabby']
        tabby_website_count = len(tabby_website)
        # COD orders - website
        cod_website = website[website['payment_method'] == 'دفع عند الاستلام']
        cod_website_count = len(cod_website)

        # Source 2 --> Customer Service
        # Part 2 --> get the breakdown from when the source == لوحة التحكم
        keyword = 'جديد'
        customer_service_new = unique_orders[
            (unique_orders['source'] == 'لوحة التحكم') &
            (unique_orders['customer_note'].str.contains(keyword, na=False))
            ]
        
        customer_service = unique_orders[(unique_orders['source'] == 'لوحة التحكم')]
        # Get the payment metho breakdown for the COD because it's straight forward.
        cod_customer_service = customer_service[customer_service['payment_method'] == 'دفع عند الاستلام']
        cod_customer_service_count = len(cod_customer_service)

        # Onto breaking down the Tap and Tabby payment methods. First we filter for the 'تحويل بنكي'
        not_cod_cs = customer_service[customer_service['payment_method'] == 'تحويل بنكي']
        tabby_keywords = ['تابي', 'مدفوع تابي', 'Tabby']
        tap_keywords = ['تحويل بنكي', 'تاب', 'Tap', 'TAP', 'ابل باي', 'مدفوع بطاقة إئتمانية', 'VISA', 'تحويل', 'tap']
        # First check the customer note for one of these keywords if nothing refer to the payment method and if it is "Bank Transfer" then it's Tap
        # if one has keywords from both the lists, it's Tabby

        def determine_payment_type(notes, tabby_keywords, tap_keywords):
            # Check for Tabby keywords
            is_tabby = any(keyword in notes for keyword in tabby_keywords)
            # Check for Tap keywords
            is_tap = any(keyword in notes for keyword in tap_keywords)
            
            # Determine payment type
            if is_tabby:
                return 'Tabby'
            elif is_tap:
                return 'Tap'
            else:
                return 'Tap'  # Default to Tap if no keywords are found

        # Apply the function to the 'customer_notes' column
        not_cod_cs['payment_method'] = not_cod_cs['customer_note'].fillna('').apply(
            lambda x: determine_payment_type(x, tabby_keywords, tap_keywords)
        )

        # View results
        tabby_payments_cs = not_cod_cs[not_cod_cs['payment_method'] == 'Tabby']
        tap_payments_cs = not_cod_cs[not_cod_cs['payment_method'] == 'Tap']
        tabby_count_cs = len(tabby_payments_cs)
        tap_count_cs = len(tap_payments_cs)

        # To find out the total, sum the found counts - total counts from both website and cs - from both sources
        total_tap_count = tap_payment_count + tap_count_cs
        total_tabby_count = tabby_count_cs + tabby_website_count
        cod_total = num_cash_on_delivery_orders

    
        # Take the unqiue entries in the 'id' column
        total_orders_zid = zid['id'].astype(int).nunique()
        # Get the order percentages
        website_percentage = f"{round((len(website) / total_orders_zid) * 100, 2)}%"
        cs_percentage = f"{round((len(customer_service_new) / total_orders_zid) * 100, 2)}%"
        cs_percentage_all = f"{round((len(customer_service) / total_orders_zid) * 100, 2)}%"
        # Percentages - Website
        # COD - Website
        cod_website_percetnage_website = f"{round((cod_website_count / len(website)) * 100, 2)}%"
        cod_website_percetnage_all = f"{round((cod_website_count / total_orders_zid) * 100, 2)}%"
        # Tap - Website
        tap_website_percetnage_website = f"{round((tap_payment_count / len(website)) * 100, 2)}%"
        tap_website_percetnage_all = f"{round((tap_payment_count / total_orders_zid) * 100, 2)}%"
        # Tabby - Website
        tabby_website_percetnage_website = f"{round((tabby_website_count / len(website)) * 100, 2)}%"
        tabby_website_percetnage_all = f"{round((tabby_website_count / total_orders_zid) * 100, 2)}%"

        # Percentages - CS
        # COD - CS
        cod_cs_percetnage_cs = f"{round((cod_customer_service_count / len(customer_service)) * 100, 2)}%"
        cod_cs_percetnage_all = f"{round((cod_customer_service_count / total_orders_zid) * 100, 2)}%"
        # Tap - CS
        tap_cs_percetnage_cs = f"{round((tap_count_cs / len(customer_service)) * 100, 2)}%"
        tap_cs_percetnage_all = f"{round((tap_count_cs / total_orders_zid) * 100, 2)}%"
        # Tabby - CS
        tabby_cs_percetnage_cs = f"{round((tabby_count_cs / len(customer_service)) * 100, 2)}%"
        tabby_cs_percetnage_all = f"{round((tabby_count_cs / total_orders_zid) * 100, 2)}%"



        # Get the Cart Average
        cart_avg = total_sales / total_orders_zid

        # Store data in a list
        report_data = []

        # Section 1: General Analysis
        report_data.append(["Section 1: تحليلات عامة", ""])  # Section title
        report_data.append(["مجموع الطلبات", total_orders_zid])
        report_data.append(["متوسط السلة", cart_avg])
        report_data.append(["عدد طلبات - دفع عند الاستلام", cod_total])
        report_data.append(["عدد الطلبات - تاب", total_tap_count])
        report_data.append(["عدد الطلبات - تابي", total_tabby_count])
        report_data.append(["", ""])

        # Section 2
        # Source 1: Website Orders
        report_data.append(["تحليلات الطلبات - المصدر", ""])  # Section title
        report_data.append(["المصدر 1: الموقع", ""])  # Subsection title
        report_data.append(["عدد طلبات الموقع", len(website)])
        report_data.append(["نسبة طلبات الموقع", website_percentage])
        report_data.append(["", ""])
        report_data.append(["طلبات الدفع عند الاستلام - الموقع", cod_website_count])
        report_data.append(["(طلبات الموقع) نسبة طلبات الدفع عند الاستلام", cod_website_percetnage_website])
        report_data.append(["(الكل) نسبة طلبات الدفع عند الاستلام", cod_website_percetnage_all])
        report_data.append(["", ""])
        report_data.append(["طلبات تاب - الموقع", tap_payment_count])
        report_data.append(["(طلبات الموقع فقط) نسبة تاب", tap_website_percetnage_website])
        report_data.append(["(الكل) نسبة تاب", tap_website_percetnage_all])
        report_data.append(["", ""])
        report_data.append(["طلبات تابي - الموقع", tabby_website_count])
        report_data.append(["(طلبات الموقع فقط) نسبة تابي", tabby_website_percetnage_website])
        report_data.append(["(الكل) نسبة تابي", tabby_website_percetnage_all])
        report_data.append(["", ""])  # Blank row for separation

        # Source 2: Orders by Customer Service
        report_data.append(["المصدر 2: خدمة العملاء", ""]) 
        report_data.append(["عدد طلبات خدمة العملاء الجديدة فقط", len(customer_service_new)])
        report_data.append(["نسبة طلبات خدمة العملاء الجديدة", cs_percentage])
        report_data.append(["", ""])
        report_data.append(["عدد طلبات خدمة العملاء", len(customer_service)])
        report_data.append(["نسبة طلبات خدمة العملاء ", cs_percentage_all])

        report_data.append(["", ""])
        report_data.append(["ملاحظة: النسب تم حسابها بناءا على طلبات خدمة العملاء الكلية و ليس الجديدة فقط", ""])
        report_data.append(["", ""])
        report_data.append(["طلبات الدفع عند الاستلام - خدمة العملاء", cod_customer_service_count])
        report_data.append(["(طلبات خدمة العملاء)نسبة طلبات الدفع عند الاستلام", cod_cs_percetnage_cs])
        report_data.append(["(الكل)نسبة الدفع عند الاستلام", cod_cs_percetnage_all])
        report_data.append(["", ""])
        report_data.append(["طلبات تاب - خدمة العملاء", tap_count_cs])
        report_data.append(["(طلبات خدمة العملاء) نسبة طلبات تاب", tap_cs_percetnage_cs])
        report_data.append(["(الكل) نسبة تاب", tap_cs_percetnage_all])
        report_data.append(["", ""])
        report_data.append(["طلبات تابي  خدمة العملاء", tabby_count_cs])
        report_data.append(["(طلبات خدمة العملاء) نسبة طلبات تابي", tabby_cs_percetnage_cs])
        report_data.append(["(الكل) نسبة تابي", tabby_cs_percetnage_all])

        # Convert the data into a DataFrame
        report_df = pd.DataFrame(report_data, columns=["Metric", "Value"])

        return report_df

    # Onto processing
    # Find the total sales --> from zid's total column
    zid['total'] = zid['total'].replace('', 0).fillna(0).astype(float)
    # Sum up the entries
    # One --> Total sales
    tota_sales = zid['total'].sum()

    # Let's get the orders breakdown.
    orders_breakdown = get_source_specifications(zid, tota_sales)

    # Find out the amount spent + the influencers from the facebook file's column "Influencers Amount"
    # Amount spent for facebook
    facebook['Amount spent (SAR)'] = facebook['Amount spent (SAR)'].replace('', 0).astype(float)
    total_amount_spent_facebook = facebook['Amount spent (SAR)'].sum()
    # ADD TO THE TOTAL
    total_amount_spent += total_amount_spent_facebook

    # Amount spent for tiktok
    tiktok['Cost'] = tiktok['Cost'].fillna(0).astype(float)
    total_amount_spent_tiktok = tiktok['Cost'].sum()
    # ADD TO THE TOTAL
    total_amount_spent += total_amount_spent_tiktok

    # Amount spent for snapchat
    snapchat['Amount Spent'] = snapchat['Amount Spent'].fillna(0).astype(float)
    # Convert it to AED
    total_amount_spent_snapchat = (snapchat['Amount Spent']*3.79).sum() # Convert to AED
    # ADD TO THE TOTAL
    total_amount_spent += total_amount_spent_snapchat

    # Amount spent Google ADS
    # Skip to the third row
    # Set the second row as the new header --> this was done in the previous step
    # Find the amount spent
    google['Cost'] = google['Cost'].fillna(0).astype(float)
    total_amount_spent_google = google['Cost'].sum()
    # ADD TO THE TOTAL
    total_amount_spent += total_amount_spent_google

    total_no_influencers = total_amount_spent

    # ADD THE INFLUENCERS AMOUNT
    # From the facebook data
    #### Here filter the influencers for the period -- do the date check thing.
    influencers = get_influencer_budget(start_time, end_time)
    total_amount_spent += influencers


    # REQUIREMENT 2 --> TOTAL ROI
    # SALES / TOTAL_AMOUNT_SPENT
    roi = tota_sales / total_amount_spent

    # REQUIREMENT 2.5 --> TOTAL ROI/ BUDGET (NO INFLUENCERS)
    roi_no_influencers = tota_sales / total_no_influencers

    # REQUIREMENT 3
    # Number of Orders --> Zid
    # Take the unqiue entries in the 'id' column
    total_orders_zid_count = zid['id'].astype(int).nunique()

    # REQUIREMENT 4 --> CPA -- with influencers
    cost_per_aquisition = total_amount_spent / total_orders_zid_count
    

    # REQUIREMENT 5 --> CONVERSION RATE
    # Skip to the 10th row in the google analytics -- header cleaning
    #analytics = analytics.iloc[8:].reset_index(drop=True)  # Keep data from row 9 onwards
    #analytics.columns = analytics.iloc[0]  # Set row 10 as the header
    #analytics = analytics.iloc[1:].reset_index(drop=True) --> this was done in the previous step
    # convert
    analytics[['Active users', 'Key events']] = analytics[['Active users', 'Key events']].astype(int)
    # num_of_purchases_analytics = analytics['Key events'].sum()
    num_of_purchases_analytics = analytics['Key events'].iloc[0]
    num_of_active_users = analytics["Active users"].iloc[0]
    # fulfill
    conversion_rate = (num_of_purchases_analytics / num_of_active_users) * 100

    # REQUIREMENT 6 --> COST PER VISITOR
    cost_per_visitor = total_amount_spent / num_of_active_users
    
    cost_per_visitor_no_influencers = total_no_influencers / num_of_active_users

    # REQUIREMENT 7 --> Customer Service Orders -- been found in the get_source_specifications function. 
    # Filter ZID for source == "لوحة التحكم" and customer_note == "جديد"
    # Take the unique entries first after sorting since I want to make sure that we are keeping the first entires where everything is filled
    '''zid['source'] = zid['source'].str.strip().astype(str)
    zid['customer_note'] = zid['customer_note'].str.strip().astype(str)
    zid['payment_method_filled'] = zid['customer_name'].notna() & (zid['customer_name'] != '')
    # sort based on the newly created row
    zid = zid.sort_values(by=['payment_method_filled'], ascending=False)
    # drop and keep first occrrences
    unique_orders = zid.drop_duplicates(subset='id', keep='first')
    unique_orders = unique_orders.drop(columns=['payment_method_filled'])

    # Filer the unique orders for source == "لوحة التحكم" and customer_note == "جديد"
    keyword = 'جديد'
    unique_orders_filtered = unique_orders[
    (unique_orders['source'] == 'لوحة التحكم') &
    (unique_orders['customer_note'].str.contains(keyword, na=False))
    ]

    # Get the count of these orders
    cs_order_count = len(unique_orders_filtered)'''


    ##################### Table Creation
    # Initialize an empty df to populate with entries for each platform
    platform_df = pd.DataFrame(columns=['Platform', "Amount_Spent", 'ROAS', 'Number_of_Purchases', 'CPA', 'Total_Sales'])
    # A list of platforms
    platforms = ['Snapchat', 'Facebook', 'Tiktok']
    # Get the number of unique campaigns in the Google ADS so we add a row for each and append the entries to the Platforms List
    # Assuming your DataFrame is called df
    unique_campaign_types = google['Campaign type'].unique()

    # Create a list with the desired structure
    campaign_types_list = [f"Google_{campaign_type}" for campaign_type in unique_campaign_types]

    # Join all the platforms
    platforms = platforms + campaign_types_list

    def process_details(keyword, platform_df):
        if keyword == 'Snapchat':
            # Find the Amount Spent in AED
            snapchat_spent = (snapchat['Amount Spent']*3.79).sum()
            snapchat['Result'] = snapchat['Result'].astype(int)
            snapchat_orders = snapchat['Result'].sum()
            snapchat_cpa = snapchat_spent / snapchat_orders
            # Snapchat ROAS
            # Find the Sales
            snapchat['Purchase Roas'] = snapchat['Purchase Roas'].fillna(0).astype(float)
            # Create the sales column
            snapchat['Sales'] = (snapchat['Amount Spent'] * snapchat['Purchase Roas']) * 3.79
            snapchat_sales = snapchat['Sales'].sum()
            snapchat_roas = snapchat_sales / snapchat_spent

            # Create and add the entry
            platform_df = create_entry(snapchat_spent, snapchat_roas, snapchat_orders, snapchat_cpa, snapchat_sales, platform_df, platform='Snapchat')

        elif keyword == 'Facebook':
            # Find the Amount Spent in AED
            facebook_spent = facebook['Amount spent (SAR)'].sum()
            #facebook['Results'] = facebook['Website purchases'].fillna(0).astype(int)
            facebook['Results'] = facebook['Results'].fillna(0).astype(int)
            #facebook_orders = facebook['Website purchases'].sum()
            facebook_orders = facebook['Results'].sum()
            facebook_cpa = facebook_spent / facebook_orders if facebook_orders > 0 else 0
            # Facebook ROAS
            # Find the Sales
            facebook['Purchase ROAS (return on ad spend)'] = facebook['Purchase ROAS (return on ad spend)'].fillna(0).astype(float)
            # Create the sales column
            facebook['Sales'] = (facebook['Amount spent (SAR)'] * facebook['Purchase ROAS (return on ad spend)'])
            facebook_sales = facebook['Sales'].sum()
            facebook_roas = facebook_sales / facebook_spent if facebook_spent > 0 else 0

            # Create and add the entry
            platform_df = create_entry(facebook_spent, facebook_roas, facebook_orders, facebook_cpa, facebook_sales, platform_df, platform='Facebook')
        
        elif keyword == 'Tiktok':
            # Find the Amount Spent in AED
            tiktok_spent = tiktok['Cost'].sum()
            tiktok['Conversions'] = tiktok['Conversions'].fillna(0).astype(int)
            tiktok_orders = tiktok['Conversions'].sum()
            tiktok_cpa = tiktok_spent / tiktok_orders if tiktok_orders > 0 else 0
            # Facebook ROAS
            # Find the Sales
            tiktok['Payment completion ROAS (website)'] = tiktok['Payment completion ROAS (website)'].fillna(0).astype(float)
            # Create the sales column
            tiktok['Sales'] = (tiktok['Cost'] * tiktok['Payment completion ROAS (website)'])
            tiktok_sales = tiktok['Sales'].sum()
            tiktok_roas = tiktok_sales / tiktok_spent if tiktok_spent > 0 else 0

            # Create and add the entry
            platform_df = create_entry(tiktok_spent, tiktok_roas, tiktok_orders, tiktok_cpa, tiktok_sales, platform_df, platform='Tiktok')
        
        elif 'Google_' in keyword:
            # Google Ads calculations
            google['Conversions'] = google['Conversions'].fillna(0).astype(float)
            google['Conv. value'] = google['Conv. value'].fillna(0).astype(float)
            campaign_type = keyword.split('Google_')[1]
            google_spent = google[google['Campaign type'] == campaign_type]['Cost'].sum()
            google_orders = google[google['Campaign type'] == campaign_type]['Conversions'].sum()
            google_cpa = google_spent / google_orders if google_orders > 0 else 0
            google_sales = google[google['Campaign type'] == campaign_type]['Conv. value'].sum()
            google_roas = google_sales / google_spent if google_spent > 0 else 0
            # Check if all the entries add up to 0 no need to create an entry
            row_condition = google_spent + google_roas + google_orders + google_cpa + google_sales
            if row_condition !=  0:
                platform_df = create_entry(google_spent, google_roas, google_orders, google_cpa, google_sales, platform_df, platform=keyword)
    
        return platform_df

    # For each platform create an entry
    for platform in platforms:
        platform_df = process_details(platform, platform_df)

    # Add all the requirements into a report list
    report_data = []
    # Section 1: General Analysis
    report_data.append(["Section 1: General Analysis", ""])  # Section title
    report_data.append(["عددالمبيعات - زد", tota_sales])
    report_data.append(["العائد - مع المشاهير", roi])
    report_data.append(["العائد - بدون المشاهير", roi_no_influencers])
    #report_data.append(["عدد الطلبات", total_orders_zid_count])
    report_data.append(["تكلفة الشراء", cost_per_aquisition])
    report_data.append(["معدل التحويل", conversion_rate])
    report_data.append(["تكلفة الزائر - مشاهير", cost_per_visitor])
    #report_data.append(["عدد طلبات خدمة العملاء", cs_order_count])
    report_data.append(["صرف المشاهير", influencers])
    report_data.append(["صرف السوشل ميديا", total_no_influencers])
    report_data.append(["تكلفة الزائر - بدون مشاهير", cost_per_visitor_no_influencers])


    # Convert the data into a DataFrame
    report_df = pd.DataFrame(report_data, columns=["Metric", "Value"])


    return report_df, platform_df, facebook, tiktok, snapchat, google, zid, analytics, orders_breakdown, zid_unfiltered


#### Function 2 --
def create_product_percentage_amount_spent(facebook, tiktok, snapchat, zid, analytics, zid_unfiltered):
    # Process zid columns & filter out the cancelled and returns
    zid['quantity'] = zid['quantity'].astype(int)
    zid['sku'] = zid['sku'].str.strip().astype(str)
    zid['product name'] = zid['product name'].str.strip().astype(str)
    zid['id'] = zid['id'].astype(int)

    # Clean the rows because the unfiltered zid has not been cleaned before
    zid_unfiltered['quantity'] = zid_unfiltered['quantity'].astype(int)
    zid_unfiltered['sku'] = zid_unfiltered['sku'].str.strip().astype(str)
    zid_unfiltered['product name'] = zid_unfiltered['product name'].str.strip().astype(str)
    zid_unfiltered['id'] = zid_unfiltered['id'].astype(int)

    ## This step has been moved tot he previous function as in it has already been applied. However, it's kept just in case ;p
    exceptions = ['رسوم استبدال', 'رسوم مندوب', 'رسوم مندوب خاص']
    zid = zid[~((zid['product name'].str.contains('رسوم', na=False)) & (zid['customer_name'].isna()))]
    # Find the order ids where the order_status = one of the exceptions and remove them.
    exceptions_status = ['تم الإلغاء', 'قيد الاسترجاع', 'مسترجع']
    order_ids_to_remove = zid[zid['order_status'].isin(exceptions_status)]['id']
    # Filter out these order IDs from the DataFrame
    zid = zid[~zid['id'].isin(order_ids_to_remove)]
    #################################

    # To start off, create a logic to extract the first part of the ad name from each platform
    # Processing facebook data
    # Filter for the part where there's an amount spent
    # The sales column is already created for all from the previous step
    # Columns should be converted as well
    active_fb_ads = facebook[(facebook['Amount spent (SAR)'].notna()) & (facebook['Amount spent (SAR)'] != 0)]
    active_tiktok_ads = tiktok[tiktok['Cost'] > 0]
    active_snapchat_ads = snapchat[snapchat['Amount Spent'] > 0]

    # Product Names in each platform
    # Facebook
    # Filter for where there is amount spent
    active_fb_ads['Campaign name'] = active_fb_ads['Campaign name'].str.strip().astype(str)
    prod_list_fb = active_fb_ads['Campaign name'].str.split('-').str[0].tolist()

    # Tiktok
    active_tiktok_ads['Campaign name'] = active_tiktok_ads['Campaign name'].str.strip().astype(str)
    prod_list_tiktok = active_tiktok_ads['Campaign name'].str.split('-').str[0].tolist()

    # Snapchat
    active_snapchat_ads['Campaign Name'] = active_snapchat_ads['Campaign Name'].str.strip().astype(str)
    prod_list_snapchat = active_snapchat_ads['Campaign Name'].str.split('-').str[0].tolist()

    # final product list
    advertised_prods = prod_list_fb + prod_list_tiktok + prod_list_snapchat

    # Drop dupe entries in the list
    advertised_prods = list(set(advertised_prods))


    ####################### PART 1 FINDING PERCENTAGES
    # Now for each product, get the percentage breakage
    vanilla_prod = fetch_data_from_supabase("Vanilla_Products")
    vanilla_prod['Product Name'] = vanilla_prod['Product Name'].str.strip().astype(str)
    vanilla_prod['Name_in_AD'] = vanilla_prod['Name_in_AD'].str.strip().astype(str)
    vanilla_prod['SKU'] = vanilla_prod['SKU'].str.strip().astype(str)
    # Get the name of ad of the hotel and cloudy package and check if they're not int he advertised products add then pass the list to through below
    # Make a copy fo the advertised products since I don wanna directly add to it
    advertised_prods_copied = advertised_prods[:]
    # the skus I want to add to the percentages subsheet thing.
    added_skus = ['P-HOT3L', 'P-CLOUD', 'ANCK####3##', 'P-GUEST', 'P-SLOUD']
    for value in added_skus:
        value = value.strip()
        name_in_ad = vanilla_prod.loc[vanilla_prod['SKU'] == value, 'Name_in_AD']
        
        if not name_in_ad.empty:
            name = name_in_ad.iloc[0].strip()
            if name not in advertised_prods_copied:
                advertised_prods_copied.append(name)
            
    # Dictionary to store the product and its variation total -- filtered
    prod_variation = {}
    prod_orders = {}
    all_used_variations = []
    orders_count = {}

    # The unfiltered version
    prod_variation_unfiltered = {}
    prod_orders_unfiltered = {}
    all_used_variations_unfiltered = []
    orders_count_unfiltered = {}

    for product in advertised_prods_copied:
        product = str(product).strip()
        # Find this product in the vanilla df and get the sku
        match = vanilla_prod.loc[vanilla_prod['Name_in_AD'] == product, 'SKU']
        if match.empty:
            print(f"Skipping {product} as it is not found in 'Name_in_AD'.")
            continue 
        # Alright alright alright, now pookie the thing is that there are cases where the name in ad is matched with multiple skus right. We need to process allovem and not just take in the first value and process it
        # If there is only one SKU, just grab the single value
        if len(match) == 1:
            product_sku = match.iloc[0]
        else:
            # If multiple SKUs exist, pass them as a list
            product_sku = match.tolist()
        #product_sku = match.values[0]
        # With filtered zid --------------
        product_variation_total, vars_used, orders_per_product = breakage(product_sku, vanilla_prod, zid, product)
        # Do the breakage step for the unfiltered_zid------------
        product_variation_total_unfiltered, vars_used_unfiltered, orders_per_product_unfiltered = breakage(product_sku, vanilla_prod, zid_unfiltered, product)

        # Store the entry of the product and its variation total in a dictionary -- filtered
        prod_variation[product] = product_variation_total
        # Store the entry of the product and its variation total in a dictionary -- unfiltered
        prod_variation_unfiltered[product] = product_variation_total_unfiltered


        # Add the number of orders -- filtered
        prod_orders[product] = orders_per_product
        # Add the number of orders -- unfiltered
        prod_orders_unfiltered[product] = orders_per_product_unfiltered

        # Get the number of orders from the retireved used variables -- fitlered
        filtered_zid = zid[zid['sku'].isin(vars_used)]
        unique_orders_count = filtered_zid['id'].nunique()
        orders_count[product] = unique_orders_count

        # Get the number of orders from the retireved used variables -- unfitlered
        filtered_zid_unfiltered = zid_unfiltered[zid_unfiltered['sku'].isin(vars_used_unfiltered)]
        unique_orders_count_unfiltered = filtered_zid_unfiltered['id'].nunique()
        orders_count_unfiltered[product] = unique_orders_count_unfiltered

        # Add to the used variations - filtered
        all_used_variations += vars_used
        # Add to the used variations - unfiltered
        all_used_variations_unfiltered += vars_used_unfiltered

    
    # Convert the dictionary to a df -- filtered
    df_percentage = pd.DataFrame(list(prod_variation.items()), columns=['Product', 'Total'])
    df_orders = pd.DataFrame(list(prod_orders.items()), columns=['Product', 'Orders Count'])

    # Merge to add the orders count
    df_percentage = df_percentage.merge(df_orders, on='Product', how='left')

    # Calculate the total sum of all product variation totals
    total_variation_sum = df_percentage['Total'].sum()
    # Calculate total number of orders
    total_orders_sum = df_percentage['Orders Count'].sum()

    '''
    # Get the total_quantities of the SKUs not used from the zid file and sum up its quan, add this quan to the total_variation_sum and then use that to divide
    zid_not_used_quans = zid[~(zid['sku'].isin(all_used_variations))]['quantity'].sum()
    total_variation_sum += zid_not_used_quans
    '''

    # Compute the percentage for each product
    df_percentage['Percentage - relative to quantity'] = (df_percentage['Total'] / total_variation_sum) * 100
    df_percentage['Percentage - relative to orders'] = (df_percentage['Orders Count'] / total_orders_sum) * 100

    # Rename the 'Total' column to 'Sum of Quantities'
    df_percentage.rename(columns={
        'Total': 'Purchased Quantities'
    })

    # Return the df_percentage later in the function
    ###################################################
    '''# Make the sprevious step for the unfiltered_zid
    df_percentage_unfiltered = pd.DataFrame(list(prod_variation_unfiltered.items()), columns=['Product', 'Total'])
    df_orders_unfiltered = pd.DataFrame(list(prod_orders_unfiltered.items()), columns=['Product', 'Orders Count'])

    # Merge to add the orders count
    df_percentage_unfiltered = df_percentage_unfiltered.merge(df_orders_unfiltered, on='Product', how='left')'''
    

    ##########################
    #### PART 2 - FIND AMOUNT SPENT FOR EACH AD - is finding the amount spent for each ad for each product for each platform and then summing the amount spent for each ad for the product on all 
    # the platforms to find the total amount spent for the product

    # Facebook: Sum Amount Spent per product
    fb_spent_per_product = active_fb_ads.groupby(active_fb_ads['Campaign name'].str.split('-').str[0])['Amount spent (SAR)'].sum()

    # TikTok: Sum Cost per product
    tiktok_spent_per_product = active_tiktok_ads.groupby(active_tiktok_ads['Campaign name'].str.split('-').str[0])['Cost'].sum()

    # Prerequisites --> find the Amount Spent in AED
    active_snapchat_ads['Amount Spent (AED)'] = active_snapchat_ads['Amount Spent'] * 3.79
    # Snapchat: Sum Amount Spent per product
    snapchat_spent_per_product = active_snapchat_ads.groupby(active_snapchat_ads['Campaign Name'].str.split('-').str[0])['Amount Spent (AED)'].sum()

    ## Put them in an excel
    product_names = []
    facebook_spent_values = []
    tiktok_spent_values = []
    snapchat_spent_values = []

    # Loop through all advertised products
    for product in advertised_prods:
        product_names.append(product)
        
        # Get the spend from each platform or set 0 if the product is missing
        fb_spent = fb_spent_per_product.get(product, 0)
        tiktok_spent = tiktok_spent_per_product.get(product, 0)
        snapchat_spent = snapchat_spent_per_product.get(product, 0)
        
        # Append values to respective lists
        facebook_spent_values.append(fb_spent)
        tiktok_spent_values.append(tiktok_spent)
        snapchat_spent_values.append(snapchat_spent)

    # Create the final DataFrame
    total_spent_per_product = pd.DataFrame({
        'Product': product_names,
        'Facebook Spent': facebook_spent_values,
        'TikTok Spent': tiktok_spent_values,
        'Snapchat Spent': snapchat_spent_values
    })

    #######
    # Add a column for the total spend across all platforms
    total_spent_per_product['Total Spent'] = (
    total_spent_per_product['Facebook Spent'] +
    total_spent_per_product['TikTok Spent'] +
    total_spent_per_product['Snapchat Spent']
    )

    # Compute all the amount spent
    overall_total_spent = total_spent_per_product['Total Spent'].sum()

    # Find out the percentage of each 
    total_spent_per_product['Percentage of Total Amount Spent'] = ((total_spent_per_product['Total Spent'] / overall_total_spent) * 100).round(2)

    # I wanna get the total of amount spent I did on facebook, tiktok, snapchat, and then I want to. Lol shut up this is basically the overall_total_spent. 

    ############################## PART 3 --> INDEPTH PRODUCT AD ANALYSIS
    # Find the requirements for each platform
    # Requirement 1 --> Number of ADS for each product
    # Requirement 2 --> Amount spend for the products ads
    # Requirement 3 --> Total number of orders from the ads of the product
    # Requirement 4 --> The roas --> get the sales of the adverts / total amount spent on those ads
    # Requirement 5 --> CPA --> divide the total amount spent on the ads for that product by the sum of orders from the product ads
    # Requirement 6 --> Cart AVG --> total sales from the product ads / total number of orders from the product ads
    # Requirement 7 --> Number of Orders for the product from ZID
    # Requirement 8 --> Conversion rate --> number of purchases from the ads / active users from google analystics


    # Facebook
    # Find the sales from the og df for facebook active ads
    # Step 0
    active_fb_ads['Sales'] = active_fb_ads['Amount spent (SAR)'] * active_fb_ads['Purchase ROAS (return on ad spend)']
    # 1 
    # Facebook: Sum Amount Spent per product
    fb_ad_count_per_product = active_fb_ads.groupby(active_fb_ads['Campaign name'].str.split('-').str[0]).size().reset_index(name='Ad Count')
    # Sum the amount spent per product
    # 2
    fb_spent_per_product = active_fb_ads.groupby(active_fb_ads['Campaign name'].str.split('-').str[0])['Amount spent (SAR)'].sum().reset_index()
    # Sum the 'Results' per product to get total orders
    # 3
    fb_orders_per_product = active_fb_ads.groupby(active_fb_ads['Campaign name'].str.split('-').str[0])['Results'].sum().reset_index()

    # Merge all DataFrames on the product name
    fb_stats_per_product = fb_spent_per_product.merge(fb_ad_count_per_product, on='Campaign name', how='left') \
                                            .merge(fb_orders_per_product, on='Campaign name', how='left')

    # Rename columns for clarity
    fb_stats_per_product.rename(columns={'Amount spent (SAR)': 'Facebook Spent', 
                                        'Results': 'Total Orders'}, inplace=True)
    
    # Finding he sales for each product and that is by finding the sum of the individual roas and multiplying it by the amount spent on those adverts
    # Sum the Purchase ROAS per product
    # from the active ads df
    #fb_roas_per_product = active_fb_ads.groupby(active_fb_ads['Campaign name'].str.split('-').str[0])['Purchase ROAS (return on ad spend)'].sum().reset_index()

    # Merge with the existing df (fb_stats_per_product)
    # merge with out existing df
    #fb_stats_per_product = fb_stats_per_product.merge(fb_roas_per_product, on='Campaign name', how='left')

    # Calculate Sales (Facebook Spent * Purchase ROAS)
    #fb_stats_per_product['Sales'] = fb_stats_per_product['Facebook Spent'] * fb_stats_per_product['Purchase ROAS (return on ad spend)']

    # To find the sales, only sum up the sales for the ads
    fb_sales_per_product = active_fb_ads.groupby(active_fb_ads['Campaign name'].str.split('-').str[0])['Sales'].sum().reset_index()

    # The merge
    fb_stats_per_product = fb_stats_per_product.merge(fb_sales_per_product, on='Campaign name', how='left')

    # Rename columns for clarity
    #fb_stats_per_product.rename(columns={'Purchase ROAS (return on ad spend)': 'Total ROAS (Individual Product ADs)'}, inplace=True)

    # 4 Get the sales of the adverts / the total amount spent on the adver
    fb_stats_per_product['Roas (Actual)'] = fb_stats_per_product['Sales'] / fb_stats_per_product['Facebook Spent']

    # 5 divide the total amount spent on the ads for that product by the sum of orders from the product ads
    fb_stats_per_product['CPA'] = fb_stats_per_product['Facebook Spent'] / fb_stats_per_product['Total Orders']

    # 6 Cart AVG
    fb_stats_per_product['Cart AVG'] = fb_stats_per_product['Sales'] / fb_stats_per_product['Total Orders']

    # 7 & 8 Total ZID Orders & conversion rate are to be added later
    # Drop the individual roas thing
    #fb_stats_per_product.drop('Total ROAS (Individual Product ADs)', axis=1, inplace=True)

    #### PLATFORM 2 SNAPCHAT ##########################
    ###################################################
    # Find the sales from the og snapchat active ads df
    # Step 0
    active_snapchat_ads['Sales'] = active_snapchat_ads['Amount Spent (AED)'] * active_snapchat_ads['Purchase Roas']
    # Snapchat: Sum Amount Spent per product
    snap_ad_count_per_product = active_snapchat_ads.groupby(active_snapchat_ads['Campaign Name'].str.split('-').str[0]).size().reset_index(name='Ad Count')
    # Sum the amount spent per product
    # 2
    snapchat_spent_per_product = active_snapchat_ads.groupby(active_snapchat_ads['Campaign Name'].str.split('-').str[0])['Amount Spent (AED)'].sum().reset_index()
    # Sum the 'Result' per product to get total orders
    # 3
    snapchat_orders_per_product = active_snapchat_ads.groupby(active_snapchat_ads['Campaign Name'].str.split('-').str[0])['Result'].sum().reset_index()
    # Merge all DataFrames on the product name
    snapchat_stats_per_product = snapchat_spent_per_product.merge(snap_ad_count_per_product, on='Campaign Name', how='left') \
                                            .merge(snapchat_orders_per_product, on='Campaign Name', how='left')
    # Rename columns for clarity
    snapchat_stats_per_product.rename(columns={'Amount Spent (AED)': 'Snapchat Spent', 
                                        'Result': 'Total Orders'}, inplace=True)
    
    # Finding he sales for each product and that is by finding the sum of the individual roas and multiplying it by the amount spent on those adverts
    # Sum the Purchase ROAS per product
    # from the active ads df
    #snapchat_roas_per_product = active_snapchat_ads.groupby(active_snapchat_ads['Campaign Name'].str.split('-').str[0])['Purchase Roas'].sum().reset_index()
    snapchat_sales_per_product = active_snapchat_ads.groupby(active_snapchat_ads['Campaign Name'].str.split('-').str[0])['Sales'].sum().reset_index()
    # merge with out existing df
    snapchat_stats_per_product = snapchat_stats_per_product.merge(snapchat_sales_per_product, on='Campaign Name', how='left')
    # Calculate Sales (Facebook Spent * Purchase ROAS)
    #snapchat_stats_per_product['Sales'] = snapchat_stats_per_product['Snapchat Spent'] * snapchat_stats_per_product['Purchase Roas']
    # 4 Get the sales of the adverts / the total amount spent on the adver
    snapchat_stats_per_product['Roas (Actual)'] = snapchat_stats_per_product['Sales'] / snapchat_stats_per_product['Snapchat Spent']
    # 5 divide the total amount spent on the ads for that product by the sum of orders from the product ads
    snapchat_stats_per_product['CPA'] = snapchat_stats_per_product['Snapchat Spent'] / snapchat_stats_per_product['Total Orders']
    # 6 Cart AVG
    snapchat_stats_per_product['Cart AVG'] = snapchat_stats_per_product['Sales'] / snapchat_stats_per_product['Total Orders']
    
    #snapchat_stats_per_product.drop('Purchase Roas', axis=1, inplace=True)
    # Rename the Campaign Name to Campaign name to match the rest so we concat later
    snapchat_stats_per_product.rename(columns={'Campaign Name': 'Campaign name'}, inplace=True)

    #### PLATFORM 3 Tiktok ##########################
    ###################################################
    # Find the sales from the og snapchat active ads df
    # Step 0
    active_tiktok_ads['Sales'] = active_tiktok_ads['Cost'] * active_tiktok_ads['Payment completion ROAS (website)']
    # Tiktok: Sum Amount Spent per product
    tiktok_ad_count_per_product = active_tiktok_ads.groupby(active_tiktok_ads['Campaign name'].str.split('-').str[0]).size().reset_index(name='Ad Count')
    # Sum the amount spent per product
    # 2
    tiktok_spent_per_product = active_tiktok_ads.groupby(active_tiktok_ads['Campaign name'].str.split('-').str[0])['Cost'].sum().reset_index()
    # Sum the 'Result' per product to get total orders
    # 3
    tiktok_orders_per_product = active_tiktok_ads.groupby(active_tiktok_ads['Campaign name'].str.split('-').str[0])['Conversions'].sum().reset_index()
    # Merge all DataFrames on the product name
    tiktok_stats_per_product = tiktok_spent_per_product.merge(tiktok_ad_count_per_product, on='Campaign name', how='left') \
                                            .merge(tiktok_orders_per_product, on='Campaign name', how='left')
    # Rename columns for clarity
    tiktok_stats_per_product.rename(columns={'Cost': 'Tiktok Spent', 
                                        'Conversions': 'Total Orders'}, inplace=True)
    
    # Finding he sales for each product and that is by finding the sum of the individual roas and multiplying it by the amount spent on those adverts
    # Sum the Purchase ROAS per product
    # from the active ads df
    #tiktok_roas_per_product = active_tiktok_ads.groupby(active_tiktok_ads['Campaign name'].str.split('-').str[0])['Payment completion ROAS (website)'].sum().reset_index()
    tiktok_sales_per_product = active_tiktok_ads.groupby(active_tiktok_ads['Campaign name'].str.split('-').str[0])['Sales'].sum().reset_index()
    # merge with out existing df
    tiktok_stats_per_product = tiktok_stats_per_product.merge(tiktok_sales_per_product, on='Campaign name', how='left')
    # Calculate Sales (Facebook Spent * Purchase ROAS)
    #tiktok_stats_per_product['Sales'] = tiktok_stats_per_product['Tiktok Spent'] * tiktok_stats_per_product['Payment completion ROAS (website)']
    # 4 Get the sales of the adverts / the total amount spent on the adver
    tiktok_stats_per_product['Roas (Actual)'] = tiktok_stats_per_product['Sales'] / tiktok_stats_per_product['Tiktok Spent']
    # 5 divide the total amount spent on the ads for that product by the sum of orders from the product ads
    tiktok_stats_per_product['CPA'] = tiktok_stats_per_product['Tiktok Spent'] / tiktok_stats_per_product['Total Orders']
    # 6 Cart AVG
    tiktok_stats_per_product['Cart AVG'] = tiktok_stats_per_product['Sales'] / tiktok_stats_per_product['Total Orders']

    #tiktok_stats_per_product.drop('Payment completion ROAS (website)', axis=1, inplace=True)

    # Sum them all up in a single table and then add the final columns, zid orders + the conversion rate
    # Concatenate -- lol no, construct each column individually since some of them are not to be just added up wth
    # Step 1: Get a unique list of campaign names from all three DataFrames
    all_campaign_names = pd.concat([
        fb_stats_per_product[['Campaign name']], 
        snapchat_stats_per_product[['Campaign name']], 
        tiktok_stats_per_product[['Campaign name']]
    ]).drop_duplicates()

    # Step 2: Merge all DataFrames to align data by Campaign name
    merged_df = all_campaign_names \
        .merge(fb_stats_per_product.rename(columns={'Facebook Spent': 'Amount Spent'}), on='Campaign name', how='left') \
        .merge(snapchat_stats_per_product.rename(columns={'Snapchat Spent': 'Amount Spent'}), on='Campaign name', how='left') \
        .merge(tiktok_stats_per_product.rename(columns={'Tiktok Spent': 'Amount Spent'}), on='Campaign name', how='left')

    # Step 3: Sum up relevant columns
    merged_df['Amount Spent'] = merged_df[['Amount Spent_x', 'Amount Spent_y', 'Amount Spent']].sum(axis=1, skipna=True)
    merged_df['Ad Count'] = merged_df[['Ad Count_x', 'Ad Count_y', 'Ad Count']].sum(axis=1, skipna=True)
    merged_df['Total Orders'] = merged_df[['Total Orders_x', 'Total Orders_y', 'Total Orders']].sum(axis=1, skipna=True)
    merged_df['Sales'] = merged_df[['Sales_x', 'Sales_y', 'Sales']].sum(axis=1, skipna=True)

    # Step 4: Drop old columns
    merged_df = merged_df[['Campaign name', 'Amount Spent', 'Ad Count', 'Total Orders', 'Sales']]


    # Calculate the remaining columns again
    merged_df['Roas (Actual)'] = merged_df['Sales'] / merged_df['Amount Spent']
    merged_df['CPA'] = merged_df['Amount Spent'] / merged_df['Total Orders']
    merged_df['Cart AVG'] = merged_df['Sales'] / merged_df['Total Orders']

    merged_df['Campaign name'] = merged_df['Campaign name'].str.strip().astype(str)

    # Add the number of orders for each product using the orders_count dictionary
    # to store zid orders
    zid_orders = []

    # Loop through the df
    for index, row in merged_df.iterrows():
        campaign_name = row['Campaign name'].strip()  
        if campaign_name in orders_count:
            zid_orders.append(orders_count[campaign_name])  
        else:
            zid_orders.append(0)  

    # Add the ZID Orders list as a new column
    merged_df['Product Orders (Website)'] = zid_orders

    # Add the conversion rates
    # Retrieve the active users from the analytics
    num_of_active_users = analytics["Active users"].iloc[0]
    #merged_df['Conversion Rate'] = ((merged_df['Total Orders'] / num_of_active_users) * 100).round(2)

    # Find the budget breakdown for each product
    merged_df['Budget Spent (Percentage)'] = ((merged_df['Amount Spent'] / overall_total_spent) * 100).round(2)
    # Return both the percentages and the total_spent for product ads
    # We're sending back the orders_count_unfiltered as well to add to the landing page
    return df_percentage, total_spent_per_product, fb_stats_per_product, snapchat_stats_per_product, tiktok_stats_per_product, merged_df, facebook, tiktok, snapchat, zid, analytics, vanilla_prod, advertised_prods, orders_count_unfiltered, advertised_prods_copied

async def landing_performance_5_async(analytics, vanilla_db, advertised_prods, full_detailed, orders_count_unfiltered):
    # Clean vanilla_db and analytics
    vanilla_db['Name_in_AD'] = vanilla_db['Name_in_AD'].str.strip().astype(str)
    vanilla_db['Product Name'] = vanilla_db['Product Name'].str.strip().astype(str)
    vanilla_db['Variations'] = vanilla_db['Variations'].str.strip().astype(str)
    # Analytics
    analytics['Page path and screen class'] = analytics['Page path and screen class'].str.strip().astype(str)
    analytics['Add to carts'] = analytics['Add to carts'].astype(int)
    # Retrieve the databases to get the product names
    og_products_df = fetch_data_from_supabase("Generated_Skus")
    og_products_df['SKU'] = og_products_df['SKU'].str.strip().astype(str)
    og_products_df['ID'] = og_products_df['ID'].astype(int)
    og_products_df['Product Name'] = og_products_df['Product Name'].str.strip().astype(str)
    og_products_df['Source'] = 'og_products'  # Mark source

    # Fetch and prepare og_products_sw
    '''og_products_sw = fetch_data_from_supabase("Generated_Skus_SW")
    og_products_sw['SKU'] = og_products_sw['SKU'].str.strip().astype(str)
    og_products_sw['ID'] = og_products_sw['ID'].astype(int)
    og_products_sw['Product Name'] = og_products_sw['Product Name'].str.strip().astype(str)
    og_products_sw['Source'] = 'og_products_sw'  # Mark source'''

    # Combine and sort by priority: og_products first, then og_products_sw
    #combined_products_df = pd.concat([og_products_df, og_products_sw]).drop_duplicates(subset='SKU')
    combined_products_df = og_products_df.sort_values(by=['ID'])

    # Package Names
    package_skus = fetch_data_from_supabase('Package_Skus')
    package_skus['Indication'] = package_skus['Indication'].str.strip().astype(str)
    package_skus['Package Name'] = package_skus['Package Name'].str.strip().astype(str)

    # Fetch URLs for analysis
    filtered_analytics = analytics[analytics['Page path and screen class'].str.contains('/products/', na=False)]
    sleepy_website = 'https://sleepy-cloud.ae'
    urls = [f"{sleepy_website}{ext}" for ext in filtered_analytics['Page path and screen class']]

    # Fetch H1 tags for URLs asynchronously
    h1_tag_map = await fetch_all_h1_tags(urls, analytics, sleepy_website)

    # Now filter for advertised products
    advertised_prods = [str(product_name).strip().strip("'") for product_name in advertised_prods]
    vanilla_db = vanilla_db[vanilla_db['Name_in_AD'].isin(advertised_prods)]
    
    if vanilla_db.empty:
        return pd.DataFrame([{'Product Name': 'Error Fetching Database results', 'Active Users': '', 'Adds to Cart': ''}])

    # Match variations with H1 tags
    result_df = match_variations_with_h1(vanilla_db, h1_tag_map, combined_products_df, package_skus)

    # Final DataFrame processing
    result_df[['Active Users', 'Adds to Cart']] = result_df[['Active Users', 'Adds to Cart']].astype(int)
    # Group them to sum up the values from the Main page and the sub pages of a product then find the conversion rate
    result_df = result_df.groupby('Product Name', as_index=False)[['Active Users', 'Adds to Cart']].sum()
    result_df['Conversion Rate - Adds to Cart'] = ((result_df['Adds to Cart'] / result_df['Active Users']) * 100).round(2)
    result_df['Conversion Rate - Adds to Cart'] = result_df['Conversion Rate - Adds to Cart'].fillna(0)

    '''# Merge both the full detailed and the result_df to pull out the conversion rate - orders wise and the product orders
    # Clean
    result_df['Product Name'] = result_df['Product Name'].astype(str).str.strip()
    full_detailed['Campaign name'] = full_detailed['Campaign name'].astype(str).str.strip()

    # Merge
    merged_df = result_df.merge(
        full_detailed[['Campaign name', 'Conversion Rate', 'Product Orders (Website)']],
        left_on='Product Name',
        right_on='Campaign name',
        how='left'
    )

    # The Campaign Name is not needed
    merged_df = merged_df.drop(columns=['Campaign name'])
    # Rename the Columns
    merged_df.rename(columns={'Conversion Rate': 'Conversion rate - Products'}, inplace=True)'''

    # Instead of merging with the full detailed (because the full detailed has the orders count of the filtered version) what we do is that we loop and get the orders count by matching the ad name, then we find the conversion rate
    # Add the number of orders for each product using the orders_count dictionary
    # to store zid orders
    zid_orders = []

    # Loop through the df
    for index, row in result_df.iterrows():
        campaign_name = row['Product Name'].strip()  
        if campaign_name in orders_count_unfiltered:
            zid_orders.append(orders_count_unfiltered[campaign_name])  
        else:
            zid_orders.append(0)
    
    # Add the ZID Orders list as a new column
    result_df['Product Orders Unfiltered (Website)'] = zid_orders
    # Find the conversion rate
    result_df['Conversion Rate - Product Orders'] = ((result_df['Product Orders Unfiltered (Website)'] / result_df['Active Users']) * 100).round(2)
    result_df['Conversion Rate - Product Orders'] = result_df['Conversion Rate - Product Orders'].fillna(0)


    return result_df

####