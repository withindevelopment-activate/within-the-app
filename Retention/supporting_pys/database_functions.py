import pandas as pd
import os
import logging
from supabase import create_client, Client

url: str = os.environ.get('SUPABASE_URL')
key: str = os.environ.get('SUPABASE_KEY')

supabase: Client = create_client(url, key)

def fetch_data_from_supabase_specific(table_name, columns=None, filters=None, order_by=None, limit=None, count=None):
    # Construct the select query with specified columns or all columns
    if columns:
        # Wrap problematic column names with double quotes
        columns = [f'"{col}"' if " " in col or "(" in col else col for col in columns]
        select_query = ",".join(columns)
    else:
        select_query = "*"

        
    query = supabase.table(table_name).select(select_query, count=count)

    # Apply filters if specified
    if filters:
        for column, condition in filters.items():
            # Allow multiple filters on the same column
            if isinstance(condition, list):
                for cond in condition:
                    if isinstance(cond, tuple) and len(cond) == 2:
                        op, value = cond
                        if op == 'eq':
                            query = query.eq(column, value)
                        elif op == 'neq':
                            query = query.neq(column, value)
                        elif op == 'lt':
                            query = query.lt(column, value)
                        elif op == 'lte':
                            query = query.lte(column, value)
                        elif op == 'gt':
                            query = query.gt(column, value)
                        elif op == 'gte':
                            query = query.gte(column, value)
                        elif op == 'in':
                            query = query.in_(column, value)
                        elif op == 'not_in':
                            query = query.not_in(column, value)
            elif isinstance(condition, tuple) and len(condition) == 2:
                op, value = condition
                if op == 'eq':
                    query = query.eq(column, value)
                elif op == 'neq':
                    query = query.neq(column, value)
                elif op == 'lt':
                    query = query.lt(column, value)
                elif op == 'lte':
                    query = query.lte(column, value)
                elif op == 'gt':
                    query = query.gt(column, value)
                elif op == 'gte':
                    query = query.gte(column, value)
                elif op == 'in':
                    query = query.in_(column, value)
                elif op == 'not_in':
                    query = query.not_in(column, value)
            else:
                query = query.eq(column, condition)

    # Apply sorting if specified
    if order_by:
        query = query.order(order_by, desc=True)  # Assuming descending order for 'id'

    # Apply limit if specified
    if limit:
        query = query.limit(limit)

    # Execute the query
    response = query.execute()
    
    if response.data is not None:
        df = pd.DataFrame(response.data)
        total_count = response.count if hasattr(response, 'count') else len(df)

        # If DataFrame is empty, return an empty one with proper columns
        if df.empty:
            # Try to infer column names from the table structure using Supabase's metadata API
            # But for now, fallback to using the `columns` argument if provided
            if columns:
                return pd.DataFrame(columns=columns), 0
            else:
                return pd.DataFrame(), 0  # Empty with unknown columns

        # Replace NaNs with 0 in the 'Quantity' column if the table is 'Inventory'
        if table_name == "Inventory" and 'Quantity' in df.columns:
            df['Quantity'].fillna(0, inplace=True)

        return df, total_count
    else:
        raise Exception("Error fetching data: " + response.error_message)
    

def get_next_id_from_supabase_compatible_all(name, column): # getting the next id
    """
    Retrieve the last ID from a specified column in the Supabase table and return the incremented value.

    Parameters:
    - name (str): The name of the Supabase table.
    - column (str): The column name to fetch the last ID from.

    Returns:
    - int: The next available ID.
    """
    try:
        response = supabase.table(name).select(column).order(column, desc=True).limit(1).execute()
        
        if not response.data:
            # If no data is found, start with 1 as the initial ID
            return 1
        
        # Extract the last ID
        last_id = int(response.data[0][column])
        
        # Increment the ID
        next_id = last_id + 1
        
        return next_id
    
    except Exception as e:
        print(f"Error fetching the next ID from Supabase: {e}")
        raise

def batch_insert_to_supabase(df, table_name):
    """
    Inserts a batch of rows into a Supabase table without updating existing rows.

    Parameters:
        df (pd.DataFrame): DataFrame containing the rows to insert.
        table_name (str): Name of the Supabase table.

    Returns:
        bool: True if the insertion is successful, False otherwise.
    """
    try:
        # Ensure 'Distinct_ID' or 'id' columns are correctly formatted for specific tables
        int_columns = {
            'Current_Orders': 'id', 'Current_Orders_OM': 'id', 'Shipped_Orders': 'id',
            'All_Uploaded_Orders': 'Distinct_ID', 'All_Customers': 'Distinct_ID', 'Write_Inventory': 'Distinct_ID'
        }

        # Convert ID columns to integer type if applicable
        if table_name in int_columns and int_columns[table_name] in df.columns:
            df[int_columns[table_name]] = pd.to_numeric(df[int_columns[table_name]], errors='coerce').astype('Int64')

        # Fill missing values for numeric and non-numeric columns
        for column in df.columns:
            if df[column].dtype.kind in 'biufc':  # Numeric columns
                df[column].fillna(0, inplace=True)
            else:  # Non-numeric columns (strings, objects)
                df[column].fillna("", inplace=True)

        # orient
        data = df.to_dict(orient='records')

        if not data:
            logging.warning(f"No valid rows to insert into '{table_name}'.")
            return {"success": False, "rows_inserted": 0, "errors": "No valid data to insert"}

        # Insert instead of upsert for batch insertion (avoids updating existing rows)
        response = supabase.table(table_name).insert(data).execute()
        
        if response.data:  # Check if there is data in the response
            logging.info(f"Inserted {len(response.data)} rows into '{table_name}'.")
            return {"success": True, "rows_inserted": len(response.data), "errors": None}
        else:
            logging.error(f"Failed to insert, no data returned: {response}")
            return {"success": False, "rows_inserted": 0, "errors": "No data returned in response"}

    except Exception as e:
        logging.error(f"Error during batch insertion into '{table_name}': {str(e)}")
        return {"success": False, "rows_inserted": 0, "errors": str(e)}
    
def upsert_partial(df, table_name, pk):
    if pk not in df.columns:
        raise ValueError(f"Primary key '{pk}' missing from DataFrame")

    payload = df.to_dict(orient='records')

    supabase.table(table_name) \
            .upsert(payload, on_conflict=pk) \
            .execute()


    
def delete_row_from_supabase(df, table_name, pk):
    """
    Deletes rows from Supabase using the specified primary key column.
    
    """

    if pk not in df.columns:
        raise ValueError(f"Primary key '{pk}' missing from DataFrame")

    ids = (
        df[pk]
        .dropna()
        .unique()
        .tolist()
    )

    if not ids:
        return

    supabase.table(table_name) \
        .delete() \
        .in_(pk, ids) \
        .execute()

def fetch_retention_data(table_name, select_query="*", columns=None, filters=None, order_by=None, limit=None, count=None):
    """
    A specialized fetch function for the retention dashboard that handles complex queries,
    including joins and dotted notation for filters.
    """
    print("\n--- [DEBUG Retention] fetch_retention_data ---")
    print(f"[DEBUG Retention] table_name: {table_name}")
    print(f"[DEBUG Retention] select_query: {select_query}")
    print(f"[DEBUG Retention] filters: {filters}")
    print(f"[DEBUG Retention] limit: {limit}, count: {count}")
    # If a specific select_query (like one with a join) is provided, use it.
    # Otherwise, construct one from the columns list.
    if select_query == "*" and columns:
        # Wrap problematic column names with double quotes
        columns = [f'"{col}"' if " " in col or "(" in col else col for col in columns]
        final_select = ",".join(columns)
    else:
        final_select = select_query

    print(f"[DEBUG Retention] Final select statement: {final_select}")
    query = supabase.table(table_name).select(final_select, count=count)

    # Apply filters if specified
    if filters:
        for column, condition in filters.items():
            print(f"[DEBUG Retention] Applying filter: {column} {condition}")
            if isinstance(condition, tuple) and len(condition) == 2:
                op, value = condition
                # Dynamically call the filter method on the query object
                # e.g., query.eq(column, value) or query.in_(column, value)
                method_name = op if op != 'in' else 'in_'
                if hasattr(query, method_name):
                    method = getattr(query, method_name)
                    query = method(column, value)
            elif isinstance(condition, tuple) and len(condition) == 3 and condition[0] == 'between':
                # Handle 'between' which is not a direct method
                _, start, end = condition
                query = query.gte(column, start).lte(column, end)
            else:
                query = query.eq(column, condition)

    if order_by:
        query = query.order(order_by, desc=True)

    if limit:
        query = query.limit(limit)

    print("[DEBUG Retention] Executing Supabase query...")
    response = query.execute()
    df = pd.DataFrame(response.data) if response.data else pd.DataFrame()
    total_count = response.count if hasattr(response, 'count') else len(df)
    print(f"[DEBUG Retention] Query returned {len(df)} rows. Total count from Supabase: {total_count}")
    return df, total_count
