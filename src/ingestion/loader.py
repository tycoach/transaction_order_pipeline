"""
Data ingestion components for the  data pipeline.
"""
import os
import logging
import pandas as pd
import traceback
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

def ingest_csv_to_staging(file_path, engine, table_name, dtypes=None):
    """
    Load CSV file to staging table.
    """
    try:
        logger.info(f"Loading data from {file_path} to {table_name}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None  
        # Read CSV with pandas
        df = pd.read_csv(file_path, dtype=dtypes)  
        # Log data 
        logger.info(f"Loaded {len(df)} rows from {file_path}") 
        #  data validation
        missing_values = df.isnull().sum().sum()
        if missing_values > 0:
            logger.warning(f"Found {missing_values} missing values in {file_path}")
        # Load to database
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Successfully loaded data to {table_name}")
        
        return df
    except Exception as e:
        logger.error(f"Failed to load {file_path} to staging: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def load_staging_data(config, engine):
    """
    Load all source CSV files to staging tables.
    """
    try:
        # Define data types for each table
        orders_dtypes = {
            'order_id': 'int',
            'customer_id': 'str',
            'order_date': 'str',
            'total_amount': 'float'
        }
        
        order_items_dtypes = {
            'order_id': 'int',
            'item_id': 'str',
            'quantity': 'int',
            'unit_price': 'float'
        }
        
        menu_items_dtypes = {
            'item_id': 'str',
            'item_name': 'str',
            'category': 'str',
            'description': 'str'
        }
        
        # Load each CSV file to staging
        orders_df = ingest_csv_to_staging(
            config.get_input_path('orders.csv'), 
            engine, 
            'staging_orders', 
            orders_dtypes
        )
        
        order_items_df = ingest_csv_to_staging(
            config.get_input_path('order_item.csv'), 
            engine, 
            'staging_order_items', 
            order_items_dtypes
        )
        
        menu_items_df = ingest_csv_to_staging(
            config.get_input_path('menu_items.csv'), 
            engine, 
            'staging_menu_items', 
            menu_items_dtypes
        )
        
        # Check if all files were loaded successfully
        if orders_df is None or order_items_df is None or menu_items_df is None:
            logger.error("Failed to load all required data files")
            raise Exception("Data ingestion failed")
        
        return {
            'orders': orders_df,
            'order_items': order_items_df,
            'menu_items': menu_items_df
        }
    except Exception as e:
        logger.error(f"Failed to load staging data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def get_last_processed_date(engine):
    """
    Get the latest date processed from the daily_revenue_by_category table.
    """
    try:
        query = "SELECT MAX(date) as last_date FROM daily_revenue_by_category"
        result = pd.read_sql(query, engine)
        last_date = result['last_date'].iloc[0]
        
        if pd.isna(last_date):
            return None
        
        return pd.to_datetime(last_date)
    except SQLAlchemyError as e:
        logger.error(f"Error getting last processed date: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting last processed date: {str(e)}")
        return None

def load_incremental_data(config, engine, last_date):
    """
    Load only new data since the last processed date.
    """
    try:
        # Load full data first
        full_data = load_staging_data(config, engine)
        
        # Convert order_date to datetime
        orders_df = full_data['orders']
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
        
        # Filter orders for only new data
        new_orders_df = orders_df[orders_df['order_date'] > last_date]
        
        if len(new_orders_df) == 0:
            logger.info("No new data to process")
            return None
        
        logger.info(f"Processing {len(new_orders_df)} new orders")
        
        # Filter order_items for only new orders
        new_order_ids = new_orders_df['order_id'].tolist()
        order_items_df = full_data['order_items']
        new_order_items_df = order_items_df[order_items_df['order_id'].isin(new_order_ids)]
        
        # Return filtered data
        return {
            'orders': new_orders_df,
            'order_items': new_order_items_df,
            'menu_items': full_data['menu_items']  # Menu items remain the same
        }
    except Exception as e:
        logger.error(f"Error in incremental data loading: {str(e)}")
        logger.error(traceback.format_exc())
        raise