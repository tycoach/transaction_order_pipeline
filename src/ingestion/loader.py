"""
Data ingestion components for the  data pipeline.
"""
import os
import logging
import pandas as pd
import traceback
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch



load_dotenv()

# Database connection parameters 
DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password':os.getenv('POSTGRES_PASSWORD') ,
    'port': os.getenv('POSTGRES_PORT') 
}

def create_engine():
    """Create postgres engine from DB parameters"""
    return f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"

def connect_to_db():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_PARAMS['host'],
            database=DB_PARAMS['database'],
            user=DB_PARAMS['user'],
            password=DB_PARAMS['password'],
            port=DB_PARAMS['port']
        )
        logger.info("Successfully connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise




logger = logging.getLogger(__name__)


def ingest_csv_to_staging(file_path, conn, table_name, dtypes=None):
    """
    Load CSV file to staging table 
    
    """
    cursor = None
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

        # Data validation
        missing_values = df.isnull().sum().sum()
        if missing_values > 0:
            logger.warning(f"Found {missing_values} missing values in {file_path}")
        
        # Handle duplicate keys in menu_items
        if table_name == 'staging_menu_items':
            # Check for duplicate item_id values
            duplicate_items = df[df.duplicated(subset=['item_id'], keep=False)]
            if not duplicate_items.empty:
                logger.warning(f"Found {len(duplicate_items)} duplicate item_id values in {file_path}")
                logger.info("Keeping only the first occurrence of each item_id")
                # Keep only the first occurrence of each item_id
                df = df.drop_duplicates(subset=['item_id'], keep='first')
        
        # Use the existing connection
        try:
            # Create cursor
            cursor = conn.cursor()
            
            # Check if table exists
            try:
                cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                table_exists = True
            except Exception:
                table_exists = False
                conn.rollback()  # Important: roll back after exception
            
            # Create or truncate table
            if not table_exists:
                # Determine appropriate column types
                columns = []
                for col in df.columns:
                    if col == 'order_id' and table_name == 'staging_orders':
                        columns.append(f"{col} INTEGER PRIMARY KEY")
                    elif col == 'item_id' and table_name == 'staging_menu_items':
                        columns.append(f"{col} TEXT PRIMARY KEY")
                    elif 'date' in col.lower():
                        columns.append(f"{col} TEXT")
                    elif df[col].dtype.kind in 'iuf':  # Integer, unsigned int, or float
                        columns.append(f"{col} NUMERIC")
                    else:
                        columns.append(f"{col} TEXT")
                
                create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
                cursor.execute(create_sql)
                conn.commit()
                logger.info(f"Created table {table_name}")
            else:
                # Truncate existing table
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                conn.commit()
                logger.info(f"Truncated existing data from {table_name}")
            
            # Insert data in chunks
            if len(df) > 0:
                # Use execute_batch 
                try:
                    from psycopg2.extras import execute_batch
                    
                    # Prepare insert query
                    columns = df.columns
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    
                    # Convert DataFrame to list of tuples
                    records = [tuple(row) for _, row in df.iterrows()]
                    
                    # Execute batch insert
                    execute_batch(cursor, insert_sql, records, page_size=100)
                    conn.commit()
                    logger.info(f"Inserted {len(df)} rows into {table_name}")
                except ImportError:
                    # Fall back to regular executemany
                    logger.warning("psycopg2.extras.execute_batch not available, using standard executemany")
                    
                    placeholders = ', '.join(['%s'] * len(df.columns))
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
                    
                    # Insert in batches of 100 rows
                    batch_size = 100
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i:i+batch_size]
                        cursor.executemany(insert_sql, batch.values.tolist())
                        conn.commit()
                        logger.info(f"Inserted batch {i//batch_size + 1} ({len(batch)} rows) into {table_name}")
            
            logger.info(f"Successfully loaded data to {table_name}")
            return df
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error during database operations: {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Failed to load {file_path} to staging: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    finally:
        if cursor:
            cursor.close()


def load_staging_data(config, conn):
    """
    Load all source CSV files to staging tables.
    
    Args:
        config: Configuration object
        conn: Database connection
        
    Returns:
        dict: Dictionary of loaded DataFrames
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
        # Using separate connection for each to avoid transaction issues
        orders_df = ingest_csv_to_staging(
            config.get_input_path('orders.csv'), 
            conn, 
            'staging_orders', 
            orders_dtypes
        )
        
        order_items_df = ingest_csv_to_staging(
            config.get_input_path('order_item.csv'), 
            conn, 
            'staging_order_items', 
            order_items_dtypes
        )
        
        menu_items_df = ingest_csv_to_staging(
            config.get_input_path('menu_items.csv'), 
            conn, 
            'staging_menu_items', 
            menu_items_dtypes
        )
        
        # Check if all files were loaded successfully
        if orders_df is None or order_items_df is None or menu_items_df is None:
            logger.error("Failed to load one or more data files into staging")
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

def get_last_processed_date(conn):
    """
    Get the latest date processed from the daily_revenue_by_category table.
    
    Args:
        conn: Database connection
    
    Returns:
        datetime or None: Last processed date
    """
    cursor = None
    try:
        cursor = conn.cursor()
        query = "SELECT MAX(date) as last_date FROM daily_revenue_by_category"
        
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result is None or result[0] is None:
                return None
            
            # Convert to datetime
            last_date = pd.to_datetime(result[0])
            logger.info(f"Last processed date: {last_date}")
            return last_date
        except Exception as e:
            logger.warning(f"Error getting last processed date: {str(e)}")
            # Table might not exist yet
            return None
    except Exception as e:
        logger.error(f"Unexpected error getting last processed date: {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()


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