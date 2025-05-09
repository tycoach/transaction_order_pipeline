"""
Data loading components for the restaurant data pipeline.
"""
import logging
import pandas as pd
import traceback
from sqlalchemy.exc import SQLAlchemyError
import os

logger = logging.getLogger(__name__)

def load_transformed_data(conn, transformed_data, incremental=False):
    """
    Load transformed data to target tables.
    
    Args:
        conn: Database connection
        transformed_data (dict): Dictionary of transformed DataFrames
        incremental (bool): If True, use incremental loading
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Loading transformed data to target tables (incremental={incremental})")
        
        # Load complete orders
        if 'complete_orders' in transformed_data:
            _load_table(
                conn, 
                transformed_data['complete_orders'], 
                'complete_orders', 
                incremental
            )
        
        # Load daily revenue by category
        if 'daily_revenue' in transformed_data:
            _load_table(
                conn, 
                transformed_data['daily_revenue'], 
                'daily_revenue_by_category', 
                incremental
            )
        
        # Load top selling items
        if 'top_items' in transformed_data:
            if incremental:
                _load_top_items_incremental(conn, transformed_data['top_items'])
            else:
                _load_table(
                    conn, 
                    transformed_data['top_items'], 
                    'top_selling_items', 
                    False
                )
        
        # Load category metrics if available
        if 'category_metrics' in transformed_data:
            _load_table(
                conn, 
                transformed_data['category_metrics'], 
                'category_metrics', 
                False  # Always replace for metrics
            )
        
        logger.info("Data loading completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error in data loading: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def _load_table(conn, df, table_name, incremental=False):
    """
    Load DataFrame to a database table with proper error handling.
    
    Args:
        conn: Database connection
        df (DataFrame): Data to load
        table_name (str): Target table name
        incremental (bool): If True, append data; otherwise replace
    """
    if df is None or len(df) == 0:
        logger.warning(f"No data to load for table {table_name}")
        return
    
    cursor = None
    try:
        # Create a new cursor for this operation
        cursor = conn.cursor()
        
        # Check if the table exists
        try:
            cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            table_exists = True
        except Exception:
            table_exists = False
            # Important: roll back the transaction after exception
            conn.rollback()
        
        # Prepare table schema
        if not table_exists:
            logger.info(f"Table {table_name} doesn't exist. Creating it.")
            
            # Determine column types based on DataFrame
            column_definitions = []
            for col in df.columns:
                # Special handling for ID columns
                if col == 'id':
                    column_definitions.append(f"{col} SERIAL PRIMARY KEY")
                elif col == 'order_id' and table_name == 'complete_orders':
                    column_definitions.append(f"{col} INTEGER")
                elif col == 'item_id' and (table_name == 'top_selling_items' or table_name == 'complete_orders'):
                    column_definitions.append(f"{col} TEXT")
                # Date columns
                elif 'date' in col.lower():
                    column_definitions.append(f"{col} DATE")
                # Numeric columns
                elif df[col].dtype.kind in 'iuf':  # Integer, unsigned int, or float
                    column_definitions.append(f"{col} NUMERIC")
                # Default to text
                else:
                    column_definitions.append(f"{col} TEXT")
            
            # Add unique constraints if needed
            unique_constraint = ""
            if table_name == 'daily_revenue_by_category':
                unique_constraint = ", UNIQUE(date, category)"
            elif table_name == 'top_selling_items':
                unique_constraint = ", UNIQUE(item_id)"
            elif table_name == 'category_metrics':
                unique_constraint = ", UNIQUE(category)"
            
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(column_definitions)}{unique_constraint}
            )
            """
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info(f"Created table {table_name}")
        elif not incremental:
            # If table exists and not in incremental mode, truncate it
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
            logger.info(f"Truncated table {table_name}")
        
        # Insert data
        if len(df) > 0:
            from psycopg2.extras import execute_batch
            
            # Prepare insert query
            cols = df.columns
            placeholders = ', '.join(['%s'] * len(cols))
            
            if incremental and table_name in ['daily_revenue_by_category', 'top_selling_items', 'category_metrics']:
                # For tables with unique constraints in incremental mode, use ON CONFLICT
                conflict_action = ""
                if table_name == 'daily_revenue_by_category':
                    conflict_action = """
                    ON CONFLICT (date, category) 
                    DO UPDATE SET
                        total_revenue = EXCLUDED.total_revenue,
                        order_count = EXCLUDED.order_count
                    """
                elif table_name == 'top_selling_items':
                    conflict_action = """
                    ON CONFLICT (item_id) 
                    DO UPDATE SET
                        total_quantity_sold = EXCLUDED.total_quantity_sold,
                        total_revenue = EXCLUDED.total_revenue
                    """
                elif table_name == 'category_metrics':
                    conflict_action = """
                    ON CONFLICT (category) 
                    DO UPDATE SET
                        order_count = EXCLUDED.order_count,
                        total_revenue = EXCLUDED.total_revenue,
                        total_items_sold = EXCLUDED.total_items_sold,
                        unique_items_count = EXCLUDED.unique_items_count,
                        avg_revenue_per_order = EXCLUDED.avg_revenue_per_order,
                        avg_items_per_order = EXCLUDED.avg_items_per_order
                    """
                
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(cols)})
                    VALUES ({placeholders})
                    {conflict_action}
                """
            else:
                # Regular insert for non-incremental or tables without unique constraints
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(cols)})
                    VALUES ({placeholders})
                """
            
            # Convert DataFrame to list of tuples
            records = [tuple(row) for _, row in df.iterrows()]
            
            # Insert in batches
            batch_size = 100
            total_rows = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                execute_batch(cursor, insert_query, batch, page_size=100)
                conn.commit()
                total_rows += len(batch)
                if len(records) > batch_size:
                    logger.info(f"Inserted batch {i//batch_size + 1} ({len(batch)} rows) into {table_name}")
            
            logger.info(f"Successfully loaded {total_rows} rows to {table_name}")
        else:
            logger.warning(f"No rows to insert into {table_name}")
        
    except Exception as e:
        logger.error(f"Error loading to {table_name}: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

def _load_top_items_incremental(conn, new_top_items):
    """
    Incrementally update top selling items.

    """
    try:
        # Simply use the regular load function with incremental flag
        _load_table(conn, new_top_items, 'top_selling_items', True)
    except Exception as e:
        logger.error(f"Error incrementally loading top_selling_items: {str(e)}")
        if conn:
            conn.rollback()
        raise


def export_results_to_csv(transformed_data, output_dir):
    """
    Export transformed data to CSV files.

    """
    try:
        
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        exported_files = {}
        
        # Export each DataFrame to CSV
        for name, df in transformed_data.items():
            if df is not None and len(df) > 0:
                file_path = os.path.join(output_dir, f"{name}.csv")
                df.to_csv(file_path, index=False)
                exported_files[name] = file_path
                logger.info(f"Exported {len(df)} rows to {file_path}")
        
        return exported_files
    except Exception as e:
        logger.error(f"Error exporting results to CSV: {str(e)}")
        logger.error(traceback.format_exc())
        return {}