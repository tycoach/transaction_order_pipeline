"""
Data loading components for the restaurant data pipeline.
"""
import logging
import pandas as pd
import traceback
from sqlalchemy.exc import SQLAlchemyError
import os

logger = logging.getLogger(__name__)

def load_transformed_data(engine, transformed_data, incremental=False):
    """
    Load transformed data to target tables.
    """
    try:
        logger.info(f"Loading transformed data to target tables (incremental={incremental})")
        
        # Load complete orders
        if 'complete_orders' in transformed_data:
            _load_table(
                engine, 
                transformed_data['complete_orders'], 
                'complete_orders', 
                incremental
            )
        
        # Load daily revenue by category
        if 'daily_revenue' in transformed_data:
            _load_table(
                engine, 
                transformed_data['daily_revenue'], 
                'daily_revenue_by_category', 
                incremental
            )
        
        # Load top selling items
        if 'top_items' in transformed_data:
            if incremental:
                _load_top_items_incremental(engine, transformed_data['top_items'])
            else:
                _load_table(
                    engine, 
                    transformed_data['top_items'], 
                    'top_selling_items', 
                    False
                )
        
        # Load category metrics if available
        if 'category_metrics' in transformed_data:
            _load_table(
                engine, 
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

def _load_table(engine, df, table_name, incremental=False):
    """
    Load DataFrame to a database table.
    """
    if df is None or len(df) == 0:
        logger.warning(f"No data to load for table {table_name}")
        return
    
    try:
        if_exists = 'append' if incremental else 'replace'
        
        logger.info(f"Loading {len(df)} rows to {table_name} (if_exists={if_exists})")
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logger.info(f"Successfully loaded data to {table_name}")
    except SQLAlchemyError as e:
        logger.error(f"Database error loading to {table_name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading to {table_name}: {str(e)}")
        raise

def _load_top_items_incremental(engine, new_top_items):
    """
    Incrementally update top selling items.
    """
    try:
        # Get existing top items
        try:
            existing_top_items = pd.read_sql('SELECT * FROM top_selling_items', engine)
        except:
            # If table doesn't exist, just load the new data
            logger.info("No existing top_selling_items table found, creating new")
            new_top_items.to_sql('top_selling_items', engine, if_exists='replace', index=False)
            return
        
        # Merge with new top items
        merged_top_items = pd.concat([existing_top_items, new_top_items])
        
        # Group by item and recalculate totals
        updated_top_items = merged_top_items.groupby([
            'item_id',
            'item_name',
            'category'
        ]).agg({
            'total_quantity_sold': 'sum',
            'total_revenue': 'sum'
        }).reset_index()
        
        # Update top_selling_items table
        updated_top_items.to_sql('top_selling_items', engine, if_exists='replace', index=False)
        logger.info(f"Incrementally updated top_selling_items table with {len(new_top_items)} new records")
    except Exception as e:
        logger.error(f"Error updating top_selling_items incrementally: {str(e)}")
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