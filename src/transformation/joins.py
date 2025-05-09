"""
Data joining operations for the data pipeline.
"""
import logging
import pandas as pd
import traceback

logger = logging.getLogger(__name__)

def join_order_data(orders_df, order_items_df, menu_items_df):
    """
    Join orders, order_items, and menu_items tables.
    """
    try:
        logger.info("Joining orders, order_items, and menu_items tables")
        
        # Make copies to avoid modifying the original dataframes
        orders = orders_df.copy()
        order_items = order_items_df.copy()
        menu_items = menu_items_df.copy()
        
        # Convert order_date to datetime
        orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
        
        # Check for invalid dates
        invalid_dates = orders[orders['order_date'].isnull()]
        if len(invalid_dates) > 0:
            logger.warning(f"Found {len(invalid_dates)} orders with invalid dates")
            # Set invalid dates to a default date
            orders.loc[orders['order_date'].isnull(), 'order_date'] = pd.Timestamp('2000-01-01')
        
        # Join orders and order_items
        orders_items_df = pd.merge(
            order_items,
            orders,
            how='inner',
            on='order_id'
        )
        
        # Join with menu_items
        complete_orders_df = pd.merge(
            orders_items_df,
            menu_items,
            how='inner',
            on='item_id'
        )
        
        # Calculate item revenue
        complete_orders_df['item_revenue'] = complete_orders_df['quantity'] * complete_orders_df['unit_price']
        
        logger.info(f"Joined data has {len(complete_orders_df)} rows")
        
        return complete_orders_df
    
    except Exception as e:
        logger.error(f"Error joining order data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def check_for_missing_relationships(orders_df, order_items_df, menu_items_df):
    """
    Check for missing relationships between the datasets.
    """
    try:
        # Check for order items with no corresponding order
        order_ids_in_orders = set(orders_df['order_id'])
        order_ids_in_items = set(order_items_df['order_id'])
        orphaned_items = order_ids_in_items - order_ids_in_orders
        
        # Check for order items with no corresponding menu item
        item_ids_in_menu = set(menu_items_df['item_id'])
        item_ids_in_orders = set(order_items_df['item_id'])
        unknown_items = item_ids_in_orders - item_ids_in_menu
        
        # Check for orders with no items
        orders_with_no_items = order_ids_in_orders - order_ids_in_items
        
        # Check for menu items that have never been ordered
        unused_menu_items = item_ids_in_menu - item_ids_in_orders
        
        results = {
            'orphaned_items_count': len(orphaned_items),
            'orphaned_items': list(orphaned_items)[:10],  # Limit to first 10 for logging purpose
            'unknown_items_count': len(unknown_items),
            'unknown_items': list(unknown_items)[:10],
            'orders_with_no_items_count': len(orders_with_no_items),
            'orders_with_no_items': list(orders_with_no_items)[:10],
            'unused_menu_items_count': len(unused_menu_items),
            'unused_menu_items': list(unused_menu_items)[:10]
        }
        
        # Log the issues found
        if results['orphaned_items_count'] > 0:
            logger.warning(f"Found {results['orphaned_items_count']} order items with no corresponding order")
            
        if results['unknown_items_count'] > 0:
            logger.warning(f"Found {results['unknown_items_count']} order items with unknown menu items")
            
        if results['orders_with_no_items_count'] > 0:
            logger.warning(f"Found {results['orders_with_no_items_count']} orders with no items")
            
        if results['unused_menu_items_count'] > 0:
            logger.info(f"Found {results['unused_menu_items_count']} menu items that have never been ordered")
        
        return results
        
    except Exception as e:
        logger.error(f"Error checking for missing relationships: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'error': str(e),
            'orphaned_items_count': 0,
            'unknown_items_count': 0,
            'orders_with_no_items_count': 0,
            'unused_menu_items_count': 0
        }