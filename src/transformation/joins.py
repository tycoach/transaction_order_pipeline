"""
Data joining operations for the data pipeline.
"""
import logging
import pandas as pd
import traceback

logger = logging.getLogger(__name__)

def join_order_data(orders, order_items, menu_items):
    """
    Join orders, order items, and menu items into a single DataFrame.
    
    """
    try:
        logger.info("Joining orders, order_items, and menu_items tables")
        
        # Clean column names by stripping whitespace
        orders = orders.rename(columns=lambda x: x.strip() if isinstance(x, str) else x)
        order_items = order_items.rename(columns=lambda x: x.strip() if isinstance(x, str) else x)
        menu_items = menu_items.rename(columns=lambda x: x.strip() if isinstance(x, str) else x)
        
        # Log column names after cleaning
        logger.info(f"Orders columns: {orders.columns.tolist()}")
        logger.info(f"Order items columns: {order_items.columns.tolist()}")
        logger.info(f"Menu items columns: {menu_items.columns.tolist()}")
        
        # Convert order_date to datetime
        try:
            orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
            
            # Handle invalid dates
            invalid_dates = orders[orders['order_date'].isnull()]
            if len(invalid_dates) > 0:
                logger.warning(f"Found {len(invalid_dates)} orders with invalid dates")
                
                # Try to fix dates by trying multiple formats
                for idx in invalid_dates.index:
                    original_date = orders.loc[idx, 'order_date_original'] if 'order_date_original' in orders.columns else orders.loc[idx, 'order_date']
                    
                    # Try different date formats
                    for date_format in ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y']:
                        try:
                            orders.loc[idx, 'order_date'] = pd.to_datetime(original_date, format=date_format)
                            break
                        except:
                            continue
                
                # Set remaining invalid dates to a default
                still_invalid = orders[orders['order_date'].isnull()]
                if len(still_invalid) > 0:
                    logger.warning(f"Setting {len(still_invalid)} orders with still invalid dates to default date")
                    orders.loc[orders['order_date'].isnull(), 'order_date'] = pd.Timestamp('2000-01-01')
        except Exception as e:
            logger.error(f"Error converting dates: {str(e)}")
            # Create a dummy order_date column if conversion fails
            orders['order_date'] = pd.Timestamp('2000-01-01')
        
        # First join order_items with menu_items
        items_with_details = pd.merge(
            order_items,
            menu_items,
            on='item_id',
            how='left'
        )
        
        # Check for items with no matching menu item
        unknown_items = items_with_details[items_with_details['item_name'].isnull()]
        if len(unknown_items) > 0:
            logger.warning(f"Found {len(unknown_items)} order items with unknown menu items")
            
            # Fill in missing menu item details
            items_with_details.loc[items_with_details['item_name'].isnull(), 'item_name'] = 'Unknown Item'
            items_with_details.loc[items_with_details['category'].isnull(), 'category'] = 'Unknown'
            items_with_details.loc[items_with_details['description'].isnull(), 'description'] = 'Unknown'
        
        # Then join with orders
        complete_orders = pd.merge(
            orders,
            items_with_details,
            on='order_id',
            how='inner'  # Only include orders with items
        )
        
        # Calculate revenue per item
        complete_orders['item_revenue'] = complete_orders['quantity'] * complete_orders['unit_price']
        
        # Check for orders with no items
        orders_with_no_items = set(orders['order_id']) - set(complete_orders['order_id'])
        if orders_with_no_items:
            logger.warning(f"Found {len(orders_with_no_items)} orders with no items")
        
        # Check for orphaned order items
        order_items_with_no_order = set(order_items['order_id']) - set(orders['order_id'])
        if order_items_with_no_order:
            logger.warning(f"Found {len(order_items_with_no_order)} order items with no corresponding order")
        
        logger.info(f"Joined data has {len(complete_orders)} rows")
        return complete_orders
    
    except Exception as e:
        logger.error(f"Error joining data: {str(e)}")
        logger.error(traceback.format_exc())
        # Return empty DataFrame with basic columns
        return pd.DataFrame(columns=[
            'order_id', 'customer_id', 'order_date', 'item_id', 
            'item_name', 'category', 'quantity', 'unit_price', 'item_revenue'
        ])
    

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