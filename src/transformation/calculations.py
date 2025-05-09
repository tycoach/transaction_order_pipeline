"""
Business metrics calculations for data pipeline.
"""
import logging
import pandas as pd
import traceback

logger = logging.getLogger(__name__)

def calculate_daily_revenue_by_category(complete_orders_df):
    """
    Calculate daily revenue by category.
    """
    try:
        logger.info("Calculating daily revenue by category")
        
        # Group by date and category
        daily_revenue_df = complete_orders_df.groupby([
            pd.Grouper(key='order_date', freq='D'),
            'category'
        ]).agg({
            'item_revenue': 'sum',
            'order_id': 'nunique'
        }).reset_index()
        
        # Rename columns
        daily_revenue_df.rename(columns={
            'order_date': 'date',
            'item_revenue': 'total_revenue',
            'order_id': 'order_count'
        }, inplace=True)
        
        logger.info(f"Calculated daily revenue for {len(daily_revenue_df)} day-category combinations")
        
        return daily_revenue_df
    except Exception as e:
        logger.error(f"Error calculating daily revenue: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def identify_top_selling_items(complete_orders_df, top_n=None):
    """
    Identify top selling items based on quantity and revenue.
    """
    try:
        logger.info("Identifying top selling items")
        
        # Group by item
        top_items_df = complete_orders_df.groupby([
            'item_id',
            'item_name',
            'category'
        ]).agg({
            'quantity': 'sum',
            'item_revenue': 'sum'
        }).reset_index()
        
        # Rename columns
        top_items_df.rename(columns={
            'quantity': 'total_quantity_sold',
            'item_revenue': 'total_revenue'
        }, inplace=True)
        
        # Sort by quantity sold
        top_items_df = top_items_df.sort_values('total_quantity_sold', ascending=False)
        
        # Limit to top N if specified
        if top_n is not None and top_n > 0:
            top_items_df = top_items_df.head(top_n)
        
        logger.info(f"Identified {len(top_items_df)} top selling items")
        
        return top_items_df
    except Exception as e:
        logger.error(f"Error identifying top selling items: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def calculate_category_metrics(complete_orders_df):
    """
    Calculate metrics by category.
    """
    try:
        logger.info("Calculating category metrics")
        
        # Group by category
        category_metrics = complete_orders_df.groupby('category').agg({
            'order_id': 'nunique',
            'item_revenue': 'sum',
            'quantity': 'sum',
            'item_id': 'nunique'
        }).reset_index()
        
        # Rename columns
        category_metrics.rename(columns={
            'order_id': 'order_count',
            'item_revenue': 'total_revenue',
            'quantity': 'total_items_sold',
            'item_id': 'unique_items_count'
        }, inplace=True)
        
        # Calculate average revenue per order
        category_metrics['avg_revenue_per_order'] = (
            category_metrics['total_revenue'] / category_metrics['order_count']
        )
        
        # Calculate average items per order
        category_metrics['avg_items_per_order'] = (
            category_metrics['total_items_sold'] / category_metrics['order_count']
        )
        
        # Sort by total revenue
        category_metrics = category_metrics.sort_values('total_revenue', ascending=False)
        
        logger.info(f"Calculated metrics for {len(category_metrics)} categories")
        
        return category_metrics
    except Exception as e:
        logger.error(f"Error calculating category metrics: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# def verify_totals(orders_df, order_items_df):
#     """
#     Verify that total amounts in orders match calculated totals from order items.
#     """
#     try:
#         logger.info("Verifying order total amounts")
        
#         # Calculate total amount from order items
#         order_totals = order_items_df.groupby('order_id').apply(
#             lambda x: (x['quantity'] * x['unit_price']).sum()
#         ).reset_index(name='calculated_total')
        
#         # Merge with orders
#         merged_totals = pd.merge(
#             orders_df[['order_id', 'total_amount']],
#             order_totals,
#             on='order_id',
#             how='inner'
#         )
        
#         # Calculate difference
#         merged_totals['difference'] = (
#             merged_totals['total_amount'] - merged_totals['calculated_total']
#         ).abs()
        
#         # Consider differences greater than 0.01 as discrepancies (accounting for float precision)
#         discrepancies = merged_totals[merged_totals['difference'] > 0.01]
        
#         if len(discrepancies) > 0:
#             logger.warning(f"Found {len(discrepancies)} orders with total amount discrepancies")
#         else:
#             logger.info("All order total amounts match calculated totals")
        
#         return discrepancies
#     except Exception as e:
#         logger.error(f"Error verifying order totals: {str(e)}")
#         logger.error(traceback.format_exc())
#         raise

def verify_totals(orders_df, order_items_df):
    """
    Verify that total amounts in orders match calculated totals from order items.
    """
    try:
        logger.info("Verifying order total amounts")
        
        # Check and print available columns in orders_df for debugging
        logger.info(f"Available columns in orders_df: {orders_df.columns.tolist()}")
        
        # Find the correct column name for total_amount
        total_amount_column = None
        possible_names = ['total_amount', 'total amount', 'totalamount', 'total', 'amount', 'price']
        
        for col in orders_df.columns:
            # Check for exact matches first
            if col.lower() == 'total_amount':
                total_amount_column = col
                break
            # Then check for similar names
            for name in possible_names:
                if name in col.lower():
                    total_amount_column = col
                    break
            if total_amount_column:
                break
        
        if not total_amount_column:
            logger.error(f"Could not find a total amount column in orders data")
            return pd.DataFrame(columns=['order_id', 'total_amount', 'calculated_total', 'difference'])
        
        logger.info(f"Using column '{total_amount_column}' for total amount verification")
        
        # Calculate total amount from order items
        order_totals = order_items_df.groupby('order_id').apply(
            lambda x: (x['quantity'] * x['unit_price']).sum()
        ).reset_index(name='calculated_total')
        
        # Make a copy of orders_df with the needed columns
        orders_copy = orders_df[['order_id']].copy()
        # Add the total_amount column to our copy with the detected name
        orders_copy['total_amount'] = orders_df[total_amount_column]
        
        # Merge with orders
        merged_totals = pd.merge(
            orders_copy,
            order_totals,
            on='order_id',
            how='inner'
        )
        
        # Calculate difference
        merged_totals['difference'] = (
            merged_totals['total_amount'] - merged_totals['calculated_total']
        ).abs()
        
        # Consider differences greater than 0.01 as discrepancies (accounting for float precision)
        discrepancies = merged_totals[merged_totals['difference'] > 0.01]
        
        if len(discrepancies) > 0:
            logger.warning(f"Found {len(discrepancies)} orders with total amount discrepancies")
        else:
            logger.info("All order total amounts match calculated totals")
        
        return discrepancies
    except Exception as e:
        logger.error(f"Error verifying order totals: {str(e)}")
        logger.error(traceback.format_exc())
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=['order_id', 'total_amount', 'calculated_total', 'difference'])