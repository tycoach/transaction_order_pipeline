"""
Data quality checks for the restaurant data pipeline.
"""
import logging
import pandas as pd
import numpy as np
import traceback

logger = logging.getLogger(__name__)

def run_data_quality_checks(data_frames):
    """
    Run a series of data quality checks on the input data.
    
    """
    try:
        logger.info("Running data quality checks")
        
        quality_results = {}
        
        # Run individual checks
        quality_results['missing_values'] = check_missing_values(data_frames)
        quality_results['duplicate_keys'] = check_duplicate_keys(data_frames)
        quality_results['value_ranges'] = check_value_ranges(data_frames)
        quality_results['referential_integrity'] = check_referential_integrity(data_frames)
        
        # Log summary of issues
        total_issues = sum(
            issue_count for check_result in quality_results.values() 
            for _, issue_count in check_result.items() if isinstance(issue_count, int)
        )
        
        if total_issues > 0:
            logger.warning(f"Found a total of {total_issues} data quality issues")
        else:
            logger.info("All data quality checks passed")
        
        return quality_results
    except Exception as e:
        logger.error(f"Error running data quality checks: {str(e)}")
        logger.error(traceback.format_exc())
        return {'error': str(e)}

def check_missing_values(data_frames):
    """
    Check for missing values in each DataFrame.
    """
    results = {}
    
    for table_name, df in data_frames.items():
        # Get count of missing values by column
        missing_by_column = df.isnull().sum()
        total_missing = missing_by_column.sum()
        
        # Only include columns with missing values
        missing_columns = missing_by_column[missing_by_column > 0].to_dict()
        
        results[table_name] = {
            'total_missing': total_missing,
            'missing_columns': missing_columns
        }
        
        if total_missing > 0:
            logger.warning(f"Table '{table_name}' has {total_missing} missing values")
            for col, count in missing_columns.items():
                logger.warning(f"  - Column '{col}': {count} missing values")
    
    return results

def check_duplicate_keys(data_frames):
    """
    Check for duplicate primary keys in each DataFrame.
    """
    results = {}
    
    # Define primary keys for each table
    primary_keys = {
        'orders': ['order_id'],
        'order_items': ['order_id', 'item_id'],  # Composite key
        'menu_items': ['item_id']
    }
    
    for table_name, df in data_frames.items():
        if table_name in primary_keys:
            pk_columns = primary_keys[table_name]
            
            # Skip if not all primary key columns exist
            if not all(col in df.columns for col in pk_columns):
                results[table_name] = {
                    'duplicate_count': 0,
                    'error': f"Not all primary key columns {pk_columns} exist in table"
                }
                continue
            
            # Check for duplicates
            duplicates = df[df.duplicated(subset=pk_columns, keep=False)]
            duplicate_count = len(duplicates)
            
            results[table_name] = {
                'duplicate_count': duplicate_count,
                'duplicate_keys': duplicates[pk_columns].head(10).values.tolist() if duplicate_count > 0 else []
            }
            
            if duplicate_count > 0:
                logger.warning(f"Table '{table_name}' has {duplicate_count} duplicate primary keys")
        else:
            results[table_name] = {'duplicate_count': 0, 'error': 'No primary key defined'}
    
    return results

def check_value_ranges(data_frames):
    """
    Check for values outside of expected ranges.
    """
    results = {}
    
    # Define expected value ranges and conditions
    range_checks = {
        'orders': {
            'total_amount': lambda x: x >= 0,  # Total amount should be non-negative
        },
        'order_items': {
            'quantity': lambda x: x > 0,  # Quantity should be positive
            'unit_price': lambda x: x >= 0,  # Unit price should be non-negative
        }
    }
    
    for table_name, df in data_frames.items():
        table_results = {}
        
        if table_name in range_checks:
            for column, condition in range_checks[table_name].items():
                if column in df.columns:
                    # Apply condition and count failures
                    invalid_mask = ~df[column].apply(condition)
                    invalid_count = invalid_mask.sum()
                    
                    table_results[column] = {
                        'invalid_count': invalid_count,
                        'invalid_examples': df.loc[invalid_mask, column].head(5).tolist() if invalid_count > 0 else []
                    }
                    
                    if invalid_count > 0:
                        logger.warning(f"Table '{table_name}' has {invalid_count} invalid values in column '{column}'")
                else:
                    table_results[column] = {'error': f"Column '{column}' not found in table"}
        
        results[table_name] = table_results
    
    return results

def check_referential_integrity(data_frames):
    """
    Check referential integrity between tables.
    """
    results = {}
    
    # Define foreign key relationships
    foreign_keys = [
        {'table': 'order_items', 'key': 'order_id', 'ref_table': 'orders', 'ref_key': 'order_id'},
        {'table': 'order_items', 'key': 'item_id', 'ref_table': 'menu_items', 'ref_key': 'item_id'}
    ]
    
    for fk in foreign_keys:
        # Check if all required tables and columns exist
        if (fk['table'] in data_frames and fk['ref_table'] in data_frames and
            fk['key'] in data_frames[fk['table']].columns and
            fk['ref_key'] in data_frames[fk['ref_table']].columns):
            
            # Get all foreign key values
            fk_values = set(data_frames[fk['table']][fk['key']].unique())
            
            # Get all reference key values
            ref_values = set(data_frames[fk['ref_table']][fk['ref_key']].unique())
            
            # Find orphaned values (foreign keys without matching reference keys)
            orphaned = fk_values - ref_values
            orphaned_count = len(orphaned)
            
            relationship = f"{fk['table']}.{fk['key']} -> {fk['ref_table']}.{fk['ref_key']}"
            results[relationship] = {
                'orphaned_count': orphaned_count,
                'orphaned_examples': list(orphaned)[:10] if orphaned_count > 0 else []
            }
            
            if orphaned_count > 0:
                logger.warning(
                    f"Referential integrity issue: {orphaned_count} values in "
                    f"{fk['table']}.{fk['key']} have no matching {fk['ref_table']}.{fk['ref_key']}"
                )
        else:
            relationship = f"{fk['table']}.{fk['key']} -> {fk['ref_table']}.{fk['ref_key']}"
            results[relationship] = {'error': 'Missing table or column'}
    
    return results

# def apply_data_fixes(data_frames, quality_results):
#     """
#     Apply fixes to data quality issues.
#     This function modifies the input DataFrames based on the results of the quality checks.
#     It handles missing values and orphaned foreign keys.
#     """
#     try:
#         logger.info("Applying data quality fixes")
        
#         # Create deep copies to avoid modifying originals
#         fixed_data = {
#             table: df.copy() for table, df in data_frames.items()
#         }
        
#         # Fix missing values
#         for table, result in quality_results.get('missing_values', {}).items():
#             if table in fixed_data and result['total_missing'] > 0:
#                 logger.info(f"Fixing {result['total_missing']} missing values in '{table}'")
                
#                 df = fixed_data[table]
                
#                 # Apply different fixes based on column types
#                 for col in result['missing_columns']:
#                     if col in df.columns:
#                         if pd.api.types.is_numeric_dtype(df[col]):
#                             # Fill numeric columns with 0
#                             df[col].fillna(0, inplace=True)
#                         elif pd.api.types.is_datetime64_dtype(df[col]):
#                             # Fill date columns with a default date
#                             df[col].fillna(pd.Timestamp('2000-01-01'), inplace=True)
#                         else:
#                             # Fill string columns with 'Unknown'
#                             df[col].fillna('Unknown', inplace=True)
        
#         # Fix orphaned foreign keys by filtering them out
#         for rel, result in quality_results.get('referential_integrity', {}).items():
#             if isinstance(result, dict) and result.get('orphaned_count', 0) > 0:
#                 # Parse relationship string
#                 parts = rel.split(' -> ')
#                 if len(parts) == 2:
#                     fk_table_col = parts[0].split('.')
#                     if len(fk_table_col) == 2:
#                         fk_table, fk_col = fk_table_col
                        
#                         if fk_table in fixed_data and fk_col in fixed_data[fk_table].columns:
#                             orphaned_values = set(result.get('orphaned_examples', []))
#                             if orphaned_values:
#                                 logger.info(f"Filtering out {len(orphaned_values)} orphaned foreign keys from '{fk_table}'")
                                
#                                 # Filter out rows with orphaned foreign keys
#                                 fixed_data[fk_table] = fixed_data[fk_table][
#                                     ~fixed_data[fk_table][fk_col].isin(orphaned_values)
#                                 ]
        
#         return fixed_data
#     except Exception as e:
#         logger.error(f"Error applying data quality fixes: {str(e)}")
#         logger.error(traceback.format_exc())
#         # Return original data if fixes fail
#         return data_frames

def apply_data_fixes(data_frames, quality_results):
    """
    Apply fixes to data quality issues.

    """
    try:
        logger.info("Applying data quality fixes")
        
        # Create deep copies to avoid modifying originals
        fixed_data = {
            table: df.copy() for table, df in data_frames.items()
        }
        
        #  Fix missing values
        for table, result in quality_results.get('missing_values', {}).items():
            if table in fixed_data and result['total_missing'] > 0:
                logger.info(f"Fixing {result['total_missing']} missing values in '{table}'")
                
                df = fixed_data[table]
                
                # Apply different fixes based on column types
                for col in result['missing_columns']:
                    if col in df.columns:
                        if pd.api.types.is_numeric_dtype(df[col]):
                            # Fill numeric columns with 0
                            df[col].fillna(0, inplace=True)
                        elif pd.api.types.is_datetime64_dtype(df[col]):
                            # Fill date columns with a default date
                            df[col].fillna(pd.Timestamp('2000-01-01'), inplace=True)
                        else:
                            # Fill string columns with 'Unknown'
                            df[col].fillna('Unknown', inplace=True)
        
        #  Fix duplicate primary keys
        for table, result in quality_results.get('duplicate_keys', {}).items():
            if table in fixed_data and result.get('duplicate_count', 0) > 0:
                logger.info(f"Removing {result['duplicate_count']} duplicate keys from '{table}'")
                
                # Determine primary key column(s)
                pk_columns = []
                if table == 'orders':
                    pk_columns = ['order_id']
                elif table == 'order_items':
                    # Composite key
                    pk_columns = ['order_id', 'item_id']
                elif table == 'menu_items':
                    pk_columns = ['item_id']
                
                if pk_columns:
                    # Keep the first occurrence of each primary key
                    fixed_data[table] = fixed_data[table].drop_duplicates(
                        subset=pk_columns, 
                        keep='first'
                    )
        
        #  Fix invalid values
        for table, column_results in quality_results.get('value_ranges', {}).items():
            if table in fixed_data:
                for column, result in column_results.items():
                    if column in fixed_data[table].columns and result.get('invalid_count', 0) > 0:
                        logger.info(f"Fixing {result['invalid_count']} invalid values in '{table}.{column}'")
                        
                        # Apply fixes based on column
                        if table == 'order_items' and column == 'quantity':
                            # Replace non-positive quantities with 1
                            mask = fixed_data[table][column] <= 0
                            if mask.any():
                                fixed_data[table].loc[mask, column] = 1
                        
                        elif table == 'order_items' and column == 'unit_price':
                            # Replace negative prices with absolute value
                            mask = fixed_data[table][column] < 0
                            if mask.any():
                                fixed_data[table].loc[mask, column] = fixed_data[table].loc[mask, column].abs()
                        
                        elif table == 'orders' and column == 'total_amount':
                            # Handle negative total amounts
                            mask = fixed_data[table][column] < 0
                            if mask.any():
                                fixed_data[table].loc[mask, column] = fixed_data[table].loc[mask, column].abs()
        
        #  Fix orphaned foreign keys by filtering them out
        for rel, result in quality_results.get('referential_integrity', {}).items():
            if isinstance(result, dict) and result.get('orphaned_count', 0) > 0:
                # Parse relationship string
                parts = rel.split(' -> ')
                if len(parts) == 2:
                    fk_table_col = parts[0].split('.')
                    if len(fk_table_col) == 2:
                        fk_table, fk_col = fk_table_col
                        
                        if fk_table in fixed_data and fk_col in fixed_data[fk_table].columns:
                            orphaned_values = set(result.get('orphaned_examples', []))
                            if orphaned_values:
                                logger.info(f"Filtering out orphaned foreign keys from '{fk_table}.{fk_col}'")
                                
                                # Filter out rows with orphaned foreign keys
                                fixed_data[fk_table] = fixed_data[fk_table][
                                    ~fixed_data[fk_table][fk_col].isin(orphaned_values)
                                ]
        
        #  Fix invalid dates in orders table
        if 'orders' in fixed_data and 'order_date' in fixed_data['orders'].columns:
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_dtype(fixed_data['orders']['order_date']):
                # Save original values before conversion
                fixed_data['orders']['order_date_original'] = fixed_data['orders']['order_date']
                # Convert to datetime
                fixed_data['orders']['order_date'] = pd.to_datetime(
                    fixed_data['orders']['order_date'], 
                    errors='coerce'
                )
            
            # Check for invalid dates
            invalid_dates = fixed_data['orders'][fixed_data['orders']['order_date'].isnull()]
            if len(invalid_dates) > 0:
                logger.info(f"Fixing {len(invalid_dates)} invalid dates in orders table")
                
                # If we have original values, try different formats
                if 'order_date_original' in fixed_data['orders'].columns:
                    for idx in invalid_dates.index:
                        if idx in fixed_data['orders'].index:
                            date_str = fixed_data['orders'].loc[idx, 'order_date_original']
                            if pd.notna(date_str):
                                # Try different date formats
                                for fmt in ['%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y']:
                                    try:
                                        fixed_data['orders'].loc[idx, 'order_date'] = pd.to_datetime(date_str, format=fmt)
                                        break
                                    except:
                                        continue
                
                # Any remaining invalid dates get set to a default
                still_invalid = fixed_data['orders'][fixed_data['orders']['order_date'].isnull()]
                if len(still_invalid) > 0:
                    fixed_data['orders'].loc[fixed_data['orders']['order_date'].isnull(), 'order_date'] = pd.Timestamp('2000-01-01')
                
                # Remove the temporary column if it exists
                if 'order_date_original' in fixed_data['orders'].columns:
                    fixed_data['orders'] = fixed_data['orders'].drop(columns=['order_date_original'])
        
        #  Standardize column names across all DataFrames
        # This helps with issues like the 'total_amount' problem
        column_mapping = {
            'orders': {
                'orderid': 'order_id',
                'customerid': 'customer_id', 
                'customer id': 'customer_id',
                'orderdate': 'order_date',
                'order date': 'order_date',
                'totalamount': 'total_amount',
                'total': 'total_amount',
                'amount': 'total_amount'
            },
            'order_items': {
                'orderid': 'order_id',
                'order id': 'order_id',
                'itemid': 'item_id',
                'item id': 'item_id',
                'unitprice': 'unit_price',
                'unit price': 'unit_price',
                'price': 'unit_price'
            },
            'menu_items': {
                'itemid': 'item_id',
                'item id': 'item_id',
                'itemname': 'item_name',
                'item name': 'item_name',
                'name': 'item_name'
            }
        }
        
        for table, df in fixed_data.items():
            if table in column_mapping:
                # Create a mapping for this specific DataFrame
                df_mapping = {}
                for old_col, new_col in column_mapping[table].items():
                    # Check if old column exists and new column doesn't
                    if old_col in df.columns and new_col not in df.columns:
                        df_mapping[old_col] = new_col
                
                # Apply the mapping if needed
                if df_mapping:
                    logger.info(f"Standardizing column names in '{table}': {df_mapping}")
                    fixed_data[table] = fixed_data[table].rename(columns=df_mapping)
        
        # Log a summary of changes
        for table, original_df in data_frames.items():
            if table in fixed_data:
                fixed_df = fixed_data[table]
                rows_diff = len(fixed_df) - len(original_df)
                if rows_diff != 0:
                    change_type = "removed" if rows_diff < 0 else "added"
                    logger.info(f"{abs(rows_diff)} rows {change_type} in '{table}'")
        
        logger.info("Data quality fixes applied successfully")
        return fixed_data
    except Exception as e:
        logger.error(f"Error applying data quality fixes: {str(e)}")
        logger.error(traceback.format_exc())
        # Return original data if fixes fail
        return data_frames