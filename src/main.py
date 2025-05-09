"""
Main pipeline orchestration for the  data pipeline.
"""
import logging
import argparse
import time
import traceback
from datetime import datetime
from config import Config
from db.engine import create_db_engine, init_db
from db.models import Base
from ingestion.loader import load_staging_data, get_last_processed_date, load_incremental_data
from transformation.joins import join_order_data, check_for_missing_relationships
from transformation.calculations import (
    calculate_daily_revenue_by_category,
    identify_top_selling_items,
    calculate_category_metrics,
    verify_totals
)
from transformation.quality import run_data_quality_checks, apply_data_fixes
from loading.writer import load_transformed_data, export_results_to_csv

logger = logging.getLogger(__name__)

def run_pipeline(config_file='config.ini', incremental=None, quality_check=None, export_csv=False):
    start_time = time.time()
    statistics = {
        'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'failed',
        'stages': {},
    }
    
    try:
        logger.info("Starting data pipeline")
        
        # Load configuration
        config = Config(config_file)
        
        # Override config settings if provided
        if incremental is not None:
            config.config['PIPELINE']['incremental'] = str(incremental).lower()
        
        if quality_check is not None:
            config.config['PIPELINE']['quality_check'] = str(quality_check).lower()
        
        # Determine if we should run in incremental mode
        run_incremental = config.is_incremental()
        run_quality_check = config.is_quality_check_enabled()
        
        logger.info(f"Pipeline mode: incremental={run_incremental}, quality_check={run_quality_check}")
        
        # Create database engine
        engine = create_db_engine(config)
        
        # Initialize database tables
        init_db(engine, Base)
        
        #  Data Ingestion
        stage_start = time.time()
        
        if run_incremental:
            # Get last processed date
            last_date = get_last_processed_date(engine)
            
            if last_date is None:
                logger.info("No previous data found, running full pipeline")
                staged_data = load_staging_data(config, engine)
                statistics['stages']['ingestion'] = {
                    'mode': 'full',
                    'rows_processed': {
                        table: len(df) for table, df in staged_data.items()
                    }
                }
            else:
                logger.info(f"Incremental mode: processing data after {last_date}")
                staged_data = load_incremental_data(config, engine, last_date)
                
                if staged_data is None:
                    logger.info("No new data to process")
                    statistics['status'] = 'success'
                    statistics['message'] = 'No new data to process'
                    statistics['duration'] = time.time() - start_time
                    return statistics
                
                statistics['stages']['ingestion'] = {
                    'mode': 'incremental',
                    'last_processed_date': str(last_date),
                    'rows_processed': {
                        table: len(df) for table, df in staged_data.items()
                    }
                }
        else:
            # Full load
            staged_data = load_staging_data(config, engine)
            statistics['stages']['ingestion'] = {
                'mode': 'full',
                'rows_processed': {
                    table: len(df) for table, df in staged_data.items()
                }
            }
        
        statistics['stages']['ingestion']['duration'] = time.time() - stage_start
        
        # ---- Data Validation & Quality Checks
        if run_quality_check:
            stage_start = time.time()
            
            # Check for missing relationships
            relationship_issues = check_for_missing_relationships(
                staged_data['orders'],
                staged_data['order_items'],
                staged_data['menu_items']
            )
            
            # Run data quality checks
            quality_results = run_data_quality_checks(staged_data)
            
            # Apply fixes if there are issues
            has_issues = any(
                isinstance(result, dict) and result.get('total_missing', 0) > 0
                for result in quality_results.get('missing_values', {}).values()
            )
            
            if has_issues:
                logger.info("Applying data quality fixes")
                staged_data = apply_data_fixes(staged_data, quality_results)
            
            statistics['stages']['quality_check'] = {
                'duration': time.time() - stage_start,
                'issues_found': has_issues,
                'fixes_applied': has_issues
            }
        
        # ---------Data Transformation
        stage_start = time.time()
        
        # Join the data
        complete_orders_df = join_order_data(
            staged_data['orders'],
            staged_data['order_items'],
            staged_data['menu_items']
        )
        
        # Calculate metrics
        daily_revenue_df = calculate_daily_revenue_by_category(complete_orders_df)
        top_items_df = identify_top_selling_items(complete_orders_df)
        category_metrics_df = calculate_category_metrics(complete_orders_df)
        
        # Check for total amount discrepancies
        discrepancies_df = verify_totals(staged_data['orders'], staged_data['order_items'])
        
        # Prepare transformed data
        transformed_data = {
            'complete_orders': complete_orders_df,
            'daily_revenue': daily_revenue_df,
            'top_items': top_items_df,
            'category_metrics': category_metrics_df
        }
        
        statistics['stages']['transformation'] = {
            'duration': time.time() - stage_start,
            'rows_generated': {
                table: len(df) for table, df in transformed_data.items()
            },
            'total_amount_discrepancies': len(discrepancies_df)
        }
        
        # -------Data Loading
        stage_start = time.time()
        
        # Load transformed data
        success = load_transformed_data(engine, transformed_data, run_incremental)
        
        statistics['stages']['loading'] = {
            'duration': time.time() - stage_start,
            'success': success,
            'mode': 'incremental' if run_incremental else 'full'
        }
        
        # Export results to CSV if requested
        if export_csv:
            exported_files = export_results_to_csv(
                transformed_data,
                config.get_output_path()
            )
            statistics['stages']['export'] = {
                'files_exported': len(exported_files),
                'file_paths': exported_files
            }
        
        statistics['status'] = 'success'
        logger.info("Data pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        logger.error(traceback.format_exc())
        statistics['status'] = 'failed'
        statistics['error'] = str(e)
    
    # Calculate total duration
    statistics['duration'] = time.time() - start_time
    
    return statistics

def main():
    """Command line entry point."""
    parser = argparse.ArgumentParser(description='Restaurant Data Pipeline')
    parser.add_argument('--config', default='config.ini', help='Path to configuration file')
    parser.add_argument('--incremental', action='store_true', help='Run in incremental mode')
    parser.add_argument('--full', action='store_true', help='Run in full load mode')
    parser.add_argument('--quality-check', action='store_true', help='Run data quality checks')
    parser.add_argument('--no-quality-check', action='store_true', help='Skip data quality checks')
    parser.add_argument('--export-csv', action='store_true', help='Export results to CSV files')
    
    args = parser.parse_args()
    
    # Determine incremental mode
    incremental = None
    if args.incremental:
        incremental = True
    elif args.full:
        incremental = False
    
    # Determine quality check mode
    quality_check = None
    if args.quality_check:
        quality_check = True
    elif args.no_quality_check:
        quality_check = False
    
    # Run the pipeline
    results = run_pipeline(
        config_file=args.config,
        incremental=incremental,
        quality_check=quality_check,
        export_csv=args.export_csv
    )
    
    # Print summary
    print("\nPipeline Execution Summary:")
    print(f"Status: {results['status']}")
    print(f"Duration: {results['duration']:.2f} seconds")
    
    if results['status'] == 'failed' and 'error' in results:
        print(f"Error: {results['error']}")
    
    for stage, stats in results.get('stages', {}).items():
        print(f"\n{stage.capitalize()} stage:")
        for key, value in stats.items():
            if key != 'rows_processed' and key != 'rows_generated' and key != 'file_paths':
                print(f"  {key}: {value}")

if __name__ == "__main__":
    main()