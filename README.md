

# Restaurant Data Pipeline - Project Summary

## Overview

This project implements a modular, scalable data pipeline for processing restaurant order data. The pipeline ingests data from CSV files, performs various transformations to extract business insights, and loads the results into a database.

## Key Features

- **Modular Architecture**: Organized into clear components for ingestion, transformation, and loading
- **Data Quality Checking**: Built-in validation and error handling for data issues
- **Incremental Loading**: Support for processing only new data
- **Flexible Database Support**: Works with SQLite, PostgreSQL, MySQL, and SQL Server
- **CI/CD automation**: pipeline automation with GIT action
- **Documentation**: Clear instructions and explanations

## Project Structure

The project follows a modular structure that separates concerns and promotes maintainability:

```
transaction_data_pipeline/
----.github
     -- workflow
     - data_pipeline.yml
│
├── src/                       # Source code
│   ├── config.py              # Configuration handling
│   ├── db/                    # Database related code
│   │   ├── models.py          # Database models/schemas
│   │   └── engine.py          # Database connection handling
│   │
│   ├── ingestion/             # Data ingestion components
│   │   └── loader.py          # Loading data from CSVs to staging
│   │
│   ├── transformation/        # Data transformation components
│   │   ├── joins.py           # Joining datasets
│   │   ├── calculations.py    # Business metrics calculations
│   │   └── quality.py         # Data quality checks
│   │
│   ├── loading/               # Data loading components
│   │   └── writer.py          # Writing transformed data to target tables
│   │
│   └── main.py            # Main pipeline orchestration
│


## Pipeline Workflow

1. **Data Ingestion**
   - Load CSV files into staging tables
   - Perform initial data validation
   - Support incremental processing based on date

2. **Data Transformation**
   - Join orders, order items, and menu items data
   - Calculate daily revenue by category
   - Identify top selling items
   - Run data quality checks and apply fixes
   - Verify order totals

3. **Data Loading**
   - Load transformed data to target tables
   - Support both full and incremental loading
   - Optional export to CSV files

## Database Schema

### Staging Tables
- `staging_orders`: Raw order data
- `staging_order_items`: Raw order item data
- `staging_menu_items`: Raw menu item data

### Transformed Tables
- `complete_orders`: Joined order data with all information
- `daily_revenue_by_category`: Daily revenue aggregated by category
- `top_selling_items`: Top-selling items by quantity and revenue
- `category_metrics`: Various metrics calculated by category

## Key Technical Decisions

1. **POSTGRESSQL**: Used for database abstraction and portability
2. **Pandas**: Used for data manipulation and transformations
3. **Configuration-driven**: All settings externalized in config.ini
4. **Error handling**: Comprehensive try/except blocks with detailed logging
5. **Containerization**: Docker support for easy deployment
6. **Testing**: Pytest for unit and integration testing

## Running the Pipeline

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python run_pipeline.py --config config.ini
```

## Implementation Details



### Key Components

1. **Trigger Events**
   - Scheduled runs (daily at 1 AM)
   - Code pushes to main/master branch
   - Pull requests
   - Manual triggering with configurable parameters

2. **Job: Lint and Test**
   - Code quality checks with flake8, black, and isort
   - Unit tests with pytest
   - Ensures pipeline code meets quality standards

3. **Job: Run Pipeline**
   - Spins up a PostgreSQL database service
   - Configures the pipeline settings
   - Runs the ETL process
   - Archives logs and output files as artifacts

4. **Job: Deploy Data**
   - Uploads processed data to a data warehouse (optional)
   - Sends notification emails with results
   - Triggers any dependent systems


### Required Secrets

The following GitHub repository secrets should be configured:

- `POSTGRES_PASSWORD`: For database access (in production)
- `MAIL_SERVER`, `MAIL_PORT`, `MAIL_USERNAME`, `MAIL_PASSWORD`: For email notifications

```

## Next Steps and Improvements

1. **Real-time Processing**: Add streaming data support
2. **API Layer**: Create REST API for querying results
3. **Dashboard Integration**: Connect to visualization tools
4. **Advanced Analytics**: Add predictive analytics capabilities
5. **Orchestration**: Integrate with Apache Airflow for scheduling
6. **Monitoring**: Add advanced monitoring and alerting
