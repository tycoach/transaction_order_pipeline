"""
Configuration handling for the restaurant data pipeline.
"""
import os
import logging
import configparser
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
# Load environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

class Config:
    """Configuration manager for the restaurant data pipeline."""
    
    def __init__(self, config_file='config.ini'):
        """
        Initialize configuration from config file.
        """
        self.config = configparser.ConfigParser()
        
        # Set default values
        self._set_defaults()
        
        # Try to read from config file
        config_path = Path(config_file)
        if config_path.exists():
            self.config.read(config_path)
            self._setup_logging()
        else:
            print(f"Warning: Config file {config_file} not found. Using defaults.")
    
    def _set_defaults(self):
        """Set default configuration values."""
        self.config['DATABASE'] = {
            'type': 'postgres',
            'name': POSTGRES_DB,
            'host': POSTGRES_HOST,
            'port': POSTGRES_PORT,
            'user': POSTGRES_USER,
            'password': POSTGRES_PASSWORD
        }
        
        self.config['LOGGING'] = {
            'level': 'INFO',
            'file': 'logs/pipeline.log'
        }
        
        self.config['PATHS'] = {
            'input_dir': 'data/input',
            'output_dir': 'data/output'
        }
        
        self.config['PIPELINE'] = {
            'incremental': 'false',
            'quality_check': 'true'
        }
    
    def _setup_logging(self):
        """Configure logging based on settings."""
        log_config = self.config['LOGGING']
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'logs/pipeline.log')
        
        # Create directory for log file if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def get_database_config(self):
        """
        Get database configuration.
    
        """
        return {
            'type': self.config['DATABASE'].get('type'),
            'name': self.config['DATABASE'].get('name'),
            'host': self.config['DATABASE'].get('host'),
            'port': self.config['DATABASE'].get('port'),
            'user': self.config['DATABASE'].get('user'),
            'password': self.config['DATABASE'].get('password')
        }
    
    def get_input_path(self, filename=None):
        """
        Get input directory or file path.
        
        """
        input_dir = self.config['PATHS'].get('input_dir', 'data/input')
        
        # Create directory if it doesn't exist
        if not os.path.exists(input_dir):
            os.makedirs(input_dir)
            
        if filename:
            return os.path.join(input_dir, filename)
        return input_dir
    
    def get_output_path(self, filename=None):
        """
        Get output directory or file path.
        
        """
        output_dir = self.config['PATHS'].get('output_dir', 'data/output')
        
        # Create directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        if filename:
            return os.path.join(output_dir, filename)
        return output_dir
    
    def is_incremental(self):
        """
        Check if incremental loading is enabled.
        """
        return self.config['PIPELINE'].getboolean('incremental', False)
    
    def is_quality_check_enabled(self):
        """
        Check if data quality checks are enabled.
        
        """
        return self.config['PIPELINE'].getboolean('quality_check', True)