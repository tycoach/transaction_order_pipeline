"""
Database connection handling for the restaurant data pipeline.
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import Config

logger = logging.getLogger(__name__)

def create_db_engine(config=None):
   
    try:
        if config is None:
            config = Config()
        
        db_config = config.get_database_config()
        
        if db_config['type'] == 'sqlite':
            connection_string = f"sqlite:///{db_config['name']}"
        elif db_config['type'] == 'postgresql':
            connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        elif db_config['type'] == 'mysql':
            connection_string = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
       
        else:
            raise ValueError(f"Unsupported database type: {db_config['type']}")
        
        engine = create_engine(connection_string)
        logger.info(f"Database connection created for {db_config['type']}")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database connection: {str(e)}")
        raise

def create_session(engine):
    """
    Create a SQLAlchemy session for the engine.
    """
    Session = sessionmaker(bind=engine)
    return Session()

def init_db(engine, base):
    """
    Initialize database tables.
    """
    base.metadata.create_all(engine)
    logger.info("Database tables initialized")