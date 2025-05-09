"""
Database models for the data pipeline.
"""
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StagingOrder(Base):
    """Staging table for raw orders data."""
    __tablename__ = 'staging_orders'
    
    order_id = Column(Integer, primary_key=True)
    customer_id = Column(String(50))
    order_date = Column(String(50))
    total_amount = Column(Float)

class StagingOrderItem(Base):
    """Staging table for raw order items data."""
    __tablename__ = 'staging_order_items'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer)
    item_id = Column(String(50))
    quantity = Column(Integer)
    unit_price = Column(Float)

class StagingMenuItem(Base):
    """Staging table for raw menu items data."""
    __tablename__ = 'staging_menu_items'
    
    item_id = Column(String(50), primary_key=True)
    item_name = Column(String(100))
    category = Column(String(50))
    description = Column(String(500))

class DailyRevenue(Base):
    """Target table for daily revenue by category."""
    __tablename__ = 'daily_revenue_by_category'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    category = Column(String(50))
    total_revenue = Column(Float)
    order_count = Column(Integer)

class TopSellingItem(Base):
    """Target table for top selling items."""
    __tablename__ = 'top_selling_items'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(String(50))
    item_name = Column(String(100))
    category = Column(String(50))
    total_quantity_sold = Column(Integer)
    total_revenue = Column(Float)

class CompleteOrder(Base):
    """Joined table with complete order information."""
    __tablename__ = 'complete_orders'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer)
    customer_id = Column(String(50))
    order_date = Column(Date)
    item_id = Column(String(50))
    item_name = Column(String(100))
    category = Column(String(50))
    quantity = Column(Integer)
    unit_price = Column(Float)
    item_revenue = Column(Float)