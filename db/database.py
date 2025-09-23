"""
Database connection and session management
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from .models import Base
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/inventario.db')

# Create engine with appropriate settings
if DATABASE_URL.startswith('sqlite'):
    # SQLite specific settings
    engine = create_engine(
        DATABASE_URL,
        poolclass=StaticPool,
        connect_args={
            "check_same_thread": False,
        },
        echo=False  # Set to True for SQL debugging
    )
    # Ensure data directory exists for SQLite
    os.makedirs('data', exist_ok=True)
else:
    # PostgreSQL or other databases
    engine = create_engine(DATABASE_URL, echo=False)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_session():
    """Get database session"""
    session = SessionLocal()
    try:
        return session
    except Exception as e:
        session.close()
        raise e

def init_database(force_recreate=False):
    """Initialize database tables"""
    try:
        if force_recreate:
            # Drop all tables and recreate them
            Base.metadata.drop_all(bind=engine)
            logger.info("Dropped existing database tables")
        
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        raise e

def get_engine():
    """Get database engine"""
    return engine

def close_session(session):
    """Close database session"""
    try:
        session.close()
    except Exception as e:
        logger.error(f"Error closing session: {e}")

# Context manager for database sessions
class DatabaseSession:
    def __init__(self):
        self.session = None
    
    def __enter__(self):
        self.session = get_session()
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            if exc_type:
                self.session.rollback()
            else:
                self.session.commit()
            close_session(self.session)
