"""
Database Connection Management
Handles connection pooling, session management, and database operations
"""
from contextlib import contextmanager
from typing import Generator, Optional
import logging

from sqlalchemy import create_engine, event, exc
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.pool import Pool

from src.config import settings


logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Singleton database connection manager"""
    
    _engine: Optional[Engine] = None
    _session_factory: Optional[sessionmaker] = None
    _scoped_session: Optional[scoped_session] = None
    
    @classmethod
    def get_engine(cls) -> Engine:
        """Get or create database engine"""
        if cls._engine is None:
            logger.info("Creating database engine...")
            cls._engine = create_engine(
                settings.database.url,
                pool_size=settings.database.pool_size,
                max_overflow=settings.database.max_overflow,
                pool_pre_ping=settings.database.pool_pre_ping,
                pool_recycle=settings.database.pool_recycle,
                echo=settings.database.echo,
            )
            
            # Add event listeners
            cls._setup_event_listeners(cls._engine)
            
            logger.info("Database engine created successfully")
        
        return cls._engine
    
    @classmethod
    def get_session_factory(cls) -> sessionmaker:
        """Get or create session factory"""
        if cls._session_factory is None:
            engine = cls.get_engine()
            cls._session_factory = sessionmaker(
                bind=engine,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
            )
            logger.info("Session factory created")
        
        return cls._session_factory
    
    @classmethod
    def get_scoped_session(cls) -> scoped_session:
        """Get or create scoped session (thread-safe)"""
        if cls._scoped_session is None:
            session_factory = cls.get_session_factory()
            cls._scoped_session = scoped_session(session_factory)
            logger.info("Scoped session created")
        
        return cls._scoped_session
    
    @classmethod
    def _setup_event_listeners(cls, engine: Engine):
        """Setup SQLAlchemy event listeners for monitoring"""
        
        @event.listens_for(Pool, "connect")
        def receive_connect(dbapi_conn, connection_record):
            """Log new database connections"""
            logger.debug("New database connection established")
        
        @event.listens_for(Pool, "checkout")
        def receive_checkout(dbapi_conn, connection_record, connection_proxy):
            """Log connection checkout from pool"""
            logger.debug("Connection checked out from pool")
        
        @event.listens_for(Pool, "checkin")
        def receive_checkin(dbapi_conn, connection_record):
            """Log connection return to pool"""
            logger.debug("Connection returned to pool")
    
    @classmethod
    def dispose(cls):
        """Dispose of engine and sessions (cleanup)"""
        if cls._scoped_session:
            cls._scoped_session.remove()
            cls._scoped_session = None
        
        if cls._engine:
            cls._engine.dispose()
            cls._engine = None
        
        cls._session_factory = None
        logger.info("Database connections disposed")
    
    @classmethod
    def healthcheck(cls) -> bool:
        """Check database connection health"""
        try:
            engine = cls.get_engine()
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Database healthcheck failed: {e}")
            return False


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions with automatic commit/rollback
    
    Usage:
        with get_session() as session:
            session.add(obj)
            session.commit()
    """
    session_factory = DatabaseConnection.get_session_factory()
    session = session_factory()
    
    try:
        yield session
        session.commit()
    except exc.SQLAlchemyError as e:
        session.rollback()
        logger.error(f"Database session error: {e}")
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error in database session: {e}")
        raise
    finally:
        session.close()


def get_scoped_session() -> scoped_session:
    """
    Get thread-safe scoped session (for multi-threaded applications)
    
    Usage:
        session = get_scoped_session()
        # Use session...
        session.remove()  # Remove at end of request/thread
    """
    return DatabaseConnection.get_scoped_session()


def init_database():
    """Initialize database (create tables if not exist)"""
    from src.database.models import Base
    
    logger.info("Initializing database...")
    engine = DatabaseConnection.get_engine()
    Base.metadata.create_all(engine)
    logger.info("Database initialized successfully")


def check_database_connection() -> bool:
    """
    Check if database connection is working
    Returns True if connection successful, False otherwise
    """
    return DatabaseConnection.healthcheck()


def dispose_database_connections():
    """Cleanup all database connections (call on application shutdown)"""
    DatabaseConnection.dispose()


# Utility functions for common database operations

def execute_raw_sql(sql: str, params: dict = None) -> list:
    """
    Execute raw SQL query
    
    Args:
        sql: SQL query string
        params: Query parameters (optional)
    
    Returns:
        List of result rows
    """
    with get_session() as session:
        result = session.execute(sql, params or {})
        return result.fetchall()


def get_table_row_count(table_name: str) -> int:
    """
    Get row count for a table
    
    Args:
        table_name: Name of the table
    
    Returns:
        Number of rows in table
    """
    sql = f"SELECT COUNT(*) FROM {table_name}"
    result = execute_raw_sql(sql)
    return result[0][0] if result else 0


def table_exists(table_name: str) -> bool:
    """
    Check if table exists in database
    
    Args:
        table_name: Name of the table
    
    Returns:
        True if table exists, False otherwise
    """
    sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = :table_name
        )
    """
    result = execute_raw_sql(sql, {'table_name': table_name})
    return result[0][0] if result else False


def get_all_table_names() -> list:
    """
    Get list of all tables in the database
    
    Returns:
        List of table names
    """
    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """
    result = execute_raw_sql(sql)
    return [row[0] for row in result]


def vacuum_analyze_table(table_name: str):
    """
    Run VACUUM ANALYZE on a table to optimize performance
    
    Args:
        table_name: Name of the table
    """
    engine = DatabaseConnection.get_engine()
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(f"VACUUM ANALYZE {table_name}")
    logger.info(f"VACUUM ANALYZE completed for {table_name}")


if __name__ == "__main__":
    # Test database connection
    logging.basicConfig(level=logging.INFO)
    
    print("Testing database connection...")
    if check_database_connection():
        print("✓ Database connection successful")
        
        # Get table info
        tables = get_all_table_names()
        print(f"\n✓ Found {len(tables)} tables in database")
        
        if tables:
            print("\nTables:")
            for table in tables:
                count = get_table_row_count(table)
                print(f"  - {table}: {count:,} rows")
    else:
        print("✗ Database connection failed")
    
    # Cleanup
    dispose_database_connections()
