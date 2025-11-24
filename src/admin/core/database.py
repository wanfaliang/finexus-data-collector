"""
Database dependencies for Admin API

Thin wrapper around existing DatabaseConnection for FastAPI compatibility.
"""
from typing import Generator
from sqlalchemy.orm import Session

from src.database.connection import DatabaseConnection


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency for database sessions

    Reuses existing DatabaseConnection singleton.

    Usage in endpoints:
        from fastapi import Depends
        from src.admin.core.database import get_db

        @router.get("/endpoint")
        def endpoint(db: Session = Depends(get_db)):
            # Use db session...
    """
    session_factory = DatabaseConnection.get_session_factory()
    session = session_factory()

    try:
        yield session
    finally:
        session.close()


def check_db_health() -> bool:
    """Check database health (reuses existing healthcheck)"""
    return DatabaseConnection.healthcheck()
