"""Initialize Database - Create all tables"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.models import Base
from src.database.connection import DatabaseConnection

def main():
    print("Initializing database...")
    
    # Get engine
    engine = DatabaseConnection.get_engine()

    print("Connecting to:", engine.url)
    
    # Create all tables
    Base.metadata.create_all(engine)
    
    print("✓ All tables created successfully!")
    
    # List created tables
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    print(f"\nCreated {len(tables)} tables:")
    for table in sorted(tables):
        print(f"  - {table}")

if __name__ == "__main__":
    main()
