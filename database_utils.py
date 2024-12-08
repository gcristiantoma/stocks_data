# database_utils.py

from sqlalchemy import create_engine
import os

def create_sqlite_engine(db_path="stocks.db"):
    """
    Creates a persistent SQLite database engine.
    """
    if not os.path.exists(db_path):
        open(db_path, "w").close()  # Create an empty database file if it doesn't exist
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    return engine