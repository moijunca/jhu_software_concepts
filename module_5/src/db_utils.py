"""Database connection utilities shared across modules."""
import os
import psycopg


def build_conninfo(app=None):
    """Return a psycopg3-compatible connection string.
    
    Args:
        app: Optional Flask app with DATABASE_URL in config
        
    Returns:
        Connection string for psycopg.connect()
    """
    url = (app.config.get("DATABASE_URL") if app else None) or os.getenv("DATABASE_URL")
    if url:
        return url
    database = os.getenv("PGDATABASE", "gradcafe")
    user = os.getenv("PGUSER", os.getenv("USER", "postgres"))
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    return f"dbname={database} user={user} host={host} port={port}"


def get_conn(app=None):
    """Open and return a psycopg3 connection.
    
    Args:
        app: Optional Flask app with DATABASE_URL in config
        
    Returns:
        psycopg.Connection
    """
    return psycopg.connect(build_conninfo(app))
