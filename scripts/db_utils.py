"""
Shared database utility module for PostgreSQL.

This module provides a unified interface for connecting to PostgreSQL databases.
"""

import json
from pathlib import Path


def load_config(config_path: str) -> dict:
    """Load database connection configuration from JSON file."""
    with open(config_path, 'r') as f:
        return json.load(f)


def get_default_config_path(base_dir: str = None) -> str:
    """
    Get the default config file path for PostgreSQL.

    Args:
        base_dir: Base directory for config files (defaults to ../config relative to scripts)

    Returns:
        Path to the config file
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent / 'config'
    else:
        base_dir = Path(base_dir)

    return str(base_dir / 'postgres_credential.json')


def get_connection(config: dict, database: str = None):
    """
    Create and return a PostgreSQL database connection.

    Args:
        config: Connection configuration dictionary
        database: Database name to connect to (optional)

    Returns:
        Database connection object

    Raises:
        ImportError: If psycopg2 is not installed
    """
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 is not installed. "
            "Install it with: pip install psycopg2-binary"
        )

    conn = psycopg2.connect(
        host=config['host'],
        port=config.get('port', 5432),
        user=config['user'],
        password=config['password'],
        database=database or config.get('database', 'postgres')
    )
    # Set autocommit to False by default (explicit commit required)
    conn.autocommit = False
    return conn


def execute_use_schema(cursor, schema: str):
    """
    Execute command to switch schema context.

    Args:
        cursor: Database cursor
        schema: Schema name
    """
    cursor.execute(f"SET search_path TO {schema}")


def execute_create_schema(cursor, schema: str):
    """
    Execute command to create a schema.

    Args:
        cursor: Database cursor
        schema: Schema name
    """
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def execute_truncate_table(cursor, table_name: str):
    """
    Execute truncate table command.

    Args:
        cursor: Database cursor
        table_name: Table name
    """
    cursor.execute(f"TRUNCATE TABLE {table_name}")


def get_show_databases_query() -> str:
    """Get query to list databases."""
    return "SELECT datname AS name FROM pg_database WHERE datistemplate = false ORDER BY datname"


def get_show_schemas_query() -> str:
    """Get query to list schemas."""
    return "SELECT schema_name AS name FROM information_schema.schemata ORDER BY schema_name"


def get_show_tables_query(schema: str = None) -> str:
    """Get query to list tables."""
    schema_filter = f"'{schema}'" if schema else "current_schema()"
    return f"SELECT table_name AS name FROM information_schema.tables WHERE table_schema = {schema_filter} ORDER BY table_name"


def create_database(config: dict, database_name: str):
    """
    Create a new database.

    Args:
        config: Connection configuration dictionary
        database_name: Name of the database to create

    Returns:
        True if database was created, False if it already exists
    """
    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError:
        raise ImportError(
            "psycopg2 is not installed. "
            "Install it with: pip install psycopg2-binary"
        )

    # Connect to default 'postgres' database to create new database
    conn = psycopg2.connect(
        host=config['host'],
        port=config.get('port', 5432),
        user=config['user'],
        password=config['password'],
        database='postgres'
    )
    conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction

    cursor = conn.cursor()
    try:
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (database_name,)
        )
        if cursor.fetchone():
            print(f"Database '{database_name}' already exists.")
            return False

        # Create the database
        cursor.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
        )
        print(f"Database '{database_name}' created successfully.")
        return True
    finally:
        cursor.close()
        conn.close()
