"""
Script to create database tables from DDL.csv files.

PostgreSQL backend only.

DDL.csv format:
- table_name: Name of the table
- description: Table description
- DDL: Full CREATE TABLE statement

Usage:
    python scripts/create_tables.py --database INDIA_POPULATION_CENSUS
    python scripts/create_tables.py --database INDIA_POPULATION_CENSUS --use-database indicdb --use-schema public
    python scripts/create_tables.py --database INDIA_POPULATION_CENSUS --dry-run
"""

import csv
import argparse
from pathlib import Path

from db_utils import load_config, get_connection, get_default_config_path, execute_use_schema, execute_create_schema


def parse_ddl_csv(ddl_path: str) -> list[dict]:
    """
    Parse DDL.csv file and return list of table definitions.

    Args:
        ddl_path: Path to DDL.csv file

    Expected CSV format:
    - table_name: Name of the table
    - description: Table description
    - DDL: Full CREATE TABLE statement
    """
    tables = []
    with open(ddl_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ddl = row.get('DDL', '').strip()
            tables.append({
                'table_name': row.get('table_name', '').strip(),
                'description': row.get('description', '').strip(),
                'ddl': ddl
            })
    return tables


def execute_ddl(cursor, table_name: str, ddl: str):
    """Execute a DDL statement to create a table."""
    try:
        # DDL may contain multiple statements (DROP + CREATE)
        statements = [s.strip() for s in ddl.split(';') if s.strip()]
        for stmt in statements:
            cursor.execute(stmt)
        print(f"  Created table: {table_name}")
        return True
    except Exception as e:
        print(f"  Error creating {table_name}: {e}")
        return False


def process_database_folder(db_folder: Path, config: dict,
                           use_database: str = None, use_schema: str = None,
                           create_schema: bool = False,
                           dry_run: bool = False):
    """
    Process a database folder containing DDL.csv.

    Args:
        db_folder: Path to the database folder (e.g., databases/INDIA_POPULATION_CENSUS/INDIA_POPULATION_CENSUS/)
        config: Database connection configuration
        use_database: Override database to use
        use_schema: Override schema to use
        create_schema: Create the schema if it doesn't exist
        dry_run: If True, only print statements without executing
    """
    ddl_path = db_folder / "DDL.csv"

    if not ddl_path.exists():
        print(f"Warning: DDL.csv not found in {db_folder}")
        return

    # Use override values or derive from folder structure
    target_database = use_database if use_database else db_folder.parent.name
    target_schema = use_schema if use_schema else db_folder.name

    print(f"\n{'='*60}")
    print(f"Source: {db_folder.parent.name}/{db_folder.name}")
    print(f"Target: {target_database}.{target_schema}")
    print(f"{'='*60}")

    # Parse DDL
    tables = parse_ddl_csv(str(ddl_path))
    print(f"Found {len(tables)} table definitions\n")

    if dry_run:
        print("[DRY RUN] Would execute the following DDL statements:\n")
        if create_schema:
            print(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        print(f"SET search_path TO {target_schema};")
        print()
        for table in tables:
            print(f"Table: {table['table_name']}")
            print(f"Description: {table['description']}")
            print(f"DDL:\n{table['ddl']}")
            print("-" * 40)
        return

    # Connect to database
    conn = get_connection(config, database=target_database.lower())

    try:
        cursor = conn.cursor()

        # Create schema if requested
        if create_schema:
            execute_create_schema(cursor, target_schema)
            print(f"Created schema: {target_schema}")

        # Set schema context
        execute_use_schema(cursor, target_schema)
        print(f"Using schema: {target_schema}")

        print(f"\nCreating tables...")
        success_count = 0
        error_count = 0

        for table in tables:
            if table['ddl']:
                if execute_ddl(cursor, table['table_name'], table['ddl']):
                    success_count += 1
                else:
                    error_count += 1
            else:
                print(f"  Skipping {table['table_name']}: No DDL statement")

        conn.commit()

        print(f"\nSummary:")
        print(f"  Tables created successfully: {success_count}")
        print(f"  Tables with errors: {error_count}")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Create database tables from DDL.csv files')
    parser.add_argument('--config', type=str,
                        help='Path to config file (defaults to config/postgres_credential.json)')
    parser.add_argument('--database', type=str,
                        help='Specific database folder to process (e.g., INDIA_POPULATION_CENSUS)')
    parser.add_argument('--databases-dir', type=str, default='../databases',
                        help='Directory containing database folders')
    parser.add_argument('--use-database', type=str,
                        help='Target database to create tables in')
    parser.add_argument('--use-schema', type=str,
                        help='Target schema to create tables in (e.g., public or india_census)')
    parser.add_argument('--create-schema', action='store_true',
                        help='Create the schema if it does not exist')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print DDL statements without executing')

    args = parser.parse_args()

    # Auto-detect config path if not specified
    if not args.config:
        args.config = get_default_config_path()

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        print(f"Please create {config_path} or specify --config path")
        return

    config = load_config(str(config_path))

    # Find database folders to process
    databases_path = Path(args.databases_dir)

    if args.database:
        # Process specific database
        db_folders = list(databases_path.glob(f"{args.database}/{args.database}"))
    else:
        # Process all databases
        db_folders = [p for p in databases_path.glob("*/*") if p.is_dir() and (p / "DDL.csv").exists()]

    if not db_folders:
        print(f"No database folders with DDL.csv found in {databases_path}")
        return

    for db_folder in db_folders:
        process_database_folder(
            db_folder, config,
            args.use_database, args.use_schema,
            args.create_schema, args.dry_run
        )

    print("\nDone!")


if __name__ == "__main__":
    main()
