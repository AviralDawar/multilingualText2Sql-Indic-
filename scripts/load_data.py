"""
Script to load CSV data files into database tables.

PostgreSQL backend only.

Each database folder should contain:
- DDL.csv: Table definitions
- data/*.csv: Full data files for each table (filename matches table name)
- *.json: Sample rows for reference (not loaded)

Usage:
    python scripts/load_data.py --database INDIA_POPULATION_CENSUS
    python scripts/load_data.py --database INDIA_POPULATION_CENSUS --table LOCATIONS --limit 1000
"""

import csv
import argparse
from pathlib import Path

from db_utils import load_config, get_connection, get_default_config_path, execute_use_schema, execute_truncate_table


def load_csv_file(csv_path: Path) -> tuple[list[str], list[list]]:
    """
    Load data from a CSV file.

    Returns:
        tuple: (column_names, rows)
    """
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)
    return headers, rows


def insert_data_from_csv(cursor, table_name: str, headers: list[str], rows: list[list],
                         batch_size: int = 1000, limit: int = None):
    """
    Insert CSV data into a database table.

    Args:
        cursor: Database cursor
        table_name: Name of the target table
        headers: Column names
        rows: Data rows
        batch_size: Number of rows to insert per batch
        limit: Maximum number of rows to insert
    """
    if not rows:
        print(f"No data to insert for {table_name}")
        return

    # Apply row limit if specified
    if limit and limit > 0:
        rows = rows[:limit]
        print(f"  Limiting to {limit} rows")

    # Quote column names for case-sensitivity
    columns_str = ", ".join(f'"{h}"' for h in headers)

    total_rows = len(rows)
    inserted = 0

    # Use psycopg2's execute_values for much faster bulk inserts
    try:
        from psycopg2.extras import execute_values
    except ImportError:
        execute_values = None

    if execute_values:
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"

        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            # Convert empty strings to None
            values = [tuple(val if val != '' else None for val in row) for row in batch]

            try:
                execute_values(cursor, insert_sql, values, page_size=batch_size)
                inserted += len(batch)
                print(f"  Inserted {inserted}/{total_rows} rows into {table_name}")
            except Exception as e:
                print(f"  Error inserting into {table_name}: {e}")
                raise

        print(f"Completed: {inserted} rows inserted into {table_name}")
        return

    # Fallback: standard executemany (slower)
    placeholders = ", ".join(["%s"] * len(headers))
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        # Convert empty strings to None
        values = [tuple(val if val != '' else None for val in row) for row in batch]

        try:
            cursor.executemany(insert_sql, values)
            inserted += len(batch)
            print(f"  Inserted {inserted}/{total_rows} rows into {table_name}")
        except Exception as e:
            print(f"  Error inserting into {table_name}: {e}")
            raise

    print(f"Completed: {inserted} rows inserted into {table_name}")


def process_database_folder(db_folder: Path, config: dict,
                           use_database: str = None, use_schema: str = None,
                           dry_run: bool = False, truncate: bool = False,
                           specific_table: str = None, limit: int = None):
    """
    Process a database folder and load all CSV data files.

    Args:
        db_folder: Path to the database folder (e.g., databases/INDIA_POPULATION_CENSUS/INDIA_POPULATION_CENSUS/)
        config: Database connection configuration
        use_database: Override database to use
        use_schema: Override schema to use
        dry_run: If True, only print statements without executing
        truncate: If True, truncate tables before loading
        specific_table: Load only this specific table
        limit: Maximum number of rows to load per table
    """
    # Use override values or derive from folder structure
    target_database = use_database if use_database else db_folder.parent.name
    target_schema = use_schema if use_schema else db_folder.name

    # Look for CSV files in data/ subdirectory
    data_dir = db_folder / "data"
    if not data_dir.exists():
        # Fallback: look for CSV files directly in the folder (excluding DDL.csv)
        csv_files = [f for f in db_folder.glob("*.csv") if f.name.upper() != "DDL.CSV"]
    else:
        # Exclude total_data.csv (source file) and only load split table files
        csv_files = [f for f in data_dir.glob("*.csv") if f.name.lower() != "total_data.csv"]

    # Filter to specific table if requested
    if specific_table:
        csv_files = [f for f in csv_files if f.stem.upper() == specific_table.upper()]

    if not csv_files:
        print(f"No CSV data files found in {db_folder}")
        return

    print(f"\n{'='*60}")
    print(f"Source: {db_folder.parent.name}/{db_folder.name}")
    print(f"Target: {target_database}.{target_schema}")
    print(f"Found {len(csv_files)} data files to load")
    print(f"{'='*60}")

    if dry_run:
        print("[DRY RUN] Would load data from:")
        for csv_file in csv_files:
            headers, rows = load_csv_file(csv_file)
            print(f"  - {csv_file.stem}: {len(rows)} rows, {len(headers)} columns")
        return

    # Connect to database
    conn = get_connection(config, database=target_database.lower())

    try:
        cursor = conn.cursor()

        # Set schema context
        execute_use_schema(cursor, target_schema)

        for csv_file in csv_files:
            table_name = csv_file.stem.upper()
            print(f"\nProcessing: {table_name}")

            # Load data from CSV
            headers, rows = load_csv_file(csv_file)

            if not rows:
                print(f"  Skipping {table_name}: empty data file")
                continue

            # Optionally truncate table first
            if truncate:
                try:
                    execute_truncate_table(cursor, table_name)
                    print(f"  Truncated table {table_name}")
                except Exception as e:
                    print(f"  Warning: Could not truncate {table_name}: {e}")

            # Insert data
            insert_data_from_csv(cursor, table_name, headers, rows, limit=limit)

        conn.commit()
        print(f"\nData loading complete for {target_database}.{target_schema}")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Load CSV data into database tables')
    parser.add_argument('--config', type=str,
                        help='Path to config file (defaults to config/postgres_credential.json)')
    parser.add_argument('--database', type=str,
                        help='Specific database folder to process (e.g., INDIA_POPULATION_CENSUS)')
    parser.add_argument('--databases-dir', type=str, default='../databases',
                        help='Directory containing database folders')
    parser.add_argument('--use-database', type=str,
                        help='Target database to load data into')
    parser.add_argument('--use-schema', type=str,
                        help='Target schema to load data into')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print actions without executing')
    parser.add_argument('--truncate', action='store_true',
                        help='Truncate tables before loading data')
    parser.add_argument('--table', type=str,
                        help='Load only a specific table')
    parser.add_argument('--limit', type=int,
                        help='Maximum number of rows to load per table')

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
        db_folders = list(databases_path.glob(f"{args.database}/{args.database}"))
    else:
        db_folders = [p for p in databases_path.glob("*/*") if p.is_dir()]

    if not db_folders:
        print(f"No database folders found in {databases_path}")
        return

    for db_folder in db_folders:
        process_database_folder(
            db_folder, config,
            args.use_database, args.use_schema,
            args.dry_run, args.truncate, args.table, args.limit
        )


if __name__ == "__main__":
    main()
