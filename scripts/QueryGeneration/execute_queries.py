"""
Script to execute all SQL queries and save results as CSV files.

PostgreSQL backend only.

This script:
1. Reads all SQL files from the gold/sql/ directory
2. Executes each query against the database
3. Saves results as CSV files in gold/exec_result/

Usage:
    python scripts/execute_queries.py
    python scripts/execute_queries.py --limit 10  # Execute first 10 queries only
    python scripts/execute_queries.py --query indicdb_001.sql  # Execute specific query
"""

import csv
import argparse
import time
import sys
from pathlib import Path

from db_utils import load_config, get_connection, get_default_config_path, execute_use_schema


def load_query_file(filepath: Path) -> str:
    """
    Load a SQL query from file.

    Returns:
        str: SQL query without comments
    """
    with open(filepath, 'r') as f:
        content = f.read()

    # Parse out the SQL, skipping comment lines
    lines = content.strip().split('\n')
    sql_lines = []
    for line in lines:
        if not line.startswith('--'):
            sql_lines.append(line)

    sql = '\n'.join(sql_lines).strip()
    # Remove trailing semicolon if present
    if sql.endswith(';'):
        sql = sql[:-1]

    return sql


def execute_and_save(cursor, query_name: str, sql: str, output_dir: Path) -> dict:
    """
    Execute a query and save results as CSV.

    Returns:
        dict with keys: name, success, rows, error, execution_time, output_file
    """
    result = {
        'name': query_name,
        'success': False,
        'rows': 0,
        'error': None,
        'execution_time': 0,
        'output_file': None
    }

    # Output filename: indicdb_001.sql -> indicdb_001.csv
    csv_filename = query_name.replace('.sql', '.csv')
    output_path = output_dir / csv_filename

    start_time = time.time()

    try:
        cursor.execute(sql)

        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

            # Write to CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)

            result['rows'] = len(rows)
            result['output_file'] = str(output_path)
        else:
            # Query executed but no results (e.g., DDL statement)
            # Create empty CSV with just a message
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['message'])
                writer.writerow(['Query executed successfully. No rows returned.'])
            result['output_file'] = str(output_path)

        result['success'] = True

    except Exception as e:
        result['error'] = str(e)
        # Write error to CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['error'])
            writer.writerow([str(e)])
        result['output_file'] = str(output_path)

    result['execution_time'] = round(time.time() - start_time, 3)
    return result


def main():
    parser = argparse.ArgumentParser(description='Execute SQL queries and save results as CSV')
    parser.add_argument('--config', type=str,
                        help='Path to config file (defaults to config/postgres_credential.json)')
    parser.add_argument('--queries-dir', type=str, default='../gold/sql',
                        help='Directory containing SQL query files')
    parser.add_argument('--output-dir', type=str, default='../gold/exec_result',
                        help='Directory to save CSV results')
    parser.add_argument('--database', type=str, default='INDIA_POPULATION_CENSUS',
                        help='Database to use')
    parser.add_argument('--schema', type=str, default='INDIA_POPULATION_CENSUS',
                        help='Schema to use')
    parser.add_argument('--limit', type=int,
                        help='Limit number of queries to execute')
    parser.add_argument('--query', type=str,
                        help='Execute specific query file (e.g., indicdb_001.sql)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show verbose output')

    args = parser.parse_args()

    # Auto-detect config path if not specified
    if not args.config:
        args.config = get_default_config_path()

    # Find query files
    queries_path = Path(args.queries_dir)
    if not queries_path.exists():
        print(f"Error: Queries directory not found: {queries_path}")
        return 1

    # Create output directory if needed
    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if args.query:
        query_files = [queries_path / args.query]
        if not query_files[0].exists():
            print(f"Error: Query file not found: {query_files[0]}")
            return 1
    else:
        query_files = sorted(queries_path.glob("indicdb_*.sql"))

    if args.limit:
        query_files = query_files[:args.limit]

    if not query_files:
        print("No query files found to execute.")
        return 1

    print(f"Found {len(query_files)} query files to execute")
    print(f"Database: {args.database}.{args.schema}")
    print(f"Output directory: {output_path}")
    print()

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        print(f"Please create {config_path} or specify --config path")
        return 1

    config = load_config(str(config_path))

    # Connect to database
    print("Connecting to PostgreSQL...")
    try:
        conn = get_connection(config, database=args.database.lower())
        print("Connected successfully!\n")
    except Exception as e:
        print(f"Connection failed: {e}")
        return 1

    # Execute queries
    results = []
    total_start = time.time()

    try:
        cursor = conn.cursor()

        # Set schema context
        execute_use_schema(cursor, args.schema)

        print("Executing queries and saving results...")
        print("-" * 60)

        for i, qf in enumerate(query_files, 1):
            sql = load_query_file(qf)
            result = execute_and_save(cursor, qf.name, sql, output_path)
            results.append(result)

            status = "OK" if result['success'] else "ERR"
            print(f"  [{i:3d}/{len(query_files)}] {status} {qf.name}: {result['rows']} rows ({result['execution_time']}s)")

            if not result['success'] and args.verbose:
                print(f"         Error: {result['error'][:80]}...")

    finally:
        conn.close()

    # Summary
    total_time = round(time.time() - total_start, 2)
    passed = sum(1 for r in results if r['success'])
    failed = len(results) - passed
    total_rows = sum(r['rows'] for r in results)

    print("-" * 60)
    print(f"\nExecution Complete!")
    print(f"  Total queries: {len(results)}")
    print(f"  Successful: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Total rows: {total_rows}")
    print(f"  Total time: {total_time}s")
    print(f"  Results saved to: {output_path}/")

    if failed > 0:
        print(f"\nFailed queries:")
        for r in results:
            if not r['success']:
                print(f"  - {r['name']}: {r['error'][:60]}...")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
