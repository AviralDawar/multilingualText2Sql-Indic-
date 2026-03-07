"""
Script to run SQL queries on the database.

PostgreSQL backend only.

Usage:
    python scripts/run_query.py --query "SELECT * FROM LOCATIONS LIMIT 10"
    python scripts/run_query.py --file query.sql
    python scripts/run_query.py --query "SELECT * FROM my_table" --use-database indicdb --use-schema public
    python scripts/run_query.py --interactive
    python scripts/run_query.py --create-database indicdb
"""

import argparse
from pathlib import Path
from tabulate import tabulate

try:
    from scripts.db_utils import (
        load_config, get_connection, get_default_config_path,
        execute_use_schema, get_show_databases_query, get_show_schemas_query, get_show_tables_query,
        create_database
    )
except ImportError:
    from db_utils import (
        load_config, get_connection, get_default_config_path,
        execute_use_schema, get_show_databases_query, get_show_schemas_query, get_show_tables_query,
        create_database
    )


def execute_query(
    cursor,
    query: str,
    show_results: bool = True,
    fetch_one: bool = False
) -> dict:
    """Execute a query and return results."""
    try:
        cursor.execute(query)

        if cursor.description:
            # Query returned results
            columns = [col[0] for col in cursor.description]
            if fetch_one:
                first_row = cursor.fetchone()
                rows = [first_row] if first_row is not None else []
            else:
                rows = cursor.fetchall()

            if show_results:
                if rows:
                    print(tabulate(rows, headers=columns, tablefmt='grid'))
                    if fetch_one:
                        print("\n(Showing up to 1 row)")
                    else:
                        print(f"\n({len(rows)} rows returned)")
                else:
                    print("Query executed successfully. No rows returned.")

            return {'columns': columns, 'rows': rows}
        else:
            print("Query executed successfully.")
            return {'columns': [], 'rows': []}

    except Exception as e:
        print(f"Error: {e}")
        return None


def run_interactive(conn):
    """Run interactive query mode."""
    print("\n" + "="*60)
    print("Interactive Query Mode (PostgreSQL)")
    print("="*60)
    print("Type your SQL queries and press Enter to execute.")
    print("Commands:")
    print("  exit, quit, q  - Exit interactive mode")
    print("  help           - Show this help")
    print("  databases      - Show available databases")
    print("  schemas        - Show schemas in current database")
    print("  tables         - Show tables in current schema")
    print("  use <schema>   - Switch to a schema")
    print("="*60 + "\n")

    cursor = conn.cursor()
    prompt = "postgres> "

    while True:
        try:
            query = input(prompt).strip()

            if not query:
                continue

            # Handle special commands
            lower_query = query.lower()

            if lower_query in ('exit', 'quit', 'q'):
                print("Goodbye!")
                break
            elif lower_query == 'help':
                print("Commands: exit, quit, q, help, databases, schemas, tables, use <schema>")
                continue
            elif lower_query == 'databases':
                query = get_show_databases_query()
            elif lower_query == 'schemas':
                query = get_show_schemas_query()
            elif lower_query == 'tables':
                query = get_show_tables_query()
            elif lower_query.startswith('use '):
                schema = query[4:].strip()
                query = f"SET search_path TO {schema}"

            execute_query(cursor, query)
            print()

        except KeyboardInterrupt:
            print("\nUse 'exit' to quit.")
        except EOFError:
            print("\nGoodbye!")
            break


def main():
    parser = argparse.ArgumentParser(description='Run SQL queries on database')
    parser.add_argument('--config', type=str,
                        help='Path to config file (defaults to config/postgres_credential.json)')
    parser.add_argument('--query', '-q', type=str,
                        help='SQL query to execute')
    parser.add_argument('--file', '-f', type=str,
                        help='Path to SQL file to execute')
    parser.add_argument('--use-database', type=str,
                        help='Database to use')
    parser.add_argument('--use-schema', type=str,
                        help='Schema to use')
    parser.add_argument('--interactive', '-i', action='store_true',
                        help='Run in interactive mode')
    parser.add_argument('--create-database', type=str,
                        help='Create a new database with the specified name')

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

    # Handle create-database command (doesn't need a connection first)
    if args.create_database:
        try:
            create_database(config, args.create_database)
        except Exception as e:
            print(f"Failed to create database: {e}")
        return

    # Connect
    print("Connecting to PostgreSQL...")
    try:
        database = args.use_database.lower() if args.use_database else None
        conn = get_connection(config, database=database)
        print("Connected successfully!\n")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    try:
        cursor = conn.cursor()

        # Set schema context if specified
        if args.use_schema:
            execute_use_schema(cursor, args.use_schema)

        if args.interactive:
            run_interactive(conn)
        elif args.query:
            execute_query(cursor, args.query)
        elif args.file:
            file_path = Path(args.file)
            if not file_path.exists():
                print(f"Error: SQL file not found: {file_path}")
                return

            with open(file_path, 'r') as f:
                sql = f.read()

            # Split by semicolon for multiple statements
            statements = [s.strip() for s in sql.split(';') if s.strip()]

            for i, stmt in enumerate(statements, 1):
                print(f"\n--- Executing statement {i}/{len(statements)} ---")
                print(f"{stmt[:100]}..." if len(stmt) > 100 else stmt)
                print()
                execute_query(cursor, stmt)
        else:
            # Default: show helpful info
            print("Available databases:")
            execute_query(cursor, get_show_databases_query())
            print("\nUse --query, --file, or --interactive to run queries.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
