"""
Script to validate all generated SQL queries against the database.

PostgreSQL backend only.

This script:
1. Reads all SQL files from the queries/ directory
2. Executes each query against the database
3. Reports success/failure status
4. Generates a validation report

Usage:
    python scripts/validate_queries.py
    python scripts/validate_queries.py --limit 10  # Test first 10 queries only
    python scripts/validate_queries.py --query indicdb_001.sql  # Test specific query
    python scripts/validate_queries.py --dry-run  # Just parse queries without executing
"""

import argparse
import time
import sys
from pathlib import Path
from datetime import datetime

from db_utils import load_config, get_connection, get_default_config_path, execute_use_schema


def load_query_file(filepath: Path) -> tuple[str, dict]:
    """
    Load a SQL query from file.

    Returns:
        tuple: (sql_query, metadata)
    """
    with open(filepath, 'r') as f:
        content = f.read()

    # Parse metadata from comments
    lines = content.strip().split('\n')
    metadata = {
        'complexity': None,
        'category': None
    }

    sql_lines = []
    for line in lines:
        if line.startswith('-- Complexity:'):
            metadata['complexity'] = line.replace('-- Complexity:', '').strip()
        elif line.startswith('-- Category:'):
            metadata['category'] = line.replace('-- Category:', '').strip()
        else:
            sql_lines.append(line)

    sql = '\n'.join(sql_lines).strip()
    # Remove trailing semicolon if present
    if sql.endswith(';'):
        sql = sql[:-1]

    return sql, metadata


def validate_query(cursor, query_name: str, sql: str, metadata: dict) -> dict:
    """
    Execute a query and return validation result.

    Returns:
        dict with keys: name, success, rows, columns, error, execution_time, metadata
    """
    result = {
        'name': query_name,
        'success': False,
        'rows': 0,
        'columns': 0,
        'error': None,
        'execution_time': 0,
        'metadata': metadata
    }

    start_time = time.time()

    try:
        cursor.execute(sql)

        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            result['columns'] = len(columns)
            result['rows'] = len(rows)

        result['success'] = True

    except Exception as e:
        result['error'] = str(e)

    result['execution_time'] = round(time.time() - start_time, 3)
    return result


def print_result(result: dict, verbose: bool = False):
    """Print validation result for a single query."""
    status = "OK" if result['success'] else "ERR"
    name = result['name']

    if result['success']:
        print(f"  {status} {name}: {result['rows']} rows, {result['columns']} cols ({result['execution_time']}s)")
    else:
        print(f"  {status} {name}: ERROR - {result['error'][:80]}...")
        if verbose:
            print(f"      Full error: {result['error']}")


def generate_report(results: list, output_path: Path = None) -> str:
    """Generate a summary report of validation results."""
    total = len(results)
    passed = sum(1 for r in results if r['success'])
    failed = total - passed

    report_lines = [
        "=" * 60,
        "QUERY VALIDATION REPORT",
        "Backend: PostgreSQL",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 60,
        "",
        f"Total Queries: {total}",
        f"Passed: {passed}",
        f"Failed: {failed}",
        f"Success Rate: {passed * 100 / total:.1f}%" if total > 0 else "N/A",
        "",
    ]

    # Group by complexity level
    by_complexity = {}
    for r in results:
        level = r['metadata'].get('complexity', 'Unknown')
        if level not in by_complexity:
            by_complexity[level] = {'passed': 0, 'failed': 0}
        if r['success']:
            by_complexity[level]['passed'] += 1
        else:
            by_complexity[level]['failed'] += 1

    report_lines.append("Results by Complexity Level:")
    for level in sorted(by_complexity.keys()):
        stats = by_complexity[level]
        total_level = stats['passed'] + stats['failed']
        report_lines.append(f"  {level}: {stats['passed']}/{total_level} passed")

    report_lines.append("")

    # List failed queries
    failed_queries = [r for r in results if not r['success']]
    if failed_queries:
        report_lines.append("Failed Queries:")
        for r in failed_queries:
            report_lines.append(f"  - {r['name']}")
            report_lines.append(f"    Error: {r['error'][:100]}...")
        report_lines.append("")

    # Execution time stats
    exec_times = [r['execution_time'] for r in results if r['success']]
    if exec_times:
        report_lines.append("Execution Time Statistics (successful queries):")
        report_lines.append(f"  Min: {min(exec_times):.3f}s")
        report_lines.append(f"  Max: {max(exec_times):.3f}s")
        report_lines.append(f"  Avg: {sum(exec_times) / len(exec_times):.3f}s")
        report_lines.append(f"  Total: {sum(exec_times):.3f}s")

    report_lines.append("")
    report_lines.append("=" * 60)

    report = '\n'.join(report_lines)

    if output_path:
        with open(output_path, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {output_path}")

    return report


def main():
    parser = argparse.ArgumentParser(description='Validate SQL queries against database')
    parser.add_argument('--config', type=str,
                        help='Path to config file (defaults to config/postgres_credential.json)')
    parser.add_argument('--queries-dir', type=str, default='queries',
                        help='Directory containing SQL query files')
    parser.add_argument('--database', type=str, default='INDIA_POPULATION_CENSUS',
                        help='Database to use')
    parser.add_argument('--schema', type=str, default='INDIA_POPULATION_CENSUS',
                        help='Schema to use')
    parser.add_argument('--limit', type=int,
                        help='Limit number of queries to validate')
    parser.add_argument('--query', type=str,
                        help='Validate specific query file (e.g., indicdb_001.sql)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Parse queries without executing')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show verbose output')
    parser.add_argument('--report', type=str,
                        help='Save report to file')

    args = parser.parse_args()

    # Auto-detect config path if not specified
    if not args.config:
        args.config = get_default_config_path()

    # Find query files
    queries_path = Path(args.queries_dir)
    if not queries_path.exists():
        print(f"Error: Queries directory not found: {queries_path}")
        return 1

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
        print("No query files found to validate.")
        return 1

    print(f"Found {len(query_files)} query files to validate")
    print(f"Database: {args.database}.{args.schema}")
    print()

    # Load queries
    queries = []
    for qf in query_files:
        sql, metadata = load_query_file(qf)
        queries.append({
            'name': qf.name,
            'sql': sql,
            'metadata': metadata
        })

    if args.dry_run:
        print("DRY RUN - Queries parsed but not executed:")
        for q in queries:
            print(f"  {q['name']}: {q['metadata']['complexity']} - {q['metadata']['category']}")
            if args.verbose:
                print(f"    SQL: {q['sql'][:100]}...")
        print(f"\nTotal: {len(queries)} queries ready for validation")
        return 0

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

    # Validate queries
    results = []
    try:
        cursor = conn.cursor()

        # Set schema context
        execute_use_schema(cursor, args.schema)

        print("Validating queries...")
        for q in queries:
            result = validate_query(cursor, q['name'], q['sql'], q['metadata'])
            results.append(result)
            print_result(result, args.verbose)

    finally:
        conn.close()

    # Generate report
    print()
    report = generate_report(results, Path(args.report) if args.report else None)
    print(report)

    # Return exit code based on results
    failed = sum(1 for r in results if not r['success'])
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
