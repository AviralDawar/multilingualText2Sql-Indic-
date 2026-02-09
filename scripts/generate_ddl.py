"""
Script to generate DDL.csv files from CSV data files.

PostgreSQL backend only.

Takes CSV files from a database folder and generates a DDL.csv file with:
- table_name: Name of the table
- description: Description of the table (can be empty or provided)
- DDL: Full CREATE TABLE statement

Output format matches Spider2 style:
table_name,description,DDL
TABLE_NAME,description text,"CREATE TABLE TABLE_NAME (...)"

Usage:
    python scripts/generate_ddl.py --database INDIA_POPULATION_CENSUS
    python scripts/generate_ddl.py --databases-dir databases
"""

import csv
import re
import argparse
from pathlib import Path


# Column name patterns that indicate specific types
ID_PATTERNS = ['_id', 'id']
FLAG_PATTERNS = ['is_', 'has_', 'flag', '_flag']
YEAR_PATTERNS = ['year', '_year', 'fiscal_year']
COUNT_PATTERNS = ['_count', '_num', '_total', 'count_', 'num_', 'quantity']

# Financial/measurement column patterns - these should always be FLOAT
FINANCIAL_PATTERNS = [
    'amount', 'balance', 'fund', 'expenditure', 'disbursement', 'outlay',
    'surplus', 'deficit', 'loan', 'debt', 'repayment', 'discharge',
    'deposit', 'advance', 'remittance', 'investment', 'reserve',
    'provident', 'contingency', 'suspense', 'lakhs', 'crores', 'rupees',
    'inr', 'revenue', 'capital', 'services', 'development', 'settlement'
]


def is_id_column(column_name: str) -> bool:
    """Check if column name suggests it's an ID column."""
    col_lower = column_name.lower()
    # Must end with _id or be exactly 'id'
    return col_lower.endswith('_id') or col_lower == 'id'


def is_flag_column(column_name: str) -> bool:
    """Check if column name suggests it's a boolean/flag column."""
    col_lower = column_name.lower()
    return any(pattern in col_lower for pattern in FLAG_PATTERNS)


def is_year_column(column_name: str) -> bool:
    """Check if column name suggests it's a year column."""
    col_lower = column_name.lower()
    return any(pattern in col_lower for pattern in YEAR_PATTERNS)


def is_financial_column(column_name: str) -> bool:
    """Check if column name suggests it's a financial/measurement column."""
    col_lower = column_name.lower()
    return any(pattern in col_lower for pattern in FINANCIAL_PATTERNS)


def infer_column_type(values: list, column_name: str) -> str:
    """
    Infer the column type from sample values and column name.

    Args:
        values: List of ALL values for a column (or large sample)
        column_name: Name of the column (used for heuristics)

    Returns:
        str: PostgreSQL data type
    """
    # Filter out None/empty values
    non_empty = [v for v in values if v is not None and str(v).strip() != '']

    if not non_empty:
        return "VARCHAR(255)"

    col_lower = column_name.lower()

    # 1. Check column name patterns first (most reliable)

    # ID columns - always BIGINT
    if is_id_column(column_name):
        return "BIGINT"

    # Flag/boolean columns - small integer
    if is_flag_column(column_name):
        return "SMALLINT"

    # Year columns - INTEGER
    if is_year_column(column_name):
        return "INTEGER"

    # Financial columns - ALWAYS use DOUBLE PRECISION
    if is_financial_column(column_name):
        return "DOUBLE PRECISION"

    # 2. Now check the actual data values

    # Check if all values are numeric
    all_numeric = True
    has_decimal = False
    has_negative = False
    max_int_val = 0

    for v in non_empty:
        v_str = str(v).strip()
        try:
            float_val = float(v_str)
            if '.' in v_str:
                has_decimal = True
            if float_val < 0:
                has_negative = True
            if float_val == int(float_val):
                max_int_val = max(max_int_val, abs(int(float_val)))
        except (ValueError, TypeError):
            all_numeric = False
            break

    if all_numeric:
        # If ANY value has a decimal point, use DOUBLE PRECISION
        if has_decimal:
            return "DOUBLE PRECISION"

        # If values are small integers (0-1), might be a flag
        if max_int_val <= 1 and not has_negative:
            return "SMALLINT"

        # If values look like years (1900-2100 range)
        if 1900 <= max_int_val <= 2100 and not has_negative:
            return "INTEGER"

        # For large integers, consider if this might be financial data
        numeric_patterns = ['total', 'sum', 'count', 'amount', 'value', 'number']
        if any(p in col_lower for p in numeric_patterns):
            return "DOUBLE PRECISION"

        # Default integer type
        return "BIGINT"

    # Check if values look like dates (YYYY-MM-DD format)
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}')
    if all(date_pattern.match(str(v)) for v in non_empty[:20]):
        return "DATE"

    # Default to VARCHAR with appropriate size based on actual data
    max_len = max(len(str(v)) for v in non_empty)
    # Add some buffer for future data
    max_len = int(max_len * 1.2) + 10

    if max_len <= 10:
        return "VARCHAR(10)"
    elif max_len <= 20:
        return "VARCHAR(20)"
    elif max_len <= 30:
        return "VARCHAR(30)"
    elif max_len <= 50:
        return "VARCHAR(50)"
    elif max_len <= 100:
        return "VARCHAR(100)"
    elif max_len <= 200:
        return "VARCHAR(200)"
    elif max_len <= 255:
        return "VARCHAR(255)"
    elif max_len <= 500:
        return "VARCHAR(500)"
    elif max_len <= 1000:
        return "VARCHAR(1000)"
    else:
        return "TEXT"


def generate_ddl_from_csv(csv_path: Path, table_name: str,
                          description: str = None, sample_all: bool = True) -> str:
    """
    Generate a CREATE TABLE DDL statement from a CSV file.

    Args:
        csv_path: Path to the source CSV file
        table_name: Name of the table
        description: Optional description (not used in DDL itself)
        sample_all: If True, read all rows for type inference (recommended)

    Returns:
        str: CREATE TABLE DDL statement
    """
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)

        # Read ALL rows for accurate type inference
        all_rows = []
        for row in reader:
            all_rows.append(row)
            # Limit to prevent memory issues on very large files
            if not sample_all and len(all_rows) >= 1000:
                break

    # Collect values by column for type inference
    column_values = {h: [] for h in headers}
    for row in all_rows:
        for i, h in enumerate(headers):
            if i < len(row):
                column_values[h].append(row[i])

    # Infer column types
    column_types = {h: infer_column_type(column_values[h], h) for h in headers}

    # Build DDL statement for PostgreSQL
    ddl_lines = [f'DROP TABLE IF EXISTS {table_name} CASCADE;']
    ddl_lines.append(f'CREATE TABLE {table_name} (')

    column_defs = []
    for h in headers:
        col_type = column_types[h]
        # Check if this might be a primary key (first column ending with _ID)
        is_pk = h.upper().endswith('_ID') and headers.index(h) == 0

        if is_pk:
            column_defs.append(f'\t{h.upper()} {col_type} PRIMARY KEY')
        else:
            column_defs.append(f'\t{h.upper()} {col_type}')

    ddl_lines.append(',\n'.join(column_defs))
    ddl_lines.append(');')

    return '\n'.join(ddl_lines)


def generate_ddl_csv(db_folder: Path, descriptions: dict = None):
    """
    Generate DDL.csv for all tables in a database folder.

    Args:
        db_folder: Path to the database schema folder (e.g., databases/MYDB/MYDB/)
        descriptions: Optional dict mapping table names to descriptions
    """
    data_dir = db_folder / "data"

    # Get database and schema names from folder structure
    schema_name = db_folder.name
    db_name = db_folder.parent.name

    if not data_dir.exists():
        # Try looking for CSV files directly in the folder
        csv_files = [f for f in db_folder.glob("*.csv")
                     if f.name.upper() not in ('DDL.CSV', 'TOTAL_DATA.CSV')]
        if not csv_files:
            print(f"No data directory or CSV files found in {db_folder}")
            return
    else:
        csv_files = [f for f in data_dir.glob("*.csv")
                     if f.name.lower() not in ('total_data.csv', 'ddl.csv')]

    if not csv_files:
        print(f"No CSV files found")
        return

    print(f"\nGenerating DDL.csv for: {db_name}.{schema_name}")
    print(f"Found {len(csv_files)} CSV files")

    if descriptions is None:
        descriptions = {}

    ddl_output_path = db_folder / 'DDL.csv'

    with open(ddl_output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['table_name', 'description', 'DDL'])

        for csv_file in sorted(csv_files):
            table_name = csv_file.stem.upper()
            description = descriptions.get(table_name, '')

            ddl = generate_ddl_from_csv(csv_file, table_name, description)

            writer.writerow([table_name, description, ddl])
            print(f"  Generated DDL for: {table_name}")

    print(f"  Written: {ddl_output_path}")


def main():
    parser = argparse.ArgumentParser(description='Generate DDL.csv files from CSV data')
    parser.add_argument('--database', type=str,
                        help='Specific database folder to process')
    parser.add_argument('--databases-dir', type=str, default='databases',
                        help='Directory containing database folders')

    args = parser.parse_args()

    databases_path = Path(args.databases_dir)

    if args.database:
        db_folders = list(databases_path.glob(f"{args.database}/{args.database}"))
    else:
        db_folders = [p for p in databases_path.glob("*/*") if p.is_dir()]

    if not db_folders:
        print(f"No database folders found in {databases_path}")
        return

    for db_folder in db_folders:
        generate_ddl_csv(db_folder)

    print("\nDDL generation complete!")


if __name__ == "__main__":
    main()
