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
from typing import Dict, List, Optional

try:
    import yaml
except ImportError:
    yaml = None


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


def infer_type_from_name(column_name: str) -> str:
    """Infer a reasonable type using only the column name."""
    if is_id_column(column_name):
        return "BIGINT"
    if is_year_column(column_name):
        return "INTEGER"
    if is_flag_column(column_name):
        return "SMALLINT"
    if is_financial_column(column_name):
        return "DOUBLE PRECISION"
    return "VARCHAR(255)"


def infer_key_type(column_name: str) -> str:
    """Infer key column type from naming conventions."""
    col_upper = column_name.upper()
    if col_upper == "YEAR" or is_year_column(column_name):
        return "INTEGER"
    return "BIGINT"


def load_schema_metadata(db_folder: Path) -> Dict[str, dict]:
    """
    Load schema metadata from schema_config.yaml when available.

    Returns:
        Dict keyed by uppercase table name with:
        - key_column: primary key column
        - columns: regular data columns
        - foreign_keys: [{column, references, references_column}]
    """
    config_path = db_folder / "schema_config.yaml"
    if not config_path.exists():
        return {}
    if yaml is None:
        print("Warning: PyYAML not installed; generating DDL without schema_config metadata.")
        return {}

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f) or {}

    table_metadata: Dict[str, dict] = {}
    all_tables = {}
    all_tables.update(config.get("dimension_tables", {}))
    all_tables.update(config.get("fact_tables", {}))

    for table_name, table_cfg in all_tables.items():
        key_column = (table_cfg.get("key_column") or f"{table_name}_ID").upper()

        yaml_columns = []
        for col in table_cfg.get("columns", []):
            target_name = col.get("target_name")
            if target_name:
                yaml_columns.append(target_name.upper())

        fk_defs = []
        for fk in table_cfg.get("foreign_keys", []):
            fk_column = fk.get("column")
            ref_table = fk.get("references")
            if not fk_column or not ref_table:
                continue
            fk_defs.append({
                "column": fk_column.upper(),
                "references": ref_table.upper(),
                "references_column": (fk.get("references_column") or "").upper()
            })

        table_metadata[table_name.upper()] = {
            "key_column": key_column,
            "columns": yaml_columns,
            "foreign_keys": fk_defs
        }

    # Backfill referenced columns using target table key when not explicitly set.
    for table_cfg in table_metadata.values():
        for fk in table_cfg["foreign_keys"]:
            if not fk["references_column"]:
                ref_cfg = table_metadata.get(fk["references"])
                fk["references_column"] = ref_cfg["key_column"] if ref_cfg else fk["column"]

    return table_metadata


def read_csv_schema(csv_path: Optional[Path], sample_all: bool = True) -> tuple[List[str], Dict[str, List[str]]]:
    """
    Read CSV headers and column values used for type inference.

    Returns:
        headers: Uppercase column names in original order
        column_values: Mapping uppercase column name -> observed values
    """
    if csv_path is None or not csv_path.exists():
        return [], {}

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        raw_headers = next(reader)
        headers = [h.upper() for h in raw_headers]

        all_rows = []
        for row in reader:
            all_rows.append(row)
            if not sample_all and len(all_rows) >= 1000:
                break

    column_values = {h: [] for h in headers}
    for row in all_rows:
        for i, h in enumerate(headers):
            if i < len(row):
                column_values[h].append(row[i])

    return headers, column_values


def build_column_order(headers: List[str], table_schema: Optional[dict]) -> List[str]:
    """Build final column order: PK, FKs, YAML columns, then remaining CSV columns."""
    ordered = []
    seen = set()

    def add(col: Optional[str]):
        if not col:
            return
        col_upper = col.upper()
        if col_upper not in seen:
            ordered.append(col_upper)
            seen.add(col_upper)

    if table_schema:
        add(table_schema.get("key_column"))
        for fk in table_schema.get("foreign_keys", []):
            add(fk.get("column"))
        for col in table_schema.get("columns", []):
            add(col)

    for h in headers:
        add(h)

    return ordered


def generate_ddl_from_csv(csv_path: Optional[Path], table_name: str,
                          description: str = None, sample_all: bool = True,
                          table_schema: Optional[dict] = None,
                          key_types: Optional[Dict[str, str]] = None) -> str:
    """
    Generate a CREATE TABLE DDL statement from a CSV file.

    Args:
        csv_path: Path to the source CSV file (optional for schema-only tables)
        table_name: Name of the table
        description: Optional description (not used in DDL itself)
        sample_all: If True, read all rows for type inference (recommended)
        table_schema: Optional table metadata from schema_config.yaml
        key_types: Mapping of table -> key type used for FK compatibility

    Returns:
        str: CREATE TABLE DDL statement
    """
    key_types = key_types or {}
    headers, column_values = read_csv_schema(csv_path, sample_all=sample_all)
    inferred_csv_types = {h: infer_column_type(column_values[h], h) for h in headers}

    key_column = table_schema.get("key_column") if table_schema else None
    if key_column:
        key_column = key_column.upper()
    elif headers:
        # Backward-compatible fallback for legacy datasets without schema_config.
        first_col = headers[0].upper()
        if first_col.endswith("_ID") or first_col == "ID":
            key_column = first_col
    fk_map = {}
    fk_defs = []
    if table_schema:
        for fk in table_schema.get("foreign_keys", []):
            fk_col = fk["column"].upper()
            ref_table = fk["references"].upper()
            ref_col = fk.get("references_column", "").upper()
            fk_map[fk_col] = (ref_table, ref_col)
            fk_defs.append((fk_col, ref_table, ref_col))

    ordered_columns = build_column_order(headers, table_schema)
    if not ordered_columns:
        raise ValueError(f"No columns found for table {table_name}")

    # Build DDL statement for PostgreSQL
    ddl_lines = [f'DROP TABLE IF EXISTS {table_name} CASCADE;']
    ddl_lines.append(f'CREATE TABLE {table_name} (')

    column_defs = []
    for col in ordered_columns:
        if key_column and col == key_column:
            col_type = key_types.get(table_name.upper(), infer_key_type(col))
        elif col in fk_map:
            ref_table, ref_col = fk_map[col]
            col_type = key_types.get(ref_table, infer_key_type(ref_col or col))
        elif col in inferred_csv_types:
            col_type = inferred_csv_types[col]
        else:
            col_type = infer_type_from_name(col)

        if key_column and col == key_column:
            column_defs.append(f'\t{col} {col_type} PRIMARY KEY')
        else:
            column_defs.append(f'\t{col} {col_type}')

    for fk_col, ref_table, ref_col in fk_defs:
        if fk_col in ordered_columns:
            ddl_lines_fk = f'\tFOREIGN KEY ({fk_col}) REFERENCES {ref_table} ({ref_col})'
            column_defs.append(ddl_lines_fk)

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
    else:
        csv_files = [f for f in data_dir.glob("*.csv")
                     if f.name.lower() not in ('total_data.csv', 'ddl.csv')]

    schema_metadata = load_schema_metadata(db_folder)

    if not csv_files and not schema_metadata:
        print(f"No CSV files found")
        return

    print(f"\nGenerating DDL.csv for: {db_name}.{schema_name}")
    print(f"Found {len(csv_files)} CSV files")
    if schema_metadata:
        print(f"Loaded schema_config.yaml metadata for {len(schema_metadata)} tables")

    if descriptions is None:
        descriptions = {}

    csv_by_table = {f.stem.upper(): f for f in csv_files}

    key_types = {}
    for table_name, table_cfg in schema_metadata.items():
        key_col = table_cfg.get("key_column")
        if key_col:
            key_types[table_name] = infer_key_type(key_col)

    table_names = []
    for table_name in schema_metadata.keys():
        if table_name not in table_names:
            table_names.append(table_name)
    for table_name in sorted(csv_by_table.keys()):
        if table_name not in table_names:
            table_names.append(table_name)

    ddl_output_path = db_folder / 'DDL.csv'

    with open(ddl_output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['table_name', 'description', 'DDL'])

        for table_name in table_names:
            csv_file = csv_by_table.get(table_name)
            description = descriptions.get(table_name, '')
            table_schema = schema_metadata.get(table_name)

            try:
                ddl = generate_ddl_from_csv(
                    csv_file,
                    table_name,
                    description,
                    table_schema=table_schema,
                    key_types=key_types
                )
                writer.writerow([table_name, description, ddl])
                print(f"  Generated DDL for: {table_name}")
            except ValueError as e:
                print(f"  Skipping {table_name}: {e}")

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