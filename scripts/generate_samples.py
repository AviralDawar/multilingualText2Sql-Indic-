"""
Script to generate sample JSON files from CSV data.

Takes CSV files and creates JSON files with sample rows for reference.
These JSON files are NOT used for data loading - they serve as documentation
showing the structure and sample data for each table.

Output format matches Spider2 style:
{
    "table_name": "DB.TABLE",
    "table_fullname": "DB.SCHEMA.TABLE",
    "column_names": [...],
    "column_types": [...],
    "description": [...],
    "sample_rows": [...]
}
"""

import csv
import json
import argparse
from pathlib import Path


def infer_column_type(values: list) -> str:
    """
    Infer the column type from sample values.

    Args:
        values: List of sample values for a column

    Returns:
        str: Inferred type (NUMBER, TEXT, DATE, etc.)
    """
    # Filter out None/empty values
    non_empty = [v for v in values if v is not None and v != '']

    if not non_empty:
        return "TEXT"

    # Check if all values are numeric
    all_numeric = True
    all_integer = True
    for v in non_empty:
        try:
            float_val = float(v)
            if float_val != int(float_val):
                all_integer = False
        except (ValueError, TypeError):
            all_numeric = False
            all_integer = False
            break

    if all_numeric:
        return "NUMBER"

    # Check if values look like dates (YYYY-MM-DD format)
    import re
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}')
    if all(date_pattern.match(str(v)) for v in non_empty):
        return "DATE"

    return "TEXT"


def convert_value(value: str, col_type: str):
    """
    Convert a string value to appropriate Python type.

    Args:
        value: String value from CSV
        col_type: Inferred column type

    Returns:
        Converted value (int, float, str, or None)
    """
    if value == '' or value is None:
        return None

    if col_type == "NUMBER":
        try:
            float_val = float(value)
            # Return int if it's a whole number
            if float_val == int(float_val):
                return int(float_val)
            return float_val
        except (ValueError, TypeError):
            return value

    return value


def generate_sample_json(csv_path: Path, output_path: Path, db_name: str, schema_name: str, sample_size: int = 5):
    """
    Generate a sample JSON file from a CSV file in Spider2 format.

    Args:
        csv_path: Path to the source CSV file
        output_path: Path for the output JSON file
        db_name: Database name
        schema_name: Schema name
        sample_size: Number of sample rows to include
    """
    table_name = csv_path.stem.upper()

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)

        # Read all rows to infer types (but only keep sample_size for output)
        all_rows = list(reader)

    # Collect values by column for type inference
    column_values = {h: [] for h in headers}
    for row in all_rows[:min(100, len(all_rows))]:  # Use up to 100 rows for type inference
        for i, h in enumerate(headers):
            if i < len(row):
                column_values[h].append(row[i])

    # Infer column types
    column_types = [infer_column_type(column_values[h]) for h in headers]

    # Build sample rows with proper type conversion
    sample_rows = []
    for row in all_rows[:sample_size]:
        row_dict = {}
        for i, h in enumerate(headers):
            value = row[i] if i < len(row) else None
            row_dict[h] = convert_value(value, column_types[i])
        sample_rows.append(row_dict)

    # Build output structure
    output = {
        "table_name": f"{db_name}.{table_name}",
        "table_fullname": f"{db_name}.{schema_name}.{table_name}",
        "column_names": headers,
        "column_types": column_types,
        "description": [None] * len(headers),
        "sample_rows": sample_rows
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=4, default=str)

    return len(sample_rows)


def process_database_folder(db_folder: Path, sample_size: int = 5):
    """
    Process all CSV files in a database folder's data directory
    and generate sample JSON files.

    Args:
        db_folder: Path to the database schema folder (e.g., databases/MYDB/MYDB/)
        sample_size: Number of sample rows per table
    """
    data_dir = db_folder / "data"

    # Get database and schema names from folder structure
    schema_name = db_folder.name
    db_name = db_folder.parent.name

    if not data_dir.exists():
        # Try looking for CSV files directly in the folder
        csv_files = [f for f in db_folder.glob("*.csv") if f.name.upper() != "DDL.CSV"]
        if not csv_files:
            print(f"No data directory or CSV files found in {db_folder}")
            return
    else:
        csv_files = [f for f in data_dir.glob("*.csv") if f.name.lower() != "total_data.csv"]

    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        return

    print(f"\nGenerating samples for: {db_name}.{schema_name}")
    print(f"Found {len(csv_files)} CSV files")

    for csv_file in csv_files:
        table_name = csv_file.stem.upper()
        output_path = db_folder / f"{table_name}.json"

        rows = generate_sample_json(csv_file, output_path, db_name, schema_name, sample_size)
        print(f"  Created: {table_name}.json ({rows} sample rows)")


def main():
    parser = argparse.ArgumentParser(description='Generate sample JSON files from CSV data')
    parser.add_argument('--database', type=str,
                        help='Specific database folder to process')
    parser.add_argument('--databases-dir', type=str, default='databases',
                        help='Directory containing database folders')
    parser.add_argument('--sample-size', type=int, default=5,
                        help='Number of sample rows per table (default: 5)')

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
        process_database_folder(db_folder, args.sample_size)

    print("\nSample generation complete!")


if __name__ == "__main__":
    main()