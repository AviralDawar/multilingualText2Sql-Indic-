"""
Generic Data Split Engine - Configuration-driven CSV normalization.

This script reads a schema_config.yaml file and splits a denormalized CSV
into multiple normalized tables based on the configuration.

Usage:
    python generic_split.py --config databases/MYDB/MYDB/schema_config.yaml
    python generic_split.py --config schema_config.yaml --input data/total_data.csv --output databases/MYDB/MYDB/
"""

import csv
import yaml
import json
import argparse
import re
import sys
from pathlib import Path
from collections import OrderedDict
from typing import Dict, List, Any, Callable, Optional

# Add parent directory to path to import generate_ddl
sys.path.insert(0, str(Path(__file__).parent))
from generate_ddl import generate_ddl_csv


# Built-in value transformers
TRANSFORMERS: Dict[str, Callable] = {
    "extract_year": lambda v: str(re.search(r'(\d{4})', str(v)).group(1)) if v and re.search(r'(\d{4})', str(v)) else '',
    "uppercase": lambda v: str(v).upper().strip() if v else '',
    "lowercase": lambda v: str(v).lower().strip() if v else '',
    "strip": lambda v: str(v).strip() if v else '',
    "to_int": lambda v: str(int(float(v))) if v and v.strip() else '',
    "to_float": lambda v: str(float(v)) if v and v.strip() else '',
    "clean_numeric": lambda v: str(round(float(v), 2)) if v and v.strip() else '',
}


def is_valid_value(value: str) -> bool:
    """Check if a value is valid (not empty, not null, not NA)."""
    if value is None:
        return False
    value = str(value).strip()
    if value == '' or value.lower() in ('null', 'na', 'n/a', 'none', '-'):
        return False
    return True


class SchemaConfig:
    """Parses and validates schema_config.yaml"""

    def __init__(self, config_path: Path):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.database_name = self.config.get('database_name', 'UNKNOWN')
        self.source_file = self.config.get('source_file', 'total_data.csv')
        self.dimension_tables = self.config.get('dimension_tables', {})
        self.fact_tables = self.config.get('fact_tables', {})
        self.table_descriptions = self.config.get('table_descriptions', {})
        self.validation_columns = self.config.get('validation_columns', [])

    def validate_against_csv(self, csv_headers: List[str]) -> List[str]:
        """Validate that all source_index values are valid"""
        errors = []
        max_index = len(csv_headers) - 1

        all_tables = {**self.dimension_tables, **self.fact_tables}
        for table_name, table_config in all_tables.items():
            for col in table_config.get('columns', []):
                idx = col.get('source_index')
                if idx is not None and idx > max_index:
                    errors.append(
                        f"{table_name}.{col.get('target_name', 'UNKNOWN')}: "
                        f"source_index {idx} exceeds CSV columns (max: {max_index})"
                    )

            # Validate dedup_columns for dimension tables
            for idx in table_config.get('dedup_columns', []):
                if idx > max_index:
                    errors.append(
                        f"{table_name}: dedup_column {idx} exceeds CSV columns (max: {max_index})"
                    )

        return errors

    def get_all_required_columns(self) -> set:
        """Get all column indices that need to be validated"""
        indices = set()

        # Add explicit validation columns
        indices.update(self.validation_columns)

        # Add all source indices from tables
        all_tables = {**self.dimension_tables, **self.fact_tables}
        for table_config in all_tables.values():
            for col in table_config.get('columns', []):
                if col.get('source_index') is not None:
                    indices.add(col['source_index'])
            indices.update(table_config.get('dedup_columns', []))

        return indices


class GenericSplitter:
    """Configuration-driven data splitter"""

    def __init__(self, config: SchemaConfig):
        self.config = config
        self.dimension_data: Dict[str, OrderedDict] = {}  # table -> {key: (id, row_data)}
        self.fact_data: Dict[str, List] = {}  # table -> [rows]
        self.id_counters: Dict[str, int] = {}  # table -> counter

    def process(self, input_file: Path, output_dir: Path, sample_size: int = 5):
        """Main processing pipeline"""
        output_dir.mkdir(parents=True, exist_ok=True)
        data_dir = output_dir / 'data'
        data_dir.mkdir(exist_ok=True)

        # Initialize data structures
        for table_name in self.config.dimension_tables:
            self.dimension_data[table_name] = OrderedDict()
            self.id_counters[table_name] = 1

        for table_name in self.config.fact_tables:
            self.fact_data[table_name] = []
            self.id_counters[table_name] = 1

        # Get required columns for validation
        required_columns = self.config.get_all_required_columns()

        # Statistics
        total_rows = 0
        skipped_rows = 0

        print(f"Reading: {input_file}")
        print(f"Validating {len(required_columns)} columns per row...")

        # Process CSV
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

            # Validate config against CSV
            errors = self.config.validate_against_csv(headers)
            if errors:
                print("Configuration errors:")
                for e in errors:
                    print(f"  - {e}")
                raise ValueError("Invalid configuration - source indices out of range")

            for row in reader:
                total_rows += 1

                # Validate row completeness
                if not self._is_row_complete(row, required_columns):
                    skipped_rows += 1
                    continue

                self._process_row(row)

                if total_rows % 100000 == 0:
                    print(f"  Processed {total_rows} rows...")

        print(f"\nProcessing complete!")
        print(f"  Total rows read: {total_rows}")
        print(f"  Rows skipped (missing data): {skipped_rows}")
        print(f"  Valid rows processed: {total_rows - skipped_rows}")

        for table_name in self.config.dimension_tables:
            print(f"  Unique {table_name}: {len(self.dimension_data[table_name])}")

        # Write outputs
        self._write_outputs(output_dir, data_dir, sample_size)

        return {
            'total_rows': total_rows,
            'skipped_rows': skipped_rows,
            'valid_rows': total_rows - skipped_rows
        }

    def _is_row_complete(self, row: List[str], required_indices: set) -> bool:
        """Check if all required columns have valid values"""
        for idx in required_indices:
            if idx >= len(row) or not is_valid_value(row[idx]):
                return False
        return True

    def _process_row(self, row: List[str]):
        """Process a single CSV row"""
        # Track IDs for foreign keys (both dimension and fact)
        all_ids = {}

        # Process dimension tables first (they generate IDs needed by fact tables)
        for table_name, table_config in self.config.dimension_tables.items():
            dim_id = self._process_dimension_row(table_name, table_config, row, all_ids)
            all_ids[table_name] = dim_id

        # Process fact tables in order (first fact table generates ID, others may reference it)
        fact_tables = list(self.config.fact_tables.items())
        for table_name, table_config in fact_tables:
            fact_id = self._process_fact_row(table_name, table_config, row, all_ids)
            all_ids[table_name] = fact_id

    def _process_dimension_row(self, table_name: str, config: dict, row: List[str], all_ids: Dict[str, int]) -> int:
        """Process dimension table - dedup and return ID"""
        key_source_index = config.get('key_source_index')
        key_transform = config.get('key_transform')
        natural_key = None
        if key_source_index is not None:
            natural_key = self._safe_get(row, key_source_index)
            if key_transform and key_transform in TRANSFORMERS:
                try:
                    natural_key = TRANSFORMERS[key_transform](natural_key)
                except Exception:
                    natural_key = ''
            if not is_valid_value(natural_key):
                natural_key = None

        # Generate dedup key from specified columns
        dedup_cols = config.get('dedup_columns', [])
        dedup_parts = [str(natural_key)] if natural_key is not None else []
        dedup_parts.extend(self._safe_get(row, i) for i in dedup_cols)

        # Include FK parent IDs in dedup key so child values that repeat across
        # different parents are not collapsed into one dimension row.
        for fk in config.get('foreign_keys', []):
            parent_id = all_ids.get(fk['references'])
            dedup_parts.append(str(parent_id) if parent_id is not None else '')

        key = '|'.join(dedup_parts)

        if key not in self.dimension_data[table_name]:
            # Extract column values
            row_data = self._extract_columns(config, row)

            # Add FK columns on dimensions so generated CSV/DDL matches schema_config.
            for fk in config.get('foreign_keys', []):
                fk_column = fk['column']
                fk_value = all_ids.get(fk['references'])
                row_data[fk_column] = fk_value

            if natural_key is not None:
                dim_id = natural_key
            else:
                dim_id = self.id_counters[table_name]
                self.id_counters[table_name] += 1
            self.dimension_data[table_name][key] = (dim_id, row_data)

        return self.dimension_data[table_name][key][0]

    def _process_fact_row(self, table_name: str, config: dict, row: List[str], all_ids: Dict[str, int]) -> int:
        """Process fact table row and return the generated ID"""
        key_column = config.get('key_column', f'{table_name}_ID')

        # Check if this fact table uses an ID from another table (shared key pattern)
        # This happens when foreign_keys references a fact table with the same column name as key
        shared_id = None
        for fk in config.get('foreign_keys', []):
            ref_table = fk['references']
            fk_column = fk['column']
            # If the FK column is the same as our key column, use the referenced table's ID
            if fk_column == key_column and ref_table in all_ids:
                shared_id = all_ids[ref_table]
                break

        # Use shared ID or generate new one
        if shared_id is not None:
            fact_id = shared_id
        else:
            fact_id = self.id_counters[table_name]
            self.id_counters[table_name] += 1

        # Build row data
        row_data = OrderedDict()

        # Add primary key
        row_data[key_column] = fact_id

        # Add foreign keys (skip the one that's the same as key_column)
        for fk in config.get('foreign_keys', []):
            ref_table = fk['references']
            fk_column = fk['column']
            if fk_column != key_column:
                row_data[fk_column] = all_ids.get(ref_table)

        # Add regular columns
        extracted = self._extract_columns(config, row)
        row_data.update(extracted)

        self.fact_data[table_name].append(row_data)
        return fact_id

    def _extract_columns(self, config: dict, row: List[str]) -> OrderedDict:
        """Extract and transform columns based on config"""
        result = OrderedDict()

        for col in config.get('columns', []):
            idx = col.get('source_index')
            target = col['target_name']
            value = self._safe_get(row, idx) if idx is not None else ''

            # Apply transformer if specified
            transform = col.get('transform')
            if transform and transform in TRANSFORMERS:
                try:
                    value = TRANSFORMERS[transform](value)
                except Exception:
                    value = ''

            result[target] = value

        # Handle derived columns (computed from other columns)
        for derived in config.get('derived_columns', []):
            target = derived['target_name']
            transform = derived.get('transform')
            source_col = derived.get('source_column')
            source_index = derived.get('source_index')

            # Get source value
            if source_index is not None:
                source_val = self._safe_get(row, source_index)
            elif source_col and source_col in result:
                source_val = result[source_col]
            else:
                source_val = ''

            # Apply transformer
            if transform and transform in TRANSFORMERS:
                try:
                    result[target] = TRANSFORMERS[transform](source_val)
                except Exception:
                    result[target] = ''
            else:
                result[target] = source_val

        return result

    def _safe_get(self, row: List[str], idx: Optional[int]) -> str:
        """Safely get value from row"""
        if idx is None or idx >= len(row):
            return ''
        return row[idx].strip() if row[idx] else ''

    def _write_outputs(self, output_dir: Path, data_dir: Path, sample_size: int):
        """Write CSV, JSON, and DDL files"""
        print(f"\nWriting output files to: {output_dir}")

        # Write dimension tables
        for table_name, config in self.config.dimension_tables.items():
            key_column = config.get('key_column', f'{table_name}_ID')
            fk_columns = [fk['column'] for fk in config.get('foreign_keys', [])]
            col_names = [c['target_name'] for c in config.get('columns', [])]
            headers = [key_column] + fk_columns + col_names

            rows = []
            for _, (dim_id, data) in self.dimension_data[table_name].items():
                row_values = [dim_id]
                row_values.extend(data.get(col) for col in fk_columns)
                row_values.extend(data.get(col) for col in col_names)
                rows.append(row_values)

            self._write_csv(data_dir / f'{table_name}.csv', rows, headers)
            self._write_json(output_dir / f'{table_name}.json', rows, headers, sample_size)

        # Write fact tables
        for table_name, config in self.config.fact_tables.items():
            if not self.fact_data[table_name]:
                continue

            # Get headers from first row
            first_row = self.fact_data[table_name][0]
            headers = list(first_row.keys())
            rows = [list(r.values()) for r in self.fact_data[table_name]]

            self._write_csv(data_dir / f'{table_name}.csv', rows, headers)
            self._write_json(output_dir / f'{table_name}.json', rows, headers, sample_size)

        # Generate DDL.csv
        print(f"\nWriting DDL.csv file...")
        generate_ddl_csv(output_dir, descriptions=self.config.table_descriptions)

        print(f"\nDone! All files written to {output_dir}")

    def _write_csv(self, path: Path, rows: List, headers: List[str]):
        """Write rows to CSV file"""
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        print(f"  Written: {path.name} ({len(rows)} rows)")

    def _write_json(self, path: Path, rows: List, headers: List[str], sample_size: int):
        """Write sample rows as JSON for reference"""
        samples = []
        for row in rows[:sample_size]:
            sample = OrderedDict()
            for i, h in enumerate(headers):
                sample[h] = row[i] if i < len(row) else None
            samples.append(sample)

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(samples, f, indent=2)
        print(f"  Sample JSON: {path.name} ({len(samples)} rows)")


def main():
    parser = argparse.ArgumentParser(
        description='Generic configuration-driven data splitter',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
    python generic_split.py --config databases/MYDB/MYDB/schema_config.yaml
    python generic_split.py --config config.yaml --input data.csv --output output/
        """
    )
    parser.add_argument('--config', type=str, required=True,
                        help='Path to schema_config.yaml')
    parser.add_argument('--input', type=str,
                        help='Override input CSV path (default: from config)')
    parser.add_argument('--output', type=str,
                        help='Override output directory (default: config directory)')
    parser.add_argument('--sample-size', type=int, default=5,
                        help='Number of sample rows for JSON files')

    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        return 1

    config = SchemaConfig(config_path)

    # Determine paths
    if args.input:
        input_file = Path(args.input)
    else:
        # Default: look in data/ subdirectory relative to config
        input_file = config_path.parent / 'data' / config.source_file

    if args.output:
        output_dir = Path(args.output)
    else:
        # Default: same directory as config
        output_dir = config_path.parent

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1

    # Run splitter
    splitter = GenericSplitter(config)
    stats = splitter.process(input_file, output_dir, args.sample_size)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for key, value in stats.items():
        print(f"  {key}: {value}")

    return 0


if __name__ == "__main__":
    sys.exit(main())