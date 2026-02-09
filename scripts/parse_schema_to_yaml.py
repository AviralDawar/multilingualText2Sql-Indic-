"""
Schema Parser - Convert LLM-generated schema_info.md to schema_config.yaml

This script parses the markdown output from create_schema_llm_judge.py and generates
a machine-readable YAML configuration file for use with generic_split.py.

Usage:
    python parse_schema_to_yaml.py --schema schema_info.md --csv data/total_data.csv
    python parse_schema_to_yaml.py --schema schema_info.md --csv data/total_data.csv --output schema_config.yaml
"""

import re
import csv
import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from collections import OrderedDict


class SchemaParser:
    """Parses schema_info.md markdown and generates YAML configuration."""

    def __init__(self, schema_path: Path, csv_path: Path):
        self.schema_path = schema_path
        self.csv_path = csv_path
        self.csv_headers: List[str] = []
        self.csv_headers_lower: List[str] = []
        self.dimension_tables: Dict[str, Dict] = {}
        self.fact_tables: Dict[str, Dict] = {}
        self.table_descriptions: Dict[str, str] = {}
        self.column_mapping: List[Dict] = []  # Original column -> table.column mappings

    def parse(self) -> Dict[str, Any]:
        """Main parsing pipeline."""
        # Load CSV headers
        self._load_csv_headers()

        # Load and parse markdown
        with open(self.schema_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Parse different sections
        self._parse_tables(content)
        self._parse_column_mapping(content)

        # Build YAML structure
        return self._build_yaml_config()

    def _load_csv_headers(self):
        """Load column headers from CSV file."""
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            self.csv_headers = next(reader)
            self.csv_headers_lower = [h.lower().strip() for h in self.csv_headers]

        print(f"Loaded {len(self.csv_headers)} CSV columns")

    def _find_csv_index(self, column_name: str) -> Optional[int]:
        """Find the CSV index for a given column name using fuzzy matching."""
        name_lower = column_name.lower().strip()

        # Direct match
        if name_lower in self.csv_headers_lower:
            return self.csv_headers_lower.index(name_lower)

        # Clean and normalize for matching
        def normalize(s: str) -> str:
            # Remove common prefixes/suffixes and normalize
            s = s.lower().strip()
            s = re.sub(r'[_\-\s]+', ' ', s)
            s = re.sub(r'\s+', ' ', s)
            return s

        name_norm = normalize(name_lower)

        # Try normalized matching
        for i, header in enumerate(self.csv_headers):
            if normalize(header) == name_norm:
                return i

        # Try partial matching (column name contains or is contained by header)
        for i, header in enumerate(self.csv_headers):
            header_norm = normalize(header)
            if name_norm in header_norm or header_norm in name_norm:
                return i

        return None

    def _parse_tables(self, content: str):
        """Parse dimension and fact table definitions from markdown."""
        # Split into sections
        dim_section = re.search(
            r'##\s*DIMENSION\s+TABLES.*?\n(.*?)(?=##\s*FACT\s+TABLES|$)',
            content,
            re.DOTALL | re.IGNORECASE
        )

        fact_section = re.search(
            r'##\s*FACT\s+TABLES.*?\n(.*?)(?=##\s*RELATIONAL|##\s*COLUMN\s+MAPPING|$)',
            content,
            re.DOTALL | re.IGNORECASE
        )

        if dim_section:
            self._parse_table_section(dim_section.group(1), is_dimension=True)

        if fact_section:
            self._parse_table_section(fact_section.group(1), is_dimension=False)

    def _parse_table_section(self, section: str, is_dimension: bool):
        """Parse individual table definitions from a section."""
        # Find all table headers (### **TABLE_NAME**)
        table_pattern = r'###\s*\*?\*?([A-Z][A-Z0-9_]*)\*?\*?\s*\n(.*?)(?=###|\Z)'
        tables = re.findall(table_pattern, section, re.DOTALL | re.IGNORECASE)

        for table_name, table_content in tables:
            table_name = table_name.strip().upper()
            table_info = self._parse_table_content(table_name, table_content)

            if is_dimension:
                self.dimension_tables[table_name] = table_info
            else:
                self.fact_tables[table_name] = table_info

            # Extract description
            purpose_match = re.search(r'\*\*Purpose:\*\*\s*(.+?)(?:\n|$)', table_content)
            if purpose_match:
                self.table_descriptions[table_name] = purpose_match.group(1).strip()

    def _parse_table_content(self, table_name: str, content: str) -> Dict:
        """Parse columns and keys from table content."""
        table_info = {
            'columns': [],
            'key_column': None,
            'foreign_keys': [],
            'dedup_columns': []
        }

        # Parse columns section
        columns_match = re.search(r'\*\*Columns\s*\([^)]+\):\*\*\s*\n(.*?)(?=\*\*Sample|$)', content, re.DOTALL)
        if columns_match:
            columns_text = columns_match.group(1)
            # Match column definitions like: - `column_name` (TYPE)
            col_pattern = r'-\s*`([^`]+)`\s*\(([^)]+)\)'
            for match in re.finditer(col_pattern, columns_text):
                col_name = match.group(1).strip()
                col_type_info = match.group(2).strip()

                # Check if this is a PK
                if 'PK' in col_type_info.upper():
                    table_info['key_column'] = col_name.upper()
                    continue  # PKs are auto-generated, skip

                # Check if this is a FK
                fk_match = re.search(r'FK\s*→\s*(\w+)', col_type_info, re.IGNORECASE)
                if fk_match:
                    ref_table = fk_match.group(1).upper()
                    table_info['foreign_keys'].append({
                        'column': col_name.upper(),
                        'references': ref_table
                    })
                    continue  # FKs are linked automatically

                # Regular column - try to find CSV mapping
                table_info['columns'].append({
                    'target_name': col_name.upper(),
                    'type_hint': self._parse_type(col_type_info),
                    'source_index': None  # Will be resolved later
                })

        # Set default key column if not found
        if not table_info['key_column']:
            table_info['key_column'] = f"{table_name}_ID"

        return table_info

    def _parse_type(self, type_info: str) -> str:
        """Extract data type from type info string."""
        # Remove PK/FK markers
        type_info = re.sub(r'PK|FK\s*→\s*\w+', '', type_info, flags=re.IGNORECASE)
        type_info = type_info.strip(' ,')

        # Map common types
        type_lower = type_info.lower()
        if 'int' in type_lower or 'bigint' in type_lower:
            return 'INTEGER'
        elif 'varchar' in type_lower or 'char' in type_lower or 'text' in type_lower:
            return 'VARCHAR'
        elif 'double' in type_lower or 'float' in type_lower or 'decimal' in type_lower:
            return 'DOUBLE PRECISION'
        elif 'date' in type_lower:
            return 'DATE'

        return type_info.upper() if type_info else 'VARCHAR'

    def _parse_column_mapping(self, content: str):
        """Parse the column mapping section from markdown."""
        # Look for column mapping table
        mapping_section = re.search(
            r'##\s*COLUMN\s+MAPPING.*?\n(.*?)(?=##|---\s*$|\Z)',
            content,
            re.DOTALL | re.IGNORECASE
        )

        if not mapping_section:
            print("Warning: No column mapping section found in schema_info.md")
            return

        mapping_text = mapping_section.group(1)

        # 6-column format: | Original Column | Source Index | Data Type | Mapped To Table | Mapped Column(s) | Notes |
        # Process line by line to avoid cross-line matching issues
        for line in mapping_text.split('\n'):
                line = line.strip()
                if not line or not line.startswith('|'):
                    continue

                # Split by | and filter empty parts
                parts = [p.strip() for p in line.split('|')]
                # Remove empty strings at start/end from leading/trailing |
                parts = [p for p in parts if p]

                # Need at least 5 columns (Original, Index, Type, Table, Column)
                if len(parts) < 5:
                    continue

                original_col = parts[0]
                source_idx_str = parts[1]
                data_type = parts[2]
                mapped_tables_str = parts[3]
                mapped_col = parts[4]

                # Skip header and separator rows
                if original_col.lower() in ('original column', '---', '-') or source_idx_str.lower() in ('source index', '---', '-'):
                    continue
                if '---' in original_col or '---' in source_idx_str:
                    continue

                # Parse source index - prefer explicit index over fuzzy matching
                csv_index = None
                if source_idx_str.isdigit():
                    csv_index = int(source_idx_str)
                else:
                    # Fallback to fuzzy matching
                    csv_index = self._find_csv_index(original_col)

                # Handle multiple tables (comma-separated)
                # e.g., "FACT_INSTRUCTIONAL_CALENDAR, FACT_SCHOOL_HOURS, DIM_PEDAGOGY"
                tables = [t.strip().upper() for t in mapped_tables_str.split(',') if t.strip()]

                for table in tables:
                    self.column_mapping.append({
                        'original_column': original_col,
                        'csv_index': csv_index,
                        'data_type': data_type,
                        'target_table': table,
                        'target_column': mapped_col.upper()
                    })
        print(f"Parsed {len(self.column_mapping)} column mappings")

        # Debug: show all mappings
        for m in self.column_mapping:
            print(f"  [{m['csv_index']}] {m['original_column']} -> {m['target_table']}.{m['target_column']}")

    def _resolve_column_indices(self):
        """Resolve CSV indices for all table columns using the column mapping."""
        # Build a lookup: (table, column) -> csv_index
        mapping_lookup = {}
        for m in self.column_mapping:
            if m['csv_index'] is not None:
                key = (m['target_table'], m['target_column'])
                mapping_lookup[key] = m['csv_index']

        # Also create a lookup by column name only (for fuzzy matching)
        col_to_index = {}
        for m in self.column_mapping:
            if m['csv_index'] is not None:
                col_to_index[m['target_column'].lower()] = m['csv_index']
                # Also store original column name mapping
                col_to_index[m['original_column'].lower()] = m['csv_index']

        # Resolve for dimension tables
        for table_name, table_info in self.dimension_tables.items():
            dedup_indices = []
            for col in table_info['columns']:
                target = col['target_name']
                key = (table_name, target)

                if key in mapping_lookup:
                    col['source_index'] = mapping_lookup[key]
                elif target.lower() in col_to_index:
                    col['source_index'] = col_to_index[target.lower()]
                else:
                    # Try to find by partial match
                    idx = self._find_csv_index(target)
                    if idx is not None:
                        col['source_index'] = idx

                if col['source_index'] is not None:
                    dedup_indices.append(col['source_index'])

            table_info['dedup_columns'] = dedup_indices

        # Resolve for fact tables
        for table_name, table_info in self.fact_tables.items():
            for col in table_info['columns']:
                target = col['target_name']
                key = (table_name, target)

                if key in mapping_lookup:
                    col['source_index'] = mapping_lookup[key]
                elif target.lower() in col_to_index:
                    col['source_index'] = col_to_index[target.lower()]
                else:
                    idx = self._find_csv_index(target)
                    if idx is not None:
                        col['source_index'] = idx

    def _build_yaml_config(self) -> Dict[str, Any]:
        """Build the final YAML configuration structure."""
        # Resolve column indices before building config
        self._resolve_column_indices()

        # Extract database name from path
        db_name = self.schema_path.parent.name
        if db_name == 'data':
            db_name = self.schema_path.parent.parent.name

        config = OrderedDict()
        config['database_name'] = db_name
        config['source_file'] = self.csv_path.name

        # Build dimension tables config
        config['dimension_tables'] = OrderedDict()
        for table_name, table_info in self.dimension_tables.items():
            table_config = OrderedDict()
            table_config['key_column'] = table_info['key_column']
            table_config['dedup_columns'] = table_info['dedup_columns']

            # Build columns list
            columns = []
            for col in table_info['columns']:
                col_config = OrderedDict()
                if col['source_index'] is not None:
                    col_config['source_index'] = col['source_index']
                col_config['target_name'] = col['target_name']
                columns.append(col_config)

            table_config['columns'] = columns

            # Add foreign keys if any
            if table_info['foreign_keys']:
                table_config['foreign_keys'] = table_info['foreign_keys']

            config['dimension_tables'][table_name] = table_config

        # Build fact tables config
        config['fact_tables'] = OrderedDict()
        for table_name, table_info in self.fact_tables.items():
            table_config = OrderedDict()
            table_config['key_column'] = table_info['key_column']

            # Add foreign keys
            if table_info['foreign_keys']:
                table_config['foreign_keys'] = table_info['foreign_keys']

            # Build columns list
            columns = []
            for col in table_info['columns']:
                col_config = OrderedDict()
                if col['source_index'] is not None:
                    col_config['source_index'] = col['source_index']
                col_config['target_name'] = col['target_name']
                columns.append(col_config)

            table_config['columns'] = columns
            config['fact_tables'][table_name] = table_config

        # Add table descriptions
        config['table_descriptions'] = self.table_descriptions

        return config


def represent_ordereddict(dumper, data):
    """Custom YAML representer for OrderedDict to maintain key order."""
    return dumper.represent_mapping('tag:yaml.org,2002:map', data.items())


def main():
    parser = argparse.ArgumentParser(
        description='Parse schema_info.md and generate schema_config.yaml',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
    python parse_schema_to_yaml.py --schema databases/MYDB/MYDB/schema_info.md --csv databases/MYDB/MYDB/data/total_data.csv
    python parse_schema_to_yaml.py --schema schema_info.md --csv data.csv --output config.yaml
        """
    )
    parser.add_argument('--schema', type=str, required=True,
                        help='Path to schema_info.md file')
    parser.add_argument('--csv', type=str, required=True,
                        help='Path to source CSV file (for header matching)')
    parser.add_argument('--output', type=str,
                        help='Output path for schema_config.yaml (default: same directory as schema)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show verbose output including unresolved columns')

    args = parser.parse_args()

    schema_path = Path(args.schema)
    csv_path = Path(args.csv)

    if not schema_path.exists():
        print(f"Error: Schema file not found: {schema_path}")
        return 1

    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        return 1

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = schema_path.parent / 'schema_config.yaml'

    print(f"Parsing: {schema_path}")
    print(f"CSV headers from: {csv_path}")

    # Parse schema
    parser_obj = SchemaParser(schema_path, csv_path)
    config = parser_obj.parse()

    # Report parsing results
    print(f"\nParsed schema:")
    print(f"  Database: {config['database_name']}")
    print(f"  Dimension tables: {len(config['dimension_tables'])}")
    for table_name in config['dimension_tables']:
        cols = config['dimension_tables'][table_name].get('columns', [])
        resolved = sum(1 for c in cols if c.get('source_index') is not None)
        print(f"    - {table_name}: {len(cols)} columns ({resolved} resolved)")

    print(f"  Fact tables: {len(config['fact_tables'])}")
    for table_name in config['fact_tables']:
        cols = config['fact_tables'][table_name].get('columns', [])
        resolved = sum(1 for c in cols if c.get('source_index') is not None)
        print(f"    - {table_name}: {len(cols)} columns ({resolved} resolved)")

    # Show unresolved columns if verbose
    if args.verbose:
        print("\nUnresolved columns (need manual mapping):")
        for table_type in ['dimension_tables', 'fact_tables']:
            for table_name, table_config in config[table_type].items():
                for col in table_config.get('columns', []):
                    if col.get('source_index') is None:
                        print(f"  {table_name}.{col['target_name']}")

    # Register custom representer for OrderedDict
    yaml.add_representer(OrderedDict, represent_ordereddict)

    # Write YAML output
    with open(output_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"\nWritten: {output_path}")
    print("\nNext step: Review and manually fix any unresolved source_index values,")
    print("then run: python generic_split.py --config " + str(output_path))

    return 0


if __name__ == "__main__":
    sys.exit(main())
