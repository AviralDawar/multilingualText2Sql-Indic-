"""
Database Pipeline Orchestrator - Single entry point for database creation workflow.

This script orchestrates the complete workflow from raw CSV to loaded database:
1. Schema Design (LLM-based) → schema_info.md
2. Schema Parsing → schema_config.yaml
3. Data Splitting → Normalized CSVs + DDL.csv
4. Table Creation → Execute DDL
5. Data Loading → Load CSVs into database

Usage:
    # Full pipeline from raw CSV
    python pipeline.py --database INDIA_NEW_DATA --input data/raw.csv --full

    # Skip schema design (use existing config)
    python pipeline.py --database INDIA_NEW_DATA --skip-design

    # Only generate schema (no DB operations)
    python pipeline.py --database INDIA_NEW_DATA --input data/raw.csv --schema-only

    # Dry run (show what would be done)
    python pipeline.py --database INDIA_NEW_DATA --input data/raw.csv --dry-run
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Optional


class PipelineRunner:
    """Orchestrates the database creation pipeline."""

    def __init__(self, database_name: str, base_dir: Path):
        self.database_name = database_name
        self.base_dir = base_dir
        self.scripts_dir = base_dir / 'scripts'
        self.databases_dir = base_dir / 'databases'
        self.db_dir = self.databases_dir / database_name / database_name
        self.data_dir = self.db_dir / 'data'
        self.config_dir = base_dir / 'config'

    def run_command(self, cmd: list, description: str, dry_run: bool = False) -> bool:
        """Run a command and return success status."""
        print(f"\n{'=' * 60}")
        print(f"STEP: {description}")
        print(f"{'=' * 60}")

        cmd_str = ' '.join(str(c) for c in cmd)
        print(f"Command: {cmd_str}\n")

        if dry_run:
            print("[DRY RUN] Would execute above command")
            return True

        try:
            result = subprocess.run(cmd, cwd=str(self.base_dir), check=True)
            print(f"\n✓ {description} completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"\n✗ {description} failed with exit code {e.returncode}")
            return False
        except FileNotFoundError as e:
            print(f"\n✗ Command not found: {e}")
            return False

    def step_prepare_directories(self, dry_run: bool = False) -> bool:
        """Create necessary directories."""
        print(f"\n{'=' * 60}")
        print("STEP: Preparing directories")
        print(f"{'=' * 60}")

        dirs_to_create = [
            self.db_dir,
            self.data_dir
        ]

        for d in dirs_to_create:
            if dry_run:
                print(f"[DRY RUN] Would create: {d}")
            else:
                d.mkdir(parents=True, exist_ok=True)
                print(f"Created: {d}")

        return True

    def step_copy_input(self, input_file: Path, dry_run: bool = False) -> bool:
        """Copy input CSV to data directory."""
        target = self.data_dir / 'total_data.csv'

        print(f"\n{'=' * 60}")
        print("STEP: Copying input file")
        print(f"{'=' * 60}")
        print(f"Source: {input_file}")
        print(f"Target: {target}")

        if dry_run:
            print("[DRY RUN] Would copy file")
            return True

        import shutil
        try:
            shutil.copy(input_file, target)
            print(f"\n✓ File copied successfully")
            return True
        except Exception as e:
            print(f"\n✗ Failed to copy file: {e}")
            return False

    def step_schema_design(self, csv_path: Path, dry_run: bool = False) -> bool:
        """Run LLM-based schema design."""
        script = self.scripts_dir / 'create_schema_llm_judge.py'
        if not script.exists():
            print(f"Warning: Schema design script not found: {script}")
            print("Skipping schema design step.")
            return True

        return self.run_command(
            ['python', str(script), str(csv_path)],
            "LLM Schema Design (create_schema_llm_judge.py)",
            dry_run
        )

    def step_parse_schema(self, dry_run: bool = False) -> bool:
        """Parse schema_info.md to schema_config.yaml."""
        script = self.scripts_dir / 'parse_schema_to_yaml.py'
        schema_md = self.db_dir / 'schema_info.md'
        csv_file = self.data_dir / 'total_data.csv'

        if not schema_md.exists():
            print(f"Error: Schema file not found: {schema_md}")
            return False

        return self.run_command(
            ['python', str(script), '--schema', str(schema_md), '--csv', str(csv_file), '-v'],
            "Schema Parsing (parse_schema_to_yaml.py)",
            dry_run
        )

    def step_split_data(self, config_path: Optional[Path] = None, dry_run: bool = False) -> bool:
        """Split data using generic splitter or legacy script."""
        # Check for schema_config.yaml first
        if config_path is None:
            config_path = self.db_dir / 'schema_config.yaml'

        if config_path.exists():
            # Use generic splitter
            script = self.scripts_dir / 'generic_split.py'
            return self.run_command(
                ['python', str(script), '--config', str(config_path)],
                "Data Splitting (generic_split.py)",
                dry_run
            )
        else:
            # Try legacy split script
            legacy_script = self.scripts_dir / 'DataSplitScripts' / f'split_{self.database_name.lower()}_data.py'
            if legacy_script.exists():
                return self.run_command(
                    ['python', str(legacy_script), str(self.db_dir)],
                    f"Data Splitting (legacy: {legacy_script.name})",
                    dry_run
                )
            else:
                print(f"Error: No split configuration found.")
                print(f"  Expected: {config_path}")
                print(f"  Or legacy: {legacy_script}")
                return False

    def step_create_tables(self, use_database: str, dry_run: bool = False) -> bool:
        """Create database tables."""
        script = self.scripts_dir / 'create_tables.py'
        ddl_file = self.db_dir / 'DDL.csv'

        if not ddl_file.exists():
            print(f"Error: DDL file not found: {ddl_file}")
            return False

        return self.run_command(
            [
                'python', str(script),
                '--ddl', str(ddl_file),
                '--database', use_database,
                '--schema', self.database_name
            ],
            "Table Creation (create_tables.py)",
            dry_run
        )

    def step_load_data(self, use_database: str, dry_run: bool = False) -> bool:
        """Load data into database."""
        script = self.scripts_dir / 'load_data.py'

        return self.run_command(
            [
                'python', str(script),
                '--data-dir', str(self.data_dir),
                '--database', use_database,
                '--schema', self.database_name
            ],
            "Data Loading (load_data.py)",
            dry_run
        )


def main():
    parser = argparse.ArgumentParser(
        description='Database Pipeline Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Full pipeline from raw CSV
    python pipeline.py --database NEW_DB --input data/raw.csv --full --use-database indicdb

    # Skip schema design (use existing schema_config.yaml)
    python pipeline.py --database EXISTING_DB --skip-design --use-database indicdb

    # Schema design and parsing only (no DB operations)
    python pipeline.py --database NEW_DB --input data/raw.csv --schema-only

    # Data splitting and DB operations only (schema already exists)
    python pipeline.py --database EXISTING_DB --split-only --use-database indicdb

    # Dry run to see what would happen
    python pipeline.py --database NEW_DB --input data/raw.csv --full --dry-run
        """
    )

    # Required arguments
    parser.add_argument('--database', type=str, required=True,
                        help='Database name (e.g., INDIA_POPULATION_CENSUS)')

    # Input options
    parser.add_argument('--input', type=str,
                        help='Path to input CSV file')
    parser.add_argument('--config', type=str,
                        help='Path to existing schema_config.yaml')

    # Pipeline stages
    parser.add_argument('--full', action='store_true',
                        help='Run full pipeline (design + parse + split + create + load)')
    parser.add_argument('--skip-design', action='store_true',
                        help='Skip LLM schema design (use existing schema_info.md or schema_config.yaml)')
    parser.add_argument('--schema-only', action='store_true',
                        help='Only run schema design and parsing (no DB operations)')
    parser.add_argument('--split-only', action='store_true',
                        help='Only run data splitting (assumes schema exists)')
    parser.add_argument('--db-only', action='store_true',
                        help='Only run DB operations (create tables + load data)')

    # Database connection
    parser.add_argument('--use-database', type=str,
                        help='PostgreSQL database to use (e.g., indicdb)')

    # Other options
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without executing')
    parser.add_argument('--base-dir', type=str,
                        help='Base directory (default: parent of scripts/)')

    args = parser.parse_args()

    # Determine base directory
    if args.base_dir:
        base_dir = Path(args.base_dir)
    else:
        base_dir = Path(__file__).parent.parent

    # Validate arguments
    if args.full and not args.input:
        print("Error: --full requires --input")
        return 1

    if (args.full or args.db_only) and not args.use_database:
        print("Error: --full or --db-only requires --use-database")
        return 1

    # Create pipeline runner
    runner = PipelineRunner(args.database, base_dir)

    print(f"\n{'#' * 60}")
    print(f"# DATABASE PIPELINE: {args.database}")
    print(f"{'#' * 60}")
    print(f"Base directory: {base_dir}")
    print(f"Database directory: {runner.db_dir}")

    success = True
    steps_run = []

    # Determine which steps to run
    if args.full:
        run_design = True
        run_parse = True
        run_split = True
        run_create = True
        run_load = True
    elif args.skip_design:
        run_design = False
        run_parse = True  # Still try to parse if schema_info.md exists
        run_split = True
        run_create = bool(args.use_database)
        run_load = bool(args.use_database)
    elif args.schema_only:
        run_design = bool(args.input)
        run_parse = True
        run_split = False
        run_create = False
        run_load = False
    elif args.split_only:
        run_design = False
        run_parse = False
        run_split = True
        run_create = False
        run_load = False
    elif args.db_only:
        run_design = False
        run_parse = False
        run_split = False
        run_create = True
        run_load = True
    else:
        # Default: just split data
        run_design = False
        run_parse = False
        run_split = True
        run_create = False
        run_load = False

    # Step 1: Prepare directories
    if args.input or run_design:
        success = runner.step_prepare_directories(args.dry_run)
        if not success:
            return 1
        steps_run.append("Prepare directories")

    # Step 2: Copy input file
    if args.input and success:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: Input file not found: {input_path}")
            return 1
        success = runner.step_copy_input(input_path, args.dry_run)
        steps_run.append("Copy input file")

    # Step 3: Schema design
    if run_design and success:
        csv_path = runner.data_dir / 'total_data.csv'
        success = runner.step_schema_design(csv_path, args.dry_run)
        steps_run.append("Schema design")

    # Step 4: Parse schema
    if run_parse and success:
        schema_config = runner.db_dir / 'schema_config.yaml'
        schema_md = runner.db_dir / 'schema_info.md'

        # Only parse if schema_info.md exists and config doesn't
        if schema_md.exists() and not schema_config.exists():
            success = runner.step_parse_schema(args.dry_run)
            steps_run.append("Parse schema")
        elif schema_config.exists():
            print(f"\nSkipping parse: schema_config.yaml already exists")
        elif not schema_md.exists() and run_design:
            print(f"\nWarning: schema_info.md not found - parsing skipped")

    # Step 5: Split data
    if run_split and success:
        config_path = Path(args.config) if args.config else None
        success = runner.step_split_data(config_path, args.dry_run)
        steps_run.append("Split data")

    # Step 6: Create tables
    if run_create and success:
        success = runner.step_create_tables(args.use_database, args.dry_run)
        steps_run.append("Create tables")

    # Step 7: Load data
    if run_load and success:
        success = runner.step_load_data(args.use_database, args.dry_run)
        steps_run.append("Load data")

    # Summary
    print(f"\n{'#' * 60}")
    print("# PIPELINE SUMMARY")
    print(f"{'#' * 60}")
    print(f"Database: {args.database}")
    print(f"Steps executed: {', '.join(steps_run) if steps_run else 'None'}")
    print(f"Status: {'SUCCESS' if success else 'FAILED'}")

    if success and not args.dry_run:
        print(f"\nOutput location: {runner.db_dir}")
        if args.use_database:
            print(f"Database: {args.use_database}")
            print(f"Schema: {args.database}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
