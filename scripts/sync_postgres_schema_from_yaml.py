"""
Synchronize PostgreSQL foreign-key schema with schema_config.yaml.

This script is intended for databases that were already created from DDL.csv
without complete FK information. It can:
1) generate idempotent ALTER TABLE SQL
2) optionally apply those statements to PostgreSQL

Usage:
    python3 scripts/sync_postgres_schema_from_yaml.py \
      --schema-config databases/MY_DB/MY_DB/schema_config.yaml

    python3 scripts/sync_postgres_schema_from_yaml.py \
      --schema-config databases/MY_DB/MY_DB/schema_config.yaml \
      --apply --database indicdb --schema my_db
"""

import argparse
import hashlib
import re
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from db_utils import load_config, get_connection, get_default_config_path


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def ensure_identifier(name: str, label: str) -> str:
    """Validate SQL identifier shape used by this project."""
    if not IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid {label}: {name}")
    return name


def infer_key_type(column_name: str) -> str:
    """Infer key type from naming conventions used in this repo."""
    col = column_name.upper()
    if col == "YEAR" or "YEAR" in col:
        return "INTEGER"
    return "BIGINT"


def make_constraint_name(table: str, fk_column: str, ref_table: str) -> str:
    """Generate a deterministic FK constraint name within Postgres length limits."""
    base = f"fk_{table.lower()}_{fk_column.lower()}_{ref_table.lower()}"
    if len(base) <= 63:
        return base
    digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:8]
    prefix = base[:54]
    return f"{prefix}_{digest}"


def make_unique_constraint_name(table: str, column: str) -> str:
    """Generate a deterministic UNIQUE constraint name within Postgres limits."""
    base = f"uq_{table.lower()}_{column.lower()}"
    if len(base) <= 63:
        return base
    digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:8]
    prefix = base[:54]
    return f"{prefix}_{digest}"


def load_fk_contract(schema_config_path: Path) -> Tuple[Dict[str, str], List[dict]]:
    """
    Read table PK/FK contract from schema_config.yaml.

    Returns:
        key_columns: table -> key column
        fk_specs: list of {table, fk_column, ref_table, ref_column}
    """
    with open(schema_config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    table_defs = {}
    table_defs.update(cfg.get("dimension_tables", {}))
    table_defs.update(cfg.get("fact_tables", {}))

    key_columns: Dict[str, str] = {}
    for table_name, table_cfg in table_defs.items():
        t = table_name.upper()
        key_columns[t] = (table_cfg.get("key_column") or f"{table_name}_ID").upper()

    fk_specs = []
    for table_name, table_cfg in table_defs.items():
        t = table_name.upper()
        for fk in table_cfg.get("foreign_keys", []):
            fk_col = fk.get("column")
            ref_table = fk.get("references")
            if not fk_col or not ref_table:
                continue
            ref_t = ref_table.upper()
            ref_col = (fk.get("references_column") or key_columns.get(ref_t) or fk_col).upper()
            fk_specs.append({
                "table": t,
                "fk_column": fk_col.upper(),
                "ref_table": ref_t,
                "ref_column": ref_col
            })

    return key_columns, fk_specs


def generate_sync_sql(schema_name: str, fk_specs: List[dict]) -> List[str]:
    """Build idempotent SQL statements for missing FK columns/constraints."""
    ensure_identifier(schema_name, "schema name")
    statements: List[str] = []
    ensured_ref_keys = set()

    for spec in fk_specs:
        table = ensure_identifier(spec["table"], "table name")
        fk_col = ensure_identifier(spec["fk_column"], "foreign key column")
        ref_table = ensure_identifier(spec["ref_table"], "referenced table")
        ref_col = ensure_identifier(spec["ref_column"], "referenced column")

        ref_key = (ref_table, ref_col)
        if ref_key not in ensured_ref_keys:
            uq_name = make_unique_constraint_name(ref_table, ref_col)
            uq_name = ensure_identifier(uq_name, "unique constraint name")
            statements.append(
                "DO $$\n"
                "BEGIN\n"
                "    IF NOT EXISTS (\n"
                "        SELECT 1\n"
                "        FROM information_schema.table_constraints tc\n"
                "        JOIN information_schema.key_column_usage kcu\n"
                "          ON tc.constraint_name = kcu.constraint_name\n"
                "         AND tc.table_schema = kcu.table_schema\n"
                "         AND tc.table_name = kcu.table_name\n"
                f"        WHERE tc.table_schema = '{schema_name.lower()}'\n"
                f"          AND tc.table_name = '{ref_table.lower()}'\n"
                "          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')\n"
                f"          AND kcu.column_name = '{ref_col.lower()}'\n"
                "          AND (\n"
                "              SELECT COUNT(*)\n"
                "              FROM information_schema.key_column_usage k2\n"
                "              WHERE k2.constraint_name = tc.constraint_name\n"
                "                AND k2.table_schema = tc.table_schema\n"
                "                AND k2.table_name = tc.table_name\n"
                "          ) = 1\n"
                "    ) THEN\n"
                f"        ALTER TABLE {schema_name}.{ref_table}\n"
                f"        ADD CONSTRAINT {uq_name}\n"
                f"        UNIQUE ({ref_col});\n"
                "    END IF;\n"
                "END\n"
                "$$;"
            )
            ensured_ref_keys.add(ref_key)

        fk_type = infer_key_type(ref_col)
        constraint_name = make_constraint_name(table, fk_col, ref_table)
        constraint_name = ensure_identifier(constraint_name, "constraint name")

        statements.append(
            f"ALTER TABLE {schema_name}.{table} "
            f"ADD COLUMN IF NOT EXISTS {fk_col} {fk_type};"
        )

        statements.append(
            "DO $$\n"
            "BEGIN\n"
            "    IF NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM pg_constraint c\n"
            "        JOIN pg_class t ON t.oid = c.conrelid\n"
            "        JOIN pg_namespace n ON n.oid = t.relnamespace\n"
            f"        WHERE c.conname = '{constraint_name}'\n"
            f"          AND n.nspname = '{schema_name.lower()}'\n"
            f"          AND t.relname = '{table.lower()}'\n"
            "    ) THEN\n"
            f"        ALTER TABLE {schema_name}.{table}\n"
            f"        ADD CONSTRAINT {constraint_name}\n"
            f"        FOREIGN KEY ({fk_col}) REFERENCES {schema_name}.{ref_table} ({ref_col}) NOT VALID;\n"
            "    END IF;\n"
            "END\n"
            "$$;"
        )

    return statements


def apply_sql(config: dict, database: str, statements: List[str]):
    """Execute SQL statements against PostgreSQL."""
    conn = get_connection(config, database=database)
    try:
        cursor = conn.cursor()
        for idx, stmt in enumerate(statements, start=1):
            try:
                cursor.execute(stmt)
            except Exception:
                print(f"Failed statement #{idx}:\n{stmt}\n")
                raise
        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Sync PostgreSQL FK columns/constraints from schema_config.yaml"
    )
    parser.add_argument(
        "--schema-config",
        type=str,
        required=True,
        help="Path to schema_config.yaml"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to postgres config (defaults to config/postgres_credential.json)"
    )
    parser.add_argument(
        "--database",
        type=str,
        help="Target PostgreSQL database name (defaults to config value)"
    )
    parser.add_argument(
        "--schema",
        type=str,
        help="Target PostgreSQL schema name (defaults to folder name containing schema_config.yaml)"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply SQL to PostgreSQL (default: print only)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Optional output path for generated SQL"
    )

    args = parser.parse_args()

    schema_config_path = Path(args.schema_config)
    if not schema_config_path.exists():
        raise FileNotFoundError(f"schema_config not found: {schema_config_path}")

    target_schema = args.schema or schema_config_path.parent.name
    ensure_identifier(target_schema, "schema name")

    _, fk_specs = load_fk_contract(schema_config_path)
    statements = generate_sync_sql(target_schema, fk_specs)

    sql_text = "\n\n".join(statements) + ("\n" if statements else "")

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(sql_text, encoding="utf-8")
        print(f"Wrote SQL: {output_path}")

    if not args.apply:
        print(sql_text)
        return

    config_path = args.config or get_default_config_path()
    config = load_config(config_path)
    target_database = args.database or config.get("database")
    if not target_database:
        raise ValueError("No target database specified; use --database or set 'database' in config.")

    apply_sql(config, target_database, statements)
    print(f"Applied {len(statements)} statements to {target_database}.{target_schema}")


if __name__ == "__main__":
    main()
