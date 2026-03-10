#!/usr/bin/env python3
"""
Run DSQG-Syn synthesis for multiple schemas in parallel batches.

Default behavior:
- Discovers schema files under databases/*/*/schema_config.yaml
- Runs only the currently missing-output datasets
- Runs up to 6 synthesis jobs concurrently
- Uses model google/gemini-3-flash-preview
"""

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List


DEFAULT_EXCLUDES = {
}

DEFAULT_INCLUDES = {
    "india_ihds_2005_individual_survey",
    "india_ihds_2011_individual_survey",
    "india_population_census",
    "india_school_infrastructure",
    "india_village_amenities_directory_2001",
}


def discover_schema_paths(root: Path) -> List[Path]:
    """Discover schema_config.yaml files under databases directory."""
    return sorted(root.glob("databases/*/*/schema_config.yaml"))


def dataset_name_from_schema(schema_path: Path) -> str:
    """
    Extract dataset directory name.
    For databases/X/X/schema_config.yaml -> X
    """
    return schema_path.parents[1].name


def should_exclude(schema_path: Path, exclude_names: set) -> bool:
    dataset_name = dataset_name_from_schema(schema_path).lower()
    return dataset_name in exclude_names


def should_include(schema_path: Path, include_names: set) -> bool:
    dataset_name = dataset_name_from_schema(schema_path).lower()
    return dataset_name in include_names


def run_one(schema_path: Path, model: str, extra_args: List[str]) -> int:
    cmd = [
        sys.executable,
        "-m",
        "scripts.dsqg_syn.run_synthesis",
        "--schema",
        str(schema_path),
        "--model",
        model,
    ] + extra_args

    dataset_name = dataset_name_from_schema(schema_path)
    print(f"[START] {dataset_name}")
    result = subprocess.run(cmd)
    if result.returncode == 0:
        print(f"[DONE ] {dataset_name}")
    else:
        print(f"[FAIL ] {dataset_name} (exit={result.returncode})")
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run DSQG-Syn over multiple schemas in parallel batches."
    )
    parser.add_argument(
        "--model",
        type=str,
        default="google/gemini-3-flash-preview",
        help="Model passed to scripts.dsqg_syn.run_synthesis",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=6,
        help="Maximum number of parallel jobs (default: 6)",
    )
    parser.add_argument(
        "--include",
        type=str,
        nargs="*",
        default=list(DEFAULT_INCLUDES),
        help="Dataset directory names to include (case-insensitive)",
    )
    parser.add_argument(
        "--exclude",
        type=str,
        nargs="*",
        default=list(DEFAULT_EXCLUDES),
        help="Dataset directory names to exclude (case-insensitive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print selected schema paths and exit",
    )
    args, extra_args = parser.parse_known_args()

    root = Path(__file__).resolve().parents[2]
    all_schemas = discover_schema_paths(root)
    include_names = {x.lower() for x in args.include}
    exclude_names = {x.lower() for x in args.exclude}
    selected = [
        s for s in all_schemas
        if should_include(s, include_names) and not should_exclude(s, exclude_names)
    ]

    print(f"Discovered schemas: {len(all_schemas)}")
    print(f"Included datasets: {sorted(include_names)}")
    print(f"Excluded datasets: {sorted(exclude_names)}")
    print(f"Selected schemas : {len(selected)}")

    if args.dry_run:
        for schema in selected:
            print(schema)
        return 0

    failures = 0
    with ThreadPoolExecutor(max_workers=args.max_parallel) as pool:
        futures = [
            pool.submit(run_one, schema, args.model, extra_args)
            for schema in selected
        ]
        for future in as_completed(futures):
            rc = future.result()
            if rc != 0:
                failures += 1

    if failures:
        print(f"Completed with failures: {failures}/{len(selected)}")
        return 1

    print(f"Completed successfully: {len(selected)}/{len(selected)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
