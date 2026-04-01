#!/usr/bin/env python3
"""
Recompute EM on existing one-shot evaluation JSONLs and summarize EM/EX metrics.

This script:
1. Scans output/**/eval_files_oneshot*/*.jsonl
2. Recomputes EM using the shared normalized SQL comparator
3. Keeps one file per (db, model, language), preferring the latest timestamp
4. Writes per-file and macro-averaged summaries
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Dict, Iterable, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from sql_eval_utils import calculate_em


LANGUAGE_MAP = {
    "bengali": "Bengali",
    "hindi": "Hindi",
    "hinglish": "Hinglish",
    "marathi": "Marathi",
    "tamil": "Tamil",
    "telugu": "Telugu",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recompute EM/EX summaries for one-shot eval files")
    parser.add_argument("--output-root", default="output", help="Root output directory to scan")
    parser.add_argument(
        "--summary-dir",
        default="output/metric_summaries",
        help="Directory where summary CSVs will be written",
    )
    return parser.parse_args()


def extract_timestamp(path: Path) -> str:
    match = re.search(r"_text2sql_(\d{8}_\d{6})", path.name)
    return match.group(1) if match else ""


def detect_model(eval_dir_name: str) -> str:
    if eval_dir_name == "eval_files_oneshot":
        return "default"
    prefix = "eval_files_oneshot_"
    if eval_dir_name.startswith(prefix):
        return eval_dir_name[len(prefix):]
    return eval_dir_name


def detect_language(path: Path) -> Optional[str]:
    stem = path.stem
    if not stem.endswith("_evaluated"):
        return None
    core = stem[: -len("_evaluated")]
    suffix = core.split("_")[-1].lower()
    if suffix in {"raw", "fewshot"}:
        return None
    return LANGUAGE_MAP.get(suffix, "English")


def detect_db(path: Path) -> str:
    # Nested layout: output/<DB>/eval_files_oneshot*/file.jsonl
    try:
        output_idx = path.parts.index("output")
    except ValueError:
        output_idx = None

    if output_idx is not None and len(path.parts) > output_idx + 2:
        candidate = path.parts[output_idx + 1]
        if candidate != "eval_files_oneshot":
            return candidate

    stem = path.stem
    if "_text2sql_" in stem:
        return stem.split("_text2sql_")[0]
    return stem


def file_rank(path: Path) -> Tuple[str, int, str]:
    """
    Prefer later timestamps; if tied, prefer the more nested/per-database path.
    """
    depth_bonus = len(path.parts)
    return (extract_timestamp(path), depth_bonus, str(path))


def iter_candidate_files(output_root: Path) -> Iterable[Path]:
    for path in sorted(output_root.glob("**/eval_files_oneshot*/*.jsonl")):
        name_lower = path.name.lower()
        if "fewshot" in name_lower:
            continue
        if name_lower.endswith("raw_evaluated.jsonl") or "_raw_" in name_lower:
            continue
        if detect_language(path) is None:
            continue
        yield path


def choose_latest_files(paths: Iterable[Path]) -> List[Path]:
    best: Dict[Tuple[str, str, str], Path] = {}
    for path in paths:
        db = detect_db(path)
        model = detect_model(path.parent.name)
        language = detect_language(path)
        if language is None:
            continue
        key = (db, model, language)
        current = best.get(key)
        if current is None or file_rank(path) > file_rank(current):
            best[key] = path
    return sorted(best.values())


def load_jsonl(path: Path) -> List[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def summarize_file(path: Path) -> dict:
    rows = load_jsonl(path)
    if not rows:
        raise ValueError(f"No rows found in {path}")

    em_old = 0
    em_new = 0
    ex_total = 0
    changed = 0

    for row in rows:
        gold_sql = row.get("gold_sql") or row.get("sql") or ""
        pred_sql = row.get("predicted_sql") or ""
        old_em = int(row.get("em") or 0)
        new_em = calculate_em(gold_sql, pred_sql)
        ex = int(row.get("ex") or 0)

        em_old += old_em
        em_new += new_em
        ex_total += ex
        if old_em != new_em:
            changed += 1

    total = len(rows)
    return {
        "db": detect_db(path),
        "model": detect_model(path.parent.name),
        "language": detect_language(path),
        "path": str(path),
        "timestamp": extract_timestamp(path),
        "rows": total,
        "em_old_mean": em_old / total,
        "em_new_mean": em_new / total,
        "ex_mean": ex_total / total,
        "em_changed_rows": changed,
    }


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    output_root = PROJECT_ROOT / args.output_root
    summary_dir = PROJECT_ROOT / args.summary_dir

    selected_files = choose_latest_files(iter_candidate_files(output_root))
    file_rows = [summarize_file(path) for path in selected_files]

    per_file_csv = summary_dir / "oneshot_em_ex_by_db_language_model.csv"
    write_csv(
        per_file_csv,
        file_rows,
        [
            "db",
            "model",
            "language",
            "timestamp",
            "rows",
            "em_old_mean",
            "em_new_mean",
            "ex_mean",
            "em_changed_rows",
            "path",
        ],
    )

    grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for row in file_rows:
        grouped[(row["model"], row["language"])].append(row)

    macro_rows = []
    for (model, language), rows in sorted(grouped.items()):
        macro_rows.append(
            {
                "model": model,
                "language": language,
                "db_count": len(rows),
                "row_count": sum(r["rows"] for r in rows),
                "macro_em_old_mean": mean(r["em_old_mean"] for r in rows),
                "macro_em_new_mean": mean(r["em_new_mean"] for r in rows),
                "macro_ex_mean": mean(r["ex_mean"] for r in rows),
                "micro_em_new_mean": sum(r["em_new_mean"] * r["rows"] for r in rows) / sum(r["rows"] for r in rows),
                "micro_ex_mean": sum(r["ex_mean"] * r["rows"] for r in rows) / sum(r["rows"] for r in rows),
                "total_em_changed_rows": sum(r["em_changed_rows"] for r in rows),
            }
        )

    macro_csv = summary_dir / "oneshot_em_ex_macro_avg_by_language_model.csv"
    write_csv(
        macro_csv,
        macro_rows,
        [
            "model",
            "language",
            "db_count",
            "row_count",
            "macro_em_old_mean",
            "macro_em_new_mean",
            "macro_ex_mean",
            "micro_em_new_mean",
            "micro_ex_mean",
            "total_em_changed_rows",
        ],
    )

    print(f"Selected files: {len(file_rows)}")
    print(f"Wrote: {per_file_csv}")
    print(f"Wrote: {macro_csv}")


if __name__ == "__main__":
    main()
