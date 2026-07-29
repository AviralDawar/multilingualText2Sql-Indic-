#!/usr/bin/env python3
"""
Build the causal-decomposition case-study dataset.

Selects a stratified sample of N tasks (default 500) from output/*/sampled_tasks,
restricted to tasks that exist in every core language (English + 5 Indic), and
writes one aligned JSONL per language into case_studies/datasets/.

Stratification: DB x difficulty, proportional allocation with largest-remainder
rounding. Within a stratum, tasks that also have a Hinglish translation are
preferred so the optional Hinglish arm stays as complete as possible.

Each emitted record carries the artifacts the decomposition conditions need:
  schema_used   -> condition B (oracle schema linking)
  gold_literals -> condition C (oracle value grounding)
"""

import argparse
import json
import os
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "output"

INDIC = ["hindi", "bengali", "tamil", "telugu", "marathi"]
CORE = ["english"] + INDIC          # must be present for a task to be eligible
OPTIONAL = ["hinglish"]             # nice to have, not required
ALL_LANGS = CORE + OPTIONAL

TASK_RE = re.compile(r"_text2sql_(\d{8}_\d{6})(?:_([a-z]+))?\.jsonl$")


def discover_slices():
    """Map db_id -> {lang: path} for the timestamp stem with the best language coverage."""
    slices = {}
    for db in sorted(os.listdir(OUTPUT_DIR)):
        d = OUTPUT_DIR / db / "sampled_tasks"
        if not d.is_dir():
            continue
        stems = defaultdict(dict)
        for f in d.glob("*.jsonl"):
            if f.name.endswith("_evaluated.jsonl"):
                continue
            m = TASK_RE.search(f.name)
            if not m:
                continue
            stems[m.group(1)][m.group(2) or "english"] = f
        if not stems:
            continue
        stem, per = max(
            stems.items(), key=lambda kv: sum(1 for l in ALL_LANGS if l in kv[1])
        )
        if not all(l in per for l in CORE):
            continue
        slices[db] = (stem, {l: p for l, p in per.items() if l in ALL_LANGS})
    return slices


def read_jsonl(path):
    """pair_id -> record."""
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "pair_id" in r:
                out[r["pair_id"]] = r
    return out


def extract_literals(sql):
    """Single-quoted string literals from gold SQL, deduped and sorted.

    Sorted rather than in SQL order so the condition-C hint does not leak
    clause ordering. Unquoted numerics are excluded on purpose: bare numbers
    survive translation intact, so they are not part of the value-grounding
    problem being isolated.
    """
    if not sql:
        return []
    return sorted(set(re.findall(r"'([^']*)'", sql)))


def allocate(strata_sizes, target):
    """Proportional allocation with largest-remainder rounding."""
    total = sum(strata_sizes.values())
    target = min(target, total)
    raw = {k: v * target / total for k, v in strata_sizes.items()}
    alloc = {k: int(v) for k, v in raw.items()}
    short = target - sum(alloc.values())
    for k, _ in sorted(raw.items(), key=lambda kv: kv[1] - int(kv[1]), reverse=True):
        if short <= 0:
            break
        if alloc[k] < strata_sizes[k]:
            alloc[k] += 1
            short -= 1
    # redistribute if any stratum was capped
    while short > 0:
        grew = False
        for k in sorted(strata_sizes, key=lambda k: -strata_sizes[k]):
            if short <= 0:
                break
            if alloc[k] < strata_sizes[k]:
                alloc[k] += 1
                short -= 1
                grew = True
        if not grew:
            break
    return alloc


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--num-tasks", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=REPO / "case_studies" / "datasets")
    ap.add_argument(
        "--require-literals",
        action="store_true",
        help="restrict the eligible pool to tasks whose gold SQL has >=1 string "
             "literal, so all N tasks are usable for condition C (oracle value "
             "grounding) instead of only the ~70%% that have one by chance",
    )
    args = ap.parse_args()

    rng = random.Random(args.seed)
    slices = discover_slices()
    if not slices:
        sys.exit("No usable slices found.")

    # ---- build eligible pool -------------------------------------------------
    pool = []          # (db, pair_id, difficulty, has_hinglish)
    records = {}       # (db, pair_id) -> {lang: record}
    sql_mismatch = 0

    for db, (stem, per) in slices.items():
        loaded = {l: read_jsonl(p) for l, p in per.items()}
        ids = set.intersection(*[set(loaded[l]) for l in CORE])
        hing = set(loaded.get("hinglish", {}))
        for pid in sorted(ids):
            en = loaded["english"][pid]
            # gold SQL must agree across languages; only the question is translated
            golds = {loaded[l][pid].get("sql", "") for l in CORE if pid in loaded[l]}
            if len(golds) > 1:
                sql_mismatch += 1
            if args.require_literals and not extract_literals(en.get("sql", "")):
                continue
            records[(db, pid)] = {l: loaded[l][pid] for l in loaded if pid in loaded[l]}
            pool.append((db, pid, en.get("difficulty", "unknown"), pid in hing))

    strata = defaultdict(list)
    for db, pid, diff, has_h in pool:
        strata[(db, diff)].append((pid, has_h))

    sizes = {k: len(v) for k, v in strata.items()}
    alloc = allocate(sizes, args.num_tasks)

    # ---- select --------------------------------------------------------------
    selected = []
    for key in sorted(strata):
        items = sorted(strata[key])
        rng.shuffle(items)
        # prefer Hinglish-available within the stratum
        items.sort(key=lambda t: not t[1])
        for pid, has_h in items[: alloc[key]]:
            selected.append((key[0], pid, key[1], has_h))

    selected.sort(key=lambda t: (t[0], t[1]))

    # ---- write ---------------------------------------------------------------
    outdir = args.out
    outdir.mkdir(parents=True, exist_ok=True)

    written = {}
    for lang in ALL_LANGS:
        path = outdir / f"tasks_{lang}.jsonl"
        n = 0
        with open(path, "w", encoding="utf-8") as fh:
            for db, pid, diff, has_h in selected:
                rec = records[(db, pid)].get(lang)
                if rec is None:
                    continue
                gold_sql = rec.get("sql", "")
                fh.write(json.dumps({
                    "pair_id": pid,
                    "db_id": db,
                    "language": lang,
                    "difficulty": diff,
                    "question": rec.get("question", ""),
                    "gold_sql": gold_sql,
                    "schema_used": rec.get("schema_used", []),
                    "gold_literals": extract_literals(gold_sql),
                    "has_hinglish": has_h,
                    "source_stem": slices[db][0],
                }, ensure_ascii=False) + "\n")
                n += 1
        written[lang] = n

    # ---- manifest ------------------------------------------------------------
    by_db = defaultdict(int)
    by_diff = defaultdict(int)
    by_db_diff = defaultdict(int)
    for db, pid, diff, _ in selected:
        by_db[db] += 1
        by_diff[diff] += 1
        by_db_diff[f"{db}|{diff}"] += 1

    lit_counts = [
        len(extract_literals(records[(db, pid)]["english"].get("sql", "")))
        for db, pid, _, _ in selected
    ]

    manifest = {
        "n_selected": len(selected),
        "requested": args.num_tasks,
        "seed": args.seed,
        "require_literals": args.require_literals,
        "eligible_pool": len(pool),
        "databases": len(by_db),
        "languages_written": written,
        "hinglish_coverage": sum(1 for s in selected if s[3]),
        "by_database": dict(sorted(by_db.items())),
        "by_difficulty": dict(sorted(by_diff.items())),
        "by_database_difficulty": dict(sorted(by_db_diff.items())),
        "source_stems": {db: stem for db, (stem, _) in sorted(slices.items())},
        "gold_literals": {
            "tasks_with_zero_literals": sum(1 for c in lit_counts if c == 0),
            "mean_per_task": round(sum(lit_counts) / len(lit_counts), 3) if lit_counts else 0,
            "max_per_task": max(lit_counts) if lit_counts else 0,
        },
        "gold_sql_mismatches_across_languages": sql_mismatch,
    }
    with open(outdir / "manifest.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)

    # ---- report --------------------------------------------------------------
    print(f"selected {len(selected)} / {len(pool)} eligible tasks across {len(by_db)} DBs")
    print(f"difficulty: {dict(sorted(by_diff.items()))}")
    print(f"hinglish coverage: {manifest['hinglish_coverage']}/{len(selected)}")
    print(f"gold_sql mismatches across languages: {sql_mismatch}")
    print(f"tasks with zero extractable literals: {manifest['gold_literals']['tasks_with_zero_literals']}"
          f"  (mean {manifest['gold_literals']['mean_per_task']}/task)")
    print("\nper-language rows written:")
    for l in ALL_LANGS:
        print(f"  {l:<10} {written[l]}")
    print(f"\nwrote -> {outdir}")


if __name__ == "__main__":
    main()
