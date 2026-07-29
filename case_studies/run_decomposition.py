#!/usr/bin/env python3
"""
Causal-decomposition runner for the Indic Gap.

Runs oracle-ablation conditions over case_studies/datasets/tasks_<lang>.jsonl and
scores EM / EX against PostgreSQL.

Conditions
----------
  baseline  full DDL,                      no literal hint
  B         DDL pruned to `schema_used`,   no literal hint     (oracle schema linking)
  C         full DDL,                      + `gold_literals`   (oracle value grounding)
  BC        DDL pruned,                    + `gold_literals`

Prompt shape is deliberately minimal and identical across conditions:
no sample rows, no few-shot example, no evidence file. Only the schema block and
the literal hint vary. This differs from scripts/OneShot_FewShot/run_oneshot.py,
which injects sample rows — so the `baseline` condition here must be run rather
than reused from output/metric_summaries/.

Backend is any OpenAI-compatible chat endpoint (vLLM, SGLang, llama.cpp server):

    export LLM_BASE_URL="http://gpunode6:47919/v1/chat/completions"
    export LLM_MODEL="openai/gpt-oss-120b"
    export LLM_TIMEOUT_S=180

Example
-------
    python3 case_studies/run_decomposition.py \\
        --conditions baseline B C BC \\
        --languages english hindi bengali tamil telugu marathi hinglish \\
        --workers 16 --pg-db indicdb
"""

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from tqdm import tqdm

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "scripts" / "OneShot_FewShot"))

from db_utils import load_config, get_connection, get_default_config_path, execute_use_schema  # noqa: E402
from sql_eval_utils import calculate_em  # noqa: E402

CONDITIONS = ["baseline", "B", "C", "BC"]
ALL_LANGS = ["english", "hindi", "bengali", "tamil", "telugu", "marathi", "hinglish"]

_thread = threading.local()
_write_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------

class OpenAICompatBackend:
    """Chat-completions client for a locally hosted OpenAI-compatible server."""

    def __init__(self, base_url: str, model: str, timeout_s: int = 180,
                 api_key: Optional[str] = None, max_retries: int = 3):
        base = base_url.rstrip("/")
        # accept either ".../v1" or the full ".../v1/chat/completions"
        self.url = base if base.endswith("/chat/completions") else base + "/chat/completions"
        self.model = model
        self.timeout_s = timeout_s
        self.api_key = api_key or os.environ.get("LLM_API_KEY", "EMPTY")
        self.max_retries = max_retries

    def generate(self, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": 2000,
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        last_err = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.post(self.url, headers=headers,
                                     data=json.dumps(payload),
                                     timeout=(15, self.timeout_s))
                if resp.status_code != 200:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
                msg = resp.json()["choices"][0]["message"]
                # some servers put chain-of-thought in a separate field; ignore it
                return (msg.get("content") or "").strip()
            except Exception as e:  # noqa: BLE001
                last_err = e
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"backend failed after {self.max_retries} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a professional database administrator and SQL expert.
Your task is to translate a natural language question into a syntactically correct PostgreSQL query based on the provided database schema.

### LANGUAGE & TRANSLATION RULES:
1. The input question may be in English OR a target foreign language (e.g., Hindi, Bengali, Spanish, Arabic, Chinese, French, Korean, Italian, etc.).
2. Regardless of the input language, you MUST comprehend the question's intent and generate the SQL query targeting the provided English database schema.
3. If the question contains specific entity names in a foreign language (e.g., city names, states, categories), implicitly translate or transliterate them to match the exact English string literals found in the database schema or sample data.

### POSTGRESQL RULES:
1. DO NOT use double quotes (") for table and column names unless they strictly require it (e.g., they contain spaces). If you must use double quotes, you MUST use lowercase for the identifier (e.g., "dim_state"."state_name") because tables and columns in this PostgreSQL database are stored as lowercase. It is safest to leave identifiers unquoted so Postgres automatically folds them to lowercase.
2. Always use single quotes (') for string literals/values (e.g., 'Assam').
3. For case-insensitive string matching, ALWAYS use the `ILIKE` operator instead of `LIKE` or `=` (e.g., state_name ILIKE 'assam').
4. Cast data types explicitly if needed using `::` (e.g., column_name::TEXT ILIKE '%value%').
5. You MUST ONLY output the final SQL query. Do NOT wrap it in markdown formatting (like ```sql ... ```). Do NOT include any explanations, prefaces, or apologies. Just the raw SQL string ending with a semicolon (;).
"""


def load_ddl_tables(db_dir: Path) -> "OrderedDict[str, str]":
    """table_name(upper) -> rendered DDL block, in file order."""
    ddl_path = db_dir / "DDL.csv"
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL.csv not found in {db_dir}")
    tables: "OrderedDict[str, str]" = OrderedDict()
    with open(ddl_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = (row.get("table_name") or "").strip()
            if not name:
                continue
            block = [f"-- Table: {name}"]
            desc = (row.get("description") or "").strip()
            if desc:
                block.append(f"-- Description: {desc}")
            block.append((row.get("DDL") or "").strip())
            tables[name.upper()] = "\n".join(block)
    return tables


def render_ddl(tables: "OrderedDict[str, str]", keep: Optional[List[str]] = None) -> Tuple[str, int]:
    """Render the schema block; `keep` prunes to those tables (condition B/BC)."""
    if keep:
        wanted = {t.upper() for t in keep}
        sel = [(n, b) for n, b in tables.items() if n in wanted]
        if not sel:  # oracle listed nothing we recognise -> fall back to full schema
            sel = list(tables.items())
    else:
        sel = list(tables.items())
    return "\n\n".join(b for _, b in sel), len(sel)


def build_user_prompt(question: str, ddl: str, literals: Optional[List[str]]) -> str:
    p = f"### DATABASE SCHEMA ###\n{ddl}\n\n"
    p += "### TASK ###\n"
    p += f"Question: {question}\n"
    if literals:
        rendered = ", ".join(f"'{v}'" for v in literals)
        p += f"Relevant database literals: {rendered}\n"
    p += ("\nOutput only the valid PostgreSQL query (ending with a semicolon) "
          "that answers the Question. Do not include markdown formatting.\nSQL: ")
    return p


def extract_sql(raw: str) -> str:
    s = raw.strip()
    # strip reasoning blocks some open models emit before the answer
    s = re.sub(r"<think>.*?</think>", "", s, flags=re.DOTALL | re.IGNORECASE).strip()
    s = re.sub(r"^```sql\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^```\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    s = re.sub(r"^SQL:\s*", "", s, flags=re.IGNORECASE)
    return s.strip()


# ---------------------------------------------------------------------------
# Execution accuracy
# ---------------------------------------------------------------------------

def get_conn(pg_config, pg_db):
    if not hasattr(_thread, "conn") or _thread.conn.closed:
        _thread.conn = get_connection(pg_config, database=pg_db)
        _thread.schema = None
    return _thread.conn


def calculate_ex(pg_config, pg_db, schema, gold_sql, pred_sql, timeout_ms=5000) -> Tuple[int, Optional[str]]:
    """Mirror of run_oneshot.calculate_ex, but re-applies search_path per call.

    The runner sweeps 11 databases inside one process, so a thread-cached
    connection pinned to the first schema (as in run_oneshot) would silently
    execute later DBs against the wrong schema.
    """
    if not pred_sql:
        return 0, "Empty prediction"
    conn = get_conn(pg_config, pg_db)
    cursor = conn.cursor()
    if getattr(_thread, "schema", None) != schema.lower():
        execute_use_schema(cursor, schema.lower())
        conn.commit()
        _thread.schema = schema.lower()

    def fetch(sql):
        try:
            cursor.execute(f"SET local statement_timeout TO {timeout_ms};")
            cursor.execute(sql)
            rows = cursor.fetchall()
            return [tuple(str(v).strip() if v is not None else None for v in r) for r in rows], None
        except Exception as e:  # noqa: BLE001
            return [], str(e).strip()
        finally:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass

    gold_res, gold_err = fetch(gold_sql)
    if gold_err:
        return 0, f"Gold SQL Error: {gold_err}"
    pred_res, pred_err = fetch(pred_sql)
    if pred_err:
        return 0, f"Pred SQL Error: {pred_err}"

    order_matters = "order by" in gold_sql.lower()
    ok = (gold_res == pred_res) if order_matters else (Counter(gold_res) == Counter(pred_res))
    if ok:
        return 1, None
    return 0, (f"Mismatch (Order matters: {order_matters}). "
               f"Gold count: {len(gold_res)}. Pred count: {len(pred_res)}.")


# ---------------------------------------------------------------------------
# Task execution
# ---------------------------------------------------------------------------

def process(task, condition, ddl_cache, backend, pg_config, pg_db):
    db_id = task["db_id"]
    tables = ddl_cache[db_id]

    prune = condition in ("B", "BC")
    hint = condition in ("C", "BC")
    ddl, n_tables = render_ddl(tables, task.get("schema_used") if prune else None)
    literals = task.get("gold_literals") if hint else None

    user_prompt = build_user_prompt(task["question"], ddl, literals)

    try:
        raw = backend.generate(SYSTEM_PROMPT, user_prompt)
        pred = extract_sql(raw)
        llm_err = None
    except Exception as e:  # noqa: BLE001
        raw, pred, llm_err = "", "", str(e)

    gold = task["gold_sql"]
    em = calculate_em(gold, pred) if pred else 0
    ex, ex_err = calculate_ex(pg_config, pg_db, db_id, gold, pred)

    return {
        "pair_id": task["pair_id"],
        "db_id": db_id,
        "language": task["language"],
        "condition": condition,
        "difficulty": task.get("difficulty"),
        "question": task["question"],
        "gold_sql": gold,
        "predicted_sql": pred,
        "raw_output": raw,
        "em": em,
        "ex": ex,
        "n_tables_in_prompt": n_tables,
        "n_tables_full": len(tables),
        "literals_given": literals or [],
        "error": llm_err or ex_err,
    }


def load_tasks(dataset_dir: Path, lang: str) -> List[dict]:
    path = dataset_dir / f"tasks_{lang}.jsonl"
    if not path.exists():
        raise FileNotFoundError(path)
    return [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-dir", type=Path, default=REPO / "case_studies" / "datasets")
    ap.add_argument("--out", type=Path, default=REPO / "case_studies" / "results")
    ap.add_argument("--databases-dir", type=Path, default=REPO / "databases")
    ap.add_argument("--conditions", nargs="+", default=CONDITIONS, choices=CONDITIONS)
    ap.add_argument("--languages", nargs="+", default=ALL_LANGS, choices=ALL_LANGS)
    ap.add_argument("--model-slug", help="output subdirectory name (default: derived from LLM_MODEL)")
    ap.add_argument("--base-url", default=os.environ.get("LLM_BASE_URL"))
    ap.add_argument("--model", default=os.environ.get("LLM_MODEL"))
    ap.add_argument("--timeout", type=int, default=int(os.environ.get("LLM_TIMEOUT_S", "180")))
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, help="cap tasks per (condition, language) — for smoke tests")
    ap.add_argument("--pg-config")
    ap.add_argument("--pg-db", default="indicdb")
    ap.add_argument("--dump-prompts", type=int, default=0,
                    help="write the first N assembled prompts per slice to prompts_preview/ and exit")
    args = ap.parse_args()

    if not args.base_url or not args.model:
        sys.exit("Set LLM_BASE_URL and LLM_MODEL (or pass --base-url / --model).")

    model_slug = args.model_slug or re.sub(r"[^A-Za-z0-9]+", "_", args.model).strip("_").lower()

    # preload DDL for every DB referenced by the dataset
    probe = load_tasks(args.dataset_dir, "english")
    db_ids = sorted({t["db_id"] for t in probe})
    ddl_cache = {db: load_ddl_tables(args.databases_dir / db / db) for db in db_ids}
    print(f"loaded DDL for {len(ddl_cache)} databases")

    # optional: dump prompts for eyeballing, then stop
    if args.dump_prompts:
        pdir = args.out / model_slug / "prompts_preview"
        pdir.mkdir(parents=True, exist_ok=True)
        for lang in args.languages:
            tasks = load_tasks(args.dataset_dir, lang)
            for cond in args.conditions:
                with open(pdir / f"{cond}__{lang}.txt", "w", encoding="utf-8") as fh:
                    for t in tasks[: args.dump_prompts]:
                        tb = ddl_cache[t["db_id"]]
                        ddl, n = render_ddl(tb, t.get("schema_used") if cond in ("B", "BC") else None)
                        lits = t.get("gold_literals") if cond in ("C", "BC") else None
                        fh.write(f"{'='*70}\npair_id={t['pair_id']} db={t['db_id']} "
                                 f"cond={cond} tables={n}/{len(tb)} literals={lits}\n{'='*70}\n")
                        fh.write(build_user_prompt(t["question"], ddl, lits) + "\n\n")
        print(f"wrote prompt previews -> {pdir}")
        return

    # deferred until after --dump-prompts so previews need no DB / endpoint
    backend = OpenAICompatBackend(args.base_url, args.model, args.timeout)
    pg_config = load_config(args.pg_config or get_default_config_path())

    summary = []
    for cond in args.conditions:
        for lang in args.languages:
            tasks = load_tasks(args.dataset_dir, lang)
            if args.limit:
                tasks = tasks[: args.limit]

            out_dir = args.out / model_slug / cond
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{lang}.jsonl"

            done = set()
            if out_file.exists():
                for line in open(out_file, encoding="utf-8"):
                    if line.strip():
                        try:
                            done.add(json.loads(line)["pair_id"])
                        except Exception:  # noqa: BLE001
                            pass
            todo = [t for t in tasks if t["pair_id"] not in done]
            if not todo:
                print(f"[{cond}/{lang}] complete ({len(done)} rows) — skipping")

            if todo:
                with ThreadPoolExecutor(max_workers=args.workers) as ex_pool:
                    futs = [ex_pool.submit(process, t, cond, ddl_cache, backend,
                                           pg_config, args.pg_db) for t in todo]
                    with open(out_file, "a", encoding="utf-8") as fh:
                        for fut in tqdm(as_completed(futs), total=len(futs),
                                        desc=f"{cond}/{lang}"):
                            res = fut.result()
                            with _write_lock:
                                fh.write(json.dumps(res, ensure_ascii=False) + "\n")
                                fh.flush()

            rows = [json.loads(l) for l in open(out_file, encoding="utf-8") if l.strip()]
            if rows:
                em = sum(r["em"] for r in rows) / len(rows)
                exa = sum(r["ex"] for r in rows) / len(rows)
                errs = sum(1 for r in rows if r.get("error"))
                summary.append((cond, lang, len(rows), em, exa, errs))
                print(f"[{cond}/{lang}] n={len(rows)}  EM={em*100:.2f}%  EX={exa*100:.2f}%  errors={errs}")

    print(f"\n{'cond':<10}{'lang':<10}{'n':>6}{'EM':>9}{'EX':>9}{'err':>7}")
    for cond, lang, n, em, exa, errs in summary:
        print(f"{cond:<10}{lang:<10}{n:>6}{em*100:>8.2f}%{exa*100:>8.2f}%{errs:>7}")
    print(f"\nresults -> {args.out / model_slug}")


if __name__ == "__main__":
    main()
