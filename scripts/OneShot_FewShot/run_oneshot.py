#!/usr/bin/env python3
"""
One-Shot Text-to-SQL Evaluator (Parallel & Batch)

Evaluates an LLM's Text-to-SQL capabilities using a 1-shot prompt.
Incorporates database schema (DDL), sample data, and external knowledge.
Calculates Exact Match (EM) and Execution Accuracy (EX) against a PostgreSQL database.
Supports processing multiple files from task_files and outputting to eval_files_oneshot.
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
import threading
from collections import Counter
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import requests
from tqdm import tqdm

# Add parent dir to path to import db_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_utils import load_config, get_connection, get_default_config_path, execute_use_schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Thread-local storage for DB connections
thread_data = threading.local()

# ---------------------------------------------------------------------------
# LLM Backends
# ---------------------------------------------------------------------------

class AnthropicBackend:
    def __init__(self, api_key: str, model: str):
        try:
            from anthropic import Anthropic
        except ImportError:
            raise ImportError("Please install anthropic: pip install anthropic")
        self.client = Anthropic(api_key=api_key)
        self.model = model

    def generate(self, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> str:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=2000,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return response.content[0].text.strip()


class OpenRouterBackend:
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"

    def generate(self, system_prompt: str, user_prompt: str, temperature: float = 0.0) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": 2000,
        }
        response = requests.post(
            self.base_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=(15, 120),
        )
        if response.status_code != 200:
            raise RuntimeError(f"OpenRouter API error (HTTP {response.status_code}): {response.text}")
        return response.json()["choices"][0]["message"]["content"].strip()


def create_backend(provider: str, model: str, api_key: Optional[str] = None):
    if provider == "openrouter":
        key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not key:
            raise ValueError("OPENROUTER_API_KEY not found. Set via --api-key or environment variable.")
        return OpenRouterBackend(api_key=key, model=model)
    else:
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise ValueError("ANTHROPIC_API_KEY not found. Set via --api-key or environment variable.")
        return AnthropicBackend(api_key=key, model=model)


# ---------------------------------------------------------------------------
# Data Loading & Prompt Assembly
# ---------------------------------------------------------------------------

def load_ddl(db_dir: Path) -> str:
    ddl_path = db_dir / "DDL.csv"
    if not ddl_path.exists():
        raise FileNotFoundError(f"DDL.csv not found in {db_dir}")
    import csv
    ddl_lines = []
    with open(ddl_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ddl_lines.append(f"-- Table: {row['table_name']}")
            ddl_lines.append(f"-- Description: {row['description']}")
            ddl_lines.append(row['DDL'].strip())
            ddl_lines.append("")
    return "\n".join(ddl_lines)


def load_sample_data(db_dir: Path) -> str:
    samples = []
    for json_file in db_dir.glob("*.json"):
        table_name = json_file.stem
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not data: continue
                samples.append(f"/* 3 example rows for table {table_name}:")
                for i, row in enumerate(data[:3]):
                    samples.append(f"Row {i+1}: {row}")
                samples.append("*/\n")
        except Exception as e: logger.warning(f"Failed to read sample data from {json_file}: {e}")
    return "\n".join(samples)


def load_knowledge(knowledge_path: Path) -> Dict[str, str]:
    knowledge_map = {}
    if not knowledge_path or not knowledge_path.exists(): return knowledge_map
    with open(knowledge_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for item in data:
            if 'pair_id' in item and 'evidence' in item:
                knowledge_map[item['pair_id']] = item['evidence']
    return knowledge_map


def load_one_shot_example(examples_path: Path, current_db_id: str) -> Tuple[str, str]:
    if not examples_path or not examples_path.exists(): return ("", "")
    candidates = []
    with open(examples_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip(): continue
            item = json.loads(line)
            if item.get('db_id') != current_db_id: candidates.append(item)
    if not candidates: return ("", "")
    exam = random.choice(candidates)
    example_text = f"==== EXAMPLE ====\nQuestion: {exam['question']}\n"
    if 'evidence' in exam and exam['evidence']: example_text += f"Evidence: {exam['evidence']}\n"
    example_text += f"SQL: {exam['sql']}\n=================\n"
    return example_text, exam['sql']


def build_system_prompt() -> str:
    return """You are a professional database administrator and SQL expert.
Your task is to translate a natural language question into a syntactically correct PostgreSQL query based on the provided database schema.

### LANGUAGE & TRANSLATION RULES:
1. The input question may be in English OR an Indic language (e.g., Hindi, Bengali, Tamil, Telugu, etc.).
2. Regardless of the input language, you MUST comprehend the question's intent and generate the SQL query targeting the provided English database schema.
3. If the question contains specific entity names in an Indic language (e.g., city names, states, categories), implicitly translate or transliterate them to match the exact English string literals found in the database schema or sample data.

### POSTGRESQL RULES:
1. DO NOT use double quotes (") for table and column names unless they strictly require it (e.g., they contain spaces). If you must use double quotes, you MUST use lowercase for the identifier (e.g., "dim_state"."state_name") because tables and columns in this PostgreSQL database are stored as lowercase. It is safest to leave identifiers unquoted so Postgres automatically folds them to lowercase.
2. Always use single quotes (') for string literals/values (e.g., 'Assam').
3. For case-insensitive string matching, ALWAYS use the `ILIKE` operator instead of `LIKE` or `=` (e.g., state_name ILIKE 'assam').
4. Cast data types explicitly if needed using `::` (e.g., column_name::TEXT ILIKE '%value%').
5. You MUST ONLY output the final SQL query. Do NOT wrap it in markdown formatting (like ```sql ... ```). Do NOT include any explanations, prefaces, or apologies. Just the raw SQL string ending with a semicolon (;).
"""


def build_user_prompt(question: str, evidence: str, ddl: str, samples: str, one_shot: str) -> str:
    prompt = f"### DATABASE SCHEMA ###\n{ddl}\n\n"
    if samples: prompt += f"### SAMPLE DATA ###\n{samples}\n\n"
    if one_shot: prompt += f"### 1-SHOT LEARNING EXAMPLE ###\n{one_shot}\n\n"
    prompt += "### TASK ###\n"
    prompt += f"Question: {question}\n"
    if evidence: prompt += f"Evidence / External Knowledge: {evidence}\n"
    prompt += "\nOutput only the valid PostgreSQL query (ending with a semicolon) that answers the Question. Do not include markdown formatting.\nSQL: "
    return prompt


# ---------------------------------------------------------------------------
# Evaluation Engine
# ---------------------------------------------------------------------------

def extract_sql(llm_output: str) -> str:
    sql = llm_output.strip()
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^```\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    sql = re.sub(r'^SQL:\s*', '', sql, flags=re.IGNORECASE)
    return sql.strip()


def calculate_em(gold_sql: str, pred_sql: str) -> int:
    if not pred_sql or not gold_sql: return 0
    from sqlglot import parse_one, exp
    def _extract_sql_components(sql: str):
        try:
            sql_clean = sql.replace('"', '').replace(';', '')
            ast = parse_one(sql_clean, read="postgres")
        except: return None
        for node in ast.find_all(exp.Identifier):
            node.set("this", str(node.name).lower())
            node.set("quoted", False)
        for node in ast.find_all(exp.Column):
            node.set("table", None)
            node.set("db", None)
        for node in ast.find_all(exp.Table):
            node.set("db", None)
            node.set("catalog", None)
            if isinstance(node.parent, exp.Alias): node.parent.replace(node)
        for node in ast.find_all(exp.Literal):
            if node.is_string: node.set("this", str(node.this).lower())
        components = {"select": set(), "from": set(), "where": set(), "group": set(), "order": []}
        if isinstance(ast, exp.Select):
            for e in ast.expressions:
                if isinstance(e, exp.Alias): components["select"].add(e.this.sql().lower())
                else: components["select"].add(e.sql().lower())
            for table in ast.find_all(exp.Table): components["from"].add(table.this.sql().lower())
            where = ast.args.get("where")
            if where:
                def flatten_ands(node):
                    if isinstance(node, exp.And): return flatten_ands(node.left) | flatten_ands(node.right)
                    return {node.sql().lower()}
                components["where"] = flatten_ands(where.this)
            group = ast.args.get("group")
            if group:
                for e in group.expressions: components["group"].add(e.sql().lower())
            order = ast.args.get("order")
            if order:
                for e in order.expressions: components["order"].append(e.sql().lower())
        return components

    gold_comps = _extract_sql_components(gold_sql)
    pred_comps = _extract_sql_components(pred_sql)
    if not gold_comps or not pred_comps:
        g_norm = ' '.join(gold_sql.lower().split()).replace('"', '').replace(';', '')
        p_norm = ' '.join(pred_sql.lower().split()).replace('"', '').replace(';', '')
        return 1 if g_norm == p_norm else 0
    checks = [gold_comps[c] == pred_comps[c] for c in ["select", "from", "where", "group", "order"]]
    return 1 if all(checks) else 0


def get_thread_connection(pg_config, db_name, schema):
    if not hasattr(thread_data, "conn") or thread_data.conn.closed:
        thread_data.conn = get_connection(pg_config, database=db_name)
        cursor = thread_data.conn.cursor()
        execute_use_schema(cursor, schema.lower())
        thread_data.conn.commit()
    return thread_data.conn


def calculate_ex(pg_config, pg_db, schema, gold_sql, pred_sql, timeout_ms=5000) -> Tuple[int, Optional[str]]:
    if not pred_sql: return 0, "Empty prediction"
    conn = get_thread_connection(pg_config, pg_db, schema)
    cursor = conn.cursor()
    
    def fetch_results(sql):
        try:
            cursor.execute(f"SET local statement_timeout TO {timeout_ms};")
            cursor.execute(sql)
            results = cursor.fetchall()
            return [tuple(str(v).strip() if v is not None else None for v in row) for row in results], None
        except Exception as e: return [], str(e).strip()
        finally:
            try: conn.rollback()
            except: pass

    gold_res, gold_err = fetch_results(gold_sql)
    if gold_err: return 0, f"Gold SQL Error: {gold_err}"
    pred_res, pred_err = fetch_results(pred_sql)
    if pred_err: return 0, f"Pred SQL Error: {pred_err}"
    
    order_matters = 'order by' in gold_sql.lower()
    is_match = (gold_res == pred_res) if order_matters else (Counter(gold_res) == Counter(pred_res))
    if is_match: return 1, None
    return 0, f"Mismatch (Order matters: {order_matters}). Gold count: {len(gold_res)}. Pred count: {len(pred_res)}."


# ---------------------------------------------------------------------------
# Task processing
# ---------------------------------------------------------------------------

def process_task(task_data):
    task, idx, args, context = task_data
    pair_id = task.get('pair_id', f'unknown_{idx}')
    question = task.get('question', '')
    gold_sql = task.get('sql', '')
    evidence = context['knowledge_map'].get(pair_id, '')

    one_shot_text, _ = load_one_shot_example(context['examples_path'], args.database)
    usr_prompt = build_user_prompt(question, evidence, context['ddl_text'], context['sample_text'], one_shot_text)

    try:
        raw_output = context['backend'].generate(context['sys_prompt'], usr_prompt)
        pred_sql = extract_sql(raw_output)
        llm_err = None
    except Exception as e:
        pred_sql, raw_output, llm_err = "", "", str(e)

    em = calculate_em(gold_sql, pred_sql)
    ex, ex_err = calculate_ex(context['pg_config'], args.pg_db, args.database, gold_sql, pred_sql)

    return {
        'pair_id': pair_id, 'question': question, 'gold_sql': gold_sql,
        'predicted_sql': pred_sql, 'raw_output': raw_output,
        'em': em, 'ex': ex, 'error': ex_err or llm_err
    }


def main():
    parser = argparse.ArgumentParser(description="Parallel One-Shot Evaluator")
    parser.add_argument("--database", required=True)
    parser.add_argument("--database-dir", default="../databases")
    parser.add_argument("--input", required=True, help="Input file or directory (task_files)")
    parser.add_argument("--output", help="Output file or directory (eval_files_oneshot)")
    parser.add_argument("--knowledge")
    parser.add_argument("--examples")
    parser.add_argument("--provider", default="openrouter", choices=["anthropic", "openrouter"])
    parser.add_argument("--model", default="claude-3-5-haiku-20241022")
    parser.add_argument("--api-key")
    parser.add_argument("--pg-config")
    parser.add_argument("--pg-db", default="indicdb")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    # Paths
    db_id = args.database
    target_db_dir = Path(args.database_dir) / db_id / db_id
    if not target_db_dir.exists(): raise FileNotFoundError(f"DB dir not found: {target_db_dir}")

    input_path = Path(args.input)
    input_files = [input_path] if input_path.is_file() else list(input_path.glob("*.jsonl"))
    if not input_files: raise FileNotFoundError(f"No .jsonl files in {input_path}")

    output_dir = Path(args.output) if args.output else input_path.parent.parent / "eval_files_oneshot"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Context
    backend = create_backend(args.provider, args.model, args.api_key)
    pg_config = load_config(args.pg_config if args.pg_config else get_default_config_path())
    context = {
        'backend': backend, 'pg_config': pg_config, 'sys_prompt': build_system_prompt(),
        'ddl_text': load_ddl(target_db_dir), 'sample_text': load_sample_data(target_db_dir),
        'knowledge_map': load_knowledge(Path(args.knowledge)) if args.knowledge else {},
        'examples_path': Path(args.examples) if args.examples else None
    }

    for in_file in input_files:
        logger.info(f"Processing: {in_file.name}")
        out_file = output_dir / f"{in_file.stem}_evaluated.jsonl"
        with open(in_file, 'r', encoding='utf-8') as f:
            tasks = [json.loads(line) for line in f if line.strip()]
        if args.limit: tasks = tasks[:args.limit]

        results, em_total, ex_total = [], 0, 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(process_task, (t, i, args, context)) for i, t in enumerate(tasks)]
            for future in tqdm(as_completed(futures), total=len(tasks), desc=in_file.name):
                res = future.result()
                results.append(res)
                em_total += res['em']
                ex_total += res['ex']
                with open(out_file, 'a' if len(results) > 1 else 'w', encoding='utf-8') as f:
                    f.write(json.dumps(res, ensure_ascii=False) + '\n')

        logger.info(f"Done: {in_file.name} | EM: {em_total}/{len(tasks)} | EX: {ex_total}/{len(tasks)}")

if __name__ == "__main__":
    main()
