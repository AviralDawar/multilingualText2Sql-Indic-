#!/usr/bin/env python3
"""
Few-Shot Text-to-SQL Evaluator

Evaluates an LLM's Text-to-SQL capabilities using dynamic multi-shot prompting.
Retrieves the most relevant $k$ examples from a task pool based on question similarity.
Calculates Exact Match (EM) and Execution Accuracy (EX) against a PostgreSQL database.
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from difflib import SequenceMatcher

import psycopg2
import requests
from tqdm import tqdm

# Add parent dir to path to import db_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_utils import load_config, get_connection, get_default_config_path, execute_use_schema

try:
    import torch
    from sentence_transformers import SentenceTransformer
except ImportError:
    torch = None
    SentenceTransformer = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
# Retrieval Logic (Adapted from SEED.py)
# ---------------------------------------------------------------------------

def normalize_tokens(text: str) -> List[str]:
    return [t for t in re.split(r"[^a-z0-9]+", text.lower()) if t]

def text_similarity(a: str, b: str) -> float:
    tokens_a = set(normalize_tokens(a))
    tokens_b = set(normalize_tokens(b))
    if not tokens_a or not tokens_b:
        return 0.0
    jaccard = len(tokens_a & tokens_b) / len(tokens_a | tokens_b)
    ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
    return 0.6 * jaccard + 0.4 * ratio

class SimilarQuestionFinder:
    """Similarity-based similar question retrieval for few-shot selection."""

    def __init__(self, train_data: List[Dict[str, Any]], use_embeddings: bool = False, model_name: str = ""):
        self.train_data = train_data
        self.questions = [str(item["question"]) for item in self.train_data]
        self.db_ids = [str(item.get("db_id", "")) for item in self.train_data]
        self.use_embeddings = use_embeddings and (SentenceTransformer is not None)
        
        if self.use_embeddings:
            logger.info(f"Initializing embedding model: {model_name}")
            self.model = SentenceTransformer(model_name, trust_remote_code=True)
            self.embeddings = self.model.encode(
                self.questions,
                convert_to_tensor=True,
                show_progress_bar=False,
            )
        else:
            if use_embeddings:
                logger.warning("sentence-transformers or torch not found. Falling back to text similarity.")
            self.embeddings = None

    def find_similar_questions(
        self,
        target_question: str,
        target_db_id: str,
        k: int
    ) -> List[Dict[str, Any]]:
        if self.use_embeddings:
            with torch.no_grad():
                target_embedding = self.model.encode(
                    [target_question], convert_to_tensor=True, show_progress_bar=False
                )
                similarities = self.model.similarity_pairwise(
                    target_embedding, self.embeddings
                ).squeeze(0)
                sorted_indices = torch.argsort(similarities, descending=True).tolist()
        else:
            scores = [text_similarity(target_question, q) for q in self.questions]
            sorted_indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)

        output: List[Dict[str, Any]] = []
        for idx in sorted_indices:
            item = self.train_data[idx]
            # Leakage prevention: Example must not be from the target database
            if item.get('db_id') != target_db_id:
                output.append(item)
            if len(output) >= k:
                break
        return output

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
                if not data:
                    continue
                samples.append(f"/* 3 example rows for table {table_name}:")
                for i, row in enumerate(data[:3]):
                    samples.append(f"Row {i+1}: {row}")
                samples.append("*/\n")
        except Exception as e:
            logger.warning(f"Failed to read sample data from {json_file}: {e}")
    return "\n".join(samples)

def load_knowledge(knowledge_path: Path) -> Dict[str, str]:
    knowledge_map = {}
    if not knowledge_path or not knowledge_path.exists():
        return knowledge_map
    with open(knowledge_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for item in data:
            if 'pair_id' in item and 'evidence' in item:
                knowledge_map[item['pair_id']] = item['evidence']
    return knowledge_map

def load_example_pool(examples_path: Path) -> List[Dict[str, Any]]:
    if not examples_path or not examples_path.exists():
        return []
    pool = []
    with open(examples_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                pool.append(json.loads(line))
    return pool

def format_few_shot_examples(examples: List[Dict[str, Any]]) -> str:
    if not examples:
        return ""
    
    blocks = ["### FEW-SHOT LEARNING EXAMPLES ###"]
    for i, exam in enumerate(examples, 1):
        block = (
            f"==== EXAMPLE {i} ====\n"
            f"Question: {exam['question']}\n"
        )
        if 'evidence' in exam and exam['evidence']:
            block += f"Evidence: {exam['evidence']}\n"
        block += f"SQL: {exam['sql']}\n"
        block += "=================\n"
        blocks.append(block)
    return "\n".join(blocks)

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

def build_user_prompt(
    question: str, 
    evidence: str, 
    ddl: str, 
    samples: str, 
    few_shot_text: str
) -> str:
    prompt = f"### DATABASE SCHEMA ###\n{ddl}\n\n"
    if samples:
        prompt += f"### SAMPLE DATA ###\n{samples}\n\n"
    if few_shot_text:
        prompt += f"{few_shot_text}\n\n"
    prompt += "### TASK ###\n"
    prompt += f"Question: {question}\n"
    if evidence:
        prompt += f"Evidence / External Knowledge: {evidence}\n"
    prompt += "\nOutput only the valid PostgreSQL query (ending with a semicolon) that answers the Question. Do not include markdown formatting.\nSQL: "
    return prompt

# ---------------------------------------------------------------------------
# Evaluation Engine (Reused from run_oneshot.py)
# ---------------------------------------------------------------------------

def extract_sql(llm_output: str) -> str:
    sql = llm_output.strip()
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^```\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    sql = re.sub(r'^SQL:\s*', '', sql, flags=re.IGNORECASE)
    return sql.strip()

def _extract_sql_components(sql: str) -> Optional[Dict[str, Any]]:
    from sqlglot import parse_one, exp
    try:
        sql_clean = sql.replace('"', '').replace(';', '')
        ast = parse_one(sql_clean, read="postgres")
    except Exception:
        return None
    for node in ast.find_all(exp.Identifier):
        node.set("this", str(node.name).lower())
        node.set("quoted", False)
    for node in ast.find_all(exp.Column):
        node.set("table", None)
        node.set("db", None)
    for node in ast.find_all(exp.Table):
        node.set("db", None)
        node.set("catalog", None)
        if isinstance(node.parent, exp.Alias):
            node.parent.replace(node)
    for node in ast.find_all(exp.Literal):
        if node.is_string:
            node.set("this", str(node.this).lower())
    components = {"select": set(), "from": set(), "where": set(), "group": set(), "order": []}
    if isinstance(ast, exp.Select):
        for e in ast.expressions:
            if isinstance(e, exp.Alias): components["select"].add(e.this.sql().lower())
            else: components["select"].add(e.sql().lower())
        for table in ast.find_all(exp.Table):
            components["from"].add(table.this.sql().lower())
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

def calculate_em(gold_sql: str, pred_sql: str) -> int:
    if not pred_sql or not gold_sql: return 0
    gold_comps = _extract_sql_components(gold_sql)
    pred_comps = _extract_sql_components(pred_sql)
    if not gold_comps or not pred_comps:
        g_norm = ' '.join(gold_sql.lower().split()).replace('"', '').replace(';', '')
        p_norm = ' '.join(pred_sql.lower().split()).replace('"', '').replace(';', '')
        return 1 if g_norm == p_norm else 0
    checks = [
        gold_comps["select"] == pred_comps["select"],
        gold_comps["from"] == pred_comps["from"],
        gold_comps["where"] == pred_comps["where"],
        gold_comps["group"] == pred_comps["group"],
        gold_comps["order"] == pred_comps["order"]
    ]
    return 1 if all(checks) else 0

def fetch_results(cursor, sql: str, timeout_ms: int = 5000) -> Tuple[List[Any], Optional[str]]:
    try:
        cursor.execute(f"SET local statement_timeout TO {timeout_ms};")
        cursor.execute(sql)
        results = cursor.fetchall()
        str_results = []
        for row in results:
            str_results.append(tuple(str(v).strip() if v is not None else None for v in row))
        return str_results, None
    except psycopg2.Error as e: return [], str(e).strip()
    except Exception as e: return [], str(e).strip()
    finally:
        try: cursor.connection.rollback()
        except: pass

def calculate_ex(cursor, gold_sql: str, pred_sql: str) -> Tuple[int, Optional[str]]:
    if not pred_sql: return 0, "Empty prediction"
    gold_res, gold_err = fetch_results(cursor, gold_sql)
    if gold_err: return 0, f"Gold SQL Error: {gold_err}"
    pred_res, pred_err = fetch_results(cursor, pred_sql)
    if pred_err: return 0, f"Pred SQL Error: {pred_err}"
    order_matters = 'order by' in gold_sql.lower()
    is_match = (gold_res == pred_res) if order_matters else (Counter(gold_res) == Counter(pred_res))
    if is_match: return 1, None
    if len(gold_res) < 10 and len(pred_res) < 10:
        return 0, f"Mismatch (Order: {order_matters}). Gold: {gold_res} | Pred: {pred_res}"
    return 0, f"Mismatch (Order: {order_matters}). Gold count: {len(gold_res)}. Pred count: {len(pred_res)}."

# ---------------------------------------------------------------------------
# Main Routine
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Few-Shot Text-to-SQL Evaluator")
    parser.add_argument("--database", required=True, help="Target database ID")
    parser.add_argument("--database-dir", default="../databases", help="Base dir containing database folders")
    parser.add_argument("--input", required=True, help="Input tasks JSONL file")
    parser.add_argument("--output", help="Output results JSONL file")
    parser.add_argument("--knowledge", help="Optional knowledge/evidence JSON file")
    parser.add_argument("--examples", required=True, help="Pool of tasks for few-shot selection")
    
    # Few-shot arguments
    parser.add_argument("--k", type=int, default=5, help="Number of few-shot examples")
    parser.add_argument("--use-embeddings", action="store_true", help="Use sentence embeddings for retrieval")
    parser.add_argument("--embedding-model", default="sentence-transformers/all-mpnet-base-v2", help="Embedding model name")
    
    # LLM arguments
    parser.add_argument("--provider", default="anthropic", choices=["anthropic", "openrouter"], help="LLM Provider")
    parser.add_argument("--model", default="claude-3-haiku-20240307", help="LLM Model name")
    parser.add_argument("--api-key", help="API Key (overrides env var)")
    
    # Postgres arguments
    parser.add_argument("--pg-config", help="Path to postgres config json")
    parser.add_argument("--pg-db", default="indicdb", help="Target postgres logical database name")
    
    # Run arguments
    parser.add_argument("--limit", type=int, help="Limit number of queries to evaluate")
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="Sleep between API calls")
    
    args = parser.parse_args()
    
    db_id = args.database
    db_base_dir = Path(args.database_dir)
    target_db_dir = db_base_dir / db_id / db_id
    
    if not target_db_dir.exists():
        logger.error(f"Database directory not found: {target_db_dir}")
        sys.exit(1)
        
    # 1. Initialize Retrieval
    logger.info(f"Loading few-shot example pool from {args.examples}...")
    example_pool = load_example_pool(Path(args.examples))
    finder = SimilarQuestionFinder(example_pool, use_embeddings=args.use_embeddings, model_name=args.embedding_model)
    
    # 2. Load Backend
    logger.info(f"Initializing LLM backend: {args.provider} ({args.model})")
    backend = create_backend(args.provider, args.model, args.api_key)
    
    # 3. Load DB Connection
    logger.info(f"Connecting to PostgreSQL logic db: {args.pg_db}, schema: {db_id.lower()}")
    pg_config_path = args.pg_config if args.pg_config else get_default_config_path()
    pg_config = load_config(pg_config_path)
    conn = get_connection(pg_config, database=args.pg_db)
    cursor = conn.cursor()
    execute_use_schema(cursor, db_id.lower())
    conn.commit()
    
    # 4. Load semantic context
    ddl_text = load_ddl(target_db_dir)
    sample_text = load_sample_data(target_db_dir)
    knowledge_map = load_knowledge(Path(args.knowledge)) if args.knowledge else {}
    sys_prompt = build_system_prompt()
    
    # 5. Load tasks
    in_file = Path(args.input)
    with open(in_file, 'r', encoding='utf-8') as f:
        tasks = [json.loads(line) for line in f if line.strip()]
    if args.limit: tasks = tasks[:args.limit]
    
    out_file = args.output or str(in_file.parent / f"{in_file.stem}_fewshot_evaluated.jsonl")
    
    results = []
    em_total = 0
    ex_total = 0
    
    # 6. Evaluation Loop
    for idx, task in enumerate(tqdm(tasks, desc="Evaluating Few-Shot")):
        pair_id = task.get('pair_id', f'unknown_{idx}')
        question = task.get('question', '')
        gold_sql = task.get('sql', '')
        evidence = knowledge_map.get(pair_id, '')
        
        # DYNAMIC RETRIEVAL
        retrieved_examples = finder.find_similar_questions(question, db_id, k=args.k)
        few_shot_text = format_few_shot_examples(retrieved_examples)
        
        usr_prompt = build_user_prompt(question, evidence, ddl_text, sample_text, few_shot_text)
        
        try:
            raw_output = backend.generate(sys_prompt, usr_prompt, temperature=0.0)
            pred_sql = extract_sql(raw_output)
            llm_error = None
        except Exception as e:
            pred_sql, raw_output, llm_error = "", "", str(e)
            logger.warning(f"LLM Error on {pair_id}: {e}")
            
        em = calculate_em(gold_sql, pred_sql)
        ex, exec_err = calculate_ex(cursor, gold_sql, pred_sql)
        
        em_total += em
        ex_total += ex
            
        task_res = {
            'pair_id': pair_id, 'question': question, 'gold_sql': gold_sql,
            'predicted_sql': pred_sql, 'raw_output': raw_output,
            'em': em, 'ex': ex, 'error': exec_err or llm_error
        }
        results.append(task_res)
        
        with open(out_file, 'a' if idx > 0 else 'w', encoding='utf-8') as f:
            f.write(json.dumps(task_res, ensure_ascii=False) + '\n')
            
        if args.sleep_seconds > 0: time.sleep(args.sleep_seconds)
            
    conn.close()
    logger.info(f"\nFEW-SHOT COMPLETE: {len(results)} queries")
    logger.info(f"EM: {em_total/len(results)*100:.2f}% | EX: {ex_total/len(results)*100:.2f}%")
    logger.info(f"Results saved to: {out_file}")

if __name__ == "__main__":
    main()
