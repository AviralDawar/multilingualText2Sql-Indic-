from __future__ import annotations

import argparse
import json
import os
import re
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from difflib import SequenceMatcher, get_close_matches
from pathlib import Path
from typing import Any

import requests
from psycopg2 import sql

from db_utils import (
    execute_use_schema,
    get_connection,
    get_default_config_path,
    load_config,
)

try:
    import torch
    from sentence_transformers import SentenceTransformer
except ImportError:
    torch = None
    SentenceTransformer = None


MAX_QUESTIONS = 100


def make_prompt(question: str, concat_schema: str) -> tuple[str, str, str, str]:
    evidence_generation_system_prompt = """### You are a data science expert and should assist your colleague in creating SQL. \
Your colleague is an expert in SQL, but he does not have domain knowledge. \
So you should analyze the given question and create a clear, concise and accurate evidence to help your colleague create SQL. \
Perform the steps below to create an evidence and describe in detail the reasoning of each step.

Step-by-Step Instructions:
1. **Refer to Sample SQL results**: Your another colleague generated sample SQL for words considered keyword in question and executed it in database. Refer to the value and format. Note that your colleague may have chosen the wrong schema.
2. **The question may not be in english, But the final evidence should be in english.**
3. **Analyze the Question and Schema**: Identify key elements in the question that need mapping (columns, tables, values). Clarify ambiguities by referencing the database schema.
4. **Analyze the Few-shot samples**: Read the samples and understand the relationship between question and evidence and database schema and descriptions.
5. **Generate Evidence**: Based on what you analyzed earlier, generate evidence so that it is as short as possible and contains as much information as possible.
6. **Consideration of cautions**: Make sure that the generated evidence does not violate the cautions below.
    Cautions 1. Schema-Specific Language: Use precise terminology from the database schema to avoid ambiguity.
    Cautions 2. Schema Formatting: When mentioning a column, mention the table containing that column together. Use the form (`table`.`column`) to refer to columns.
    Cautions 3. Case Sensitivity: Reflect the exact case of database values in your evidence to prevent mismatches. Refer to db value for accurate case utilization.
7. **Output Reasoning**: Describe the reasoning of each step in detail and print it out.
8. **Output Evidence**: Provide the output in the following unannotated JSON format:
    {
      "evidence": "Provide clear, concise and accurate evidence"
    }
"""

    evidence_generation_user_prompt = f"""### problem ####################################################
1. schema of question
{{
{concat_schema}
}}

2. question
{{
  "question": "{question}",
  "evidence":
}}
### Let's think step by step.
"""

    keyword_extract_system_prompt = """### As a data science expert, you need to perform preliminary tasks before the text-to-SQL process. \
To assist in this task, you will extract the schema and values from the given question to generate sample SQL queries. \
The goal is to verify the database structure and content by extracting a diverse set of plausible schema-value combinations. \
Since this process is exploratory, adopt a lenient and comprehensive approach that accounts for potential ambiguities or multiple interpretations. \
For example, include all plausible schemas and values, even if they are uncertain or overlapping. \
This will maximize the potential for finding relevant information during the text-to-SQL task. \
The question may not be in english, But the final evidence should be in english.

Please follow these steps to extract the keywords and provide a detailed explanation for each step:
### Steps for Extraction:
1. **Problem Analysis**
2. **Keyword Detection (Direct Schema)**
3. **Keyword Detection (Similar Schema)**
4. **Keyword Detection (Values)**
5. **Schema-Value Pairing**
6. **Mapping to Tables**
7. **Output Reasoning**
8. **Output Generation** in JSON:
{
  "schema-value-pair": [
    {"schema": "<table.column>", "value": "<value>"},
    {"schema": "<table.column>", "value": null}
  ]
}
"""

    keyword_extract_user_prompt = f"""### problem ####################################################
1. schema of question
{{
{concat_schema}
}}

2. question
{{
  "question": "{question}",
  "schema-value-pair":
}}
### Let's think step by step.
"""

    return (
        evidence_generation_system_prompt,
        evidence_generation_user_prompt,
        keyword_extract_system_prompt,
        keyword_extract_user_prompt,
    )


def make_schema_summary_prompt(question: str, concat_schema: str) -> tuple[str, str]:
    system_prompt = """### As a data science expert, your task is to prepare a schema for efficient text-to-SQL operations through schema linking. \
The given schema includes comments for each column, providing descriptions and value samples. \
Identify and remove columns that are irrelevant to the provided question. \
However, ensure that columns designated as primary keys or foreign keys are preserved. \
For all remaining columns, retain their associated comments, including descriptions and value samples.

Present results in the following JSON format without description:
{
  "summarized_schema": "..."
}
"""
    user_prompt = f"""#######################################################
1. schema of question
{{
{concat_schema}
}}

2. question
{{
  "question": "{question}"
}}
### Let's think step by step.
"""
    return system_prompt, user_prompt


def parse_json_object(content: str) -> dict[str, Any] | None:
    text = content.strip()
    try:
        maybe = json.loads(text)
        if isinstance(maybe, dict):
            return maybe
    except json.JSONDecodeError:
        pass

    code_block_matches = re.findall(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    for candidate in reversed(code_block_matches):
        try:
            maybe = json.loads(candidate)
            if isinstance(maybe, dict):
                return maybe
        except json.JSONDecodeError:
            continue

    brace_matches = re.findall(r"\{[\s\S]*\}", text)
    for candidate in reversed(brace_matches):
        try:
            maybe = json.loads(candidate)
            if isinstance(maybe, dict):
                return maybe
        except json.JSONDecodeError:
            continue

    return None


def extract_json_field(content: str, field: str) -> Any:
    parsed = parse_json_object(content)
    if parsed is None:
        return None
    return parsed.get(field)


def normalize_tokens(text: str) -> list[str]:
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
    """Embedding-based similar question retrieval for few-shot selection."""

    def __init__(self, train_data: list[dict[str, Any]], model_name: str):
        if SentenceTransformer is None or torch is None:
            raise ImportError("sentence-transformers and torch are required for embedding retrieval")

        self.train_data = [
            item
            for item in train_data
            if item.get("question")
            and item.get("db_id")
            and item.get("evidence")
            and str(item.get("evidence")).strip().lower() not in {"", "false;"}
        ]
        self.questions = [str(item["question"]) for item in self.train_data]
        self.db_ids = [str(item["db_id"]) for item in self.train_data]
        self.evidences = [str(item["evidence"]) for item in self.train_data]
        self.model = SentenceTransformer(model_name, trust_remote_code=True)
        self.embeddings = self.model.encode(
            self.questions,
            convert_to_tensor=True,
            show_progress_bar=False,
        )

    def find_similar_questions(
        self,
        target_question: str,
        top_k: int,
        top_n_same_db: int,
    ) -> list[tuple[str, str, str, list[tuple[str, str]]]]:
        with torch.no_grad():
            target_embedding = self.model.encode(
                [target_question], convert_to_tensor=True, show_progress_bar=False
            )
            similarities = self.model.similarity_pairwise(
                target_embedding, self.embeddings
            ).squeeze(0)
            sorted_indices = torch.argsort(similarities, descending=True).tolist()

        unique_db_ids: set[str] = set()
        top_primary_indices: list[int] = []
        for idx in sorted_indices:
            db_id = self.db_ids[idx]
            if db_id in unique_db_ids:
                continue
            unique_db_ids.add(db_id)
            top_primary_indices.append(idx)
            if len(top_primary_indices) >= top_k:
                break

        output: list[tuple[str, str, str, list[tuple[str, str]]]] = []
        for primary_idx in top_primary_indices:
            db_id = self.db_ids[primary_idx]
            question = self.questions[primary_idx]
            evidence = self.evidences[primary_idx]

            same_db_indices = [i for i, db in enumerate(self.db_ids) if db == db_id]
            same_db_scores = similarities[same_db_indices]
            ranked_local = torch.argsort(same_db_scores, descending=True).tolist()

            extras: list[tuple[str, str]] = []
            for ridx in ranked_local:
                global_idx = same_db_indices[ridx]
                if global_idx == primary_idx:
                    continue
                extras.append((self.questions[global_idx], self.evidences[global_idx]))
                if len(extras) >= max(0, top_n_same_db - 1):
                    break

            output.append((question, db_id, evidence, extras))
        return output


class OpenRouterClient:
    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str = "https://openrouter.ai/api/v1/chat/completions",
        rate_limiter: "RateLimiter | None" = None,
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
    ):
        self.model = model
        self.api_key = api_key
        self.base_url = base_url
        self.rate_limiter = rate_limiter
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def chat(self, messages: list[dict[str, str]], temperature: float = 0.0, max_tokens: int = 3000) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        for attempt in range(1, self.max_retries + 1):
            if self.rate_limiter is not None:
                self.rate_limiter.acquire()
            response = requests.post(self.base_url, headers=headers, data=json.dumps(payload), timeout=120)
            if response.status_code == 200:
                content = response.json()["choices"][0]["message"]["content"]
                return content or ""
            if attempt < self.max_retries and self._is_retryable_status(response.status_code):
                time.sleep(self.retry_backoff_seconds * attempt)
                continue
            raise RuntimeError(f"OpenRouter API error {response.status_code}: {response.text}")
        return ""

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code in {408, 409, 429, 500, 502, 503, 504}


class RateLimiter:
    def __init__(self, requests_per_minute: float):
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self.interval = 60.0 / requests_per_minute
        self._lock = threading.Lock()
        self._next_allowed_time = 0.0

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed_time:
                    self._next_allowed_time = now + self.interval
                    return
                wait_time = self._next_allowed_time - now
            time.sleep(wait_time)


@dataclass
class SeedConfig:
    output_path: Path
    pg_config_path: Path
    use_database: str | None
    use_schema: str | None
    top_k: int
    top_n_same_db: int
    max_samples_per_column: int
    schema_summary: bool
    llm_model: str
    use_embeddings: bool
    embedding_model: str
    max_workers: int
    max_requests_per_minute: float
    max_retries: int
    retry_backoff_seconds: float


def read_dataset(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        if not isinstance(data, list):
            raise ValueError(f"Expected list in {path}")
        return data


def truncate_dataset(rows: list[dict[str, Any]], max_questions: int = MAX_QUESTIONS) -> list[dict[str, Any]]:
    return rows[:max_questions]


def resolve_target_schema(config: SeedConfig, db_id: str) -> str:
    if config.use_schema:
        return config.use_schema.lower()
    return db_id.lower()


def load_knowledge_schema(db_id: str) -> str:
    schema_path = Path("knowledge_files") / f"{db_id}.sql"
    if not schema_path.exists():
        return ""
    return schema_path.read_text(encoding="utf-8").strip()


def open_pg_connection(config: SeedConfig):
    pg_cfg = load_config(str(config.pg_config_path))
    target_db = config.use_database.lower() if config.use_database else None
    return get_connection(pg_cfg, database=target_db)


def load_schema_from_postgres(cursor, schema_name: str) -> str:
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema_name,),
    )
    tables = [r[0] for r in cursor.fetchall()]
    if not tables:
        return ""

    parts: list[str] = []
    for table in tables:
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table),
        )
        cols = cursor.fetchall()

        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            (schema_name, table),
        )
        pk_cols = [r[0] for r in cursor.fetchall()]

        cursor.execute(
            """
            SELECT
              kcu.column_name,
              ccu.table_name AS foreign_table_name,
              ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            """,
            (schema_name, table),
        )
        fks = cursor.fetchall()

        lines = [f"CREATE TABLE {table} ("]
        for col_name, data_type, is_nullable in cols:
            nullable = "" if is_nullable == "YES" else " NOT NULL"
            lines.append(f"  {col_name} {data_type}{nullable},")
        if pk_cols:
            lines.append(f"  PRIMARY KEY ({', '.join(pk_cols)}),")
        for col_name, fk_table, fk_col in fks:
            lines.append(f"  FOREIGN KEY ({col_name}) REFERENCES {fk_table}({fk_col}),")
        if lines[-1].endswith(","):
            lines[-1] = lines[-1][:-1]
        lines.append(");")
        parts.append("\n".join(lines))

    return "\n\n".join(parts)


def parse_schema_index(schema_text: str) -> dict[str, list[str]]:
    index: dict[str, list[str]] = {}
    pattern = re.compile(r"CREATE\s+TABLE\s+([A-Za-z0-9_]+)\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)
    for table, body in pattern.findall(schema_text):
        columns: list[str] = []
        for line in body.splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            prefix = line.split()[0].strip("`\"")
            upper = prefix.upper()
            if upper in {"PRIMARY", "FOREIGN", "UNIQUE", "CONSTRAINT"}:
                continue
            columns.append(prefix)
        if columns:
            index[table] = columns
    return index


def heuristic_schema_value_pairs(question: str, schema_index: dict[str, list[str]], limit: int = 12) -> list[dict[str, Any]]:
    quoted_values = re.findall(r"'([^']+)'|\"([^\"]+)\"", question)
    values = [v1 or v2 for v1, v2 in quoted_values if (v1 or v2)]

    scored: list[tuple[float, str]] = []
    for table, columns in schema_index.items():
        for column in columns:
            fq = f"{table}.{column}"
            score = max(text_similarity(question, column), text_similarity(question, fq), text_similarity(question, table))
            scored.append((score, fq))
    scored.sort(key=lambda x: x[0], reverse=True)

    pairs: list[dict[str, Any]] = []
    for _, fq in scored[:limit]:
        if values:
            for value in values[:2]:
                pairs.append({"schema": fq, "value": value})
        else:
            pairs.append({"schema": fq, "value": None})
    return pairs


def coerce_string(v: Any) -> str:
    if v is None:
        return "null"
    return str(v)


def extract_sample_results(
    schema_value_pairs: list[dict[str, Any]],
    cursor,
    schema_name: str,
    max_samples: int,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for pair in schema_value_pairs:
        schema = str(pair.get("schema", ""))
        raw_value = pair.get("value")
        if schema.count(".") != 1:
            continue
        table, column = schema.split(".", 1)
        try:
            distinct_query = sql.SQL(
                """
                SELECT DISTINCT LEFT(CAST({col} AS text), 100)
                FROM {schema}.{table}
                WHERE {col} IS NOT NULL
                LIMIT %s
                """
            ).format(
                col=sql.Identifier(column),
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table),
            )
            cursor.execute(distinct_query, (max_samples,))
            values = [r[0] for r in cursor.fetchall()]
        except Exception:
            continue

        if not values:
            continue

        distinct: list[str] = []
        seen = set()
        for item in values:
            s = coerce_string(item)
            if s not in seen:
                seen.add(s)
                distinct.append(s)
            if len(distinct) >= max_samples:
                break

        output.append(
            {
                "schema": schema,
                "value": raw_value,
                "sql_desc": f"{max_samples} sample values of `{table}`.`{column}`",
                "sample_sql": f"SELECT DISTINCT {column} FROM {schema_name}.{table} LIMIT {max_samples};",
                "sql_results": distinct,
            }
        )

        if raw_value not in (None, "null", "NULL", ""):
            search = str(raw_value).lower()
            contains_unique: list[str] = []
            try:
                like_query = sql.SQL(
                    """
                    SELECT DISTINCT LEFT(CAST({col} AS text), 100)
                    FROM {schema}.{table}
                    WHERE CAST({col} AS text) ILIKE %s
                    LIMIT %s
                    """
                ).format(
                    col=sql.Identifier(column),
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table),
                )
                cursor.execute(like_query, (f"%{search}%", max_samples))
                contains_unique = [coerce_string(r[0]) for r in cursor.fetchall()]
            except Exception:
                contains_unique = []

            output.append(
                {
                    "schema": schema,
                    "value": raw_value,
                    "sql_desc": f"Values in `{table}`.`{column}` matching '%{raw_value}%'",
                    "sample_sql": f"SELECT {column} FROM {schema_name}.{table} WHERE CAST({column} AS text) ILIKE '%{search}%';",
                    "sql_results": contains_unique,
                }
            )

            closest = get_close_matches(str(raw_value), [coerce_string(v) for v in values], n=1, cutoff=0.0)
            output.append(
                {
                    "schema": schema,
                    "value": raw_value,
                    "sql_desc": f"Closest value in `{table}`.`{column}` to '{raw_value}'",
                    "sample_sql": f"Approx nearest from sampled distinct values in {schema_name}.{table}.{column}",
                    "sql_results": closest,
                }
            )

    return output


def build_fewshot_examples(
    question: str,
    train_data: list[dict[str, Any]],
    top_k: int,
    top_n_same_db: int,
    finder: SimilarQuestionFinder | None = None,
) -> list[tuple[str, str, str, list[tuple[str, str]]]]:
    if finder is not None:
        return finder.find_similar_questions(question, top_k, top_n_same_db)

    eligible = [
        x for x in train_data
        if x.get("question") and x.get("db_id") and x.get("evidence")
        and str(x.get("evidence")).strip().lower() not in {"", "false;"}
    ]
    if not eligible:
        return []

    scored = [(text_similarity(question, item["question"]), item) for item in eligible]
    scored.sort(key=lambda x: x[0], reverse=True)

    unique_db_ids: set[str] = set()
    top_primary: list[dict[str, Any]] = []
    for _, item in scored:
        db_id = str(item["db_id"])
        if db_id in unique_db_ids:
            continue
        unique_db_ids.add(db_id)
        top_primary.append(item)
        if len(top_primary) >= top_k:
            break

    output = []
    for item in top_primary:
        db_id = str(item["db_id"])
        related = [x for x in eligible if str(x["db_id"]) == db_id and x["question"] != item["question"]]
        related.sort(key=lambda x: text_similarity(question, x["question"]), reverse=True)
        extras = [(x["question"], x["evidence"]) for x in related[: max(0, top_n_same_db - 1)]]
        output.append((item["question"], db_id, item["evidence"], extras))
    return output


def render_sample_sql_prompt(sample_results: list[dict[str, Any]]) -> str:
    lines = ["### Sample SQL results for Data Exploration ####################################################"]
    for item in sample_results:
        lines.append(f"# {item['sql_desc']}: {item['sql_results']}")
    lines.append("##################################################################")
    return "\n".join(lines)


def build_schema_plus_samples(schema_text: str, cursor, schema_name: str, per_column_samples: int = 3) -> str:
    schema_index = parse_schema_index(schema_text)
    lines = [schema_text]
    lines.append("\n-- Column value examples")
    for table, columns in schema_index.items():
        for col in columns:
            vals: list[str] = []
            try:
                q = sql.SQL(
                    """
                    SELECT DISTINCT LEFT(CAST({col} AS text), 100)
                    FROM {schema}.{table}
                    WHERE {col} IS NOT NULL
                    LIMIT %s
                    """
                ).format(
                    col=sql.Identifier(col),
                    schema=sql.Identifier(schema_name),
                    table=sql.Identifier(table),
                )
                cursor.execute(q, (per_column_samples,))
                vals = [coerce_string(r[0]) for r in cursor.fetchall()]
            except Exception:
                vals = []
            if vals:
                lines.append(f"-- {table}.{col}: {', '.join(vals)}")
    return "\n".join(lines)


def safe_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").strip()


def generate_evidence_for_item(
    item: dict[str, Any],
    config: SeedConfig,
    api_key: str,
    train_data: list[dict[str, Any]],
    finder: SimilarQuestionFinder | None = None,
    rate_limiter: RateLimiter | None = None,
) -> str:
    db_id = config.use_schema
    # db_id = str(item.get("db_id", "")).strip()
    instance_id = str(item.get("instance_id", "")).strip() or "N/A"
    question = safe_string(item.get("question") or item.get("instruction"))
    if not db_id or not question:
        return ""

    schema_name = resolve_target_schema(config, db_id)
    conn = open_pg_connection(config)
    try:
        cursor = conn.cursor()
        execute_use_schema(cursor, schema_name)
        schema = load_schema_from_postgres(cursor, schema_name)
        concat_schema = build_schema_plus_samples(schema, cursor, schema_name, per_column_samples=3)
    finally:
        conn.close()

    if config.schema_summary:
        summary_client = OpenRouterClient(
            model=config.llm_model,
            api_key=api_key,
            rate_limiter=rate_limiter,
            max_retries=config.max_retries,
            retry_backoff_seconds=config.retry_backoff_seconds,
        )
        summary_system, summary_user = make_schema_summary_prompt(question, concat_schema)
        summary_resp = summary_client.chat(
            [{"role": "system", "content": summary_system}, {"role": "user", "content": summary_user}],
            temperature=0.0,
            max_tokens=2500,
        )
        summarized = extract_json_field(summary_resp, "summarized_schema")
        if isinstance(summarized, str) and summarized.strip():
            concat_schema = summarized

    (
        evidence_sys,
        evidence_user,
        keyword_sys,
        keyword_user,
    ) = make_prompt(question, concat_schema)

    keyword_client = OpenRouterClient(
        model=config.llm_model,
        api_key=api_key,
        rate_limiter=rate_limiter,
        max_retries=config.max_retries,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )
    keyword_resp = keyword_client.chat(
        [{"role": "system", "content": keyword_sys}, {"role": "user", "content": keyword_user}],
        temperature=0.0,
        max_tokens=2500,
    )
    pairs = extract_json_field(keyword_resp, "schema-value-pair")
    schema_index = parse_schema_index(schema)

    if not isinstance(pairs, list):
        pairs = heuristic_schema_value_pairs(question, schema_index)

    conn = open_pg_connection(config)
    try:
        cursor = conn.cursor()
        execute_use_schema(cursor, schema_name)
        sample_results = extract_sample_results(
            schema_value_pairs=pairs,
            cursor=cursor,
            schema_name=schema_name,
            max_samples=config.max_samples_per_column,
        )
    finally:
        conn.close()
    
    prompt_messages = [{"role": "system", "content": evidence_sys}]

    fewshots = build_fewshot_examples(
        question=question,
        train_data=train_data,
        top_k=config.top_k,
        top_n_same_db=config.top_n_same_db,
        finder=finder,
    )

    for i, (q0, shot_db_id, ev0, extras) in enumerate(fewshots, start=1):
        shot_schema = load_knowledge_schema(shot_db_id)
        shot_text = [
            f"### few-shot sample {i} ####################################################",
        ]
        if shot_schema:
            shot_text.extend([
                "schema of few-shot example",
                "{",
                shot_schema,
                "}",
            ])
        shot_text.extend([
            f'{{"db_id":"{shot_db_id}","question":"{safe_string(q0)}","evidence":"{safe_string(ev0)}"}}',
        ])
        for eq, ee in extras:
            shot_text.append(f'{{"question":"{safe_string(eq)}","evidence":"{safe_string(ee)}"}}')
        shot_text.append("##################################################################")
        prompt_messages.append({"role": "user", "content": "\n".join(shot_text)})

    prompt_messages.append({"role": "user", "content": render_sample_sql_prompt(sample_results)})
    prompt_messages.append({"role": "user", "content": evidence_user})
    
    evidence_client = OpenRouterClient(
        model=config.llm_model,
        api_key=api_key,
        rate_limiter=rate_limiter,
        max_retries=config.max_retries,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )
    evidence_resp = evidence_client.chat(prompt_messages, temperature=0.0, max_tokens=3000)
    evidence = extract_json_field(evidence_resp, "evidence")

    if isinstance(evidence, dict):
        final_evidence = ", ".join([f"{k}: {v}" for k, v in evidence.items()])
        return final_evidence
    if isinstance(evidence, str) and evidence.strip():
        final_evidence = evidence.strip().replace("\n", ", ")
        return final_evidence
    return ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SEED evidence generation for IndicDB project")
    parser.add_argument("--dataset", required=True, help="Path to input dataset (.json or .jsonl)")
    parser.add_argument("--config", type=str, help="Path to postgres credential file")
    parser.add_argument("--use-database", type=str, help="Target PostgreSQL database")
    parser.add_argument("--use-schema", type=str, help="Target PostgreSQL schema override")
    parser.add_argument("--output", required=True, help="Path to write output JSON")
    parser.add_argument("--train-data", help="Optional train data with existing evidence for few-shot retrieval")
    parser.add_argument("--openrouter-key", default=os.environ.get("OPENROUTER_API_KEY", ""))
    parser.add_argument("--model", default="deepseek/deepseek-v3.2")
    parser.add_argument("--schema-summary", action="store_true", help="Enable schema summarization stage")
    parser.add_argument("--use-embeddings", action="store_true", help="Use sentence embeddings for few-shot retrieval")
    parser.add_argument("--embedding-model", default="sentence-transformers/all-mpnet-base-v2", help="Embedding model name")
    parser.add_argument("--top-k", type=int, default=3, help="Number of few-shot primary DB samples")
    parser.add_argument("--top-n-same-db", type=int, default=5, help="Questions per selected few-shot DB")
    parser.add_argument("--max-samples-per-column", type=int, default=10, help="Sample values for each schema-value pair")
    parser.add_argument("--max-workers", type=int, default=4, help="Number of questions to process in parallel")
    parser.add_argument("--max-requests-per-minute", type=float, default=60.0, help="Global OpenRouter request cap across all workers")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries for retryable OpenRouter failures")
    parser.add_argument("--retry-backoff-seconds", type=float, default=2.0, help="Base backoff for retryable OpenRouter failures")
    return parser.parse_args()


def process_dataset_item(
    idx: int,
    total: int,
    item: dict[str, Any],
    config: SeedConfig,
    api_key: str,
    train_data: list[dict[str, Any]],
    finder: SimilarQuestionFinder | None,
    rate_limiter: RateLimiter,
) -> tuple[int, dict[str, Any]]:
    row = dict(item)
    try:
        row["evidence"] = generate_evidence_for_item(
            item=row,
            config=config,
            api_key=api_key,
            train_data=train_data,
            finder=finder,
            rate_limiter=rate_limiter,
        )
    except Exception as exc:
        row["evidence"] = ""
        row["evidence_error"] = str(exc)
        print(str(exc))

    print(f"[{idx}/{total}] db_id={row.get('db_id')} evidence_len={len(row.get('evidence', ''))}\nevidence : {row["evidence"]}\n")
    
    return idx, row


def main() -> None:
    args = parse_args()
    if not args.openrouter_key:
        raise ValueError("Missing OpenRouter key. Pass --openrouter-key or set OPENROUTER_API_KEY.")

    dataset = truncate_dataset(read_dataset(Path(args.dataset)))
    #train_data, to build few shot examples
    train_data = read_dataset(Path(args.train_data)) if args.train_data else []

    pg_config_path = Path(args.config) if args.config else Path(get_default_config_path())
    if not pg_config_path.exists():
        raise FileNotFoundError(f"Config file not found: {pg_config_path}")

    config = SeedConfig(
        output_path=Path(args.output),
        pg_config_path=pg_config_path,
        use_database=args.use_database,
        use_schema=args.use_schema,
        top_k=args.top_k,
        top_n_same_db=args.top_n_same_db,
        max_samples_per_column=args.max_samples_per_column,
        schema_summary=args.schema_summary,
        llm_model=args.model,
        use_embeddings=args.use_embeddings,
        embedding_model=args.embedding_model,
        max_workers=args.max_workers,
        max_requests_per_minute=args.max_requests_per_minute,
        max_retries=args.max_retries,
        retry_backoff_seconds=args.retry_backoff_seconds,
    )

    finder: SimilarQuestionFinder | None = None
    if config.use_embeddings and train_data:
        try:
            print(f"[SEED] Loading embedding model: {config.embedding_model}")
            finder = SimilarQuestionFinder(train_data, config.embedding_model)
            print("[SEED] Embedding retrieval enabled")
        except Exception as exc:
            warnings.warn(
                f"Embedding retrieval unavailable ({exc}). Falling back to lexical similarity."
            )

    rate_limiter = RateLimiter(config.max_requests_per_minute)
    output_rows: list[dict[str, Any]] = [None] * len(dataset)  # type: ignore[list-item]
    with ThreadPoolExecutor(max_workers=max(1, config.max_workers)) as executor:
        futures = {
            executor.submit(
                process_dataset_item,
                idx,
                len(dataset),
                item,
                config,
                args.openrouter_key,
                train_data,
                finder,
                rate_limiter,
            ): idx
            for idx, item in enumerate(dataset, start=1)
        }
        for future in as_completed(futures):
            idx, row = future.result()
            output_rows[idx - 1] = row

    with config.output_path.open("w", encoding="utf-8") as f:
        json.dump(output_rows, f, ensure_ascii=False, indent=2)
    print(f"Wrote: {config.output_path}")


if __name__ == "__main__":
    main()
