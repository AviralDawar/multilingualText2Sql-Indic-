"""
Step 2: Question-Guided SQL-NLQ Synthesis

This module synthesizes SQL-NLQ pairs from domain-specific questions through:
1. Schema Linking - selecting relevant tables/columns for each question (using MAC-SQL Selector approach)
2. SQL Skeleton Generation - generating abstract SQL templates
3. SQL Generation - filling in skeletons with actual schema
4. NLQ Synthesis - generating natural language from SQL

Unlike random SQL generation, this approach ensures domain relevance and intent consistency.
"""

import json
import re
import uuid
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

from .config import (
    DSQGConfig, SchemaInfo, TableInfo, GeneratedQuestion,
    SQLSkeleton, NLQSQLPair, QuestionType
)
from .schema_selector import SchemaSelector


class SQLNLQSynthesizer:
    """
    Synthesizes SQL-NLQ pairs guided by domain-specific questions.

    Key insight from paper: Don't generate SQL directly from questions (limited by
    Text-to-SQL accuracy). Instead:
    1. Use schema linking to find relevant tables/columns
    2. Generate SQL skeletons (abstract templates)
    3. Fill skeletons with schema to generate SQL
    4. Generate NLQ from SQL to ensure intent consistency
    """

    def __init__(
        self,
        config: DSQGConfig,
        llm_client,
        data_path: str = None,
        sql_llm_client=None,
        nlq_llm_client=None
    ):
        if llm_client is None:
            raise ValueError("LLM client is required for SQL-NLQ synthesis")
        self.config = config
        self.llm_client = llm_client
        self.sql_llm_client = sql_llm_client or llm_client
        self.nlq_llm_client = nlq_llm_client or llm_client
        self.data_path = data_path  # Path to CSV data files for sample values

        # Initialize the Schema Selector (MAC-SQL approach)
        self.schema_selector = SchemaSelector(llm_client, data_path)

    def _llm_generate(self, prompt: str, temperature: float) -> str:
        """Generate with temperature; fallback for legacy clients without temperature arg."""
        try:
            return self.llm_client.generate(prompt, temperature=temperature)
        except TypeError:
            return self.llm_client.generate(prompt)

    def _llm_generate_with_client(self, client, prompt: str, temperature: float) -> str:
        """Generate with an explicit client; fallback for legacy clients."""
        try:
            return client.generate(prompt, temperature=temperature)
        except TypeError:
            return client.generate(prompt)

    def _get_difficulty_instructions_for_skeletons(self, difficulty: str) -> str:
        """Difficulty guidance for skeleton generation (primary complexity control)."""
        difficulty = (difficulty or 'medium').lower()
        instructions = {
            'easy': (
                "Difficulty target: easy.\n"
                "- Keep skeletons simple and short.\n"
                "- Prefer single-table queries or at most one JOIN.\n"
                "- Prefer straightforward WHERE filters.\n"
                "- Avoid nested subqueries, CTEs, and window functions."
            ),
            'medium': (
                "Difficulty target: medium.\n"
                "- Create moderate-complexity skeletons.\n"
                "- Prefer one JOIN where relevant.\n"
                "- Allow GROUP BY / ORDER BY / HAVING (one of them) when useful.\n"
                "- No nesting."
            ),
            'hard': (
                "Difficulty target: hard.\n"
                "- Create complex skeletons.\n"
                "- Prefer two JOINs where relevant.\n"
                "- Across the hard skeleton batch, diversify JOIN types when semantically valid "
                "(e.g., INNER JOIN, LEFT JOIN; use RIGHT/FULL only if truly needed).\n"
                "- Include aggregation patterns with GROUP BY and at least one of COUNT/SUM/AVG/MIN/MAX "
                "when the question intent supports aggregation.\n"
                "- Include advanced patterns such as subqueries/CTEs and HAVING when appropriate.\n"
                "- Keep templates realistic and executable after filling."
            ),
        }
        return instructions.get(difficulty, instructions['medium'])

    def _get_difficulty_instructions_for_sql(self, difficulty: str) -> str:
        """Difficulty guidance for SQL filling (secondary control under skeleton constraints)."""
        difficulty = (difficulty or 'medium').lower()
        instructions = {
            'easy': (
                "Difficulty target: easy.\n"
                "Approximate token length per SQL: < 60.\n"
                "Keep SQL concise while following the provided template."
            ),
            'medium': (
                "Difficulty target: medium.\n"
                "Approximate token length per SQL: 60 to 120.\n"
                "Keep SQL moderately detailed while following the provided template."
            ),
            'hard': (
                "Difficulty target: hard.\n"
                "Approximate token length per SQL: > 120.\n"
                "Use richer SQL construction while still strictly following the template and schema.\n"
                "When semantically appropriate, prefer diverse JOIN types (INNER/LEFT) and "
                "aggregation functions (COUNT/SUM/AVG/MIN/MAX) with GROUP BY/HAVING.\n"
                "Use CTEs/subqueries where they genuinely improve correctness and clarity."
            ),
        }
        return instructions.get(difficulty, instructions['medium'])

    def _get_difficulty_plan(self, num_skeletons: int) -> List[Tuple[str, int]]:
        """
        Compute how many skeletons to generate per difficulty.

        Returns a list of (difficulty, count) with non-zero counts.
        """
        mode = getattr(self.config, 'sql_difficulty', 'mixed').lower()
        if mode in ('easy', 'medium', 'hard'):
            return [(mode, num_skeletons)]

        mix = getattr(self.config, 'sql_difficulty_mix', None) or {
            'easy': 0.4,
            'medium': 0.4,
            'hard': 0.2
        }
        ratios = {
            'easy': max(float(mix.get('easy', 0.0)), 0.0),
            'medium': max(float(mix.get('medium', 0.0)), 0.0),
            'hard': max(float(mix.get('hard', 0.0)), 0.0),
        }

        total = ratios['easy'] + ratios['medium'] + ratios['hard']
        if total <= 0:
            ratios = {'easy': 0.5, 'medium': 0.3, 'hard': 0.2}
            total = 1.0

        # Largest remainder apportionment for exact total count.
        raw = {k: num_skeletons * (v / total) for k, v in ratios.items()}
        counts = {k: int(raw[k]) for k in raw}
        assigned = sum(counts.values())
        remainders = sorted(
            ((raw[k] - counts[k], k) for k in counts),
            reverse=True
        )
        idx = 0
        while assigned < num_skeletons:
            _, key = remainders[idx % len(remainders)]
            counts[key] += 1
            assigned += 1
            idx += 1

        return [(k, counts[k]) for k in ('easy', 'medium', 'hard') if counts[k] > 0]

    def schema_linking(
        self,
        question: GeneratedQuestion,
        schema: SchemaInfo,
        evidence: str = ""
    ) -> Dict[str, List[str]]:
        """
        Schema Linking: Select relevant tables and columns for a question.

        Uses the MAC-SQL Selector approach:
        - If schema is small (≤30 cols, avg ≤6 per table): use full schema
        - Otherwise: use LLM to select minimal relevant schema with sample values
        """
        return self.schema_selector.select_schema(
            question=question,
            schema=schema,
            data_path=self.data_path,
            evidence=evidence,
            temperature=self.config.schema_selector_temperature
        )

    def generate_sql_skeletons(
        self,
        question: GeneratedQuestion,
        linked_schema: Dict[str, List[str]],
        num_skeletons: int = 5
    ) -> List[SQLSkeleton]:
        """
        Generate SQL skeletons (abstract SQL templates) for a question.

        SQL skeletons use placeholders like col_1, table_1, value_1
        instead of actual schema names.

        Paper finding: Skeleton generation accuracy > direct SQL generation accuracy
        """
        difficulty_plan = self._get_difficulty_plan(num_skeletons)
        skeletons: List[SQLSkeleton] = []

        for difficulty, count in difficulty_plan:
            prompt = self._build_skeleton_generation_prompt(
                question, linked_schema, count, difficulty
            )
            response = self._llm_generate(
                prompt,
                self.config.skeleton_generation_temperature
            )
            batch = self._parse_skeleton_response(response)
            for skel in batch:
                if len(skeletons) >= num_skeletons:
                    break
                skel.difficulty = difficulty
                skel.skeleton_id = f"skel_{len(skeletons)}"
                skeletons.append(skel)

        return skeletons

    def _build_skeleton_generation_prompt(
        self,
        question: GeneratedQuestion,
        linked_schema: Dict[str, List[str]],
        num_skeletons: int,
        difficulty: str
    ) -> str:
        """Build prompt for SQL skeleton generation."""
        schema_str = json.dumps(linked_schema, indent=2)
        difficulty_instructions = self._get_difficulty_instructions_for_skeletons(difficulty)

        return f"""Please generate {num_skeletons} SQL templates based on the given question and schema. Ensure that a mix of SQL
clauses are included, such as SELECT, FROM, JOIN, WHERE, GROUP BY, ORDER BY, and HAVING.
Use placeholders for specific table and column names as follows:

1. Use col_# for column names.
2. Use table_# for table names.
3. Use value_# for constant values.

{difficulty_instructions}

Example:
Input:
{{"question": "Show me the redshift of spectroscopic object with subclass \\
of STARFORMING"}}
Schema:
CREATE TABLE specobj (
2983
specobjid number Example Values[(Decimal('299489952322840576'),), ...],
subclass text Example Values[(None,), ('BROADLINE',), ('STARFORMING',)],
z number Example Values[(7.01124,), (0.00415325,),
(0.00415325,)],
. . . . . .
primary key (specobjid)
)
Output:
{{
  "templates": [
    {{"template": "SELECT col_0 FROM table_1 WHERE col_0 = value_0"}},
    {{"template": "SELECT col_0 FROM table_1 WHERE col_1 > value_0"}},
    ...
  ]
}}
The "templates" list must contain exactly {num_skeletons} items.

Now, apply the same transformation to the question below. Do not let specific table names,
column names, or constant values (like "description", "name", "GALAXY", or "BROADLINE")
appear in the template.

Input:
{{"question": "{question.question_text}"}}
Schema:
{schema_str}
Output in JSON format:
{{
  "templates": [
    {{"template": "..."}},
    ...
  ]
}}
The "templates" list must contain exactly {num_skeletons} items.
"""

    def _extract_sample_values(
        self,
        linked_schema: Dict[str, List[str]],
        schema: SchemaInfo
    ) -> Dict[str, Dict[str, List]]:
        """
        Extract sample values for columns in linked schema.

        Returns:
            Dict[table_name, Dict[column_name, List[sample_values]]]
        """
        sample_values_dict = {}

        for table_name, columns in linked_schema.items():
            table_info = schema.tables.get(table_name)
            if not table_info:
                continue

            table_samples = {}
            for col_name in columns:
                # Get sample values from TableInfo.sample_values
                if col_name in table_info.sample_values:
                    table_samples[col_name] = table_info.sample_values[col_name]

            if table_samples:
                sample_values_dict[table_name] = table_samples

        return sample_values_dict

    def generate_sql_from_skeleton(
        self,
        question: GeneratedQuestion,
        skeleton: SQLSkeleton,
        linked_schema: Dict[str, List[str]],
        schema: SchemaInfo,
        num_sqls: int = 3
    ) -> List[str]:
        """
        Generate concrete SQL queries by filling skeleton with actual schema.

        For each skeleton, generate multiple SQL variants using different
        columns/values from the linked schema.
        """
        # Extract sample values for columns in linked schema
        sample_values_dict = self._extract_sample_values(linked_schema, schema)

        prompt = self._build_sql_generation_prompt(
            question, skeleton, linked_schema, schema, num_sqls, sample_values_dict
        )
        response = self._llm_generate_with_client(
            self.sql_llm_client,
            prompt,
            self.config.sql_generation_temperature
        )
        sqls = self._parse_sql_response(response)
        sqls = self._filter_type_unsafe_sql(sqls, linked_schema, schema)
        return sqls

    def _is_numeric_sql_type(self, sql_type: str) -> bool:
        """Return True if type string is numeric-like."""
        t = (sql_type or "").upper()
        numeric_tokens = (
            "INT", "INTEGER", "BIGINT", "SMALLINT",
            "DOUBLE", "FLOAT", "REAL", "NUMERIC", "DECIMAL"
        )
        return any(tok in t for tok in numeric_tokens)

    def _is_temporal_sql_type(self, sql_type: str) -> bool:
        """Return True if type string is date/time-like."""
        t = (sql_type or "").upper()
        temporal_tokens = ("DATE", "TIME", "TIMESTAMP")
        return any(tok in t for tok in temporal_tokens)

    def _build_linked_column_type_map(
        self,
        linked_schema: Dict[str, List[str]],
        schema: SchemaInfo
    ) -> Dict[str, str]:
        """Build map TABLE.COLUMN -> SQL type for linked schema columns."""
        col_type_map: Dict[str, str] = {}
        for table_name, columns in linked_schema.items():
            table_info = schema.tables.get(table_name)
            if not table_info:
                continue
            for col in table_info.columns:
                col_name = col.get("name")
                if not col_name or (columns and col_name not in columns):
                    continue
                key = f"{table_name.upper()}.{col_name.upper()}"
                col_type_map[key] = col.get("type", "TEXT")
        return col_type_map

    def _filter_type_unsafe_sql(
        self,
        sqls: List[str],
        linked_schema: Dict[str, List[str]],
        schema: SchemaInfo
    ) -> List[str]:
        """
        Drop SQL queries that apply numeric-only operations to text columns.

        Current checks:
        - AVG/SUM applied to non-numeric columns
        - Numeric comparisons (>, >=, <, <=) between column and numeric literal
          where column is not numeric/date-like.
        """
        if not sqls:
            return []

        col_type_map = self._build_linked_column_type_map(linked_schema, schema)
        filtered = []
        dropped = 0

        agg_re = re.compile(
            r"\b(AVG|SUM)\s*\(\s*(?:DISTINCT\s+)?([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\)",
            flags=re.IGNORECASE
        )
        comp_left_re = re.compile(
            r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|>|<)\s*[-+]?\d+(?:\.\d+)?",
            flags=re.IGNORECASE
        )
        comp_right_re = re.compile(
            r"[-+]?\d+(?:\.\d+)?\s*(>=|<=|>|<)\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)",
            flags=re.IGNORECASE
        )

        for sql in sqls:
            valid = True

            # Build alias -> table map from FROM/JOIN clauses
            alias_to_table: Dict[str, str] = {}
            for m in re.finditer(
                r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)",
                sql,
                flags=re.IGNORECASE
            ):
                table_name = m.group(1).upper()
                alias = m.group(2).upper()
                alias_to_table[alias] = table_name
                alias_to_table[table_name] = table_name

            def resolve_type(alias: str, col: str) -> str:
                table_name = alias_to_table.get(alias.upper(), alias.upper())
                return col_type_map.get(f"{table_name}.{col.upper()}", "")

            # Check AVG/SUM target types
            for _, alias, col in agg_re.findall(sql):
                col_type = resolve_type(alias, col)
                if not self._is_numeric_sql_type(col_type):
                    valid = False
                    break

            if not valid:
                dropped += 1
                continue

            # Check numeric comparisons against literals
            for alias, col, _ in comp_left_re.findall(sql):
                col_type = resolve_type(alias, col)
                if not (self._is_numeric_sql_type(col_type) or self._is_temporal_sql_type(col_type)):
                    valid = False
                    break

            if valid:
                for _, alias, col in comp_right_re.findall(sql):
                    col_type = resolve_type(alias, col)
                    if not (self._is_numeric_sql_type(col_type) or self._is_temporal_sql_type(col_type)):
                        valid = False
                        break

            if valid:
                filtered.append(sql)
            else:
                dropped += 1

        if dropped:
            print(f"        [SQL Gen] Dropped {dropped} type-unsafe SQL candidate(s)")

        return filtered

    def _build_sql_generation_prompt(
        self,
        question: GeneratedQuestion,
        skeleton: SQLSkeleton,
        linked_schema: Dict[str, List[str]],
        schema: SchemaInfo,
        num_sqls: int,
        sample_values_dict: Dict[str, Dict[str, List]] = None
    ) -> str:
        """Build prompt for SQL generation from skeleton."""
        schema_str = self._format_linked_schema_with_types(linked_schema, schema)
        difficulty_instructions = self._get_difficulty_instructions_for_sql(skeleton.difficulty)

        # Build allowed FK join paths for the linked sub-schema.
        linked_table_set = set(linked_schema.keys())
        fk_lines = []
        for fk in schema.foreign_keys:
            from_table = fk.get('from_table', '')
            from_col = fk.get('from_column', '')
            to_table = fk.get('to_table', '')
            to_col = fk.get('to_column', '')
            if (
                from_table in linked_table_set
                and to_table in linked_table_set
                and from_col and to_col
            ):
                fk_lines.append(f"- {from_table}.{from_col} = {to_table}.{to_col}")

        fk_lines = sorted(set(fk_lines))
        fk_constraints_str = "\n".join(fk_lines) if fk_lines else "- None provided"

        # Build explicit type hints to prevent text-vs-numeric operator mistakes.
        col_type_map = self._build_linked_column_type_map(linked_schema, schema)
        numeric_cols = sorted(
            [k for k, v in col_type_map.items() if self._is_numeric_sql_type(v)]
        )
        non_numeric_cols = sorted(
            [k for k, v in col_type_map.items() if not self._is_numeric_sql_type(v)]
        )
        numeric_cols_str = "\n".join(f"- {c}" for c in numeric_cols) if numeric_cols else "- None"
        non_numeric_cols_str = "\n".join(f"- {c}" for c in non_numeric_cols[:80]) if non_numeric_cols else "- None"

        # Format sample values for the prompt
        sample_values_str = ""
        if sample_values_dict:
            sample_values_str = "\n\n4. Sample values from the database columns (use ONLY these values in WHERE/HAVING clauses).\n\nSample Values Available:\n"
            for table_name, columns in sample_values_dict.items():
                sample_values_str += f"\nTable: {table_name}\n"
                for col_name, values in columns.items():
                    # Format values appropriately (strings in quotes, numbers as-is)
                    formatted_values = []
                    for v in values[:6]:  # Limit to 6 samples
                        if isinstance(v, str):
                            formatted_values.append(f"'{v}'")
                        elif v is None:
                            formatted_values.append('NULL')
                        else:
                            formatted_values.append(str(v))
                    sample_values_str += f"  - {col_name}: [{', '.join(formatted_values)}]\n"

        return f"""You are an expert in a specific domain and a PostgreSQL SQL expert. You are provided with:
1. An SQL query template.
2. A question that the query needs to answer.
3. The schema of the relevant database.{sample_values_str}

Your task is to:
1. Strictly use the information from the provided schema to complete PostgreSQL queries.
   Ensure that all necessary table names, column names, and clauses (such as FROM and JOIN)
   come from the schema only.
2. **CRITICAL JOIN RULE**: Use ONLY foreign-key-valid join predicates. A JOIN condition must
   exactly match one of the allowed FK relationships listed below (direction can be reversed).
   Do NOT join semantically unrelated IDs (e.g., STATE_ID = STATION_ID) just because data types match.
3. **CRITICAL**: When using literal values in WHERE, HAVING, IN, or other filter clauses,
   you MUST use ONLY the sample values provided above. Do NOT make up or hallucinate values.
   This ensures the generated queries will return actual results when executed against the database.
4. **CRITICAL TYPE RULE**: Use numeric operators/aggregates only on numeric columns.
   - AVG/SUM require numeric columns.
   - Numeric comparisons (>, >=, <, <=) require numeric/date columns.
   - For text columns, use equality/inequality, IN, LIKE, IS NULL, COUNT, GROUP BY.
   - Do not cast text columns to numeric unless values are guaranteed numeric in schema context.
5. Avoid introducing any table names, column names, or other elements that are not explicitly
   defined in the schema.
6. Generate {num_sqls} PostgreSQL SQL queries that are directly related to the given question
   and fit the SQL query template.
7. Use PostgreSQL-compatible syntax only.
8. Keep the output in JSON format.

{difficulty_instructions}

Allowed FK relationships for JOINs:
{fk_constraints_str}

Numeric columns (safe for AVG/SUM and numeric comparisons):
{numeric_cols_str}

Non-numeric columns (do NOT use AVG/SUM or numeric comparisons):
{non_numeric_cols_str}

Example:
Input:
SQL Query Template:
SELECT col_1, col_2 FROM table_1 JOIN table_0 WHERE col_3 = value_0;
Question:
What are the names and descriptions of the different types of photos associated
with objects in the astrophysical classifications from the specobj table?
Database Schema:
CREATE TABLE photo_type (
    value number,
    name text,
    description text,
    primary key (value)
);

CREATE TABLE specobj (
    specobjid number,
    bestobjid number,
    survey text,
    class text,
    subclass text,
    primary key (specobjid),
    foreign key (bestobjid) references photoobj(objid)
);

Sample Values Available:
Table: specobj
  - class: ['GALAXY', 'STAR', 'QSO']
  - subclass: ['BROADLINE', 'STARFORMING', 'STARBURST']
  - survey: ['boss', 'sdss', 'eboss']

Output:
{{
  "queries": [
    "SELECT p.name, p.description FROM photo_type p JOIN specobj s ON p.value = s.bestobjid WHERE s.class = 'STAR';",
    "SELECT p.name, p.description FROM photo_type p JOIN specobj s ON p.value = s.bestobjid WHERE s.subclass = 'BROADLINE';",
    "SELECT p.name, p.description FROM photo_type p JOIN specobj s ON p.value = s.bestobjid WHERE s.class = 'GALAXY';"
  ]
}}
Note: The WHERE clause values ('STAR', 'BROADLINE', 'GALAXY') are taken from the Sample Values provided.

Now, it's your turn.
Input:
SQL Query Template:
{skeleton.template}
Question:
{question.question_text}
Database Schema:
{schema_str}
Output in JSON format:
{{
  "queries": [
    "..."
  ]
}}
"""

    def synthesize_nlq_from_sql(
        self,
        sql: str,
        schema: SchemaInfo
    ) -> str:
        """
        SQL to NLQ synthesis: Generate natural language question from SQL.

        This ensures intent consistency between NLQ and SQL.
        """
        prompt = self._build_nlq_synthesis_prompt(sql, schema)
        response = self._llm_generate_with_client(
            self.nlq_llm_client,
            prompt,
            self.config.nlq_synthesis_temperature
        )
        return self._parse_nlq_response(response)

    def _build_nlq_synthesis_prompt(self, sql: str, schema: SchemaInfo) -> str:
        """Build prompt for NLQ synthesis from SQL."""
        # Previous prompt retained for reference:
        # return f"""Generate a natural language question that corresponds to this SQL query.
        #
        # SQL Query:
        # {sql}
        #
        # Database Domain: {schema.domain}
        #
        # Generate a clear, natural question that a user might ask to get this query result.
        # The question should:
        # 1. Be domain-relevant and use natural terminology
        # 2. Clearly express the query intent
        # 3. Be grammatically correct
        #
        # Example 1:
        # Input:
        # "sql": "SELECT count(*) FROM biomarker_fda_test_trial
        # JOIN biomarker_fda_test
        # ON
        # biomarker_fda_test_trial.test_submission = biomarker_fda_test.test_submission
        # AND
        # biomarker_fda_test_trial.test_trade_name = biomarker_fda_test.test_trade_name
        # WHERE biomarker_fda_test.test_manufacturer = '23andMe'",
        # Output:
        # {{
        #   "question": "Show number of test trials of 23andMe"
        # }}
        #
        # Example 2:
        # Input:
        # "sql": "SELECT DISTINCT T3.name, T2.name
        # FROM healthy_expression AS T1
        # JOIN anatomical_entity AS T3 ON T1.uberon_anatomical_id = T3.id
        # JOIN cancer_tissue AS T4 ON T3.id = T4.uberon_anatomical_id
        # JOIN disease AS T2 ON T4.doid = T2.id;",
        # Output:
        # {{
        #   "question": "which diseases have related anatomical entities?"
        # }}
        #
        # Now, it's your turn. Just return 1 user query in JSON format.
        #
        #
                # Output in JSON format:
        # {{
        #     "question": "<your natural language question>"
        # }}
        # """
        return f"""You are an expert Data Scientist specializing in Text-to-SQL dataset curation. Your goal is to transform a SQL query into a high-fidelity Natural Language Question (NLQ).

### NATURALNESS GUIDELINES:
1. **Selection Conciseness:** You may not list every single column from the `SELECT` clause if a collective term (e.g., "details," "information," "profile") is more natural.
2. **Implicit Filters:** Integrate filter criteria naturally as adjectives or qualifiers (e.g., "rural schools") rather than literal mappings (e.g., "schools where the location is 'Rural'").
3. **Intent-based CTEs:** For queries using CTEs or complex subqueries, describe the *functional intent* (e.g., "For the most recently recorded data...") rather than the *execution logic* (e.g., "Find the maximum year and then...").
4. **Varied Phrasing:** Use a mix of questions, commands ("List all..."), and requests ("Show the...") to maintain variety.
5. **No Logic Leakage:** Ensure the question does not explicitly "leak" the internal SQL structure (like JOIN conditions or specific table aliases). Use domain terminology.

### EXAMPLES:

#### Example 1 (Easy: Single Table, Simple Filter)
Input SQL: "SELECT STATION_NAME, TYPE_OF_WATER_BODY FROM DIM_STATION WHERE STATE_ID = 'ST_001' AND TYPE_OF_WATER_BODY = 'LAKE'"
Output JSON: {{ "question": "What are the names and water body types of all stations located near lakes in the first state?" }}

#### Example 2 (Medium: Join, Aggregation, Group By)
Input SQL: "SELECT T1.STATE_NAME, AVG(T3.MAX_TEMPERATURE_C) FROM DIM_STATE AS T1 JOIN DIM_STATION AS T2 ON T1.STATE_ID = T2.STATE_ID JOIN FACT_THERMAL AS T3 ON T2.STATION_ID = T3.STATION_ID GROUP BY T1.STATE_NAME"
Output JSON: {{ "question": "Show the average maximum temperature for each state based on available thermal station data." }}

#### Example 3 (Hard: CTE, Multiple Joins, Specific Filter)
Input SQL: "WITH top_districts AS (SELECT district_id FROM fact_census WHERE population > 1000000) SELECT d.district_name, s.school_name, s.total_students FROM top_districts td JOIN dim_district d ON td.district_id = d.district_id JOIN dim_school s ON d.district_id = s.district_id WHERE s.school_type = 'Secondary'"
Output JSON: {{ "question": "For districts with a population over one million, list the names of secondary schools along with their total student counts." }}

### TASK:
Input SQL: "{sql}"
Output JSON:
{{
  "question": "<your natural language question>"
}}"""

    def synthesize_pairs(
        self,
        questions: List[GeneratedQuestion],
        schema: SchemaInfo
    ) -> List[NLQSQLPair]:
        """
        Main entry point: Synthesize NLQ-SQL pairs from domain questions.

        For each question:
        1. Perform schema linking
        2. Generate SQL skeletons
        3. Generate SQL from skeletons
        4. Synthesize NLQ from SQL
        """
        all_pairs = []
        total_questions = len(questions)
        skeletons_per_q = self.config.skeletons_per_question
        sqls_per_skel = self.config.sqls_per_skeleton

        print(f"  Processing {total_questions} questions ({skeletons_per_q} skeletons × {sqls_per_skel} SQLs each)...")

        for i, question in enumerate(questions):
            q_type = question.question_type.value
            print(f"    [{i+1}/{total_questions}] {q_type}: schema linking...", end=" ", flush=True)

            # Step 1: Schema linking
            linked_schema = self.schema_linking(question, schema)

            if not linked_schema:
                print("✗ (no schema linked)")
                continue

            print("skeletons...", end=" ", flush=True)

            # Step 2: Generate SQL skeletons
            print("[start]", end=" ", flush=True)
            skeletons = self.generate_sql_skeletons(
                question,
                linked_schema,
                self.config.skeletons_per_question
            )
            print(f"[done: {len(skeletons)}]", end=" ", flush=True)

            pairs_for_question = 0
            total_skeletons = len(skeletons)
            for sk_idx, skeleton in enumerate(skeletons, 1):
                # Step 3: Generate SQL from skeleton
                print(
                    f"\n        [SQL Gen] skeleton {sk_idx}/{total_skeletons} ({skeleton.skeleton_id}, difficulty={skeleton.difficulty}) start...",
                    end=" ",
                    flush=True
                )
                sqls = self.generate_sql_from_skeleton(
                    question,
                    skeleton,
                    linked_schema,
                    schema,
                    self.config.sqls_per_skeleton
                )
                print(f"done ({len(sqls)} SQLs)", flush=True)

                total_sqls = len(sqls)
                for sql_idx, sql in enumerate(sqls, 1):
                    # Step 4: Synthesize NLQ from SQL
                    print(
                        f"        [NLQ Gen] skeleton {sk_idx}/{total_skeletons}, sql {sql_idx}/{total_sqls} start...",
                        end=" ",
                        flush=True
                    )
                    nlq = self.synthesize_nlq_from_sql(sql, schema)
                    print("done", flush=True)

                    if nlq and sql:
                        pair = NLQSQLPair(
                            pair_id=str(uuid.uuid4())[:8],
                            original_question_id=question.question_id,
                            nlq=nlq,
                            sql=sql,
                            schema_used=list(linked_schema.keys()),
                            linked_schema=linked_schema,
                            skeleton_id=skeleton.skeleton_id,
                            difficulty=skeleton.difficulty
                        )
                        all_pairs.append(pair)
                        pairs_for_question += 1
            print(f"✓ ({pairs_for_question} pairs)")

        print(f"  Total raw pairs synthesized: {len(all_pairs)}")
        return all_pairs

    def _format_schema_for_linking(self, schema: SchemaInfo) -> str:
        """Format full schema for linking prompt."""
        lines = []
        for table_name, table_info in schema.tables.items():
            lines.append(f"Table: {table_name}")
            for col in table_info.columns:
                lines.append(f"  - {col['name']}: {col.get('type', 'unknown')}")
        return '\n'.join(lines)

    def _format_linked_schema_with_types(
        self,
        linked_schema: Dict[str, List[str]],
        schema: SchemaInfo
    ) -> str:
        """Format linked schema with column types."""
        lines = []
        for table_name, columns in linked_schema.items():
            if table_name in schema.tables:
                lines.append(f"Table: {table_name}")
                table_info = schema.tables[table_name]
                for col in table_info.columns:
                    if col['name'] in columns:
                        lines.append(f"  - {col['name']}: {col.get('type', 'unknown')}")
        return '\n'.join(lines)

    def _extract_json_from_response(self, response) -> str:
        """Extract JSON from LLM response, handling markdown code blocks."""
        if response is None:
            return ""
        if not isinstance(response, str):
            try:
                response = json.dumps(response)
            except Exception:
                response = str(response)

        response = response.strip()
        if not response:
            return ""

        # Handle markdown code blocks
        if response.startswith('```'):
            lines = response.split('\n')
            if lines[0].startswith('```'):
                lines = lines[1:]
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            response = '\n'.join(lines)

        return response.strip()

    def _parse_schema_linking_response(self, response: str) -> Dict[str, List[str]]:
        """Parse LLM schema linking response."""
        try:
            clean_response = self._extract_json_from_response(response)
            data = json.loads(clean_response)
            return data.get('tables', {})
        except json.JSONDecodeError:
            return {}

    def _parse_skeleton_response(self, response: str) -> List[SQLSkeleton]:
        """Parse LLM skeleton generation response."""
        clean_response = self._extract_json_from_response(response)
        templates: List[str] = []

        try:
            data = json.loads(clean_response)

            # Primary expected formats (current prompt examples):
            # {"template": "..."} or [{"template": "..."}]
            if isinstance(data, dict):
                if isinstance(data.get('template'), str):
                    templates.append(data['template'])
                elif isinstance(data.get('templates'), list):
                    for item in data['templates']:
                        if isinstance(item, dict) and isinstance(item.get('template'), str):
                            templates.append(item['template'])
                        elif isinstance(item, str):
                            templates.append(item)
                # Backward compatibility for older wrapper format.
                elif isinstance(data.get('skeletons'), list):
                    for item in data['skeletons']:
                        if isinstance(item, dict) and isinstance(item.get('template'), str):
                            templates.append(item['template'])
                        elif isinstance(item, str):
                            templates.append(item)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and isinstance(item.get('template'), str):
                        templates.append(item['template'])
                    elif isinstance(item, str):
                        templates.append(item)

        except json.JSONDecodeError:
            # Fallback: extract inline template fields from partially valid output.
            templates = re.findall(r'"template"\s*:\s*"([^"]+)"', clean_response)

        skeletons = []
        for i, template in enumerate(templates):
            template = template.strip()
            if not template:
                continue
            skeletons.append(SQLSkeleton(
                skeleton_id=f"skel_{i}",
                template=template,
                operations=[]
            ))

        return skeletons

    def _parse_sql_response(self, response: str) -> List[str]:
        """Parse LLM SQL generation response."""
        try:
            clean_response = self._extract_json_from_response(response)
            if not clean_response:
                print("        [SQL Gen] Warning: Empty/None LLM response while parsing SQL queries")
                return []
            data = json.loads(clean_response)

            if isinstance(data, dict):
                queries = data.get('queries', [])
                if isinstance(queries, list):
                    return [q for q in queries if isinstance(q, str) and q.strip()]
                return []

            if isinstance(data, list):
                # Allow direct list-of-SQL output.
                return [q for q in data if isinstance(q, str) and q.strip()]

            return []
        except json.JSONDecodeError:
            return []

    def _parse_nlq_response(self, response: str) -> str:
        """Parse LLM NLQ synthesis response."""
        try:
            clean_response = self._extract_json_from_response(response)
            data = json.loads(clean_response)
            return data.get('question', '')
        except json.JSONDecodeError:
            return ''
