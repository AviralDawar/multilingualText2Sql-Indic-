import os
import argparse
import json
import requests


KNOWLEDGE_GENERATOR_PROMPT = """
Role: Senior Database Knowledge Curator for Text-to-SQL Support.

Input:
- Database Schema (includes table names, column names, data types, primary keys, foreign keys):
{schema}

- Optional External Domain Knowledge:
{external_knowledge}

Task:
Generate a high-precision, schema-faithful Knowledge File optimized for Text-to-SQL reasoning reliability.

CRITICAL RULES:
1. Use ONLY schema-valid tables, columns, keys, and explicitly inferable relationships.
2. Do NOT invent tables, columns, relationships, distributions, or constraints.
3. Do NOT fabricate statistics, thresholds, or assumptions about data distribution.
4. External knowledge may clarify semantics of existing schema terms ONLY.
   - It must NOT introduce new entities, attributes, or inferred structure.
5. Output STRICT MARKDOWN only.
6. Do NOT include SQL snippets, pseudo-SQL, or query examples.
7. Keep content compact, structured, and operational — avoid narrative explanations.
8. If something cannot be inferred strictly from schema, omit it.

==================================================

# DATABASE PURPOSE
- 2–4 concise bullets.
- Focus strictly on what entities and relationships are captured.
- No speculation about business intent beyond schema semantics.

==================================================

# TABLE GRANULARITY
For each table:
- State the row-level grain (e.g., one row per X).
- Identify primary key (including composite keys).
- Clarify whether table is:
  - Entity table
  - Transaction/event table
  - Bridge/junction table
  - Snapshot table
- Do NOT assume fact/dimension roles unless structurally evident.

==================================================

# RELATIONSHIP STRUCTURE
- Explicitly list foreign key relationships.
- Describe join direction using real column names.
- Clarify cardinality if inferable (1:1, 1:N, N:M).
- Identify bridge tables explicitly.
- Do NOT infer undocumented relationships.

==================================================

# COMMON JOIN PATHS
- Describe valid join chains using schema-valid columns.
- Only include paths supported by declared foreign keys.
- Avoid hypothetical joins.
- Keep entries short and structural.

==================================================

# DOMAIN CLASSIFICATIONS & COLUMN SEMANTICS
For meaningful columns only:

- Categorical/code mappings (only if explicitly inferable)
- Boolean/flag interpretation rules
- Date/time role definitions (event date vs snapshot date)
- Numeric semantic groupings ONLY if implied by column meaning
- Transformation logic directly inferable from column names

Rules:
- Do NOT invent thresholds.
- Do NOT assume value distributions.
- Omit if semantics are unclear.

==================================================

# TEMPORAL STRUCTURE
- List all date/time columns.
- Define their semantic role when inferable:
  - Creation timestamp
  - Update timestamp
  - Effective period
  - Event time
- Do NOT assume time zone or granularity unless specified.

==================================================

# NULL & MISSING VALUE RULES
- Identify nullable foreign keys if inferable.
- Identify nullable metric columns.
- Clarify how NULL affects:
  - Set membership
  - Ratio metrics
  - Group inclusion
- Do NOT speculate beyond schema constraints.

==================================================

# IDENTIFIER & UNIQUENESS RULES
- List primary keys.
- Identify composite keys.
- Clarify distinctness implications.
- Avoid assuming uniqueness for non-key columns.

==================================================

# AMBIGUITY & DISAMBIGUATION NOTES
- Identify columns with overlapping names across tables.
- Clarify semantic differences when possible.
- Note potential aggregation ambiguity.
- Omit if no ambiguity exists.

==================================================

# SET DEFINITIONS
Define core entity sets using mathematical-style notation:

Example format:
- Customers = {{c | c.customer_id ∈ Customers.customer_id AND c.customer_id IS NOT NULL}}

Rules:
- Use real identifier columns.
- State null-handling explicitly.
- Only define meaningful analytical sets.

==================================================

# DERIVED METRICS
Provide:
- Metric name
- Formal definition (formula only, no SQL)
- Denominator safety note (e.g., exclude zero or NULL)
- Granularity caveats (distinct vs non-distinct)

Rules:
- Metrics must be constructible from schema columns.
- No assumed thresholds or business KPIs.

==================================================

# GROUP CONSTRUCTION LOGIC
Define comparison groups ONLY if directly supported by schema columns.

- Use explicit column-based criteria.
- Prefer mutually exclusive group definitions.
- Avoid arbitrary segmentation.
- State null-handling rules.

Omit section if no clear grouping attributes exist.
"""


KNOWLEDGE_AUDITOR_PROMPT = """
Role: Senior Knowledge-File Auditor and Schema Fidelity Validator for Text-to-SQL Systems.

Input:
- Database Schema (tables, columns, data types, primary keys, foreign keys):
{schema}

- Optional External Domain Knowledge:
{external_knowledge}

- Knowledge File (Markdown):
{knowledge_md}

Audit Objective:
Validate strict schema fidelity, structural correctness, logical validity, aggregation safety, and SQL-generation reliability.

==================================================
AUDIT CHECKLIST

SECTION A — SCHEMA FIDELITY
1. Flag hallucinated table names.
2. Flag hallucinated column names.
3. Verify every referenced table/column exists.
4. Flag invented foreign-key relationships.
5. Flag joins that are not inferable from declared keys.
6. Flag incorrect cardinality assumptions (1:1 vs 1:N vs N:M).
7. Flag missing bridge tables when N:M relationships are implied.
8. Flag misuse of nested/repeated fields (if applicable).
9. Flag assumptions about uniqueness for non-key columns.

SECTION B — GRANULARITY & AGGREGATION VALIDATION
10. Validate declared table grain (row-level meaning).
11. Flag metrics that mix incompatible table grains.
12. Flag aggregations that ignore necessary grouping keys.
13. Flag implicit DISTINCT assumptions without key justification.
14. Validate that derived metrics align with table grain.

SECTION C — LOGICAL & MATHEMATICAL VALIDATION
15. Validate classification boundaries (non-overlapping unless explicitly allowed).
16. Validate set definitions:
    - coherent predicates
    - valid key usage
    - explicit null handling
17. Validate derived metrics:
    - no undefined variables
    - no circular definitions
    - valid numerator/denominator pairing
18. Ensure ratio metrics include divide-by-zero handling guidance.
19. Validate temporal consistency:
    - no mixing event and snapshot timestamps improperly
    - no undefined time windows
20. Flag contradictory rules across sections.

SECTION D — NULLABILITY & CONSTRAINT CONSISTENCY
21. Validate handling of NULL values in:
    - foreign keys
    - metric columns
    - grouping attributes
22. Flag contradictions between null-handling rules and schema constraints.
23. Flag misuse of sentinel codes if not schema-supported.

SECTION E — CONTENT COVERAGE & STRUCTURAL COMPLETENESS
24. Check whether all major entity tables are represented in sets or structure.
25. Check whether transaction/event tables are reflected in derived metrics.
26. Check whether bridge tables are explicitly handled.
27. Check whether time columns are documented in temporal structure.
28. Check whether categorical code mappings are provided when explicitly inferable.
29. Check whether grouping logic is based on explicit schema columns.
30. Ensure patterns are schema-grounded, not generic best practices.

SECTION F — CLARITY & OPERATIONAL PRECISION
31. Flag vague, non-operational wording.
32. Flag filler or generic analytical advice not grounded in schema.
33. Identify high-impact missing structural details affecting SQL generation.

==================================================
REPORTING REQUIREMENTS

- Use concise bullets.
- For each issue, include:
  - impacted section
  - referenced table/column names
  - why it is problematic for SQL generation
- If no issues for a section, return `NONE`.
- Do NOT rewrite the knowledge file.
- Do NOT suggest SQL queries.
- Be strict: if unsure, flag for review.

Return STRICT MARKDOWN in this exact structure:

# AUDIT REPORT

## SCHEMA FIDELITY ISSUES
- <issue or NONE>

## GRANULARITY OR AGGREGATION RISKS
- <issue or NONE>

## LOGICAL OR MATHEMATICAL ERRORS
- <issue or NONE>

## NULL OR CONSTRAINT INCONSISTENCIES
- <issue or NONE>

## STRUCTURAL COVERAGE GAPS
- <issue or NONE>

## PRACTICAL WEAKNESSES
- <issue or NONE>

## HALLUCINATED ELEMENTS
- <issue or NONE>
"""

KNOWLEDGE_REFINER_PROMPT = """
Role: Senior Knowledge-File Refiner and Semantic Corrector for Text-to-SQL Systems.

Input:
- Database Schema (tables, columns, data types, primary keys, foreign keys):
{schema}

- Optional External Domain Knowledge:
{external_knowledge}

- Original Knowledge File (Markdown):
{knowledge_md}

- Audit Feedback:
{audit_report}

==================================================
MISSION

Return a FULL corrected Knowledge File in STRICT MARKDOWN.

You must:
- Fix every valid issue in the audit report.
- Preserve all correct, schema-faithful knowledge.
- Improve structural precision and SQL-generation reliability.
- Maintain strict schema fidelity and logical consistency.

You must NOT:
- Invent tables, columns, relationships, or constraints.
- Fabricate statistics or numeric thresholds.
- Introduce generic filler text.
- Include audit commentary in the output.
- Include SQL snippets, pseudo-SQL, or query examples.

==================================================
CORRECTION REQUIREMENTS

SECTION A — SCHEMA FIDELITY REPAIR
1. Remove hallucinated tables, columns, relationships.
2. Correct invalid join descriptions.
3. Ensure all relationships are supported by declared foreign keys.
4. Remove unsupported cardinality assumptions.
5. Correct distinctness assumptions for non-key columns.
6. Ensure every referenced column exists in schema.

SECTION B — GRANULARITY & AGGREGATION ALIGNMENT
7. Align all entity sets to proper primary keys.
8. Correct metrics that mix incompatible table grains.
9. Add grouping key clarification where aggregation is implied.
10. Remove invalid DISTINCT assumptions.
11. Ensure derived metrics respect table grain.

SECTION C — LOGICAL & MATHEMATICAL CORRECTION
12. Resolve overlapping or contradictory classifications.
13. Correct invalid formulas and undefined variables.
14. Add denominator safety notes for ratio metrics.
15. Add explicit NULL-handling for:
    - foreign keys
    - metric columns
    - grouping attributes
16. Remove circular or ambiguous definitions.
17. Ensure classification thresholds are schema-inferable only.

SECTION D — TEMPORAL & COHORT CONSISTENCY
18. Align time-window logic to correct date/timestamp columns.
19. Remove mixing of event-time and snapshot-time semantics.
20. Clarify cohort definitions using schema-valid columns only.

SECTION E — STRUCTURAL COMPLETENESS
21. Ensure all major entity tables are represented where analytically relevant.
22. Ensure bridge tables are explicitly handled if applicable.
23. Ensure time columns are reflected in temporal logic if present.
24. Improve categorical/code mappings only when schema-supported.
25. Replace vague statements with operational, column-grounded rules.

SECTION F — STRUCTURE ENFORCEMENT
26. Keep these top-level sections exactly once and in this order:
   - DATABASE PURPOSE
   - TABLE GRANULARITY
   - RELATIONSHIP STRUCTURE
   - DOMAIN CLASSIFICATIONS & FUNCTIONS
   - SET DEFINITIONS
   - DERIVED METRICS
   - GROUP CONSTRUCTION LOGIC
27. Maintain strict markdown formatting.
28. Do not introduce additional top-level sections.
29. Remove empty sections if not applicable.

==================================================
SELF-CHECK BEFORE OUTPUT

- No hallucinated elements remain.
- All relationships map to explicit foreign keys.
- All formulas use schema-valid columns only.
- Ratio metrics include divide-by-zero handling.
- Aggregations respect table grain.
- No contradictory rules remain.
- No SQL snippets are present.
- Output is only the corrected Knowledge File.

Return ONLY the final corrected Knowledge File in STRICT MARKDOWN.

"""

class OpenRouterLLM:
    """OpenRouter API client for various LLM models with reasoning support."""

    def __init__(self, model: str = "deepseek/deepseek-chat", api_key: str = None,
                 temperature: float = 0, max_tokens: int = 12000, reasoning: bool = False):
        self.model = model
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY not found. Set it via --openrouter-key or environment variable.")
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.reasoning = reasoning
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        # Conversation history for multi-turn reasoning
        self.messages = []

    def _make_request(self, messages: list) -> dict:
        """Make API request and return the full message object."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }

        if self.reasoning:
            payload["reasoning"] = {"enabled": True}

        response = requests.post(self.base_url, headers=headers, data=json.dumps(payload))

        if response.status_code != 200:
            raise RuntimeError(f"OpenRouter API error: {response.status_code} - {response.text}")

        return response.json()['choices'][0]['message']

    def invoke(self, prompt: str) -> str:
        """Send a prompt and return response content (stateless call)."""
        messages = [{"role": "user", "content": prompt}]
        response_msg = self._make_request(messages)
        return response_msg.get('content', '')

    def invoke_with_history(self, prompt: str) -> str:
        """Send a prompt while preserving conversation history and reasoning details."""
        # Add user message to history
        self.messages.append({"role": "user", "content": prompt})

        # Make request with full history
        response_msg = self._make_request(self.messages)

        # Preserve assistant response with reasoning_details for next turn
        assistant_msg = {"role": "assistant", "content": response_msg.get('content', '')}
        if self.reasoning and response_msg.get('reasoning_details'):
            assistant_msg['reasoning_details'] = response_msg['reasoning_details']

        self.messages.append(assistant_msg)

        return response_msg.get('content', '')

    def reset_history(self):
        """Clear conversation history."""
        self.messages = []


def read_external_knowledge(schema_path: str) -> str:
    # external_knowledge.md is expected in the same directory as schema_info.md
    schema_dir = os.path.dirname(schema_path)
    external_knowledge_path = os.path.join(schema_dir, "external_knowledge.md")

    if not os.path.exists(external_knowledge_path):
        return "NONE"

    with open(external_knowledge_path, "r", encoding="utf-8") as f:
        content = f.read().strip()

    return content if content else "NONE"


def run_knowledge_pipeline(schema_text: str, external_knowledge: str,
                           api_key:str, output_path: str, model: str, reasoning: bool = False):

    llm = OpenRouterLLM(model=model, api_key=api_key, reasoning=reasoning)

    print("--- Step 1: Generating Knowledge File ---")

    generator_input = KNOWLEDGE_GENERATOR_PROMPT.format(
        schema=schema_text,
        external_knowledge=external_knowledge
    )

    if reasoning:
        knowledge_draft = llm.invoke_with_history(generator_input)
    else:
        knowledge_draft = llm.invoke(generator_input)

    print("--- Step 2: Auditing Knowledge File ---")

    auditor_input = KNOWLEDGE_AUDITOR_PROMPT.format(
        schema=schema_text,
        external_knowledge=external_knowledge,
        knowledge_md=knowledge_draft
    )

    if reasoning:
        audit_report = llm.invoke_with_history(auditor_input)
    else:
        audit_report = llm.invoke(auditor_input)

    print(f"\nAUDIT REPORT:\n{audit_report}\n")

    print("--- Step 3: Refining Knowledge File ---")

    refiner_input = KNOWLEDGE_REFINER_PROMPT.format(
        schema=schema_text,
        external_knowledge=external_knowledge,
        knowledge_md=knowledge_draft,
        audit_report=audit_report
    )

    if reasoning:
        final_knowledge = llm.invoke_with_history(refiner_input)
    else:
        final_knowledge = llm.invoke(refiner_input)

    if not final_knowledge.strip():
        raise RuntimeError("Final output is empty.")


    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_knowledge)

    print(f"Knowledge file saved to: {output_path}")


# Main entry point with argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a Knowledge file from database schema using LLM Judge pattern for Text-to-SQL benchmarks."
    )
    parser.add_argument(
        "schema_path",
        type=str,
        help="Path to the schema file (e.g., databases/INDIA_IHDS_2005_HOUSEHOLD_SURVEY/INDIA_IHDS_2005_HOUSEHOLD_SURVEY/schema_info.md)"
    )
    parser.add_argument(
        "--backend",
        type=str,
        choices=["openrouter"],
        default="openrouter",
        help="LLM backend to use"
    )
    parser.add_argument(
        "--model",
        type=str,
        default="deepseek/deepseek-v3.2",
        help="Model to use. Defaults: openrouter='deepseek/deepseek-chat'"
    )
    parser.add_argument(
        "--openrouter-key",
        type=str,
        default=None,
        help="OpenRouter API key (or set OPENROUTER_API_KEY env var)"
    )
    parser.add_argument(
        "--reasoning",
        action="store_true",
        help="Enable reasoning mode for OpenRouter models that support it (e.g., deepseek-r1)"
    )
    args = parser.parse_args()

    # Determine output path
    database_dir = os.path.dirname(args.schema_path)    
    output_path = os.path.join(database_dir, "Knowledge_file.md")

    # Read schema text from markdown (or plain text) file.
    with open(args.schema_path, "r", encoding="utf-8") as f:
        schema_text = f.read()

    total_data_csv_path = os.path.join(database_dir, "data", "total_data.csv")
    external_knowledge = read_external_knowledge(args.schema_path)

    # Run with selected backend
    if args.backend == "openrouter":
        model = args.model or "deepseek/deepseek-chat"
        run_knowledge_pipeline(
            schema_text=schema_text,
            external_knowledge=external_knowledge,
            api_key=args.openrouter_key,
            output_path=output_path,
            model=model,
            reasoning=args.reasoning
        )
