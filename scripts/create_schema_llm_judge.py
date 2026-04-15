import os
import csv
import argparse
import requests
import json
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser


def parse_csv_columns(csv_path: str) -> list[str]:
    """Parse the column headers from a CSV file."""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        columns = next(reader)  # Read the first row (headers)
    return columns


def format_indexed_columns(columns: list[str]) -> str:
    """Format columns with their indices for better LLM context."""
    lines = []
    for idx, col in enumerate(columns):
        lines.append(f"[{idx}] {col}")
    return "\n".join(lines)


# --- PROMPT TEMPLATES ---
ARCHITECT_PROMPT = """
Role: Senior Database Architect.
Task: Analyze these CSV columns (with their 0-based indices):

{indexed_columns}

CRITICAL PRIORITY: Domain Isolation.
Instead of one massive fact table, you must divide the data into 4-10 distinct THEMATIC TABLES based on the categories/domains of the data.

Requirements:
1. Each table should represent a single cohesive domain.
2. NO TABLE should have more than 12-15 columns. If a domain is too large, split it into sub-domains.
3. Every table must have a Primary Key.
4. Link tables via Foreign Keys to maintain a relational structure.
5. Ensure the total columns across all tables range between 40-80. If you have to drop some columns to do this - feel free. But make sure that you drop full categories and all the columns related to that categories.

NAMING CONVENTIONS (CRITICAL):
- Use UPPERCASE_WITH_UNDERSCORES for all table and column names
- Dimension tables MUST start with DIM_ prefix (e.g., DIM_GEOGRAPHY)
- Fact tables MUST start with FACT_ prefix (e.g., FACT_SALES, FACT_POPULATION)
- Column names should be descriptive with underscores (e.g., TOTAL_POPULATION, CENSUS_YEAR)

IMPORTANT: Do NOT include any indexing recommendations.
Output a draft table list with columns assigned to each.
"""

AUDITOR_PROMPT = """
Role: Database Normalization & Domain Auditor.
Input Schema: {draft_schema}

Task: Audit the schema for "Column Clumping."
1. **Width Check:** Does any single table contain more than 15 columns? If yes, it is a "God Table" and MUST be split.
2. **Cohesion Check:** Are there columns in a table that don't belong to its theme? (e.g., placing 'Education' metrics in a 'Public Debt' table).
3. **3NF Violation:** Look for transitive dependencies (e.g., State_Name and Country_Name in an Expenditure table).
4. **Complexity Check:** Ensure that answering complex questions will require JOINING at least 3 tables. If the metrics are all in one table, the schema is too "flat" for a benchmark.

List specific "Clumping Errors" and "Normalization Failures" for the Architect.
"""

REFINER_PROMPT = """
Role: Lead Architect.
Original Draft: {draft_schema}
Auditor Feedback: {audit_feedback}
Original CSV columns (with indices): {indexed_columns}

Task: Resolve Auditor warnings by aggressively splitting wide tables into thematic sub-domains.
1. If a table was too wide, split it logically (e.g., split 'Social Services' into 'Education_Social' and 'Health_Social').
2. Ensure every table has a clear Join Path to others.
3. Clean column names for the schema (remove 'UOM:INR', 'Scaling Factor', etc.). Use UPPERCASE_WITH_UNDERSCORES for all table and column names.

CRITICAL OUTPUT FORMAT - You MUST follow this exact structure for automated parsing:

1. Start with "## DIMENSION TABLES" section header
2. For each dimension table, use this EXACT format:
   ### **TABLE_NAME**
   **Purpose:** Brief description
   **Columns (N):**
   - `column_name` (PK, INT)
   - `another_column` (FK → OTHER_TABLE)
   - `data_column` (VARCHAR 100)

3. Then add "## FACT TABLES" section header
4. For each fact table, use the same format as dimension tables

5. Finally, add "## COLUMN MAPPING" section with this EXACT table format:
   | Original Column | Source Index | Data Type | Mapped To Table | Mapped Column(s) | Notes |
   |---|---|---|---|---|---|
   | Country | 0 | VARCHAR | DIM_GEOGRAPHY | COUNTRY | Geographic dimension |

   CRITICAL FOR COLUMN MAPPING:
   - The "Original Column" MUST be the EXACT column name from the CSV (copy it verbatim from the indexed columns list above)
   - The "Source Index" MUST be the 0-based index number from the indexed columns (e.g., 0, 1, 2, etc.)
   - Include ALL columns that are mapped to tables (don't skip any)
   - This enables automated data processing - incorrect names will cause failures
   - ONE-TO-ONE MAPPING ONLY: Each CSV column must map to exactly ONE target column in ONE table. Do NOT map multiple CSV columns to the same target column (no unpivoting).

Column type examples:
- Primary keys: (PK, INT) or (PK, BIGINT)
- Foreign keys: (FK → REFERENCED_TABLE)
- Strings: (VARCHAR 50) or (VARCHAR 100)
- Numbers: (INT), (BIGINT), (DOUBLE PRECISION)
- Booleans: (BOOLEAN)

Example table definition:
### **DIM_GEOGRAPHY**
**Purpose:** Geographic hierarchy dimension
**Columns (4):**
- `GEOGRAPHY_ID` (PK, INT)
- `COUNTRY` (VARCHAR 100)
- `STATE` (VARCHAR 100)
- `DISTRICT` (VARCHAR 100)

IMPORTANT:
- Use UPPERCASE names for all tables and columns
- Use underscores between words (e.g., DIM_GEOGRAPHY, not DimGeography)
- Dimension tables should start with DIM_ prefix
- Fact tables should start with FACT_ prefix
- In COLUMN MAPPING, use EXACT original CSV column names (not cleaned versions)
- NO INDEXES in the output
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


def create_langchain_llm(model: str = "claude-haiku-4-5-20251001", temperature: float = 0, max_tokens: int = 12000):
    """Create a LangChain Claude LLM instance."""
    return ChatAnthropic(model=model, temperature=temperature, max_tokens=max_tokens)


def run_with_langchain(csv_columns: list[str], output_path: str, model: str):
    """Run the pipeline using LangChain with Claude."""
    llm = create_langchain_llm(model=model)

    architect_prompt = ChatPromptTemplate.from_template(ARCHITECT_PROMPT)
    auditor_prompt = ChatPromptTemplate.from_template(AUDITOR_PROMPT)
    refiner_prompt = ChatPromptTemplate.from_template(REFINER_PROMPT)

    architect = architect_prompt | llm | StrOutputParser()
    auditor = auditor_prompt | llm | StrOutputParser()
    refiner = refiner_prompt | llm | StrOutputParser()

    indexed_columns = format_indexed_columns(csv_columns)

    print("--- Step 1: Architect is drafting the Star Schema ---")
    draft = architect.invoke({"indexed_columns": indexed_columns})

    print("--- Step 2: Auditor is checking for 3NF violations and benchmark difficulty ---")
    audit_report = auditor.invoke({"draft_schema": draft})
    print(f"\nAUDITOR REPORT:\n{audit_report}\n")

    print("--- Step 3: Refiner is finalizing the 3NF Schema ---")
    final_output = refiner.invoke({
        "draft_schema": draft,
        "audit_feedback": audit_report,
        "indexed_columns": indexed_columns
    })

    with open(output_path, "w") as f:
        f.write(final_output)

    print(f"Success! Schema info saved to: {output_path}")


def run_with_openrouter(csv_columns: list[str], output_path: str, model: str,
                        api_key: str = None, reasoning: bool = False):
    """Run the pipeline using OpenRouter API.

    When reasoning is enabled, uses multi-turn conversation to preserve
    reasoning_details across agent steps. This allows the model to build
    upon its previous reasoning chain.
    """
    llm = OpenRouterLLM(model=model, api_key=api_key, reasoning=reasoning)

    indexed_columns = format_indexed_columns(csv_columns)

    print(f"--- Using OpenRouter with model: {model} ---")
    if reasoning:
        print("--- Reasoning mode enabled: preserving reasoning chain across agents ---")

    print("--- Step 1: Architect is drafting the Star Schema ---")
    architect_input = ARCHITECT_PROMPT.format(indexed_columns=indexed_columns)

    if reasoning:
        # Use multi-turn to preserve reasoning details
        draft = llm.invoke_with_history(architect_input)
    else:
        draft = llm.invoke(architect_input)

    print("--- Step 2: Auditor is checking for 3NF violations and benchmark difficulty ---")
    auditor_input = AUDITOR_PROMPT.format(draft_schema=draft)

    if reasoning:
        # Continue conversation - model has access to its reasoning from step 1
        audit_report = llm.invoke_with_history(auditor_input)
    else:
        audit_report = llm.invoke(auditor_input)
    print(f"\nAUDITOR REPORT:\n{audit_report}\n")

    print("--- Step 3: Refiner is finalizing the 3NF Schema ---")
    refiner_input = REFINER_PROMPT.format(
        draft_schema=draft,
        audit_feedback=audit_report,
        indexed_columns=indexed_columns
    )

    if reasoning:
        # Final step - model has full reasoning chain from steps 1 & 2
        final_output = llm.invoke_with_history(refiner_input)
    else:
        final_output = llm.invoke(refiner_input)

    with open(output_path, "w") as f:
        f.write(final_output)

    print(f"Success! Schema info saved to: {output_path}")


# Main entry point with argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a 3NF Star Schema from CSV columns using LLM Judge pattern for Text-to-SQL benchmarks."
    )
    parser.add_argument(
        "csv_path",
        type=str,
        help="Path to the CSV file (e.g., databases/INDIA_EMPLOYMENT_DATA/INDIA_EMPLOYMENT_DATA/data/total_data.csv)"
    )
    parser.add_argument(
        "--backend",
        type=str,
        choices=["anthropic", "openrouter"],
        default="anthropic",
        help="LLM backend to use (default: anthropic)"
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Model to use. Defaults: anthropic='claude-haiku-4-5-20251001', openrouter='deepseek/deepseek-chat'"
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

    # Parse columns from the CSV file
    print(f"--- Parsing columns from: {args.csv_path} ---")
    columns_list = parse_csv_columns(args.csv_path)
    print(f"Found {len(columns_list)} columns: {columns_list[:5]}... (truncated)")

    # Determine output path: go up from data/ directory to the database root
    csv_dir = os.path.dirname(args.csv_path)  # .../data
    database_dir = os.path.dirname(csv_dir)    # .../INDIA_EMPLOYMENT_DATA
    output_path = os.path.join(database_dir, "schema_info.md")

    # Run with selected backend
    if args.backend == "openrouter":
        model = args.model or "deepseek/deepseek-chat"
        run_with_openrouter(columns_list, output_path, model, args.openrouter_key, args.reasoning)
    else:
        model = args.model or "claude-haiku-4-5-20251001"
        run_with_langchain(columns_list, output_path, model)