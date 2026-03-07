"""
Schema Selector for DSQG-Syn

Adapted from the Selector agent in MAC-SQL (agents.py), this module implements
intelligent schema linking by:
1. Loading schema with sample values from CSV files
2. Deciding whether pruning is needed based on schema size
3. Using LLM to select minimal relevant schema for a question

Reference: MAC-SQL paper's Selector agent approach
"""

import os
import json
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path

from .config import SchemaInfo, TableInfo, GeneratedQuestion


# Selector prompt template (from MAC-SQL agents.py)
SELECTOR_TEMPLATE = """
As an experienced and professional database administrator, your task is to analyze a user question and a database schema to provide relevant information. The database schema consists of table descriptions, each containing multiple column descriptions. Your goal is to identify the relevant tables and columns based on the user question and evidence provided.

[Instruction]:
1. Discard any table schema that is not related to the user question and evidence.
2. Sort the columns in each relevant table in descending order of relevance and keep the top 6 columns.
3. Ensure that at least 3 tables are included in the final output JSON.
4. The output should be in JSON format.

Requirements:
1. If a table has less than or equal to 10 columns, mark it as "keep_all".
2. If a table is completely irrelevant to the user question and evidence, mark it as "drop_all".
3. Prioritize the columns in each relevant table based on their relevance.

Here is a typical example:

==========
【DB_ID】 banking_system
【Schema】
# Table: account
[
(account_id, the id of the account. Value examples: [11382, 11362, 2, 1, 2367].),
(district_id, location of branch. Value examples: [77, 76, 2, 1, 39].),
(frequency, frequency of the acount. Value examples: ['POPLATEK MESICNE', 'POPLATEK TYDNE', 'POPLATEK PO OBRATU'].),
(date, the creation date of the account. Value examples: ['1997-12-29', '1997-12-28'].)
]
# Table: client
[
(client_id, the unique number. Value examples: [13998, 13971, 2, 1, 2839].),
(gender, gender. Value examples: ['M', 'F']. And F：female . M：male ),
(birth_date, birth date. Value examples: ['1987-09-27', '1986-08-13'].),
(district_id, location of branch. Value examples: [77, 76, 2, 1, 39].)
]
# Table: loan
[
(loan_id, the id number identifying the loan data. Value examples: [4959, 4960, 4961].),
(account_id, the id number identifying the account. Value examples: [10, 80, 55, 43].),
(date, the date when the loan is approved. Value examples: ['1998-07-12', '1998-04-19'].),
(amount, the id number identifying the loan data. Value examples: [1567, 7877, 9988].),
(duration, the id number identifying the loan data. Value examples: [60, 48, 24, 12, 36].),
(payments, the id number identifying the loan data. Value examples: [3456, 8972, 9845].),
(status, the id number identifying the loan data. Value examples: ['C', 'A', 'D', 'B'].)
]
# Table: district
[
(district_id, location of branch. Value examples: [77, 76].),
(A2, area in square kilometers. Value examples: [50.5, 48.9].),
(A4, number of inhabitants. Value examples: [95907, 95616].),
(A5, number of households. Value examples: [35678, 34892].),
(A6, literacy rate. Value examples: [95.6, 92.3, 89.7].),
(A7, number of entrepreneurs. Value examples: [1234, 1456].),
(A8, number of cities. Value examples: [5, 4].),
(A9, number of schools. Value examples: [15, 12, 10].),
(A10, number of hospitals. Value examples: [8, 6, 4].),
(A11, average salary. Value examples: [12541, 11277].),
(A12, poverty rate. Value examples: [12.4, 9.8].),
(A13, unemployment rate. Value examples: [8.2, 7.9].),
(A15, number of crimes. Value examples: [256, 189].)
]
【Foreign keys】
client.`district_id` = district.`district_id`
【Question】
What is the gender of the youngest client who opened account in the lowest average salary branch?
【Evidence】
Later birthdate refers to younger age; A11 refers to average salary
【Answer】
```json
{{
"account": "keep_all",
"client": "keep_all",
"loan": "drop_all",
"district": ["district_id", "A11", "A2", "A4", "A6", "A7"]
}}
```
Question Solved.

==========

Here is a new example, please start answering:

【DB_ID】 {db_id}
【Schema】
{desc_str}
【Foreign keys】
{fk_str}
【Question】
{query}
【Evidence】
{evidence}
【Answer】
"""


@dataclass
class ColumnValueInfo:
    """Information about a column with sample values."""
    name: str
    data_type: str
    description: str = ""
    is_primary_key: bool = False
    is_foreign_key: bool = False
    sample_values: List = field(default_factory=list)


@dataclass
class TableSchemaWithValues:
    """Table schema with sample values for each column."""
    name: str
    description: str
    columns: List[ColumnValueInfo]
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Tuple[str, str, str]] = field(default_factory=list)  # (from_col, to_table, to_col)

    @property
    def column_count(self) -> int:
        return len(self.columns)


class SchemaSelector:
    """
    Selects minimal relevant schema for a question.

    Implements the Selector agent pattern from MAC-SQL:
    - Loads schema with sample values from data files
    - Decides if pruning is needed based on thresholds
    - Uses LLM to select relevant tables/columns
    """

    # Thresholds for deciding whether to prune (from agents.py)
    MAX_AVG_COLUMNS = 6
    MAX_TOTAL_COLUMNS = 30
    MAX_SAMPLE_VALUES = 6

    def __init__(self, llm_client, database_path: str = None):
        """
        Args:
            llm_client: LLM client with .generate(prompt) method
            database_path: Path to database directory containing CSV files
        """
        self.llm_client = llm_client
        self.database_path = database_path
        self.schema_cache: Dict[str, TableSchemaWithValues] = {}
        self.sample_values_loaded = False

    def _llm_generate(self, prompt: str, temperature: float = None) -> str:
        """Generate with temperature; fallback for legacy clients without temperature arg."""
        if temperature is None:
            return self.llm_client.generate(prompt)
        try:
            return self.llm_client.generate(prompt, temperature=temperature)
        except TypeError:
            return self.llm_client.generate(prompt)

    def load_schema_with_values(
        self,
        schema: SchemaInfo,
        data_path: str = None
    ) -> Dict[str, TableSchemaWithValues]:
        """
        Load schema and extract sample values from CSV files or schema metadata.

        Args:
            schema: SchemaInfo object with table definitions
            data_path: Path to directory containing CSV files for each table

        Returns:
            Dict mapping table names to TableSchemaWithValues objects
        """
        data_path = data_path or self.database_path
        tables: Dict[str, TableSchemaWithValues] = {}

        # Build FK lookup for quick access
        fk_columns = set()
        fk_tuples_by_table: Dict[str, List[Tuple[str, str, str]]] = {}

        for fk in schema.foreign_keys:
            from_table = fk.get('from_table', '')
            from_col = fk.get('from_column', '')
            to_table = fk.get('to_table', '')
            to_col = fk.get('to_column', '')

            if from_table and from_col:
                fk_columns.add(f"{from_table}.{from_col}")
                if from_table not in fk_tuples_by_table:
                    fk_tuples_by_table[from_table] = []
                fk_tuples_by_table[from_table].append((from_col, to_table, to_col))

        for table_name, table_info in schema.tables.items():
            columns = []
            pk_cols = [table_info.primary_key] if table_info.primary_key else []

            for col in table_info.columns:
                col_name = col['name']
                col_type = col.get('type', 'TEXT')
                col_desc = col.get('description', '')

                is_pk = col_name == table_info.primary_key
                is_fk = f"{table_name}.{col_name}" in fk_columns

                # Get sample values if data path provided
                sample_values = []
                if data_path:
                    sample_values = self._get_column_sample_values(
                        data_path, table_name, col_name, col_type,
                        is_pk or is_fk
                    )
                else:
                    sample_values = self._get_column_sample_values_from_table_info(
                        table_info.sample_values, col_name, col_type, is_pk or is_fk
                    )

                columns.append(ColumnValueInfo(
                    name=col_name,
                    data_type=col_type,
                    description=col_desc,
                    is_primary_key=is_pk,
                    is_foreign_key=is_fk,
                    sample_values=sample_values
                ))

            tables[table_name] = TableSchemaWithValues(
                name=table_name,
                description=table_info.description,
                columns=columns,
                primary_keys=pk_cols,
                foreign_keys=fk_tuples_by_table.get(table_name, [])
            )

        self.schema_cache = tables
        self.sample_values_loaded = any(
            col.sample_values
            for table_schema in tables.values()
            for col in table_schema.columns
        )
        return tables

    def _get_column_sample_values_from_table_info(
        self,
        table_sample_values: Dict[str, List],
        column_name: str,
        column_type: str,
        is_key: bool
    ) -> List:
        """
        Extract sample values for a column from preloaded TableInfo.sample_values.
        """
        # Keep filtering behavior aligned with CSV-based path.
        if is_key:
            return []

        lower_name = column_name.lower()
        if lower_name.endswith('id') or lower_name.endswith('email') or lower_name.endswith('url'):
            return []

        if not table_sample_values:
            return []

        values = table_sample_values.get(column_name, [])
        if not isinstance(values, list):
            return []

        return self._filter_sample_values(values, column_type)

    def _get_column_sample_values(
        self,
        data_path: str,
        table_name: str,
        column_name: str,
        column_type: str,
        is_key: bool
    ) -> List:
        """
        Extract sample values for a column from CSV file.

        Adapted from _get_unique_column_values_str in agents.py
        """
        # Skip key columns (PK/FK don't need sample values for selection)
        if is_key:
            return []

        # Skip columns ending with id, email, url (from agents.py logic)
        lower_name = column_name.lower()
        if lower_name.endswith('id') or lower_name.endswith('email') or lower_name.endswith('url'):
            return []

        # Try to find and read the CSV file
        csv_paths = [
            os.path.join(data_path, 'data', f"{table_name}.csv"),
            os.path.join(data_path, f"{table_name}.csv"),
        ]

        csv_path = None
        for path in csv_paths:
            if os.path.exists(path):
                csv_path = path
                break

        if not csv_path:
            return []

        try:
            df = pd.read_csv(csv_path, nrows=1000)  # Read limited rows for efficiency
            if column_name not in df.columns:
                return []

            # Get unique values sorted by frequency (like agents.py)
            value_counts = df[column_name].value_counts()
            values = value_counts.index.tolist()

            return self._filter_sample_values(values, column_type)

        except Exception as e:
            # Silently skip - sample values are optional
            return []

    def _filter_sample_values(
        self,
        values: List,
        column_type: str
    ) -> List:
        """
        Filter and format sample values.

        Adapted from _get_value_examples_str in agents.py
        """
        if not values:
            return []

        # For numeric columns with many values, skip samples (from agents.py)
        numeric_types = ['INTEGER', 'REAL', 'NUMERIC', 'FLOAT', 'INT', 'BIGINT', 'SMALLINT']
        if len(values) > 10 and column_type.upper() in numeric_types:
            return []

        # Filter out None and empty values
        filtered = []
        for v in values:
            if v is None or pd.isna(v):
                continue
            if isinstance(v, str):
                v = v.strip()
                if v == '':
                    continue
                # Skip URLs and emails
                if 'http://' in v or 'https://' in v:
                    return []
                if '@' in v and '.' in v:  # Simple email check
                    return []
                # Skip very long strings
                if len(v) > 50:
                    return []
            filtered.append(v)

        # Return top N sample values
        return filtered[:self.MAX_SAMPLE_VALUES]

    def _is_need_prune(self, tables: Dict[str, TableSchemaWithValues]) -> bool:
        """
        Decide whether schema pruning is needed based on size thresholds.

        From agents.py:
        - If avg_column_count <= 6 AND total_column_count <= 30: no pruning
        - Otherwise: activate selector
        """
        if not tables:
            return False

        total_columns = sum(t.column_count for t in tables.values())
        avg_columns = total_columns / len(tables)

        if avg_columns <= self.MAX_AVG_COLUMNS and total_columns <= self.MAX_TOTAL_COLUMNS:
            return False
        return True

    def _build_schema_description(
        self,
        tables: Dict[str, TableSchemaWithValues]
    ) -> str:
        """
        Build schema description string with sample values.

        Format matches _build_bird_table_schema_list_str in agents.py:
        # Table: table_name
        [
          (col_name, description. Value examples: [...].),
          ...
        ]
        """
        lines = []

        for table_name, table_schema in tables.items():
            lines.append(f"# Table: {table_name}")
            if table_schema.description:
                lines.append(f"# Description: {table_schema.description}")
            lines.append("[")

            col_lines = []
            for col in table_schema.columns:
                # Build column description line
                col_line = f"({col.name}"

                # Add description if available
                if col.description:
                    desc = col.description.replace('_', ' ')
                    col_line += f", {desc}"
                else:
                    # Use column name as description (convert underscores to spaces)
                    readable_name = col.name.replace('_', ' ').lower()
                    col_line += f", {readable_name}"

                # Add sample values if available
                if col.sample_values:
                    # Format values for display
                    formatted_values = []
                    for v in col.sample_values[:5]:
                        if isinstance(v, str):
                            formatted_values.append(f"'{v}'")
                        else:
                            formatted_values.append(str(v))
                    values_str = f"[{', '.join(formatted_values)}]"
                    col_line += f". Value examples: {values_str}"

                col_line += ".),"
                col_lines.append(col_line)

            lines.append('\n'.join(col_lines).rstrip(','))
            lines.append("]")

        return '\n'.join(lines)

    def _build_fk_description(
        self,
        tables: Dict[str, TableSchemaWithValues],
        schema: SchemaInfo
    ) -> str:
        """Build foreign key relationships description in MAC-SQL format."""
        fk_lines = []

        for fk in schema.foreign_keys:
            from_table = fk.get('from_table', '')
            from_col = fk.get('from_column', '')
            to_table = fk.get('to_table', '')
            to_col = fk.get('to_column', '')

            if from_table and from_col and to_table and to_col:
                fk_lines.append(f"{from_table}.`{from_col}` = {to_table}.`{to_col}`")

        return '\n'.join(fk_lines) if fk_lines else "None"

    def _extract_json_from_response(self, response: str) -> str:
        """Extract JSON from LLM response, handling markdown code blocks."""
        response = response.strip()

        # Handle markdown code blocks
        if '```json' in response:
            start = response.find('```json') + 7
            end = response.find('```', start)
            if end > start:
                response = response[start:end]
        elif '```' in response:
            start = response.find('```') + 3
            end = response.find('```', start)
            if end > start:
                response = response[start:end]

        return response.strip()

    def _parse_selector_response(self, response: str) -> Dict[str, any]:
        """Parse LLM response for schema selection."""
        clean_response = self._extract_json_from_response(response)

        try:
            return json.loads(clean_response)
        except json.JSONDecodeError:
            return {}

    def select_schema(
        self,
        question: GeneratedQuestion,
        schema: SchemaInfo,
        data_path: str = None,
        evidence: str = "",
        force_prune: bool = False,
        temperature: float = None
    ) -> Dict[str, List[str]]:
        """
        Select minimal relevant schema for a question.

        Main entry point implementing the Selector agent pattern.

        Args:
            question: The question to answer
            schema: Full database schema
            data_path: Path to CSV data files (for sample values)
            evidence: Additional context/hints for the question
            force_prune: If True, always use LLM selection regardless of size
            temperature: Optional temperature override for selector LLM call

        Returns:
            Dict mapping table names to list of relevant column names
        """
        # Load schema with sample values
        tables = self.load_schema_with_values(schema, data_path)

        # Check if pruning is needed
        need_prune = force_prune or self._is_need_prune(tables)

        if not need_prune:
            # Return full schema (all columns for all tables)
            print("        [Selector] Schema within threshold, using full schema")
            return {
                table_name: [col.name for col in table_schema.columns]
                for table_name, table_schema in tables.items()
            }

        # Build schema description with sample values
        schema_desc = self._build_schema_description(tables)
        fk_desc = self._build_fk_description(tables, schema)

        # Build prompt using MAC-SQL selector template
        prompt = SELECTOR_TEMPLATE.format(
            db_id=schema.database_name,
            desc_str=schema_desc,
            fk_str=fk_desc,
            query=question.question_text,
            evidence=evidence or "None"
        )

        print("        [Selector] Schema exceeds threshold, using LLM selection...")
        response = self._llm_generate(prompt, temperature=temperature)
        selection = self._parse_selector_response(response)

        if not selection:
            print("        [Selector] LLM selection failed, using full schema")
            return {
                table_name: [col.name for col in table_schema.columns]
                for table_name, table_schema in tables.items()
            }

        # Process selection result (following agents.py logic)
        result = {}
        for table_name, table_schema in tables.items():
            table_selection = selection.get(table_name, "keep_all")

            if table_selection == "drop_all":
                # Skip this table entirely
                continue
            elif table_selection == "keep_all":
                # Include all columns
                result[table_name] = [col.name for col in table_schema.columns]
            elif isinstance(table_selection, list):
                # Include specified columns
                # Also ensure PK/FK columns are included (important for joins)
                important_cols = [col.name for col in table_schema.columns
                                 if col.is_primary_key or col.is_foreign_key]
                selected_cols = list(set(table_selection + important_cols))
                # Filter to only columns that actually exist
                valid_cols = [c for c in selected_cols
                             if any(col.name == c for col in table_schema.columns)]
                result[table_name] = valid_cols if valid_cols else [col.name for col in table_schema.columns]
            else:
                # Fallback: keep all columns
                result[table_name] = [col.name for col in table_schema.columns]

        # Ensure at least some tables are selected
        if not result:
            print("        [Selector] No tables selected, using full schema")
            return {
                table_name: [col.name for col in table_schema.columns]
                for table_name, table_schema in tables.items()
            }

        total_cols = sum(len(v) for v in result.values())
        print(f"        [Selector] Selected {len(result)} tables, {total_cols} columns")
        return result

    def get_schema_stats(self, schema: SchemaInfo = None) -> Dict:
        """Get statistics about the schema."""
        tables = self.schema_cache

        if schema and not tables:
            tables = self.load_schema_with_values(schema)

        if not tables:
            return {}

        total_columns = sum(t.column_count for t in tables.values())
        return {
            'table_count': len(tables),
            'total_columns': total_columns,
            'avg_columns': total_columns / len(tables) if tables else 0,
            'max_columns': max(t.column_count for t in tables.values()) if tables else 0,
            'needs_pruning': self._is_need_prune(tables)
        }
