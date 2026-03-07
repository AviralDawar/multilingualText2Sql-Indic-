"""
Step 1: Domain-Specific Question Generation

This module generates domain-relevant questions based on:
1. Database schema structure
2. Domain-specific keywords extracted from content
3. Nine predefined question types (covering major SQL operations)

The algorithm traverses the database schema using primary-foreign key relationships
to ensure comprehensive coverage of all tables.
"""

import json
import uuid
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass
import networkx as nx

from .config import (
    DSQGConfig, SchemaInfo, TableInfo, GeneratedQuestion,
    QuestionType, QUESTION_TYPE_DESCRIPTIONS
)


class DomainQuestionGenerator:
    """
    Generates domain-specific questions from database schema.

    Implements Algorithm 1 from the DSQG-Syn paper:
    1. Construct connected subgraphs from schema based on FK relationships
    2. For each node in subgraph, select k connected tables
    3. Generate domain keywords
    4. Generate questions for each table + question type combination
    """

    def __init__(self, config: DSQGConfig, llm_client):
        if llm_client is None:
            raise ValueError("LLM client is required for question generation")
        self.config = config
        self.llm_client = llm_client
        self.schema_graph = None

    def _llm_generate(self, prompt: str, temperature: float) -> str:
        """Generate with temperature; fallback for legacy clients without temperature arg."""
        try:
            return self.llm_client.generate(prompt, temperature=temperature)
        except TypeError:
            return self.llm_client.generate(prompt)

    def build_schema_graph(self, schema: SchemaInfo) -> nx.Graph:
        """
        Build a graph representation of the schema based on FK relationships.
        Each node is a table, each edge is a FK relationship.
        """
        G = nx.Graph()

        # Add all tables as nodes
        for table_name, table_info in schema.tables.items():
            G.add_node(table_name, info=table_info)

        # Add edges based on foreign key relationships
        for fk in schema.foreign_keys:
            if 'from_table' in fk and 'to_table' in fk:
                G.add_edge(
                    fk['from_table'],
                    fk['to_table'],
                    relationship=fk
                )

        self.schema_graph = G
        return G

    def extract_domain_keywords(self, schema: SchemaInfo) -> List[str]:
        """
        Extract domain-specific keywords from schema and content.
        Uses LLM to interpret field names and sample values.
        """
        prompt = self._build_keyword_extraction_prompt(schema)
        response = self._llm_generate(
            prompt,
            self.config.keyword_extraction_temperature
        )
        keywords = self._parse_keywords_response(response)
        return keywords

    def _build_keyword_extraction_prompt(self, schema: SchemaInfo) -> str:
        """Build prompt for LLM keyword extraction."""
        schema_desc = self._format_schema_for_prompt(schema)

        return f"""You are a domain expert. Please carefully review the following database schema and interpret
the field names, data types, and example values to identify relevant domain-specific terms. For
instance, recognize that certain variables (e.g., z) in a schema might represent key concepts in a
specific field, such as z representing redshift in astrophysics.

Database Schema:
{schema_desc}

Please analyze the schema and provide the following output in JSON format:
{{
    "domain": "your inferred domain",
    "keywords": ["keyword1", "keyword2", ..., "keywordN"]
}}

Important: Do not simply repeat column names. Map them to real-world domain-specific concepts and provide relevant keywords based on your understanding of the schema context.

For example, if a column is named 'DISTANCE_TO_ANGANWADI_CENTRE', recognize it relates to "child nutrition centers", "ICDS program", "early childhood care", etc.
"""

    def select_connected_tables(
        self,
        start_table: str,
        k: int
    ) -> List[str]:
        """
        Select k connected tables starting from a given table.
        Uses BFS to find connected tables based on FK relationships.
        """
        if self.schema_graph is None:
            raise ValueError("Schema graph not built. Call build_schema_graph first.")

        if start_table not in self.schema_graph:
            return [start_table]

        # BFS to find connected tables
        visited = {start_table}
        queue = [start_table]
        connected = [start_table]

        while queue and len(connected) < k:
            current = queue.pop(0)
            for neighbor in self.schema_graph.neighbors(current):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
                    connected.append(neighbor)
                    if len(connected) >= k:
                        break

        return connected

    def generate_questions_for_tables(
        self,
        tables: List[str],
        schema: SchemaInfo,
        keywords: List[str],
        question_types: List[QuestionType]
    ) -> List[GeneratedQuestion]:
        """
        Generate domain-specific questions for a set of connected tables.
        Generates one question per question type.
        """
        questions = []

        for q_type in question_types:
            prompt = self._build_question_generation_prompt(
                tables, schema, keywords, q_type
            )
            response = self._llm_generate(
                prompt,
                self.config.question_generation_temperature
            )
            question = self._parse_question_response(response, q_type, tables)

            if question:
                questions.append(question)

        return questions

    def _build_question_generation_prompt(
        self,
        tables: List[str],
        schema: SchemaInfo,
        keywords: List[str],
        question_type: QuestionType
    ) -> str:
        """Build prompt for question generation."""
        table_schemas = self._format_tables_for_prompt(tables, schema)
        type_info = QUESTION_TYPE_DESCRIPTIONS[question_type]

        return f"""You are an expert in the domain of {schema.domain or 'the given database'}.

I will provide you with a database schema and domain keywords. Generate a professional-level natural language question that:
1. Is relevant to the domain
2. Can be answered using SQL on the given tables
3. Matches the specified question type

Database Schema:
{table_schemas}

Domain Keywords:
{', '.join(keywords)}

Question Type: {question_type.value}
- SQL Operation: {type_info['sql_operation']}
- Description: {type_info['description']}
- Example: {type_info['example']}

Generate ONE natural language question that:
1. Is domain-relevant (uses domain terminology naturally)
2. Matches the {question_type.value} pattern (requires {type_info['sql_operation']} operation)
3. Can be answered using the provided tables

Output in JSON format:
{{
    "question": "<your generated question>",
    "related_tables": ["table1", "table2"],
    "keywords_used": ["keyword1", "keyword2"],
    "sql_operations": ["{type_info['sql_operation']}"]
}}

For example:
[
    {{
        "Domain": "Astrophysics",
        "NLQ": "What is the distance between each object and its neighbor?",
        "Keyword": "distance, objid, neighborobjid",
        "Related Schema": "neighbors",
        "Related SQL operator": "Scan"
    }},
    {{
        "Domain": "Astrophysics",
        "NLQ": "Which objects were detected in the 'boss' survey but not in the 'eboss' survey, and what is their average redshift?",
        "Keyword": "objects, boss survey, eboss survey, average redshift, set difference",
        "Related Schema": "specobj",
        "Related SQL operator": "Except, Aggregate"
    }},
    {{
        "Domain": "Astrophysics",
        "NLQ": "Which objects were observed in both the 'sdss' and 'boss' surveys, and sort them by their velocity dispersion (veldisp)?",
        "Keyword": "objects, sdss survey, boss survey, velocity dispersion",
        "Related Schema": "specobj",
        "Related SQL operator": "Intersect, Sort"
    }}
]
"""

    def generate_all_questions(
        self,
        schema: SchemaInfo
    ) -> List[GeneratedQuestion]:
        """
        Main entry point: Generate all domain-specific questions for the schema.

        Implements Algorithm 1 from the paper:
        1. Build schema graph
        2. Extract domain keywords
        3. For each table, select connected tables and generate questions
        """
        # Step 1: Build schema graph
        print("  [1/3] Building schema graph...")
        self.build_schema_graph(schema)

        # Step 2: Extract domain keywords
        print("  [2/3] Extracting domain keywords via LLM...")
        keywords = self.extract_domain_keywords(schema)
        schema.keywords = keywords
        print(f"        Extracted {len(keywords)} keywords")
        print(keywords)
        # Step 3: Generate questions for each table
        print("  [3/3] Generating questions for each table...")
        all_questions = []
        question_types = list(QuestionType)
        all_table_names = list(schema.tables.keys())
        table_names = all_table_names[::2]  # Temporary debug scope: process 1st, 3rd, 5th, ...
        total_tables = len(table_names)
        questions_per_table = self.config.questions_per_table

        for i, table_name in enumerate(table_names):
            print(f"        Table {i+1}/{total_tables}: {table_name} ({questions_per_table} questions)...", end=" ", flush=True)

            # Select k connected tables (k = max_join_depth from config)
            connected_tables = self.select_connected_tables(
                table_name,
                self.config.max_join_depth
            )

            # Generate questions for this table group
            questions = self.generate_questions_for_tables(
                connected_tables,
                schema,
                keywords,
                question_types[:questions_per_table]
            )

            all_questions.extend(questions)
            print(f"✓ ({len(questions)} generated)")

        return all_questions

    def _format_schema_for_prompt(self, schema: SchemaInfo) -> str:
        """Format schema information for LLM prompt."""
        lines = []
        for table_name, table_info in schema.tables.items():
            lines.append(f"\nTable: {table_name}")
            lines.append(f"Description: {table_info.description}")
            lines.append("Columns:")
            for col in table_info.columns:
                col_desc = col.get('description', '')
                lines.append(f"  - {col['name']} ({col.get('type', 'unknown')}){': ' + col_desc if col_desc else ''}")
            if table_info.sample_values:
                lines.append("Sample values:")
                for col, values in list(table_info.sample_values.items())[:3]:
                    lines.append(f"  - {col}: {values[:3]}")
        return '\n'.join(lines)

    def _format_tables_for_prompt(
        self,
        tables: List[str],
        schema: SchemaInfo
    ) -> str:
        """Format specific tables for prompt."""
        lines = []
        for table_name in tables:
            if table_name in schema.tables:
                table_info = schema.tables[table_name]
                lines.append(f"\nCREATE TABLE {table_name} (")
                col_defs = []
                for col in table_info.columns:
                    col_def = f"    {col['name']} {col.get('type', 'TEXT')}"
                    if col['name'] == table_info.primary_key:
                        col_def += " PRIMARY KEY"
                    col_defs.append(col_def)
                lines.append(',\n'.join(col_defs))
                lines.append(");")
                lines.append(f"-- Description: {table_info.description}")
        return '\n'.join(lines)

    def _extract_json_from_response(self, response: str) -> str:
        """Extract JSON from LLM response, handling markdown code blocks."""
        response = response.strip()

        # Handle markdown code blocks
        if response.startswith('```'):
            # Remove opening ```json or ```
            lines = response.split('\n')
            if lines[0].startswith('```'):
                lines = lines[1:]
            # Remove closing ```
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            response = '\n'.join(lines)

        return response.strip()

    def _parse_keywords_response(self, response: str) -> List[str]:
        """Parse LLM response for keywords."""
        try:
            clean_response = self._extract_json_from_response(response)
            data = json.loads(clean_response)
            return data.get('keywords', [])
        except json.JSONDecodeError:
            return []

    def _parse_question_response(
        self,
        response: str,
        question_type: QuestionType,
        tables: List[str]
    ) -> Optional[GeneratedQuestion]:
        """Parse LLM response for generated question."""
        try:
            clean_response = self._extract_json_from_response(response)
            data = json.loads(clean_response)

            # Some models return a JSON array with one object.
            if isinstance(data, list):
                if not data:
                    return None
                data = data[0]

            if not isinstance(data, dict):
                return None

            question_text = data.get('question', '')
            if not isinstance(question_text, str) or not question_text.strip():
                return None

            return GeneratedQuestion(
                question_id=str(uuid.uuid4())[:8],
                question_text=question_text.strip(),
                question_type=question_type,
                related_tables=data.get('related_tables', tables),
                keywords=data.get('keywords_used', []),
                sql_operations=data.get('sql_operations', [])
            )
        except json.JSONDecodeError:
            return None
