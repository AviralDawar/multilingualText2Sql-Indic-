"""
DSQG-Syn: Main Orchestration Class

This module provides the main interface for running the complete DSQG-Syn pipeline:
1. Domain-Specific Question Generation
2. Question-Guided SQL-NLQ Synthesis
3. NLQ Semantic Optimization

Based on: "DSQG-Syn: Synthesizing High-quality Data for Text-to-SQL Parsing
by Domain Specific Question Generation" (Duan et al., NAACL 2025)
"""

import json
import os
from typing import List, Dict, Optional, Any
from dataclasses import asdict
from datetime import datetime

from .config import (
    DSQGConfig, SchemaInfo, TableInfo, GeneratedQuestion,
    NLQSQLPair, QuestionType
)
from .question_generator import DomainQuestionGenerator
from .sql_synthesizer import SQLNLQSynthesizer
from .semantic_optimizer import SemanticOptimizer


class DSQGSyn:
    """
    Main class for DSQG-Syn text-to-SQL data synthesis.

    Usage:
        config = DSQGConfig(llm_model="gpt-4")
        dsqg = DSQGSyn(config, llm_client=your_llm_client)

        schema = dsqg.load_schema_from_yaml("path/to/schema_config.yaml")
        pairs = dsqg.synthesize(schema)
        dsqg.export_to_jsonl(pairs, "output.jsonl")
    """

    def __init__(
        self,
        config: DSQGConfig,
        llm_client=None,
        sql_llm_client=None,
        nlq_llm_client=None,
        embedding_model=None,
        data_path: str = None
    ):
        """
        Initialize DSQG-Syn framework.

        Args:
            config: Configuration object
            llm_client: LLM client for generation (optional, uses templates if None)
            embedding_model: Embedding model for semantic similarity (optional)
            data_path: Path to database directory with CSV files (for sample values in schema linking)
        """
        self.config = config
        self.llm_client = llm_client
        self.sql_llm_client = sql_llm_client
        self.nlq_llm_client = nlq_llm_client
        self.embedding_model = embedding_model
        self.data_path = data_path

        # Initialize components
        self.question_generator = DomainQuestionGenerator(config, llm_client)
        self.sql_synthesizer = SQLNLQSynthesizer(
            config,
            llm_client,
            data_path,
            sql_llm_client=sql_llm_client,
            nlq_llm_client=nlq_llm_client
        )
        self.semantic_optimizer = SemanticOptimizer(config, embedding_model)

        # Store intermediate results
        self.generated_questions: List[GeneratedQuestion] = []
        self.raw_pairs: List[NLQSQLPair] = []
        self.final_pairs: List[NLQSQLPair] = []

    def load_schema_from_yaml(self, yaml_path: str) -> SchemaInfo:
        """
        Load database schema from YAML configuration file.

        Expected format matches the project's schema_config.yaml structure.
        Optionally loads sample data from JSON files if data_path is provided.
        """
        import yaml
        from pathlib import Path

        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)

        tables = {}
        dimension_tables = config.get('dimension_tables', {}) or {}
        fact_tables = config.get('fact_tables', {}) or {}

        table_key_map: Dict[str, str] = {}
        for table_name, table_config in {**dimension_tables, **fact_tables}.items():
            table_key_map[table_name] = table_config.get('key_column', f'{table_name}_ID')

        # Determine data directory (same directory as YAML by default, or use self.data_path)
        yaml_dir = Path(yaml_path).parent
        data_dir = Path(self.data_path) if self.data_path else yaml_dir

        def infer_key_type(column_name: str) -> str:
            """Infer type for PK/FK-like columns used in schema prompts."""
            col = (column_name or '').upper()
            if col == 'YEAR' or 'YEAR' in col:
                return 'INTEGER'
            return 'BIGINT'

        def add_column(
            columns: List[Dict[str, str]],
            seen_columns: set,
            name: str,
            col_type: str,
            description: str
        ):
            """Append a column once to avoid duplicated entries in prompts."""
            if not name or name in seen_columns:
                return
            columns.append({
                'name': name,
                'type': col_type,
                'description': description
            })
            seen_columns.add(name)

        def load_sample_data(table_name: str) -> Optional[List[Dict]]:
            """Load sample data from JSON file if available."""
            json_file = data_dir / f"{table_name}.json"
            if json_file.exists():
                try:
                    with open(json_file, 'r') as f:
                        data = json.load(f)
                        return data if isinstance(data, list) else []
                except Exception as e:
                    print(f"Warning: Could not load sample data for {table_name}: {e}")
            return None

        def build_sample_values(columns: List[Dict[str, str]], sample_data: Optional[List[Dict]]) -> Dict[str, List]:
            """Convert row-wise samples to column-wise sample values."""
            sample_values = {}
            if not sample_data:
                return sample_values

            for col in columns:
                col_name = col['name']
                values = [row.get(col_name) for row in sample_data if col_name in row]
                unique_values = list(set([v for v in values if v is not None]))
                if unique_values:
                    sample_values[col_name] = unique_values[:6]

            return sample_values

        # Load dimension tables
        for table_name, table_config in dimension_tables.items():
            columns = []
            seen_columns = set()
            # Add key column
            key_col = table_config.get('key_column', f'{table_name}_ID')
            add_column(columns, seen_columns, key_col, infer_key_type(key_col), 'Primary key')

            # Add FK columns (dimension tables can also reference parent dimensions)
            for fk in table_config.get('foreign_keys', []):
                fk_col = fk.get('column', '')
                add_column(
                    columns,
                    seen_columns,
                    fk_col,
                    infer_key_type(fk_col),
                    f"FK to {fk.get('references', '')}"
                )

            # Add other columns
            for col_config in table_config.get('columns', []):
                add_column(
                    columns,
                    seen_columns,
                    col_config.get('target_name', ''),
                    'VARCHAR',
                    ''
                )

            # Load sample data from JSON file (e.g., DIM_COUNTRY.json)
            sample_data = load_sample_data(table_name)
            sample_values = build_sample_values(columns, sample_data)

            tables[table_name] = TableInfo(
                table_name=table_name,
                description=config.get('table_descriptions', {}).get(table_name, ''),
                columns=columns,
                primary_key=key_col,
                foreign_keys=table_config.get('foreign_keys', []),
                sample_values=sample_values
            )

        # Load fact tables
        for table_name, table_config in fact_tables.items():
            columns = []
            seen_columns = set()
            key_col = table_config.get('key_column', f'{table_name}_ID')
            add_column(columns, seen_columns, key_col, infer_key_type(key_col), 'Primary key')
            # Add FK columns
            for fk in table_config.get('foreign_keys', []):
                fk_col = fk.get('column', '')
                add_column(
                    columns,
                    seen_columns,
                    fk_col,
                    infer_key_type(fk_col),
                    f"FK to {fk.get('references', '')}"
                )
            # Add other columns
            for col_config in table_config.get('columns', []):
                add_column(
                    columns,
                    seen_columns,
                    col_config.get('target_name', ''),
                    'DOUBLE PRECISION',
                    ''
                )

            # Load sample data from JSON file (e.g., FACT_TRANSPORTATION_ACCESSIBILITY.json)
            sample_data = load_sample_data(table_name)
            sample_values = build_sample_values(columns, sample_data)

            tables[table_name] = TableInfo(
                table_name=table_name,
                description=config.get('table_descriptions', {}).get(table_name, ''),
                columns=columns,
                primary_key=key_col,
                foreign_keys=table_config.get('foreign_keys', []),
                sample_values=sample_values
            )

        # Build foreign key relationships
        foreign_keys = []
        seen_fk_edges = set()
        for table_name, table_info in tables.items():
            for fk in table_info.foreign_keys:
                from_column = fk.get('column', '')
                ref_table = fk.get('references', '')
                ref_column = fk.get('references_column') or table_key_map.get(ref_table, f"{ref_table}_ID")
                fk_edge = (table_name, from_column, ref_table, ref_column)
                if not all(fk_edge) or fk_edge in seen_fk_edges:
                    continue
                foreign_keys.append({
                    'from_table': table_name,
                    'from_column': from_column,
                    'to_table': ref_table,
                    'to_column': ref_column
                })
                seen_fk_edges.add(fk_edge)

        return SchemaInfo(
            database_name=config.get('database_name', 'unknown'),
            tables=tables,
            foreign_keys=foreign_keys,
            domain=config.get('database_name', 'unknown')
        )

    def load_schema_from_ddl(self, ddl_path: str) -> SchemaInfo:
        """Load schema from DDL CSV file."""
        import csv
        import re
        from pathlib import Path

        tables = {}
        foreign_keys = []
        seen_fk_edges = set()

        with open(ddl_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                table_name = row['table_name']
                description = row['description']
                ddl = row['DDL']

                # Parse columns and constraints from CREATE TABLE body.
                columns = []
                primary_key = None
                table_foreign_keys = []

                create_match = re.search(
                    r'CREATE\s+TABLE\s+.*?\((.*)\)\s*;',
                    ddl,
                    re.IGNORECASE | re.DOTALL
                )
                table_body = create_match.group(1) if create_match else ""

                def split_ddl_items(body: str) -> List[str]:
                    """Split column/constraint items by commas outside parentheses."""
                    items = []
                    current = []
                    depth = 0
                    for ch in body:
                        if ch == '(':
                            depth += 1
                        elif ch == ')' and depth > 0:
                            depth -= 1

                        if ch == ',' and depth == 0:
                            part = ''.join(current).strip()
                            if part:
                                items.append(part)
                            current = []
                        else:
                            current.append(ch)

                    tail = ''.join(current).strip()
                    if tail:
                        items.append(tail)
                    return items

                for item in split_ddl_items(table_body):
                    upper_item = item.upper().strip()

                    # Table-level PRIMARY KEY
                    pk_match = re.search(
                        r'(?:CONSTRAINT\s+"?[A-Za-z_][A-Za-z0-9_]*"?\s+)?'
                        r'PRIMARY\s+KEY\s*\(([^)]+)\)',
                        item,
                        re.IGNORECASE
                    )
                    if pk_match:
                        pk_cols = [c.strip().strip('"') for c in pk_match.group(1).split(',') if c.strip()]
                        if pk_cols and not primary_key:
                            primary_key = pk_cols[0]
                        continue

                    # Table-level FOREIGN KEY
                    fk_match = re.search(
                        r'(?:CONSTRAINT\s+"?[A-Za-z_][A-Za-z0-9_]*"?\s+)?'
                        r'FOREIGN\s+KEY\s*\(\s*"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\)\s*'
                        r'REFERENCES\s+("?[\w\.]+"?)\s*\(\s*"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\)',
                        item,
                        re.IGNORECASE
                    )
                    if fk_match:
                        from_col, raw_ref_table, to_col = fk_match.groups()
                        ref_table = raw_ref_table.split('.')[-1].strip('"')
                        fk_edge = (table_name, from_col, ref_table, to_col)
                        if all(fk_edge) and fk_edge not in seen_fk_edges:
                            fk_spec = {
                                'column': from_col,
                                'references': ref_table,
                                'references_column': to_col
                            }
                            table_foreign_keys.append(fk_spec)
                            foreign_keys.append({
                                'from_table': table_name,
                                'from_column': from_col,
                                'to_table': ref_table,
                                'to_column': to_col
                            })
                            seen_fk_edges.add(fk_edge)
                        continue

                    # Column definition
                    col_match = re.match(
                        r'^"?([A-Za-z_][A-Za-z0-9_]*)"?\s+(.+)$',
                        item.strip(),
                        re.IGNORECASE | re.DOTALL
                    )
                    if not col_match:
                        continue

                    col_name, col_def = col_match.groups()
                    col_def = col_def.strip()

                    if 'PRIMARY KEY' in upper_item and not primary_key:
                        primary_key = col_name

                    # Inline REFERENCES support: e.g. col_id BIGINT REFERENCES dim(col_id)
                    inline_fk_match = re.search(
                        r'REFERENCES\s+("?[\w\.]+"?)\s*\(\s*"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\)',
                        col_def,
                        re.IGNORECASE
                    )
                    if inline_fk_match:
                        raw_ref_table, to_col = inline_fk_match.groups()
                        ref_table = raw_ref_table.split('.')[-1].strip('"')
                        fk_edge = (table_name, col_name, ref_table, to_col)
                        if all(fk_edge) and fk_edge not in seen_fk_edges:
                            fk_spec = {
                                'column': col_name,
                                'references': ref_table,
                                'references_column': to_col
                            }
                            table_foreign_keys.append(fk_spec)
                            foreign_keys.append({
                                'from_table': table_name,
                                'from_column': col_name,
                                'to_table': ref_table,
                                'to_column': to_col
                            })
                            seen_fk_edges.add(fk_edge)

                    # Keep data type segment before constraints.
                    col_type_clean = re.split(
                        r'\bPRIMARY\s+KEY\b|\bNOT\s+NULL\b|\bUNIQUE\b|\bREFERENCES\b|'
                        r'\bCHECK\b|\bDEFAULT\b|\bCONSTRAINT\b',
                        col_def,
                        maxsplit=1,
                        flags=re.IGNORECASE
                    )[0].strip()

                    columns.append({
                        'name': col_name,
                        'type': col_type_clean,
                        'description': ''
                    })

                tables[table_name] = TableInfo(
                    table_name=table_name,
                    description=description,
                    columns=columns,
                    primary_key=primary_key or (columns[0]['name'] if columns else ''),
                    foreign_keys=table_foreign_keys
                )

        # Infer database name from DDL path if available
        db_name = Path(ddl_path).parent.name if ddl_path else 'unknown'

        return SchemaInfo(
            database_name=db_name,
            tables=tables,
            foreign_keys=foreign_keys,
            domain=db_name
        )

    def synthesize(
        self,
        schema: SchemaInfo,
        db_connection=None
    ) -> List[NLQSQLPair]:
        """
        Run the complete DSQG-Syn pipeline.

        Args:
            schema: Database schema information
            db_connection: Optional database connection for SQL validation

        Returns:
            List of synthesized and filtered NLQ-SQL pairs
        """
        print(f"Starting DSQG-Syn synthesis for database: {schema.database_name}")
        print(f"Tables: {len(schema.tables)}")

        # Step 1: Domain-Specific Question Generation
        print("\n=== Step 1: Domain-Specific Question Generation ===")
        self.generated_questions = self.question_generator.generate_all_questions(schema)
        print(f"Generated {len(self.generated_questions)} domain-specific questions")

        # Step 2: Question-Guided SQL-NLQ Synthesis
        print("\n=== Step 2: Question-Guided SQL-NLQ Synthesis ===")
        self.raw_pairs = self.sql_synthesizer.synthesize_pairs(
            self.generated_questions,
            schema
        )
        print(f"Synthesized {len(self.raw_pairs)} raw NLQ-SQL pairs")

        # Always save raw pairs immediately so they are never lost
        import os
        raw_output_dir = os.path.join("output")
        os.makedirs(raw_output_dir, exist_ok=True)
        raw_output_path = os.path.join(raw_output_dir, f"{schema.database_name}_raw.jsonl")
        self.export_to_jsonl(self.raw_pairs, raw_output_path)
        print(f"Raw pairs saved to: {raw_output_path}")

        # Step 3: NLQ Semantic Optimization
        print("\n=== Step 3: NLQ Semantic Optimization ===")
        try:
            self.final_pairs = self.semantic_optimizer.optimize(
                self.generated_questions,
                self.raw_pairs,
                schema.database_name,
                db_connection
            )
            print(f"Final dataset: {len(self.final_pairs)} high-quality NLQ-SQL pairs")
        except Exception as e:
            print(f"\n⚠ Semantic optimization failed: {e}")
            print("Using raw pairs as final output...")
            self.final_pairs = self.raw_pairs

        # Compute and display metrics
        metrics = self.semantic_optimizer.compute_quality_metrics(self.final_pairs)
        print("\nQuality Metrics:")
        for key, value in metrics.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.3f}")
            else:
                print(f"  {key}: {value}")

        return self.final_pairs

    def export_to_jsonl(
        self,
        pairs: List[NLQSQLPair],
        output_path: str,
        include_metadata: bool = True
    ):
        """Export NLQ-SQL pairs to JSONL format."""
        with open(output_path, 'w') as f:
            for pair in pairs:
                record = {
                    'question': pair.nlq,
                    'sql': pair.sql,
                }
                if include_metadata:
                    record.update({
                        'pair_id': pair.pair_id,
                        'original_question_id': pair.original_question_id,
                        'schema_used': pair.schema_used,
                        'linked_schema': pair.linked_schema,
                        'skeleton_id': pair.skeleton_id,
                        'difficulty': pair.difficulty,
                        'similarity_score': pair.similarity_score
                    })
                f.write(json.dumps(record) + '\n')

        print(f"Exported {len(pairs)} pairs to {output_path}")

    def export_to_json(
        self,
        pairs: List[NLQSQLPair],
        output_path: str,
        include_metadata: bool = True
    ):
        """Export NLQ-SQL pairs to JSON format."""
        records = []
        for pair in pairs:
            record = {
                'question': pair.nlq,
                'sql': pair.sql,
            }
            if include_metadata:
                record.update({
                    'pair_id': pair.pair_id,
                    'original_question_id': pair.original_question_id,
                    'schema_used': pair.schema_used,
                    'linked_schema': pair.linked_schema,
                    'skeleton_id': pair.skeleton_id,
                    'difficulty': pair.difficulty,
                    'similarity_score': pair.similarity_score
                })
            records.append(record)

        with open(output_path, 'w') as f:
            json.dump(records, f, indent=2)

        print(f"Exported {len(pairs)} pairs to {output_path}")

    def export_questions(
        self,
        output_path: str
    ):
        """Export generated questions for debugging/analysis."""
        records = []
        for q in self.generated_questions:
            records.append({
                'question_id': q.question_id,
                'question_text': q.question_text,
                'question_type': q.question_type.value,
                'related_tables': q.related_tables,
                'keywords': q.keywords,
                'sql_operations': q.sql_operations
            })

        with open(output_path, 'w') as f:
            json.dump(records, f, indent=2)

        print(f"Exported {len(records)} questions to {output_path}")

    def get_synthesis_report(self) -> Dict[str, Any]:
        """Generate a comprehensive synthesis report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'config': {
                'questions_per_table': self.config.questions_per_table,
                'skeletons_per_question': self.config.skeletons_per_question,
                'sqls_per_skeleton': self.config.sqls_per_skeleton,
                'top_k_nlq_pairs': self.config.top_k_nlq_pairs,
                'similarity_threshold': self.config.similarity_threshold
            },
            'statistics': {
                'questions_generated': len(self.generated_questions),
                'raw_pairs_synthesized': len(self.raw_pairs),
                'final_pairs': len(self.final_pairs),
                'retention_rate': len(self.final_pairs) / len(self.raw_pairs) if self.raw_pairs else 0
            },
            'quality_metrics': self.semantic_optimizer.compute_quality_metrics(self.final_pairs),
            'question_type_distribution': self._get_question_type_distribution()
        }
        return report

    def _get_question_type_distribution(self) -> Dict[str, int]:
        """Get distribution of question types."""
        distribution = {}
        for q in self.generated_questions:
            q_type = q.question_type.value
            distribution[q_type] = distribution.get(q_type, 0) + 1
        return distribution
